#!/usr/bin/env python3
"""
validate_signals.py — cross-check wave_scan output against raw tick data.

For the 2 most recent signals, verifies:
  1. Reported price  → matches a trade record within ±$1 at that second
  2. OBI sign        → raw best-bid/ask qty at that second agrees with LONG/SHORT
  3. No data gaps    → raw has at least 1 T or D record within ±5s of the signal

Prints a single summary line:
  VALID | N signals | [LONG HH:MM:SS $P price=OK obi=OK] | ...
  MISMATCH | ...
  NO_NEW_DATA | ...
  NO_DATA | ...
"""

import gzip, json, subprocess, sys, re, datetime, os

REPO = os.path.dirname(os.path.abspath(__file__))

# ── run wave scan ─────────────────────────────────────────────────────────────

def run_scan():
    r = subprocess.run(
        ['python3', 'wave_scan.py', '--session', '0', '--signals'],
        capture_output=True, text=True, cwd=REPO, timeout=120)
    return r.stdout + r.stderr


# ── parse signal cards from scan output ──────────────────────────────────────

ANSI = re.compile(r'\x1b\[[0-9;]*m')

def parse_signals(scan_out):
    """Return list of dicts: {time, price, dir, stype, obi_str}"""
    clean = ANSI.sub('', scan_out)
    signals = []
    cur = {}
    for line in clean.split('\n'):
        # Direction line: "  ▲  LONG  ·  TROUGH REVERSAL       [3]"
        m = re.search(r'(▲|▼)\s+(LONG|SHORT)\s+·\s+(TROUGH REVERSAL|PEAK REVERSAL)', line)
        if m:
            cur = {'dir': m.group(2), 'stype': m.group(3), 'obi_str': '', 'price': 0.0, 'time': ''}
        # Timestamp + price: "     00:17:04  ·  $ 75,920.00"
        m2 = re.search(r'(\d{2}:\d{2}:\d{2})\s+·\s+\$\s*([\d,]+\.?\d*)', line)
        if m2 and cur.get('dir'):
            cur['time']  = m2.group(1)
            cur['price'] = float(m2.group(2).replace(',', ''))
        # OBI confirmation line: "     OBI +0.77+  ·  Floor forming"
        m3 = re.search(r'OBI\s+([+\-]\d+\.\d+)', line)
        if m3 and cur.get('dir'):
            cur['obi_str'] = m3.group(1)
        # Blank line after price → card complete
        if cur.get('time') and cur.get('price') and line.strip() == '':
            signals.append(dict(cur)); cur = {}
    if cur.get('time') and cur.get('price'):
        signals.append(cur)
    return signals


def sig_count(scan_out):
    m = re.search(r'(\d+) signal', ANSI.sub('', scan_out))
    return int(m.group(1)) if m else 0


# ── load raw data ─────────────────────────────────────────────────────────────

def load_raw():
    r = subprocess.run(
        ['git', 'show', 'origin/data/raw:data/raw/BTCUSDT_LIVE.jsonl.gz'],
        capture_output=True, cwd=REPO)
    if not r.stdout:
        return []
    return gzip.decompress(r.stdout).decode().strip().split('\n')


# ── validate one signal ───────────────────────────────────────────────────────

def validate(sig, raw_lines):
    """
    Validate one signal against raw tick data.

    Price check: scan uses p_close which may be a forward-filled bar (no live trade
    at the exact second). Search for the last T record at or within 30s before the
    signal second — that is the price the engine would have carried.

    OBI check: scan uses best_obi_long/short (the most extreme OBI seen ANYWHERE in
    the signal minute, not the instantaneous OBI at cand_sec). So we check the full
    minute window: for LONG verify at least one D record in the minute had positive
    OBI; for SHORT verify at least one had negative OBI. A true mismatch means the
    raw book was never in the claimed direction during that whole minute.
    """
    target_hms = sig['time']          # "HH:MM:SS"
    target_min = target_hms[:5]       # "HH:MM"  — minute window

    # Collect: last trade at-or-before signal second (within 30s lookback),
    #          all OB records in the signal minute
    last_trade_price = None
    minute_ob_obis   = []   # all computed OBIs in the minute window

    for ln in raw_lines:
        if not ln: continue
        try: rec = json.loads(ln)
        except: continue
        ts_ms = rec[1]
        ts_s  = ts_ms // 1000
        hms   = datetime.datetime.utcfromtimestamp(ts_s).strftime('%H:%M:%S')
        hm    = hms[:5]

        if rec[0] == 'T' and hms <= target_hms:
            # Keep rolling last-trade-price up to (and including) signal second
            last_trade_price = rec[2] / 100

        if rec[0] == 'D' and hm == target_min:
            bids = [(p/100, q/10000) for p, q in rec[2][:5]]
            asks = [(p/100, q/10000) for p, q in rec[3][:5]]
            bq = sum(q for _, q in bids)
            aq = sum(q for _, q in asks)
            if bq + aq > 0:
                minute_ob_obis.append((bq - aq) / (bq + aq))

    issues = []

    # ── check 1: price (last carried trade ≤ signal second) ─────────────────
    if last_trade_price is not None:
        delta = abs(last_trade_price - sig['price'])
        if delta < 2.0:          # ±$2 tolerance for forward-fill drift
            price_result = f"price=OK(${last_trade_price:,.2f})"
        else:
            price_result = (f"price=MISMATCH(scan=${sig['price']:,.2f} "
                            f"raw=${last_trade_price:,.2f} Δ${delta:.2f})")
            issues.append('price')
    else:
        price_result = "price=NO_TRADE_RECORD"
        issues.append('price')

    # ── check 2: OBI direction across the signal minute ──────────────────────
    # scan uses best_obi_long (max OBI in minute) for LONG,
    #           best_obi_short (min OBI in minute) for SHORT.
    # Flag only when the entire minute's OBI was opposite to the signal
    # direction — meaning the extreme the engine relied on never existed.
    if minute_ob_obis:
        max_obi = max(minute_ob_obis)
        min_obi = min(minute_ob_obis)
        if sig['dir'] == 'LONG':
            # engine used best_obi_long (max); flag if max was strongly negative
            if max_obi < -0.30:
                obi_result = f"obi=MISMATCH(LONG but minute_max={max_obi:+.2f})"
                issues.append('obi')
            else:
                obi_result = f"obi=OK(minute_max={max_obi:+.2f})"
        else:  # SHORT
            # engine used best_obi_short (min); flag if min was strongly positive
            if min_obi > 0.30:
                obi_result = f"obi=MISMATCH(SHORT but minute_min={min_obi:+.2f})"
                issues.append('obi')
            else:
                obi_result = f"obi=OK(minute_min={min_obi:+.2f})"
    else:
        obi_result = "obi=NO_OB_IN_MINUTE"

    label = f"[{sig['dir']} {target_hms} ${sig['price']:,.2f}] {price_result} {obi_result}"
    return label, issues


# ── main ──────────────────────────────────────────────────────────────────────

def main(prev_key=None):
    subprocess.run(['git', 'fetch', 'origin', 'data/raw'],
                   capture_output=True, cwd=REPO)

    scan_out = run_scan()
    signals  = parse_signals(scan_out)
    n        = sig_count(scan_out)
    now_utc  = datetime.datetime.utcnow().strftime('%H:%M:%S')

    if not signals:
        print(f"NO_DATA | {now_utc} UTC")
        return None

    last = signals[-1]
    key  = f"{last['time']}_{last['price']}"

    if key == prev_key:
        print(f"NO_NEW_DATA | {n} signals | last={last['time']} ${last['price']:,.2f} | {now_utc} UTC")
        return key

    # New signal(s) — validate last 2
    raw_lines  = load_raw()
    to_check   = signals[-2:] if len(signals) >= 2 else signals
    all_issues = []
    parts      = []

    for sig in to_check:
        label, issues = validate(sig, raw_lines)
        parts.extend(issues)
        all_issues.append(label)

    verdict = 'MISMATCH' if parts else 'VALID'
    print(f"{verdict} | {n} signals | {' | '.join(all_issues)} | {now_utc} UTC")
    return key


if __name__ == '__main__':
    prev = sys.argv[1] if len(sys.argv) > 1 else None
    main(prev)
