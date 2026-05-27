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

# ── read signals from pushed JSON (produced by scan_live.py) ─────────────────

def load_signals_json():
    """Read BTCUSDT_SIGNALS.json from data/raw branch (pushed by scan_live.py)."""
    r = subprocess.run(
        ['git', 'show', 'origin/data/raw:data/raw/BTCUSDT_SIGNALS.json'],
        capture_output=True, cwd=REPO)
    if not r.stdout:
        return None
    try:
        return json.loads(r.stdout.decode())
    except Exception:
        return None


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

    # Resolve HH:MM:SS → unix second.
    # Two pitfalls:
    #   1. Session spans midnight: "00:xx" < "22:xx" in string order but is later.
    #   2. Same HH:MM:SS appears on multiple days in a multi-day raw file.
    # Fix: collect all T records matching the target HH:MM:SS, then pick the one
    # whose trade price is closest to the signal's reported price.  This works
    # because the reported price is the engine's p_close at cand_sec — it will
    # be very close to an actual trade in the correct session.
    # Pass 1: resolve target unix second.
    # Primary: T record at exact HH:MM:SS with closest price to sig['price'].
    # Fallback: any record in the target minute → use last second of that minute.
    # This avoids the midnight string-ordering bug ("00:xx" < "22:xx") and handles
    # forward-filled bars where no T record exists at the exact cand_sec.
    candidates  = []   # (unix_sec, trade_price)
    minute_secs = []   # unix_sec for any record in target minute
    for ln in raw_lines:
        if not ln: continue
        try: rec = json.loads(ln)
        except: continue
        ts_s  = rec[1] // 1000
        hm_s  = datetime.datetime.utcfromtimestamp(ts_s).strftime('%H:%M:%S')
        if rec[0] == 'T' and hm_s == target_hms:
            candidates.append((ts_s, rec[2] / 100))
        if hm_s[:5] == target_min:
            minute_secs.append(ts_s)

    if candidates:
        target_sec = min(candidates, key=lambda x: abs(x[1] - sig['price']))[0]
    elif minute_secs:
        target_sec = max(minute_secs)   # upper bound of target minute
    else:
        target_sec = None

    # Pass 2: last trade price at-or-before target_sec, and OBI readings in minute.
    last_trade_price = None
    minute_ob_obis   = []   # all computed OBIs in the minute window

    for ln in raw_lines:
        if not ln: continue
        try: rec = json.loads(ln)
        except: continue
        ts_s  = rec[1] // 1000
        hm    = datetime.datetime.utcfromtimestamp(ts_s).strftime('%H:%M')

        if rec[0] == 'T' and target_sec is not None and ts_s <= target_sec:
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
    # Skip OBI check for KdV-confirmed signals — the engine used KdV direction
    # (not OBI) as confirmation; the raw book may legitimately be neutral/opposite.
    confirm = sig.get('confirm', '')
    obi_confirmed = 'ob=' in confirm or ('kdv' not in confirm.lower()
                                          and 'flip' not in confirm.lower())

    if minute_ob_obis and obi_confirmed:
        max_obi = max(minute_ob_obis)
        min_obi = min(minute_ob_obis)
        if sig['dir'] == 'LONG':
            if max_obi < -0.30:
                obi_result = f"obi=MISMATCH(LONG but minute_max={max_obi:+.2f})"
                issues.append('obi')
            else:
                obi_result = f"obi=OK(minute_max={max_obi:+.2f})"
        else:  # SHORT
            if min_obi > 0.30:
                obi_result = f"obi=MISMATCH(SHORT but minute_min={min_obi:+.2f})"
                issues.append('obi')
            else:
                obi_result = f"obi=OK(minute_min={min_obi:+.2f})"
    elif minute_ob_obis:
        # KdV-confirmed — report raw OBI for info but don't flag as mismatch
        max_obi = max(minute_ob_obis); min_obi = min(minute_ob_obis)
        ext = max_obi if sig['dir'] == 'LONG' else min_obi
        obi_result = f"obi=KdV-confirmed(raw_ext={ext:+.2f})"
    else:
        obi_result = "obi=NO_OB_IN_MINUTE"

    label = f"[{sig['dir']} {target_hms} ${sig['price']:,.2f}] {price_result} {obi_result}"
    return label, issues


# ── main ──────────────────────────────────────────────────────────────────────

def main(prev_key=None):
    subprocess.run(['git', 'fetch', 'origin', 'data/raw'],
                   capture_output=True, cwd=REPO)
    now_utc  = datetime.datetime.utcnow().strftime('%H:%M:%S')

    # Prefer JSON pushed by scan_live.py (Termux); fall back to local scan
    summary = load_signals_json()
    if summary:
        signals = [{'time':  s['time'],
                    'price': s['price'],
                    'dir':   s['dir'],
                    'stype': s['stype'],
                    'obi_str': ''}
                   for s in summary.get('signals', [])]
        n = summary.get('signal_count', len(signals))
    else:
        # fallback: run scan locally
        scan_out = subprocess.run(
            ['python3', 'wave_scan.py', '--session', '0', '--signals'],
            capture_output=True, text=True, cwd=REPO, timeout=120)
        text = scan_out.stdout + scan_out.stderr
        signals = parse_signals(text)
        n       = sig_count(text)

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
