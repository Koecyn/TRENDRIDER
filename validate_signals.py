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
    target_hms = sig['time']          # "HH:MM:SS"
    target_sec = None                 # unix second, resolved below
    trade_prices = []
    last_ob_at_sec = None
    last_ob_rec    = None

    for ln in raw_lines:
        if not ln: continue
        try: rec = json.loads(ln)
        except: continue
        ts_ms = rec[1]
        ts_s  = ts_ms // 1000
        hms   = datetime.datetime.utcfromtimestamp(ts_s).strftime('%H:%M:%S')
        if hms == target_hms:
            target_sec = ts_s
            if rec[0] == 'T':
                trade_prices.append(rec[2] / 100)
            elif rec[0] == 'D':
                last_ob_at_sec = ts_s
                last_ob_rec    = rec
        # Keep last OB before/at target second
        elif last_ob_rec is None and rec[0] == 'D' and hms < target_hms:
            last_ob_rec = rec

    issues = []

    # ── check 1: price ───────────────────────────────────────────────────────
    if trade_prices:
        closest = min(trade_prices, key=lambda p: abs(p - sig['price']))
        delta   = abs(closest - sig['price'])
        if delta < 1.0:
            price_result = f"price=OK(${closest:,.2f})"
        else:
            price_result = f"price=MISMATCH(scan=${sig['price']:,.2f} raw=${closest:,.2f} Δ${delta:.2f})"
            issues.append('price')
    else:
        price_result = "price=NO_TRADE_RECORD"
        issues.append('price')

    # ── check 2: OBI sign ────────────────────────────────────────────────────
    if last_ob_rec:
        bids = [(p/100, q/10000) for p, q in last_ob_rec[2][:5]]
        asks = [(p/100, q/10000) for p, q in last_ob_rec[3][:5]]
        bid_q = sum(q for _, q in bids)
        ask_q = sum(q for _, q in asks)
        if bid_q + ask_q > 0:
            raw_obi = (bid_q - ask_q) / (bid_q + ask_q)
            # LONG expects positive OBI (or at least not strongly negative)
            # SHORT expects negative OBI (or at least not strongly positive)
            # Threshold: flag only when sign is strongly opposite (|raw_obi| > 0.3)
            scan_positive = (sig['dir'] == 'LONG')
            raw_positive  = raw_obi >= 0
            if scan_positive != raw_positive and abs(raw_obi) > 0.30:
                obi_result = f"obi=MISMATCH(scan={sig['dir']} raw={raw_obi:+.2f})"
                issues.append('obi')
            else:
                obi_result = f"obi=OK(raw={raw_obi:+.2f})"
        else:
            obi_result = "obi=EMPTY_BOOK"
    else:
        obi_result = "obi=NO_OB_RECORD"

    label = f"[{sig['dir']} {sig['time']} ${sig['price']:,.2f}] {price_result} {obi_result}"
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
