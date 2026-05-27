#!/usr/bin/env python3
"""
sim_live.py — replay historical raw data through the live pipeline.

Feeds records from BTCUSDT_LIVE.jsonl.gz one unique-second at a time into
a bounded deque.  At each minute boundary, runs wave_scan and pushes signals
exactly as scan_live.py would in production.  Validates each new signal
against the buffered raw data immediately.

Deque contract:
  - Keyed by (ts_ms // 1000) — one slot per unique second
  - maxlen = WINDOW_S seconds — oldest second falls off automatically
  - A second can only enter once (deduped by second key)

Usage:
    python sim_live.py                 # replay all sessions, 0s delay
    python sim_live.py --delay 1       # 1s pause between seconds (real-time feel)
    python sim_live.py --session 0     # latest session only
    python sim_live.py --push          # also push signals to data/raw (like production)
"""

import argparse, collections, gzip, json, os, re, subprocess, sys, time
from datetime import datetime, timezone
from pathlib import Path

REPO      = Path(__file__).resolve().parent
DATA_BRANCH = "data/raw"
TMP_IDX   = REPO / ".git" / "sim_push.idx"
WINDOW_S  = 96 * 60   # 96-minute rolling window

ANSI = re.compile(r'\x1b\[[0-9;]*m')
G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; W='\033[97m'; Z='\033[0m'

def log(m, c=Z): print(f"{c}[sim] {m}{Z}", flush=True)
def _run(*a, env=None):
    return subprocess.run(list(a), capture_output=True, text=True,
                          cwd=str(REPO), env=env)


# ── git push (same plumbing as scan_live.py) ──────────────────────────────────

def _git_push_text(content: str, branch_path: str) -> bool:
    env = {**os.environ, 'GIT_INDEX_FILE': str(TMP_IDX)}
    parent_r = _run('git', 'rev-parse', f'origin/{DATA_BRANCH}')
    parent   = parent_r.stdout.strip()
    if parent:
        _run('git', 'read-tree', f'origin/{DATA_BRANCH}', env=env)

    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write(content); tmp = f.name
    r = _run('git', 'hash-object', '-w', tmp)
    os.unlink(tmp)
    blob = r.stdout.strip()
    if not blob: TMP_IDX.unlink(missing_ok=True); return False

    _run('git', 'update-index', '--add',
         '--cacheinfo', f'100644,{blob},{branch_path}', env=env)
    r = _run('git', 'write-tree', env=env)
    tree = r.stdout.strip()
    TMP_IDX.unlink(missing_ok=True)
    if not tree: return False

    cmd = ['git', 'commit-tree', tree, '-m', f'sim {int(time.time())}']
    if parent: cmd += ['-p', parent]
    r = _run(*cmd); commit = r.stdout.strip()
    if not commit: return False
    r = _run('git', 'push', 'origin', f'{commit}:refs/heads/{DATA_BRANCH}')
    return r.returncode == 0


# ── run wave_scan against a temp raw file ─────────────────────────────────────

def _run_scan_on_buf(buf_lines: list) -> str:
    """
    Write buffer to a temp gzip file, monkey-patch _fetch_raw to read it,
    run the scan, return clean text output.

    Uses session_idx=999 so wave_scan falls through to the 'else' branch
    (t_start=first trade, t_end=last trade) — correct when the buffer
    contains only one session and trade_gaps is empty.
    """
    import tempfile, io
    import wave_scan as WS

    tmp_gz = Path(tempfile.mktemp(suffix='.jsonl.gz'))
    with gzip.open(tmp_gz, 'wt') as f:
        for ln in buf_lines:
            f.write(ln + '\n')

    orig = WS._fetch_raw
    def _patched():
        with gzip.open(tmp_gz, 'rt') as fh:
            return fh.read().strip().split('\n')
    WS._fetch_raw = _patched

    old_stdout = sys.stdout
    sys.stdout  = io.StringIO()
    try:
        WS.scan(mins_limit=None, session_idx=999, signals_only=True)
        out = sys.stdout.getvalue()
    except SystemExit:
        out = sys.stdout.getvalue()
    except Exception as e:
        out = f"ERROR: {e}"
    finally:
        sys.stdout   = old_stdout
        WS._fetch_raw = orig
        tmp_gz.unlink(missing_ok=True)

    return ANSI.sub('', out)


# ── signal parser ──────────────────────────────────────────────────────────────

def _parse(text):
    sigs = []
    cur  = {}
    for line in text.split('\n'):
        m = re.search(r'(▲|▼)\s+(LONG|SHORT)\s+·\s+(TROUGH REVERSAL|PEAK REVERSAL)\s+\[(\d+)\]', line)
        if m:
            cur = {'n': int(m.group(4)), 'dir': m.group(2), 'stype': m.group(3),
                   'time': '', 'price': 0.0, 'confirm': ''}
        m2 = re.search(r'(\d{2}:\d{2}:\d{2})\s+·\s+\$\s*([\d,]+\.?\d*)', line)
        if m2 and cur.get('dir'):
            cur['time']  = m2.group(1)
            cur['price'] = float(m2.group(2).replace(',', ''))
        # Confirmation line (first content line after timestamp)
        if cur.get('time') and not cur.get('confirm'):
            stripped = line.strip()
            if stripped and not stripped.startswith('1m') and not stripped.startswith('FLR'):
                if 'OBI' in stripped:
                    cur['confirm'] = f"ob={stripped.split()[1]}"
                elif 'KdV' in stripped:
                    cur['confirm'] = 'kdv'
        if cur.get('time') and cur.get('price') and line.strip() == '':
            sigs.append(dict(cur)); cur = {}
    if cur.get('time') and cur.get('price'):
        sigs.append(cur)
    n_m = re.search(r'(\d+) signal', text)
    return sigs, int(n_m.group(1)) if n_m else 0


# ── validate signal against raw buffer ────────────────────────────────────────

def _validate(sig, raw_lines):
    target_hms = sig['time']
    target_min = target_hms[:5]
    min_obis   = []

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
        hm_s  = datetime.utcfromtimestamp(ts_s).strftime('%H:%M:%S')
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
    last_trade = None
    for ln in raw_lines:
        if not ln: continue
        try: rec = json.loads(ln)
        except: continue
        ts_s = rec[1] // 1000
        hm   = datetime.utcfromtimestamp(ts_s).strftime('%H:%M')
        if rec[0] == 'T' and target_sec is not None and ts_s <= target_sec:
            last_trade = rec[2] / 100
        if rec[0] == 'D' and hm == target_min:
            bids = [(p/100, q/10000) for p, q in rec[2][:5]]
            asks = [(p/100, q/10000) for p, q in rec[3][:5]]
            bq = sum(q for _, q in bids); aq = sum(q for _, q in asks)
            if bq + aq > 0:
                min_obis.append((bq - aq) / (bq + aq))

    issues = []
    price_r = "price=NO_TRADE"
    if last_trade is not None:
        d = abs(last_trade - sig['price'])
        price_r = f"price=OK(${last_trade:,.2f})" if d < 2.0 else \
                  f"price=MISMATCH(Δ${d:.2f})"
        if 'MISMATCH' in price_r: issues.append('price')
    else:
        issues.append('price')

    # Skip OBI check for KdV-confirmed signals
    confirm  = sig.get('confirm', '')
    obi_conf = 'ob=' in confirm or ('kdv' not in confirm.lower())

    obi_r = "obi=NO_OB"
    if min_obis:
        mx, mn = max(min_obis), min(min_obis)
        if not obi_conf:
            ext = mx if sig['dir']=='LONG' else mn
            obi_r = f"obi=KdV-confirmed(raw_ext={ext:+.2f})"
        elif sig['dir'] == 'LONG':
            if mx >= -0.30:
                obi_r = f"obi=OK(max={mx:+.2f})"
            else:
                obi_r = f"obi=MISMATCH(LONG,max={mx:+.2f})"
                issues.append('obi')
        else:   # SHORT
            if mn <= 0.30:
                obi_r = f"obi=OK(min={mn:+.2f})"
            else:
                obi_r = f"obi=MISMATCH(SHORT,min={mn:+.2f})"
                issues.append('obi')

    return f"[{sig['dir']} {sig['time']} ${sig['price']:,.2f}] {price_r} {obi_r}", issues


# ── main replay loop ──────────────────────────────────────────────────────────

def replay(session_idx=None, delay=0.0, push=False):
    log("fetching raw data…", C)
    _run('git', 'fetch', 'origin', DATA_BRANCH)
    r = subprocess.run(
        ['git', 'show', 'origin/data/raw:data/raw/BTCUSDT_LIVE.jsonl.gz'],
        capture_output=True, cwd=REPO)
    all_lines = gzip.decompress(r.stdout).decode().strip().split('\n')

    # Group records by unique second
    by_sec = collections.defaultdict(list)
    for ln in all_lines:
        if not ln: continue
        try:
            rec = json.loads(ln)
            by_sec[rec[1] // 1000].append(ln)
        except:
            continue

    all_secs = sorted(by_sec.keys())

    # Filter to requested session
    if session_idx is not None:
        trade_secs = sorted(s for s in all_secs
                            if any(json.loads(ln)[0] == 'T'
                                   for ln in by_sec[s]))
        gaps = [i for i in range(1, len(trade_secs))
                if trade_secs[i] - trade_secs[i-1] > 1800]
        gaps_rev = sorted(gaps, reverse=True)
        if session_idx < len(gaps_rev):
            t_start = trade_secs[gaps_rev[session_idx]]
            t_end   = trade_secs[gaps_rev[session_idx-1]-1] if session_idx > 0 \
                      else trade_secs[-1]
        else:
            t_start, t_end = trade_secs[0], trade_secs[-1]
        all_secs = [s for s in all_secs if t_start <= s <= t_end]

    SCAN_EVERY = 300   # run wave_scan every 5 minutes of sim time

    log(f"replaying {len(all_secs)} seconds  "
        f"({datetime.utcfromtimestamp(all_secs[0]).strftime('%H:%M:%S')} → "
        f"{datetime.utcfromtimestamp(all_secs[-1]).strftime('%H:%M:%S')} UTC)  "
        f"delay={delay}s  scan_every={SCAN_EVERY}s  push={push}", C)

    # Bounded deque: keyed by second, max WINDOW_S slots
    sec_deque = collections.deque(maxlen=WINDOW_S)   # entries: (sec, [lines])
    seen_secs = set()

    last_sigs     = []
    total_fired   = 0
    last_scan_sec = None

    print(f"\n{W}  TIME     BUF    EVENT{Z}")
    print('─' * 65)

    for sec in all_secs:
        # ── feed one unique second into deque ─────────────────────────────
        if sec in seen_secs:
            continue
        if len(sec_deque) == WINDOW_S:
            oldest_sec, _ = sec_deque[0]
            seen_secs.discard(oldest_sec)
        sec_deque.append((sec, by_sec[sec]))
        seen_secs.add(sec)

        # ── scan every SCAN_EVERY seconds of sim-time ──────────────────────
        if last_scan_sec is None or sec - last_scan_sec >= SCAN_EVERY:
            last_scan_sec = sec
            buf_lines = [ln for _, lines in sec_deque for ln in lines]
            hms       = datetime.utcfromtimestamp(sec).strftime('%H:%M:%S')

            sys.stderr.write(f"\r  {C}{hms}  scanning…{Z}        ")
            sys.stderr.flush()

            scan_text = _run_scan_on_buf(buf_lines)
            sigs, n   = _parse(scan_text)

            prev_keys = {(s['time'], s['price']) for s in last_sigs}
            new_sigs  = [s for s in sigs
                         if (s['time'], s['price']) not in prev_keys]
            total_fired += len(new_sigs)

            if new_sigs:
                sys.stderr.write('\r' + ' '*40 + '\r')
                for s in new_sigs:
                    label, issues = _validate(s, buf_lines)
                    verdict = f"{G}VALID{Z}" if not issues else f"{R}MISMATCH{Z}"
                    arrow   = f"{G}▲{Z}" if s['dir']=='LONG' else f"{R}▼{Z}"
                    detail  = label.split(']')[1].strip()
                    print(f"  {hms}  {len(sec_deque):>4}s  "
                          f"{arrow} {s['dir']:<5} {s['time']}  "
                          f"${s['price']:>10,.2f}  {detail}  {verdict}")
            else:
                sys.stderr.write(f"\r  {C}{hms}  buf={len(sec_deque):>4}s  n={n}  quiet{Z}        ")
                sys.stderr.flush()

            last_sigs = sigs

            if push and new_sigs:
                summary = {
                    'scanned_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'signal_count': n,
                    'signals': [{'time':s['time'],'price':s['price'],
                                 'dir':s['dir'],'stype':s['stype']} for s in sigs]
                }
                _git_push_text(json.dumps(summary, indent=2),
                               'data/raw/BTCUSDT_SIGNALS.json')

        if delay:
            time.sleep(delay)

    sys.stderr.write('\r' + ' '*60 + '\r')
    print(f"\n{C}━━━ replay complete  {total_fired} signals fired ━━━{Z}\n")


if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Simulate live wave scan replay')
    p.add_argument('--session', type=int, default=None,
                   help='session index (0=latest); omit for all sessions')
    p.add_argument('--delay',   type=float, default=0.0,
                   help='seconds between each unique-second feed (0=max speed)')
    p.add_argument('--push',    action='store_true',
                   help='push signals to data/raw branch as they fire')
    args = p.parse_args()
    replay(session_idx=args.session, delay=args.delay, push=args.push)
