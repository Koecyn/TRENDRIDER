#!/usr/bin/env python3
"""
scan_live.py — run wave_scan on a loop and push results to data/raw branch.

Runs wave_scan.py --session 0 --signals every SCAN_INTERVAL seconds.
Strips ANSI, writes output to data/raw/BTCUSDT_SIGNALS.txt, pushes via
git plumbing (same mechanism as collect_raw.py — never touches code branch).

Also writes data/raw/BTCUSDT_SIGNALS.json with structured signal summary
for programmatic consumers (e.g. validate_signals.py).

Usage:
    python scan_live.py
    python scan_live.py --interval 300   # custom interval in seconds (default 300)
"""

import argparse, gzip, json, os, re, subprocess, sys, time
from pathlib import Path
from datetime import datetime, timezone

REPO         = Path(__file__).resolve().parent
RAW_DIR      = REPO / "data" / "raw"
SIGNALS_TXT  = RAW_DIR / "BTCUSDT_SIGNALS.txt"
SIGNALS_JSON = RAW_DIR / "BTCUSDT_SIGNALS.json"
DATA_BRANCH  = "data/raw"
TMP_IDX      = REPO / ".git" / "scan_push.idx"

SCAN_INTERVAL = 300   # seconds between scans (5 minutes default)

ANSI = re.compile(r'\x1b\[[0-9;]*m')

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; Z='\033[0m'
def log(m, c=Z): print(f"{c}[scan] {m}{Z}", flush=True)


# ── git plumbing push ─────────────────────────────────────────────────────────

def _run(*a, env=None):
    return subprocess.run(list(a), capture_output=True, text=True,
                          cwd=str(REPO), env=env)


def _git_push_files(paths: list) -> bool:
    """Push multiple files to DATA_BRANCH atomically using git plumbing."""
    env = {**os.environ, 'GIT_INDEX_FILE': str(TMP_IDX)}

    # Seed temp index from current data branch
    parent_r = _run('git', 'rev-parse', f'origin/{DATA_BRANCH}')
    parent   = parent_r.stdout.strip()
    if parent:
        _run('git', 'read-tree', f'origin/{DATA_BRANCH}', env=env)

    for p in paths:
        r = _run('git', 'hash-object', '-w', str(p))
        blob = r.stdout.strip()
        if not blob:
            TMP_IDX.unlink(missing_ok=True)
            return False
        rel = str(p.relative_to(REPO))
        _run('git', 'update-index', '--add',
             '--cacheinfo', f'100644,{blob},{rel}', env=env)

    r = _run('git', 'write-tree', env=env)
    tree = r.stdout.strip()
    TMP_IDX.unlink(missing_ok=True)
    if not tree:
        return False

    ts  = int(time.time())
    cmd = ['git', 'commit-tree', tree, '-m', f'signals {ts}']
    if parent:
        cmd += ['-p', parent]
    r = _run(*cmd)
    commit = r.stdout.strip()
    if not commit:
        return False

    r = _run('git', 'push', 'origin', f'{commit}:refs/heads/{DATA_BRANCH}')
    return r.returncode == 0


# ── signal parser ─────────────────────────────────────────────────────────────

def parse_signals(text):
    """Extract structured signal list from scan output."""
    clean = ANSI.sub('', text)
    signals = []
    cur = {}

    # Session header: "━━━  HH:MM:SS → HH:MM:SS UTC  ━━━"
    session_start = None
    session_end   = None
    m = re.search(r'(\d{2}:\d{2}:\d{2}) → (\d{2}:\d{2}:\d{2}) UTC', clean)
    if m:
        session_start = m.group(1)
        session_end   = m.group(2)

    for line in clean.split('\n'):
        # Direction line
        m = re.search(r'(▲|▼)\s+(LONG|SHORT)\s+·\s+(TROUGH REVERSAL|PEAK REVERSAL)\s+\[(\d+)\]', line)
        if m:
            cur = {'n': int(m.group(4)), 'dir': m.group(2),
                   'stype': m.group(3), 'time': '', 'price': 0.0,
                   'reason': '', 'decay': ''}
        # Timestamp + price
        m2 = re.search(r'(\d{2}:\d{2}:\d{2})\s+·\s+\$\s*([\d,]+\.?\d*)', line)
        if m2 and cur.get('dir'):
            cur['time']  = m2.group(1)
            cur['price'] = float(m2.group(2).replace(',', ''))
        # Confirmation line (OBI / KdV / Floor) — first non-empty line after timestamp
        if cur.get('dir') and cur.get('time') and not cur.get('reason'):
            stripped = line.strip()
            if stripped and not stripped.startswith('1m') and not stripped.startswith('FLR'):
                cur['reason'] = stripped
                # Derive confirm key: 'ob=...' for OBI, 'kdv' for KdV-flip/match
                if 'OBI' in stripped:
                    cur['confirm'] = f"ob={stripped.split()[1]}"
                elif 'KdV' in stripped:
                    cur['confirm'] = 'kdv-flip' if 'flip' in stripped.lower() else 'kdv'
                else:
                    cur['confirm'] = stripped
        # Decay line
        m3 = re.search(r'(FLOOR|FLR\?|DECAY|DEC\?|KNIFE)\s+ds=([\d.]+)\s+fos=([\d.]+)', line)
        if m3 and cur.get('dir'):
            cur['decay'] = f"{m3.group(1)} ds={m3.group(2)} fos={m3.group(3)}"
        # Card end (blank line after we have all fields)
        if cur.get('time') and cur.get('price') and line.strip() == '':
            signals.append(dict(cur)); cur = {}

    if cur.get('time') and cur.get('price'):
        signals.append(cur)

    count_m = re.search(r'(\d+) signal', clean)
    return {
        'session_start': session_start,
        'session_end':   session_end,
        'signal_count':  int(count_m.group(1)) if count_m else 0,
        'signals':       signals,
        'scanned_at':    datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    }


# ── main loop ─────────────────────────────────────────────────────────────────

def run_scan():
    """Run wave_scan and return (raw_output, clean_text)."""
    r = subprocess.run(
        [sys.executable, 'wave_scan.py', '--session', '0', '--signals'],
        capture_output=True, text=True, cwd=str(REPO), timeout=120)
    raw = r.stdout + r.stderr
    clean = ANSI.sub('', raw)
    return raw, clean


def _startup_backfill():
    """Ask whether to pull missing candle history from exchange before scanning."""
    local_1m = RAW_DIR / 'BTCUSDT_1m.json.gz'
    hint = "(no local history)" if not local_1m.exists() else "(update available)"
    try:
        ans = input(f"{C}Backfill candle history from exchange? {hint} [Y/n]: {Z}").strip().lower()
    except EOFError:
        ans = 'n'
    if ans in ('', 'y', 'yes'):
        try:
            import pull_candles
            pull_candles.backfill_local(verbose=True)
        except Exception as e:
            log(f"backfill error: {e}", R)
    else:
        log("skipping backfill — running on live data only", Y)


def main(interval=SCAN_INTERVAL):
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    _startup_backfill()

    log(f"Starting — scan every {interval}s", C)

    while True:
        t0 = time.time()
        utc = datetime.now(timezone.utc).strftime('%H:%M:%S')

        try:
            # Only git-fetch raw data if collector isn't writing it locally
            local_raw = RAW_DIR / 'BTCUSDT_LIVE.jsonl.gz'
            if not local_raw.exists():
                _run('git', 'fetch', 'origin', DATA_BRANCH)

            log(f"running wave_scan…  ({utc} UTC)", C)
            raw_out, clean_out = run_scan()

            # Parse structured summary
            summary = parse_signals(raw_out)
            n = summary['signal_count']

            # Write plain text (ANSI stripped, human-readable)
            SIGNALS_TXT.write_text(clean_out, encoding='utf-8')

            # Write JSON summary
            SIGNALS_JSON.write_text(
                json.dumps(summary, indent=2), encoding='utf-8')

            # Push both files to data/raw branch
            ok = _git_push_files([SIGNALS_TXT, SIGNALS_JSON])
            status = f"{G}pushed{Z}" if ok else f"{R}push failed{Z}"

            last = summary['signals'][-1] if summary['signals'] else None
            last_str = (f"{last['dir']} {last['time']} ${last['price']:,.2f}"
                        if last else 'none')
            log(f"{n} signals | last={last_str} | {status}", G if ok else R)

        except Exception as e:
            log(f"ERROR: {e}", R)

        # Sleep for remainder of interval
        elapsed = time.time() - t0
        wait    = max(0, interval - elapsed)
        time.sleep(wait)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--interval', type=int, default=SCAN_INTERVAL,
                   help='seconds between scans (default 300)')
    args = p.parse_args()
    main(interval=args.interval)
