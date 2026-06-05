#!/usr/bin/env python3
"""
scan_live.py — event-driven signal scanner.

Watches ~/.trendrider/BTCUSDT_LIVE.jsonl.gz for new writes from collect_raw.py.
Fires scan_incremental the instant the file is updated — no fixed interval,
no sleeping past the data. Pushes results to data/signals branch on new signal
or every 60 seconds.

Usage:
    python scan_live.py
"""

import gzip, io, json, os, re, subprocess, sys, time
from pathlib import Path
from datetime import datetime, timezone

REPO         = Path(__file__).resolve().parent
RAW_DIR      = REPO / "data" / "raw"
SIGNALS_TXT  = RAW_DIR / "BTCUSDT_SIGNALS.txt"
SIGNALS_JSON = RAW_DIR / "BTCUSDT_SIGNALS.json"
CANDLES_1M   = RAW_DIR / "BTCUSDT_1m_live.json.gz"
DATA_BRANCH  = "data/signals"
TMP_IDX      = REPO / ".git" / "scan_push.idx"
LIVE_FILE    = Path.home() / '.trendrider' / 'BTCUSDT_LIVE.jsonl.gz'

PUSH_INTERVAL = 60    # push to git at most once per minute
POLL_S        = 0.05  # mtime poll cadence — 50 ms

ANSI = re.compile(r'\x1b\[[0-9;]*m')

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; Z='\033[0m'
def log(m, c=Z): print(f"{c}[scan] {m}{Z}", flush=True)


# ── git plumbing push ─────────────────────────────────────────────────────────

def _run(*a, env=None):
    return subprocess.run(list(a), capture_output=True, text=True,
                          cwd=str(REPO), env=env)


def _git_push_files(paths: list) -> bool:
    """Push signal files to data/signals branch atomically."""
    env = {**os.environ, 'GIT_INDEX_FILE': str(TMP_IDX), 'GIT_NO_AUTO_GC': '1'}

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

    cmd = ['git', 'commit-tree', tree, '-m', f'signals {int(time.time())}']
    if parent:
        cmd += ['-p', parent]
    r = _run(*cmd)
    commit = r.stdout.strip()
    if not commit:
        return False

    r = _run('git', 'push', 'origin', f'{commit}:refs/heads/{DATA_BRANCH}')
    if r.returncode != 0:
        log(f'push stderr: {r.stderr.strip()[:200]}', R)
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

def _startup_backfill():
    """Ask whether to pull missing candle history from exchange before scanning."""
    local_1m = Path.home() / '.trendrider' / 'BTCUSDT_1m.json.gz'
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


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    _startup_backfill()

    sys.path.insert(0, str(REPO))
    import wave_scan as _ws

    state = _ws.ScanState()
    log(f"Starting — event-driven on {LIVE_FILE.name}", C)

    last_push_time = 0.0
    last_clean_out = ''
    last_mtime     = 0.0

    while True:
        # Wait for new data — fire the instant the file is touched
        try:
            mtime = LIVE_FILE.stat().st_mtime
        except FileNotFoundError:
            time.sleep(POLL_S)
            continue

        if mtime <= last_mtime:
            time.sleep(POLL_S)
            continue

        last_mtime = mtime

        try:
            buf = io.StringIO()
            old_stdout = sys.stdout
            sys.stdout = buf
            try:
                new_sigs = _ws.scan_incremental(state, signals_only=True)
            finally:
                sys.stdout = old_stdout
            raw_out   = buf.getvalue()
            clean_out = ANSI.sub('', raw_out)

            n    = state.sig_count
            last = state.signals[-1] if state.signals else None

            if clean_out:
                last_clean_out = clean_out

            if new_sigs:
                last_str = (f"{'LONG' if last['dir']>0 else 'SHORT'} "
                            f"{last['time']} ${last['price']:,.2f}"
                            if last else 'none')
                log(f"NEW +{len(new_sigs)} → {n} total | last={last_str}", G)

            now = time.time()
            if new_sigs or (now - last_push_time >= PUSH_INTERVAL):
                SIGNALS_TXT.write_text(last_clean_out or '(no signals yet)\n',
                                       encoding='utf-8')
                summary = {
                    'signal_count': n,
                    'signals':      state.signals,
                    'scanned_at':   datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'new_this_run': len(new_sigs),
                    'live':         getattr(state, 'live', {}),
                    'timeframes': {
                        'tf5m':  (state.tf5m[-30:]  if getattr(state, 'tf5m',  None) else []),
                        'tf15m': (state.tf15m[-20:] if getattr(state, 'tf15m', None) else []),
                        'tf1h':  (state.tf1h[-12:]  if getattr(state, 'tf1h',  None) else []),
                        'tf4h':  (state.tf4h[-6:]   if getattr(state, 'tf4h',  None) else []),
                    },
                }
                SIGNALS_JSON.write_text(json.dumps(summary, indent=2), encoding='utf-8')
                if state.closed_1m:
                    CANDLES_1M.parent.mkdir(parents=True, exist_ok=True)
                    with gzip.open(CANDLES_1M, 'wb') as _cf:
                        _cf.write(json.dumps(state.closed_1m[-200:]).encode())
                push_files = [SIGNALS_TXT, SIGNALS_JSON]
                if CANDLES_1M.exists():
                    push_files.append(CANDLES_1M)
                ok = _git_push_files(push_files)
                last_push_time = time.time()
                if not ok:
                    log('push failed', R)

        except Exception as e:
            import traceback
            log(f"ERROR: {e}", R)
            traceback.print_exc()


if __name__ == '__main__':
    main()
