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


def _agg_tf(bars_1m, n, keep=20):
    """Aggregate 1m bars into n-minute bars using sliding window from the end."""
    if not bars_1m or n <= 1:
        return bars_1m[-keep:]
    result = []
    i = len(bars_1m)
    while i >= n and len(result) < keep:
        chunk = bars_1m[i - n:i]
        result.insert(0, {
            'ts':        chunk[0]['ts'],
            'open':      chunk[0]['open'],
            'high':      max(b['high'] for b in chunk),
            'low':       min(b['low'] for b in chunk),
            'close':     chunk[-1]['close'],
            'volume':    round(sum(b.get('volume', 0) for b in chunk), 6),
            'taker_buy': round(sum(b.get('taker_buy', b.get('taker', 0)) for b in chunk), 6),
        })
        i -= n
    return result


def _live_5m_bar(state):
    """Build the current incomplete 5m bar from accumulated second bars on state."""
    s1       = getattr(state, 's1_by_sec', {})
    last_sec = getattr(state, 'last_sec',  0)
    if not s1 or not last_sec:
        return None
    win_start = (last_sec // 300) * 300
    secs = sorted(s for s in s1 if win_start <= s <= last_sec)
    if not secs:
        return None
    bars     = [s1[s] for s in secs]
    last_ob  = bars[-1].get('ob', ([], []))
    bids, asks = last_ob if len(last_ob) == 2 else ([], [])
    return {
        'ts':        win_start * 1000,
        'open':      bars[0]['open'],
        'high':      max(b['high'] for b in bars),
        'low':       min(b['low']  for b in bars),
        'close':     bars[-1]['close'],
        'volume':    round(sum(b['volume'] for b in bars), 6),
        'taker_buy': round(sum(b.get('taker_buy', b['volume'] * 0.5) for b in bars), 6),
        'ob_mid':    round((asks[0][0] + bids[0][0]) / 2, 2) if bids and asks else None,
        'ob_spr':    round(asks[0][0] - bids[0][0], 2)       if bids and asks else None,
        'live':      True,
    }


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
                # ── order book depth snapshot ────────────────────────────
                ob_depth = {}
                ob_by_sec = getattr(state, 'ob_by_sec', {})
                if ob_by_sec:
                    latest_ob = ob_by_sec.get(max(ob_by_sec.keys()))
                    if latest_ob and len(latest_ob) == 2:
                        bids, asks = latest_ob
                        ob_depth['bids'] = [[round(p,2), round(q,4)] for p,q in (bids[:20] if bids else [])]
                        ob_depth['asks'] = [[round(p,2), round(q,4)] for p,q in (asks[:20] if asks else [])]
                        if bids and asks:
                            ob_depth['mid']  = round((asks[0][0] + bids[0][0]) / 2, 2)
                            ob_depth['spr']  = round(asks[0][0] - bids[0][0], 2)
                            for depth in [5, 10, 20]:
                                b = bids[:depth]; a = asks[:depth]
                                bv = sum(x[1] for x in b); av = sum(x[1] for x in a)
                                tot = bv + av
                                ob_depth[f'obi{depth}'] = round((bv - av) / tot, 4) if tot > 0 else 0
                            def _wall(levels):
                                if not levels: return None
                                avg = sum(x[1] for x in levels) / len(levels)
                                for i, (p, q) in enumerate(levels):
                                    if q >= avg:
                                        return {'lvl': i, 'price': round(p,2), 'qty': round(q,4)}
                                return None
                            ob_depth['bid_wall'] = _wall(bids[:20])
                            ob_depth['ask_wall'] = _wall(asks[:20])
                            # total liquidity each side (20 levels)
                            ob_depth['bid_liq'] = round(sum(x[1] for x in bids[:20]), 4)
                            ob_depth['ask_liq'] = round(sum(x[1] for x in asks[:20]), 4)

                # ── knife decay buffer ───────────────────────────────────
                kb = getattr(state, 'knife_buf', None)
                knife_data = {}
                if kb:
                    knife_data = {
                        'state':    getattr(kb, 'state', 0),
                        'label':    getattr(kb, 'label', '?'),
                        'decay_sc': round(getattr(kb, 'decay_score', 0), 4),
                        'floor_sc': round(getattr(kb, 'floor_score', 0), 4),
                        'lows':     getattr(kb, 'lows', []),
                    }

                # ── live 5m bar from second bars ─────────────────────────
                _live5        = _live_5m_bar(state)
                _tf5c         = getattr(state, 'tf5m', [])
                _live5_ts     = (_live5 or {}).get('ts')
                tf5m_live     = [b for b in _tf5c[-30:] if b['ts'] != _live5_ts]
                if _live5:
                    tf5m_live.append(_live5)

                summary = {
                    'signal_count': n,
                    'signals':      state.signals,
                    'scanned_at':   datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'new_this_run': len(new_sigs),
                    'live':         getattr(state, 'live', {}),
                    'tf_states':    getattr(state, 'last_tf_states', {}),
                    'tf_physics':   getattr(state, 'tf_live', {}),
                    'shelves':      getattr(state, 'session_shelves', []),
                    'knife':        knife_data,
                    'ob_depth':     ob_depth,
                    'timeframes': {
                        # 1m/5m/10m — live edge snaps on 1m close
                        'tf1m':  (state.closed_1m[-60:]  if getattr(state, 'closed_1m', None) else []),
                        'tf5m':  tf5m_live,
                        'tf10m': _agg_tf(getattr(state, 'closed_1m', []), 10, keep=20),
                        # 15m/30m/45m/1h/4h — live edge snaps on 5m close (live 5m bar included)
                        'tf15m': (state.tf15m[-20:]      if getattr(state, 'tf15m', None) else []),
                        'tf30m': (state.tf30m[-12:]      if getattr(state, 'tf30m', None) else []),
                        'tf45m': (state.tf45m[-10:]      if getattr(state, 'tf45m', None) else []),
                        'tf1h':  (state.tf1h[-12:]       if getattr(state, 'tf1h',  None) else []),
                        'tf4h':  (state.tf4h[-6:]        if getattr(state, 'tf4h',  None) else []),
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
