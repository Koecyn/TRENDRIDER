#!/usr/bin/env python3
"""
collect_raw.py — WebSocket → deque → pipeline to wave_scan + GitHub backup.

Pipeline:
  WebSocket → deque(maxlen=15000) → ~/.trendrider/BTCUSDT_LIVE.jsonl.gz
                                  → git push queue → GitHub data/raw

Data dir is ~/.trendrider/ — always writable, outside the repo, not git-tracked.
Git push is queued (non-blocking): write never waits on network.

Trade line : ["T", ts_ms, price_cents, qty_units, side]
Depth line : ["D", ts_ms, [[p_c,q_u],...bids], [[p_c,q_u],...asks]]
"""

import asyncio, collections, gc, gzip, json, os, queue, resource
import signal, subprocess, sys, threading, time
from pathlib import Path
from datetime import datetime, timezone

try:
    import aiohttp
except ImportError:
    print("ERROR: pip install aiohttp"); sys.exit(1)

REPO      = Path(__file__).resolve().parent
DATA_DIR  = Path.home() / '.trendrider'          # pipeline data dir — always writable
GZ_FILE   = DATA_DIR / 'BTCUSDT_LIVE.jsonl.gz'  # wave_scan reads this
TMP_IDX   = REPO / '.git' / 'data_push.idx'

DATA_BRANCH   = 'data/raw'
GIT_TREE_PATH = 'data/raw/BTCUSDT_LIVE.jsonl.gz'

WS_URL = ('wss://stream.binance.us:9443/stream'
          '?streams=btcusdt@depth20@100ms/btcusdt@aggTrade')

WINDOW_LINES   = 15_000   # bounded deque — ~125 min at 2 records/sec
PUSH_S         = 2        # write snapshot every N seconds
GC_EVERY       = 60
LOG_MEM_EVERY  = 300
SIG_PUSH_S     = 60       # push signals to git at most once per minute

SIGNALS_DIR  = DATA_DIR / 'signals'
SIGNALS_TXT  = SIGNALS_DIR / 'BTCUSDT_SIGNALS.txt'
SIGNALS_JSON = SIGNALS_DIR / 'BTCUSDT_SIGNALS.json'
SIG_BRANCH   = 'data/signals'
SIG_IDX      = REPO / '.git' / 'scan_push.idx'

_raw_deque    = collections.deque(maxlen=WINDOW_LINES)
_deque_lock   = threading.Lock()
_push_queue   = queue.Queue(maxsize=1)   # non-blocking git push pipeline
_scan_trigger = threading.Event()        # set by on_trade/on_depth; scanner also wakes on timeout
_git_lock     = threading.Lock()         # serialize all git operations — one at a time
_last_trade_sec = 0
_last_depth_sec = 0

GIT_TIMEOUT = 60   # seconds before a git subprocess is killed

G='\033[92m'; R='\033[91m'; Y='\033[93m'; Z='\033[0m'
def log(m, c=Z): print(f'{c}[raw] {m}{Z}', flush=True)

def _run(*a, env=None):
    return subprocess.run(list(a), capture_output=True, text=True,
                          cwd=str(REPO), env=env, timeout=GIT_TIMEOUT)


# ── Git push thread — reads from queue, never blocks the write loop ───────────

def _del_obj(sha):
    """Delete a loose git object by SHA — no-op if already packed or missing."""
    if sha and len(sha) >= 4:
        (REPO / '.git' / 'objects' / sha[:2] / sha[2:]).unlink(missing_ok=True)

def _clear_git_locks():
    """Remove stale ref lock files left by killed git push subprocesses."""
    git_dir = REPO / '.git'
    for branch in (DATA_BRANCH, SIG_BRANCH):
        lock = git_dir / 'refs' / 'heads' / Path(branch.replace('/', os.sep) + '.lock')
        lock.unlink(missing_ok=True)
    (git_dir / 'index.lock').unlink(missing_ok=True)


def _git_push_worker():
    """Dedicated thread: drains _push_queue and pushes to GitHub.

    Each push cycle creates exactly 3 loose objects (blob, tree, commit).
    We delete them immediately after a successful push so the phone's
    .git/objects/ never accumulates data — zero net growth per cycle.
    GIT_NO_AUTO_GC=1 prevents git from packing the objects before we can
    delete them.
    """
    while True:
        gz_path = _push_queue.get()   # blocks until a snapshot is queued
        if gz_path is None:
            break
        blob = tree = commit = ''
        try:
            with _git_lock:
                _clear_git_locks()
                if not gz_path.exists():
                    log(f'raw: gz missing {gz_path}', R)
                    continue
                sz = gz_path.stat().st_size
                if sz == 0:
                    log(f'raw: gz empty', R)
                    continue
                env_gc  = {**os.environ, 'GIT_NO_AUTO_GC': '1',
                           'GIT_INDEX_FILE': str(TMP_IDX)}
                env_obj = {**os.environ, 'GIT_NO_AUTO_GC': '1'}  # no index for hash-object
                TMP_IDX.unlink(missing_ok=True)
                r = _run('git', 'hash-object', '-w', str(gz_path), env=env_obj)
                blob = r.stdout.strip()
                if not blob:
                    # One retry after clearing any stale locks
                    _clear_git_locks()
                    r = _run('git', 'hash-object', '-w', str(gz_path), env=env_obj)
                    blob = r.stdout.strip()
                if not blob:
                    log(f'raw: hash-object failed (sz={sz}): {r.stderr.strip()[:200]}', R)
                    continue
                _run('git', 'update-index', '--add',
                     '--cacheinfo', f'100644,{blob},{GIT_TREE_PATH}', env=env_gc)
                r = _run('git', 'write-tree', env=env_gc)
                tree = r.stdout.strip()
                TMP_IDX.unlink(missing_ok=True)
                if not tree:
                    log(f'raw: write-tree failed: {r.stderr.strip()[:80]}', R)
                    continue
                env_no_gc = {k: v for k, v in env_gc.items()
                             if k != 'GIT_INDEX_FILE'}
                r = _run('git', 'commit-tree', tree, '-m', f'raw {int(time.time())}',
                         env=env_no_gc)
                commit = r.stdout.strip()
                if not commit:
                    log(f'raw: commit-tree failed: {r.stderr.strip()[:80]}', R)
                    continue
                r = _run('git', 'push', '--force', 'origin',
                         f'{commit}:refs/heads/{DATA_BRANCH}')
                if r.returncode != 0:
                    log(f'raw: push failed: {r.stderr.strip()[:120]}', R)
                for sha in (blob, tree, commit):
                    _del_obj(sha)
        except Exception as e:
            log(f'git push error: {e}', R)
            for sha in (blob, tree, commit):
                _del_obj(sha)


# ── Signal push ───────────────────────────────────────────────────────────────

def _push_signals(txt: str, summary: dict) -> bool:
    SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
    SIGNALS_TXT.write_text(txt or '(no signals yet)\n', encoding='utf-8')
    SIGNALS_JSON.write_text(json.dumps(summary, indent=2), encoding='utf-8')
    with _git_lock:
        try:
            _clear_git_locks()
            env = {**os.environ, 'GIT_INDEX_FILE': str(SIG_IDX), 'GIT_NO_AUTO_GC': '1'}
            SIG_IDX.unlink(missing_ok=True)
            tree_paths = {
                SIGNALS_TXT:  'data/signals/BTCUSDT_SIGNALS.txt',
                SIGNALS_JSON: 'data/signals/BTCUSDT_SIGNALS.json',
            }
            for p in (SIGNALS_TXT, SIGNALS_JSON):
                r = _run('git', 'hash-object', '-w', str(p))
                blob = r.stdout.strip()
                if not blob:
                    log(f'sig: hash-object failed: {r.stderr.strip()[:120]}', R)
                    return False
                _run('git', 'update-index', '--add',
                     '--cacheinfo', f'100644,{blob},{tree_paths[p]}', env=env)
            r = _run('git', 'write-tree', env=env)
            tree = r.stdout.strip()
            SIG_IDX.unlink(missing_ok=True)
            if not tree:
                log(f'sig: write-tree failed: {r.stderr.strip()[:120]}', R)
                return False
            r = _run('git', 'commit-tree', tree, '-m', f'signals {int(time.time())}')
            commit = r.stdout.strip()
            if not commit:
                log(f'sig: commit-tree failed: {r.stderr.strip()[:120]}', R)
                return False
            r = _run('git', 'push', '--force', 'origin',
                     f'{commit}:refs/heads/{SIG_BRANCH}')
            if r.returncode != 0:
                log(f'sig: push failed: {r.stderr.strip()[:200]}', R)
            return r.returncode == 0
        except Exception:
            import traceback
            log(f'sig: exception: {traceback.format_exc()[:200]}', R)
            SIG_IDX.unlink(missing_ok=True)
            return False


# ── Scan loop — triggered by on_trade, reads deque directly ───────────────────

def _scan_loop(seed_bars=None):
    try:
        import traceback
        sys.path.insert(0, str(REPO))
        log('scanner: importing wave_scan…', Y)
        import wave_scan as _ws
        log('scanner: wave_scan loaded', Y)

        state          = _ws.ScanState()
        last_push_time = 0.0

        # Seed directly from exchange bars — no deque needed for startup
        log('scanner: seeding from exchange bars…', Y)
        _ws.scan_incremental(state, raw_lines=[], signals_only=True,
                             seed_bars=seed_bars or [])
        log(f'scanner: ready | seeded {len(state.closed_1m)} bars', G)

        # Wait for first live data
        log('scanner: waiting for first data…', Y)
        while True:
            _scan_trigger.wait(timeout=2.0)
            with _deque_lock:
                has_data = len(_raw_deque) > 0
            if has_data:
                break
        log('scanner: live data flowing — starting main loop', G)

        while True:
            try:
                _scan_trigger.wait(timeout=10.0)
                _scan_trigger.clear()

                with _deque_lock:
                    snapshot = list(_raw_deque)

                new_sigs = _ws.scan_incremental(state, raw_lines=snapshot,
                                                 signals_only=True)

                now = time.time()
                if new_sigs or (now - last_push_time >= SIG_PUSH_S):
                    live = {}
                    try:
                        if state.s1_by_sec:
                            last_sec = max(state.s1_by_sec)
                            bar  = state.s1_by_sec[last_sec]
                            bids, asks = bar['ob']
                            mid  = bar['close']
                            bv   = sum(q for _,q in bids[:5]); av = sum(q for _,q in asks[:5])
                            obi  = round((bv-av)/(bv+av), 3) if bv+av else 0.0
                            bv2  = sum(q for p,q in bids if abs(p-mid)<=25)
                            av2  = sum(q for p,q in asks if abs(p-mid)<=25)
                            conc = round((bv2-av2)/(bv2+av2), 3) if bv2+av2 else 0.0
                            spr  = round(asks[0][0]-bids[0][0], 2) if bids and asks else 99.0
                            buf  = state.knife_buf
                            vel  = round(buf._vel(), 3)
                            br, ar = buf._flow_rates()
                            ds   = round(buf._decay_score(), 3)
                            fos  = round(buf._floor_score(conc, spr, mid=mid, bids=bids, asks=asks), 3)
                            thr = _ws._thresholds(
                                state.hist_scores, state.hist_phases,
                                state.hist_obi, state.hist_kdv_bals,
                                state.hist_aligns)
                            thresh, peak_ph, trough_ph, obi_conf, gate_rev, gate_cont, align_cont = thr
                            live = {
                                'at':        datetime.fromtimestamp(last_sec, tz=timezone.utc).strftime('%H:%M:%SZ'),
                                'price':     round(mid, 2),
                                # ── live indicators ──────────────────────────
                                'obi':       obi,
                                'obi_need':  round(obi_conf, 3),
                                'conc':      conc,
                                'spr':       spr,
                                'vel':       vel,
                                'bid_flow':  round(br, 4),
                                'ask_flow':  round(ar, 4),
                                'phase':     buf.phase,
                                'dk':        buf.LABELS.get(buf.state, str(buf.state)),
                                'decay_sc':  ds,
                                'floor_sc':  fos,
                                'score':     round(state.hist_scores[-1], 3) if state.hist_scores else None,
                                'score_need': round(thresh, 3),
                                'kdv_bal':   round(state.hist_kdv_bals[-1], 3) if state.hist_kdv_bals else None,
                                'kdv_need_rev':  round(gate_rev, 3),
                                'kdv_need_cont': round(gate_cont, 3),
                                'micro_ph':  round(state.hist_phases[-1], 3) if state.hist_phases else None,
                                'micro_ph_peak':   round(peak_ph, 3),
                                'micro_ph_trough': round(trough_ph, 3),
                                'align_need': round(align_cont, 3),
                                'res_align':  round(state.last_align, 3),
                                'res_dir':    state.last_res_dir,
                                'htf_sup':    state.last_htf_sup,
                                'htf_bars':   f'{len(state.tf5m)}x5m {len(state.tf15m)}x15m {len(state.tf1h)}x1h {len(state.tf4h)}x4h',
                            }
                    except Exception:
                        pass
                    summary = {
                        'signal_count': state.sig_count,
                        'signals':      state.signals,
                        'scanned_at':   datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'new_this_run': len(new_sigs),
                        'live':         live,
                    }
                    if not _push_signals('', summary):
                        log('sig push failed', R)
                    else:
                        px   = live.get('price', '?')
                        ph   = live.get('phase', '?')
                        ds   = live.get('decay_sc', '?')
                        fos  = live.get('floor_sc', '?')
                        sc   = live.get('score', '?')
                        bars = len(state.closed_1m)
                        res  = f'res={state.last_res_dir}@{state.last_align:.2f}'
                        htf  = f'htf={len(state.tf5m)}x5m/{len(state.tf1h)}x1h'
                        sup  = ' SUP' if state.last_htf_sup else ''
                        log(f'push ok | ${px:,.2f}  {ph}  ds={ds}  fos={fos}  score={sc}  {res}  {htf}{sup}  bars={bars}', Y)
                    last_push_time = time.time()

            except Exception:
                log(f'scanner error:\n{traceback.format_exc()}', R)

    except Exception:
        import traceback as _tb
        log(f'scanner FATAL:\n{_tb.format_exc()}', R)


# ── Write loop — snapshot deque to disk, queue git push ──────────────────────

def _push_loop():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log(f'pipeline → {DATA_DIR}', G)
    cycle = 0
    while True:
        time.sleep(PUSH_S)
        cycle += 1
        try:
            with _deque_lock:
                if not _raw_deque: continue
                lines = list(_raw_deque)

            tmp = GZ_FILE.with_suffix('.tmp')
            with gzip.open(tmp, 'wt', compresslevel=1) as f:
                f.write('\n'.join(lines))
            os.replace(tmp, GZ_FILE)   # atomic: push thread never sees 0-byte file

            # Queue for git push — drop if queue is full (push still in progress)
            try:
                _push_queue.put_nowait(GZ_FILE)
            except queue.Full:
                pass

            if cycle % GC_EVERY == 0:
                gc.collect()

            if cycle % LOG_MEM_EVERY == 0:
                rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                sz  = GZ_FILE.stat().st_size if GZ_FILE.exists() else 0
                log(f'mem={rss}KB  lines={len(lines)}  gz={sz//1024}kB  '
                    f'cycle={cycle}', Y)
        except Exception as e:
            log(f'write error: {e}', R)



# ── WebSocket → deque (hot path — zero I/O) ───────────────────────────────────

async def stream():
    def on_trade(d):
        global _last_trade_sec
        ts_ms = int(d['T'])
        ts_s  = ts_ms // 1000
        with _deque_lock:
            _raw_deque.append(json.dumps(
                ['T', ts_ms, int(float(d['p'])*100),
                 int(float(d['q'])*10000), 0 if d.get('m') else 1],
                separators=(',',':')))
        if ts_s > _last_trade_sec:
            _last_trade_sec = ts_s
            _scan_trigger.set()   # new second → wake scanner

    def on_depth(d):
        global _last_depth_sec
        ts  = int(time.time()*1000)
        ts_s = ts // 1000
        bid = [[int(float(p)*100), int(float(q)*10000)] for p,q in d.get('bids',[])]
        ask = [[int(float(p)*100), int(float(q)*10000)] for p,q in d.get('asks',[])]
        with _deque_lock:
            _raw_deque.append(json.dumps(['D', ts, bid, ask], separators=(',',':')))
        if ts_s > _last_depth_sec:
            _last_depth_sec = ts_s
            _scan_trigger.set()   # new second of OB data → wake scanner

    log('connecting...', G)
    backoff = 1
    try:
        while True:
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.ws_connect(WS_URL, heartbeat=20,
                                               receive_timeout=30) as ws:
                        backoff = 1
                        log(f'connected  deque={WINDOW_LINES}', G)
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    d  = json.loads(msg.data)
                                    st = d.get('stream', '')
                                    da = d.get('data', {})
                                    if 'aggTrade' in st: on_trade(da)
                                    elif '@depth'  in st: on_depth(da)
                                except Exception as e:
                                    log(f'parse: {e}', R)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                log(f'error ({type(e).__name__}: {e}) retry {backoff}s', R)
                await asyncio.sleep(backoff)
                backoff = min(backoff*2, 60)
    finally:
        _push_queue.put(None)   # signal git worker to exit


if __name__ == '__main__':
    _seed_bars = []
    try:
        import pull_candles
        _seed_bars = pull_candles.fetch_seed_bars(400)  # 400 → 26x 15m bars ≥ resonance min
    except Exception as e:
        log(f'candle seed (non-fatal): {e}', R)

    threading.Thread(target=_push_loop,                    daemon=True).start()
    threading.Thread(target=_git_push_worker,              daemon=True).start()
    threading.Thread(target=_scan_loop, args=(_seed_bars,), daemon=True).start()

    loop = asyncio.new_event_loop()
    task = loop.create_task(stream())
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, task.cancel)
    try:
        loop.run_until_complete(task)
    finally:
        loop.close()
