#!/usr/bin/env python3
"""
A1.py — BTCUSDT live tick collector + candle builder.

Prints every record saved to disk, plus running candle counts per timeframe.
Data stored hyper-compressed (gzip-9) in ~/.trendrider/ and pushed to data/raw.

Termux setup (run once):
    echo "alias A1='python3 ~/TRENDRIDER/A1.py'" >> ~/.bashrc && source ~/.bashrc
    # or for zsh:
    echo "alias A1='python3 ~/TRENDRIDER/A1.py'" >> ~/.zshrc  && source ~/.zshrc

Run:
    A1
    python3 ~/TRENDRIDER/A1.py
"""

import asyncio, gzip, json, os, signal, subprocess, sys, threading, time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

try:
    import aiohttp
except ImportError:
    print("ERROR: pip install aiohttp"); sys.exit(1)

REPO     = Path(__file__).resolve().parent
DATA_DIR = Path.home() / '.trendrider'
GZ_FILE  = DATA_DIR / 'BTCUSDT_LIVE.jsonl.gz'
TMP_IDX  = REPO / '.git' / 'a1_push.idx'

WS_URL = ('wss://stream.binance.us:9443/stream'
          '?streams=btcusdt@aggTrade/btcusdt@depth20@100ms')

WRITE_S = 5    # flush ticks to disk every N seconds
BUILD_S = 30   # rebuild all candle TF files every N seconds
PUSH_S  = 60   # git push to data/raw every N seconds

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; W='\033[97m'; Z='\033[0m'
def _utc(): return datetime.now(timezone.utc).strftime('%H:%M:%S')


# ── Shared state ───────────────────────────────────────────────────────────────

_raw        = deque(maxlen=15000)   # bounded ring buffer — ~125 min at 2 rec/s
_lock       = threading.Lock()
_counts     = {}                    # {tf: bar_count} updated after each build
_last_write = 0.0
_last_build = 0.0
_last_push  = 0.0
_n_ticks    = 0
_n_depth    = 0
_last_price = 0.0
# Live 1s bar accumulator
_bar_sec    = 0
_bar        = None   # {'o','h','l','c','v','bv','sv','n'}


# ── Terminal output ────────────────────────────────────────────────────────────

def _hdr():
    sep = C + '━' * 60 + Z
    print(sep)
    print(f'{W}  A1 · BTCUSDT collector + candles{Z}')
    print(f'  {_utc()} UTC  ·  data → {DATA_DIR}')
    print(f'  WRITE={WRITE_S}s  BUILD={BUILD_S}s  PUSH={PUSH_S}s  gzip-9')
    print(sep + '\n', flush=True)


def _print_counts():
    tfs = ('1s','1m','5m','10m','15m','30m','45m','1h','4h')
    cols = '  '.join(f'{W}{tf}{Z}:{_counts.get(tf,0):>4}' for tf in tfs)
    print(f'  {C}bars{Z} │ {cols}', flush=True)


# ── Disk flush (gzip level 9) ─────────────────────────────────────────────────

def _flush():
    global _last_write
    with _lock:
        lines = list(_raw)
    if not lines:
        return 0
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw_bytes  = ('\n'.join(lines) + '\n').encode()
    compressed = gzip.compress(raw_bytes, compresslevel=9)
    GZ_FILE.write_bytes(compressed)
    _last_write = time.time()
    kb = len(compressed) / 1024
    print(f'  {G}disk{Z}  │ {len(lines):,} lines → {GZ_FILE.name}  '
          f'({kb:.1f} KB, gzip-9)', flush=True)
    return len(lines)


# ── Candle build ──────────────────────────────────────────────────────────────

def _build():
    global _last_build
    try:
        sys.path.insert(0, str(REPO))
        import build_candles as _bc
        data = _bc.build(verbose=False)
        _bc.save(data, verbose=False)
        for tf, d in data.items():
            _counts[tf] = d['count']
        _last_build = time.time()
        print(f'\n  {C}── candles {_utc()} ──{Z}', flush=True)
        _print_counts()
        print(flush=True)
    except Exception as e:
        print(f'  {Y}build error: {e}{Z}', flush=True)


# ── Git push (zero net object growth — delete blobs after each push) ──────────

def _run_git(*args, env=None):
    return subprocess.run(list(args), capture_output=True, text=True,
                          cwd=str(REPO), env=env, timeout=60)


def _del_obj(sha):
    if sha and len(sha) >= 4:
        (REPO / '.git' / 'objects' / sha[:2] / sha[2:]).unlink(missing_ok=True)


def _push():
    global _last_push
    blob = tree = commit = ''
    try:
        # Clear any stale lock files from a killed push
        for lock in [TMP_IDX,
                     REPO / '.git' / 'refs' / 'heads' / 'data' / 'raw.lock',
                     REPO / '.git' / 'index.lock']:
            lock.unlink(missing_ok=True)

        env_idx = {**os.environ, 'GIT_NO_AUTO_GC': '1',
                   'GIT_INDEX_FILE': str(TMP_IDX)}
        env_obj = {**os.environ, 'GIT_NO_AUTO_GC': '1'}

        # Files to push: live ticks + all TF candle files
        push_map = {'data/raw/BTCUSDT_LIVE.jsonl.gz': GZ_FILE}
        for tf_file in sorted(DATA_DIR.glob('tf_*.json.gz')):
            push_map[f'data/raw/{tf_file.name}'] = tf_file
        # Also push candle stats summary
        stats = DATA_DIR / 'tf_candles.json'
        if stats.exists():
            push_map['data/raw/tf_candles.json'] = stats

        # Only push files that actually exist and have content
        push_map = {k: v for k, v in push_map.items()
                    if v.exists() and v.stat().st_size > 0}
        if not push_map:
            return

        # Seed index from existing data/raw so unrelated files are preserved
        r = _run_git('git', 'rev-parse', '--verify', 'origin/data/raw')
        parent = r.stdout.strip() if r.returncode == 0 else ''
        if parent:
            _run_git('git', 'read-tree', 'origin/data/raw', env=env_idx)

        # Hash and register each file
        blobs = {}
        for git_path, local_path in push_map.items():
            r = _run_git('git', 'hash-object', '-w', str(local_path), env=env_obj)
            sha = r.stdout.strip()
            if sha:
                blobs[git_path] = sha
                _run_git('git', 'update-index', '--add', '--cacheinfo',
                         f'100644,{sha},{git_path}', env=env_idx)

        blob = blobs.get('data/raw/BTCUSDT_LIVE.jsonl.gz', next(iter(blobs.values()), ''))

        r = _run_git('git', 'write-tree', env=env_idx)
        tree = r.stdout.strip()
        TMP_IDX.unlink(missing_ok=True)
        if not tree:
            return

        with _lock:
            n = len(_raw)
        msg = f'A1 ticks={n} ts={int(time.time())}'
        cmd = ['git', 'commit-tree', tree, '-m', msg]
        if parent:
            cmd += ['-p', parent]
        r = _run_git(*cmd, env=env_obj)
        commit = r.stdout.strip()
        if not commit:
            return

        r = _run_git('git', 'push', '--force', 'origin',
                     f'{commit}:refs/heads/data/raw')
        if r.returncode != 0:
            print(f'  {Y}push failed: {r.stderr.strip()[:120]}{Z}', flush=True)
            return

        _last_push = time.time()

        # Delete loose objects so phone .git/objects never accumulates
        for sha in set(list(blobs.values()) + [tree, commit]):
            _del_obj(sha)

        print(f'\n  {G}push{Z}  │ {n:,} ticks + {len(push_map)} files → data/raw', flush=True)
        for git_path in push_map:
            local = push_map[git_path]
            kb = local.stat().st_size / 1024
            print(f'         {git_path}  ({kb:.1f} KB)', flush=True)
        print(flush=True)

    except Exception as e:
        print(f'  {Y}push error: {e}{Z}', flush=True)
        TMP_IDX.unlink(missing_ok=True)
        for sha in (blob, tree, commit):
            _del_obj(sha)


# ── Background worker — write / build / push on timers ────────────────────────

def _worker():
    _build()   # initial candle build from any historical data on disk
    while True:
        time.sleep(1)
        now = time.time()
        if now - _last_write >= WRITE_S:
            _flush()
        if now - _last_build >= BUILD_S:
            _build()
        if now - _last_push >= PUSH_S:
            _flush()     # make sure latest ticks hit disk before git reads them
            _push()


# ── WebSocket handler ─────────────────────────────────────────────────────────

async def _stream():
    global _n_ticks, _n_depth, _last_price
    global _bar_sec, _bar

    backoff = 2
    while True:
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.ws_connect(WS_URL, heartbeat=20) as ws:
                    print(f'  {G}connected{Z} → Binance.US WebSocket\n', flush=True)
                    backoff = 2

                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue

                        obj  = json.loads(msg.data)
                        d    = obj.get('data', obj)
                        name = obj.get('stream', '')

                        # ── Trade tick ───────────────────────────────────────
                        if name.endswith('aggTrade'):
                            ts_ms   = int(d['T'])
                            price   = float(d['p'])
                            qty     = float(d['q'])
                            is_sell = bool(d['m'])   # m=True: buyer is maker → sell taker
                            p_int   = round(price * 100)
                            q_int   = round(qty   * 10000)

                            # Exact record being written to file
                            line = json.dumps(['T', ts_ms, p_int, q_int, int(is_sell)])
                            with _lock:
                                _raw.append(line)
                            _n_ticks   += 1
                            _last_price = price

                            side_c = f'{R}SELL{Z}' if is_sell else f'{G}BUY {Z}'
                            chg = price - _last_price if _n_ticks > 1 else 0.0
                            chg_c = G if chg >= 0 else R
                            print(f'{_utc()} T  ${price:>12,.2f}  {qty:>10.6f} BTC  {side_c}'
                                  f'  {line}', flush=True)

                            # Live 1s bar accumulation
                            sec = ts_ms // 1000
                            if sec != _bar_sec and _bar is not None:
                                b = _bar
                                bc = G if b['c'] >= b['o'] else R
                                print(f'{_utc()} 1s O:{b["o"]:,.2f}  H:{b["h"]:,.2f}'
                                      f'  L:{b["l"]:,.2f}  C:{b["c"]:,.2f}'
                                      f'  {bc}V:{b["v"]:.4f}btc{Z}'
                                      f'  buy:{b["bv"]:.4f}  n={b["n"]}', flush=True)
                                _bar = None
                            if _bar is None:
                                _bar = {'o': price, 'h': price, 'l': price,
                                        'c': price, 'v': 0.0, 'bv': 0.0,
                                        'sv': 0.0, 'n': 0}
                                _bar_sec = sec
                            _bar['h'] = max(_bar['h'], price)
                            _bar['l'] = min(_bar['l'], price)
                            _bar['c'] = price
                            _bar['v'] += qty
                            _bar['n'] += 1
                            if is_sell:
                                _bar['sv'] += qty
                            else:
                                _bar['bv'] += qty

                        # ── Order book snapshot (depth20@100ms) ─────────────
                        elif name.endswith('depth20@100ms'):
                            ts_ms = int(time.time() * 1000)
                            bids  = [[round(float(p) * 100), round(float(q) * 10000)]
                                     for p, q in d.get('bids', [])[:20]]
                            asks  = [[round(float(p) * 100), round(float(q) * 10000)]
                                     for p, q in d.get('asks', [])[:20]]
                            line  = json.dumps(['D', ts_ms, bids, asks])
                            with _lock:
                                _raw.append(line)
                            _n_depth += 1

                            # Print every 10th depth record (10/s → 1/s on screen)
                            if _n_depth % 10 == 0 and bids and asks:
                                bid0 = bids[0][0] / 100
                                ask0 = asks[0][0] / 100
                                spr  = ask0 - bid0
                                bv20 = sum(q for _, q in bids) / 10000
                                av20 = sum(q for _, q in asks) / 10000
                                print(f'{_utc()} D  bid ${bid0:>12,.2f}'
                                      f'  ask ${ask0:>12,.2f}  spr ${spr:.2f}'
                                      f'  bv={bv20:.2f}  av={av20:.2f}'
                                      f'  D#{_n_depth}', flush=True)

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f'  {Y}ws: {e} — retry {backoff}s{Z}', flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    _hdr()

    # Fetch historical candles on startup
    print(f'  {Y}fetching historical candles…{Z}', flush=True)
    try:
        sys.path.insert(0, str(REPO))
        import ingest as _ig
        _ig.run(verbose=True)
    except Exception as e:
        print(f'  {Y}ingest error (continuing): {e}{Z}', flush=True)

    # Start background write/build/push thread
    threading.Thread(target=_worker, daemon=True).start()

    # Run WebSocket — blocks until Ctrl-C
    loop = asyncio.new_event_loop()
    task = loop.create_task(_stream())
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, task.cancel)
    try:
        loop.run_until_complete(task)
    finally:
        print(f'\n  {Y}shutting down — final flush + push…{Z}', flush=True)
        _flush()
        _push()
        loop.close()
        print('  done.\n', flush=True)
