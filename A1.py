#!/usr/bin/env python3
"""
A1.py — BTCUSDT live tick collector + real-time candle builder.

Every 1s bar that closes immediately updates the leading edge of ALL higher
timeframes in memory. TF bars print as each boundary crosses. The background
thread writes to disk (gzip-9) and pushes to git independently.

Termux setup (run once):
    echo "alias A1='python3 ~/TRENDRIDER/A1.py'" >> ~/.bashrc && source ~/.bashrc
    # zsh:
    echo "alias A1='python3 ~/TRENDRIDER/A1.py'" >> ~/.zshrc  && source ~/.zshrc
Run:
    A1
"""

import asyncio, gzip, io, json, os, signal, subprocess, sys, threading, time
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

WRITE_S = 5    # flush raw ticks to disk every N seconds
BUILD_S = 30   # persist candle files to disk / push every N seconds
PUSH_S  = 60   # git push to data/raw every N seconds

# Timeframe name → seconds per bar
TIMEFRAMES = {
    '1m': 60, '5m': 300, '10m': 600, '15m': 900,
    '30m': 1800, '45m': 2700, '1h': 3600, '4h': 14400,
}

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; W='\033[97m'; B='\033[94m'; Z='\033[0m'
def _utc(): return datetime.now(timezone.utc).strftime('%H:%M:%S')
SEP  = C + '─' * 56 + Z
SEPE = W + '═' * 56 + Z   # heavy border for status blocks
SEPS = G + '█' * 56 + Z   # solid bar for LONG signals
SEPR = R + '█' * 56 + Z   # solid bar for SHORT signals


# ── Shared state ───────────────────────────────────────────────────────────────

_raw        = deque(maxlen=15000)   # raw JSON lines — bounded ring buffer
_lock       = threading.Lock()
_last_write = 0.0
_last_build = 0.0
_last_push  = 0.0
_n_ticks    = 0
_n_depth    = 0

# Live 1s accumulator (in-progress bar for current second)
_bar_sec = 0
_bar     = None   # {o, h, l, c, v, bv, sv, n}

# Real-time TF state — updated on every 1s close
# _partial[tf] = current open bar for that TF (in-memory, not yet closed)
_partial    = {}   # {tf: {'_aligned':int, ts, open, high, low, close, volume, buy_v, sell_v, n}}
_counts     = {}   # {tf: total closed bar count} — base from file + live increments
_snap_count = 0    # 1s-bar counter; snapshot + physics print every 5 counts

# Physics engine state — set by _init_physics() at startup
_scan_state = None   # wave_scan.ScanState


# ── Terminal output ────────────────────────────────────────────────────────────

def _hdr():
    print(f'\n{SEPE}')
    print(f'{W}  A1 · BTCUSDT  real-time ticks + candles + physics{Z}')
    print(f'  {_utc()} UTC  ·  {DATA_DIR}')
    print(f'  WRITE={WRITE_S}s  BUILD={BUILD_S}s  PUSH={PUSH_S}s  gzip-9')
    print(f'{SEPE}\n', flush=True)


def _g(ok):
    return f'{G}✓{Z}' if ok else f'{R}✗{Z}'


def _print_status(new_sigs=None):
    """
    Unified status block printed every 5 1s-bar closes and on any TF close.
    Two sections: PHYSICS indicators + CANDLES leading edge.
    Signal cards printed separately at the end if new_sigs provided.
    """
    now_sec = int(time.time())
    live    = getattr(_scan_state, 'live', {}) if _scan_state else {}

    print(f'\n{SEPE}', flush=True)

    # ── Physics panel ────────────────────────────────────────────────────
    if live:
        price   = live.get('price',        0)
        dk      = live.get('dk',           '?')
        phase   = live.get('phase',        '?')
        micro   = live.get('micro_ph',     0)
        vel     = live.get('vel',          0)
        obi     = live.get('obi',          0);  obi_n   = live.get('obi_need',     1)
        score   = live.get('score',        0);  score_n = live.get('score_need',   1)
        kdv     = live.get('kdv_bal',      0);  kdv_r   = live.get('kdv_need_rev', 999)
        spr     = live.get('spr',          0)
        htf_sup = getattr(_scan_state, 'last_htf_sup', False)

        dk_ok    = (dk == 'FLOOR')
        vel_ok   = (vel > 0)
        obi_ok   = (obi >= obi_n)
        score_ok = (score >= score_n)
        kdv_ok   = (kdv >= kdv_r)
        n_pass   = sum([dk_ok, vel_ok, obi_ok, score_ok, kdv_ok])
        nc       = G if n_pass == 5 else (Y if n_pass >= 3 else R)

        htf_c  = G if htf_sup else R
        dk_c   = G if dk_ok  else R
        vel_c  = G if vel_ok else R
        mic_c  = G if micro > 0 else R

        print(f'  {W}PHYSICS{Z}  {_utc()}  ${price:>12,.2f}  '
              f'spr:${spr:.2f}  htf_sup:{htf_c}{"✓" if htf_sup else "✗"}{Z}', flush=True)
        print(f'  dk:{dk_c}{dk:<6}{Z}  phase:{phase:<8}  '
              f'micro:{mic_c}{micro:>+.3f}{Z}  vel:{vel_c}{vel:>+.4f}{Z}', flush=True)
        print(f'  obi:{obi:>+.3f}(≥{obi_n:.3f}){_g(obi_ok)}  '
              f'score:{score:.3f}(≥{score_n:.3f}){_g(score_ok)}  '
              f'kdv:{kdv:.3f}(≥{kdv_r:.3f}){_g(kdv_ok)}', flush=True)
        print(f'  gates: dk{_g(dk_ok)} vel{_g(vel_ok)} obi{_g(obi_ok)} '
              f'score{_g(score_ok)} kdv{_g(kdv_ok)}  '
              f'{nc}({n_pass}/5){Z}', flush=True)
    else:
        phys_lbl = (f'{Y}initializing…{Z}' if _scan_state is not None
                    else f'{R}DISABLED — no seed data{Z}')
        print(f'  {W}PHYSICS{Z}  {_utc()}  {phys_lbl}', flush=True)

    # ── Candle leading edge ──────────────────────────────────────────────
    print(f'  {C}{"─"*52}{Z}', flush=True)
    tfs = ('1s', '1m', '5m', '10m', '15m', '30m', '45m', '1h', '4h')
    row = '  '.join(f'{W}{tf}{Z}:{_counts.get(tf, 0):>4}' for tf in tfs)
    print(f'  {C}bars{Z}  {row}', flush=True)

    if _partial:
        for tf, tf_secs in TIMEFRAMES.items():
            p = _partial.get(tf)
            if p is None:
                continue
            elapsed = max(0, now_sec - p['_aligned'])
            pct     = min(100, int(elapsed / tf_secs * 100))
            chg     = p['close'] - p['open']
            chg_c   = G if chg >= 0 else R
            bc      = G if p['close'] >= p['open'] else R
            buy_pct = int(p['buy_v'] / p['volume'] * 100) if p['volume'] else 0
            bar_f   = ('█' * (pct // 10)).ljust(10)
            print(
                f'  {W}{tf:>4}{Z} [{bar_f}]{pct:>3}%  '
                f'{elapsed:>4}s  '
                f'{bc}{p["close"]:>12,.2f}{Z}  '
                f'{chg_c}{chg:>+7.2f}{Z}  '
                f'V:{p["volume"]:.4f}  buy:{buy_pct}%',
                flush=True
            )

    print(f'{SEPE}', flush=True)

    # ── Signal cards ─────────────────────────────────────────────────────
    if new_sigs:
        for sig in new_sigs:
            _print_signal_card(sig)


def _print_signal_card(sig: dict):
    """Large unmissable signal card — LONG=green solid bar, SHORT=red solid bar."""
    d     = sig.get('dir', 0)
    label = sig.get('label', sig.get('type', '?'))
    price = sig.get('price', sig.get('entry', 0))
    obi   = sig.get('obi',   0)
    cph   = sig.get('cph',   0)
    taker = sig.get('taker', 0)
    nn    = sig.get('n',     '?')
    BAR   = SEPS if d > 0 else SEPR
    arrow = '▲▲  LONG  ▲▲' if d > 0 else '▼▼  SHORT ▼▼'
    c     = G if d > 0 else R
    print(f'\n{BAR}', flush=True)
    print(f'{c}  {arrow}  ·  {label}  [#{nn}]{Z}', flush=True)
    print(f'{c}  {_utc()}  ·  ${price:>12,.2f}{Z}', flush=True)
    print(f'{c}  taker:{taker*100:.0f}%  obi:{obi:>+.3f}  cph:{cph:>+.3f}{Z}',
          flush=True)
    print(f'{BAR}', flush=True)
    print('\a', end='', flush=True)   # terminal bell


# ── Real-time TF aggregation ───────────────────────────────────────────────────

def _on_1s_close(bar1s: dict) -> list:
    """
    Feed one closed 1s bar into every TF partial bar.
    Returns list of (tf, closed_bar) for each TF boundary crossed.
    Increments _counts[tf] for each TF bar that closes.
    """
    ts_sec = bar1s['ts'] // 1000
    _counts['1s'] = _counts.get('1s', 0) + 1

    just_closed = []

    for tf, tf_secs in TIMEFRAMES.items():
        aligned = (ts_sec // tf_secs) * tf_secs
        prev = _partial.get(tf)

        if prev is None or prev['_aligned'] != aligned:
            # Boundary crossed — close the previous partial bar if it exists
            if prev is not None:
                just_closed.append((tf, dict(prev)))
                _counts[tf] = _counts.get(tf, 0) + 1
            # Open a new partial bar
            _partial[tf] = {
                '_aligned': aligned,
                'ts':       aligned * 1000,
                'open':     bar1s['open'],
                'high':     bar1s['high'],
                'low':      bar1s['low'],
                'close':    bar1s['close'],
                'volume':   bar1s['volume'],
                'buy_v':    bar1s['buy_v'],
                'sell_v':   bar1s['sell_v'],
                'n':        bar1s['n'],
            }
        else:
            # Same TF bar — extend the open partial bar
            p = _partial[tf]
            if bar1s['high'] > p['high']:  p['high']   = bar1s['high']
            if bar1s['low']  < p['low']:   p['low']    = bar1s['low']
            p['close']  = bar1s['close']
            p['volume'] += bar1s['volume']
            p['buy_v']  += bar1s['buy_v']
            p['sell_v'] += bar1s['sell_v']
            p['n']      += bar1s['n']

    return just_closed


def _print_tf_close(tf: str, bar: dict):
    """Compact closed-bar line — full status block prints immediately after."""
    buy_pct = int(bar['buy_v'] / bar['volume'] * 100) if bar['volume'] else 0
    bc   = G if bar['close'] >= bar['open'] else R
    chg  = bar['close'] - bar['open']
    cc   = G if chg >= 0 else R
    print(f'{SEP}', flush=True)
    print(f'  {W}{tf} CLOSED  {_utc()}{Z}  '
          f'{cc}{chg:>+.2f}{Z}  '
          f'O:{bar["open"]:>12,.2f}  H:{bar["high"]:>12,.2f}  '
          f'L:{bar["low"]:>12,.2f}  {bc}C:{bar["close"]:>12,.2f}{Z}  '
          f'vol:{bar["volume"]:.4f}  buy:{buy_pct}%  n={bar["n"]}s',
          flush=True)


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
    print(f'  {G}disk{Z}  {len(lines):,} lines → {GZ_FILE.name}'
          f'  ({kb:.1f} KB, gzip-9)', flush=True)
    return len(lines)


# ── Candle file build (disk persistence) ──────────────────────────────────────

def _build():
    """
    Rebuild all TF candle files from disk (historical klines + live ticks).
    Also resets _counts base to file-accurate values and re-seeds _partial
    from the file data so the in-memory state stays consistent.
    """
    global _last_build
    try:
        sys.path.insert(0, str(REPO))
        import build_candles as _bc

        data = _bc.build(verbose=False)
        _bc.save(data, verbose=False)

        # Reset counts from authoritative file data
        for tf, d in data.items():
            _counts[tf] = d['count']

        # Re-seed partial bars from the last bar in each TF
        # (file bars are closed; partial starts fresh at the current boundary)
        # We don't overwrite in-memory partial bars here — they continue from
        # wherever they are in the current second.  The count reset above is enough.

        _last_build = time.time()

        _print_status()   # shows updated counts + edges after build

    except Exception as e:
        print(f'  {Y}build error: {e}{Z}', flush=True)


# ── Git push ──────────────────────────────────────────────────────────────────

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
        # Clear stale lock files
        for lock in [TMP_IDX,
                     REPO / '.git' / 'refs' / 'heads' / 'data' / 'raw.lock',
                     REPO / '.git' / 'index.lock']:
            lock.unlink(missing_ok=True)

        env_idx = {**os.environ, 'GIT_NO_AUTO_GC': '1',
                   'GIT_INDEX_FILE': str(TMP_IDX)}
        env_obj = {**os.environ, 'GIT_NO_AUTO_GC': '1'}

        # Build the file map: live ticks + all TF files
        push_map = {'data/raw/BTCUSDT_LIVE.jsonl.gz': GZ_FILE}
        for f in sorted(DATA_DIR.glob('tf_*.json.gz')):
            push_map[f'data/raw/{f.name}'] = f
        stats = DATA_DIR / 'tf_candles.json'
        if stats.exists():
            push_map['data/raw/tf_candles.json'] = stats
        push_map = {k: v for k, v in push_map.items()
                    if v.exists() and v.stat().st_size > 0}
        if not push_map:
            return

        # Seed the index from the existing data/raw tree
        r = _run_git('git', 'rev-parse', '--verify', 'origin/data/raw')
        parent = r.stdout.strip() if r.returncode == 0 else ''
        if parent:
            _run_git('git', 'read-tree', 'origin/data/raw', env=env_idx)

        blobs = {}
        for git_path, local_path in push_map.items():
            r = _run_git('git', 'hash-object', '-w', str(local_path), env=env_obj)
            sha = r.stdout.strip()
            if sha:
                blobs[git_path] = sha
                _run_git('git', 'update-index', '--add', '--cacheinfo',
                         f'100644,{sha},{git_path}', env=env_idx)

        blob = next(iter(blobs.values()), '')
        r = _run_git('git', 'write-tree', env=env_idx)
        tree = r.stdout.strip()
        TMP_IDX.unlink(missing_ok=True)
        if not tree:
            return

        with _lock:
            n = len(_raw)
        cmd = ['git', 'commit-tree', tree, '-m', f'A1 ticks={n} ts={int(time.time())}']
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
        for sha in set(list(blobs.values()) + [tree, commit]):
            _del_obj(sha)

        print(f'\n  {G}push{Z}  {n:,} ticks + {len(push_map)} files → data/raw', flush=True)
        for git_path, local_path in push_map.items():
            kb = local_path.stat().st_size / 1024
            print(f'        {git_path}  ({kb:.1f} KB)', flush=True)
        print(flush=True)

    except Exception as e:
        print(f'  {Y}push error: {e}{Z}', flush=True)
        TMP_IDX.unlink(missing_ok=True)
        for sha in (blob, tree, commit):
            _del_obj(sha)


# ── Physics engine (wave_scan) ────────────────────────────────────────────────

def _init_physics(seed_bars_1m: list):
    """Seed ScanState from 1m bars. Called once at startup."""
    global _scan_state
    try:
        import wave_scan as _ws
        _scan_state = _ws.ScanState()
        _ws.scan_incremental(_scan_state, raw_lines=[], signals_only=True,
                             seed_bars=seed_bars_1m)
        print(f'  {G}physics{Z}  seeded  '
              f'({len(_scan_state.closed_1m)} 1m bars)', flush=True)
    except Exception as e:
        print(f'  {Y}physics init (non-fatal): {e}{Z}', flush=True)


def _run_physics():
    """
    Incremental physics tick — captures wave_scan stdout (we handle display),
    collects new_sigs, then calls _print_status() to render everything.
    Called every 5 1s-bar closes and on any TF boundary crossing.
    """
    try:
        import wave_scan as _ws
        with _lock:
            raw_snap = list(_raw)

        # Suppress wave_scan's own prints; we render signals our way
        _buf = io.StringIO()
        _old = sys.stdout
        sys.stdout = _buf
        try:
            new_sigs = (_ws.scan_incremental(_scan_state, raw_lines=raw_snap,
                                             signals_only=True)
                        if _scan_state is not None else [])
        finally:
            sys.stdout = _old

        _print_status(new_sigs if new_sigs else None)

    except Exception as e:
        sys.stdout = sys.__stdout__   # safety restore
        print(f'  {Y}physics: {e}{Z}', flush=True)
        _print_status()   # still show candle edges even if physics errors


# ── Background worker — write / build / push on timers ────────────────────────

def _worker():
    _flush()
    _build()   # initial build: set _counts base from any existing files
    while True:
        time.sleep(1)
        now = time.time()
        if now - _last_write >= WRITE_S:
            _flush()
        if now - _last_build >= BUILD_S:
            _flush()     # write ticks first so build sees them
            _build()
        if now - _last_push >= PUSH_S:
            _flush()
            _push()


# ── WebSocket handler ─────────────────────────────────────────────────────────

async def _stream():
    global _n_ticks, _n_depth, _bar_sec, _bar

    backoff = 2
    while True:
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.ws_connect(WS_URL, heartbeat=20) as ws:
                    print(f'  {G}connected{Z} → Binance.US  '
                          f'({_utc()} UTC)\n', flush=True)
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
                            is_sell = bool(d['m'])   # m=True → sell taker
                            p_int   = round(price * 100)
                            q_int   = round(qty   * 10000)

                            # Exact record written to BTCUSDT_LIVE.jsonl.gz
                            line = json.dumps(['T', ts_ms, p_int, q_int, int(is_sell)])
                            with _lock:
                                _raw.append(line)
                            _n_ticks += 1

                            side_c = f'{R}SELL{Z}' if is_sell else f'{G}BUY {Z}'
                            print(f'{_utc()} T  ${price:>12,.2f}  {qty:>10.6f}  '
                                  f'{side_c}  {line}', flush=True)

                            # ── 1s bar accumulation ──────────────────────────
                            sec = ts_ms // 1000

                            if sec != _bar_sec and _bar is not None:
                                # ── 1s bar just closed ───────────────────────
                                b = _bar
                                buy_pct = int(b['bv'] / b['v'] * 100) if b['v'] else 0
                                bc = G if b['c'] >= b['o'] else R
                                print(
                                    f'{_utc()} {W}1s{Z}  '
                                    f'O:{b["o"]:>12,.2f}  '
                                    f'H:{b["h"]:>12,.2f}  '
                                    f'L:{b["l"]:>12,.2f}  '
                                    f'{bc}C:{b["c"]:>12,.2f}{Z}  '
                                    f'V:{b["v"]:.4f}btc  buy:{buy_pct}%  '
                                    f'n={b["n"]}',
                                    flush=True)

                                # Feed closed 1s bar into every TF
                                bar1s = {
                                    'ts':     _bar_sec * 1000,
                                    'open':   b['o'], 'high': b['h'],
                                    'low':    b['l'], 'close': b['c'],
                                    'volume': b['v'], 'buy_v': b['bv'],
                                    'sell_v': b['sv'], 'n': b['n'],
                                }
                                just_closed = _on_1s_close(bar1s)

                                # Print any TF bars that just closed
                                # Sort by TF size so 1m prints before 5m etc.
                                just_closed.sort(key=lambda x: TIMEFRAMES[x[0]])
                                for tf, closed_bar in just_closed:
                                    _print_tf_close(tf, closed_bar)

                                # Every 5s (or on any TF close): unified status block
                                global _snap_count
                                _snap_count += 1
                                if _snap_count % 5 == 0 or just_closed:
                                    _run_physics()

                                _bar = None

                            # Accumulate into the in-progress 1s bar
                            if _bar is None:
                                _bar = {'o': price, 'h': price, 'l': price,
                                        'c': price, 'v': 0.0, 'bv': 0.0,
                                        'sv': 0.0, 'n': 0}
                                _bar_sec = sec

                            b = _bar
                            if price > b['h']:  b['h'] = price
                            if price < b['l']:  b['l'] = price
                            b['c']  = price
                            b['v'] += qty
                            b['n'] += 1
                            if is_sell:
                                b['sv'] += qty
                            else:
                                b['bv'] += qty

                        # ── Depth snapshot (depth20@100ms) ───────────────────
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

                            # Print every 10th — 10/s arrive, 1/s shows on screen
                            if _n_depth % 10 == 0 and bids and asks:
                                bid0 = bids[0][0] / 100
                                ask0 = asks[0][0] / 100
                                spr  = ask0 - bid0
                                bv20 = sum(q for _, q in bids) / 10000
                                av20 = sum(q for _, q in asks) / 10000
                                print(f'{_utc()} D  bid ${bid0:>12,.2f}'
                                      f'  ask ${ask0:>12,.2f}'
                                      f'  spr ${spr:.2f}'
                                      f'  bv={bv20:.2f}  av={av20:.2f}'
                                      f'  D#{_n_depth}',
                                      flush=True)

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

    print(f'  {Y}fetching historical candles (ingest)…{Z}', flush=True)
    try:
        sys.path.insert(0, str(REPO))
        import ingest as _ig
        _ig.run(verbose=True)
    except Exception as e:
        print(f'  {Y}ingest error (continuing): {e}{Z}', flush=True)

    # Seed physics engine from pre-built 1m candles
    print(f'  {Y}seeding physics engine…{Z}', flush=True)
    try:
        _seed_path = DATA_DIR / 'tf_1m.json.gz'
        if _seed_path.exists():
            with gzip.open(_seed_path, 'rt') as _sf:
                _raw_1m = json.loads(_sf.read())
            # wave_scan expects taker_buy field
            _seed_1m = [dict(b, taker_buy=b.get('buy_v', b.get('volume', 0) * 0.5))
                        for b in _raw_1m]
            _init_physics(_seed_1m)
        else:
            print(f'  {Y}tf_1m.json.gz not found — physics starts cold{Z}', flush=True)
    except Exception as e:
        print(f'  {Y}physics seed (non-fatal): {e}{Z}', flush=True)

    threading.Thread(target=_worker, daemon=True).start()

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
