#!/usr/bin/env python3
"""
pipeline.py — Three-layer data pipeline with process separation.

  LAYER 1 (capture)   WebSocket → memmap write ONLY. No processing, no prints.
                       On each aggTrade: price → ring buffer. That's it.

  LAYER 2 (process)   Reads memmap → scipy Hilbert decompose → JSON file.
                       Runs in thread pool. Never blocks capture layer.

  LAYER 3 (publish)   Reads JSON → git commit → git push data/live.
                       Runs in separate thread. Never blocks process layer.

Heartbeat: if no WebSocket message for 10s → reconnect immediately.
Git push debounced: max one push per 30s (bar close fires immediately).

On your device:
  python pipeline.py              # runs all 3 layers
  python pipeline.py --capture    # layer 1 only (writes memmap, no git)
  python pipeline.py --process    # layers 2+3 only (reads existing memmap)
"""

import argparse
import asyncio
import gc
import gzip
import json
import os
import resource
import signal
import subprocess
import sys
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np

try:
    from scipy.signal import butter, filtfilt, hilbert as sp_hilbert
    _SCIPY = True
except ImportError:
    _SCIPY = False
    print("[pipeline] WARNING: scipy not installed — pip install scipy", flush=True)

try:
    import aiohttp
except ImportError:
    print("[pipeline] ERROR: pip install aiohttp", flush=True)
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────

REPO     = Path(__file__).resolve().parent
WS_URL   = ("wss://stream.binance.us:9443/stream"
            "?streams=btcusdc@kline_1m/btcusdc@depth20@100ms/btcusdc@aggTrade")

# Cache dir — /tmp doesn't exist on Termux; use ~/.tr_cache instead
_CACHE    = Path.home() / '.tr_cache'
_CACHE.mkdir(exist_ok=True)

TICK_BIN  = _CACHE / 'tr_ticks.bin'      # float64 ring buffer (prices)
TICK_IDX  = _CACHE / 'tr_ticks.idx'      # write index (int64)
BARS_FILE = _CACHE / 'tr_bars.json.gz'   # closed 1m bar history (gzip)
SEC_FILE  = _CACHE / 'tr_sec.json.gz'    # recent 1s OHLCV bars (gzip)
WAVE_FILE = _CACHE / 'tr_waves.json.gz'  # latest wave decomposition (gzip)
OUT_FILE  = REPO / "physics_live_results.json"   # plain JSON — git-readable
LOG_FILE  = _CACHE / 'tr_pipeline.log'
RAW_DIR   = REPO / "raw"                   # gitignored — daily raw tick JSONL

GC_EVERY_BARS = 10    # force gc.collect() every N closed bars
MEM_LOG_BARS  = 30    # log RSS memory every N closed bars
RAW_FLUSH     = 200   # flush raw buffer to disk every N trades

RING_SIZE    = 10_000   # circular buffer length
MAX_BARS     = 300      # 1m bar history to keep
MAX_SEC_BARS = 300      # 1s bars to keep (5 minutes of 1s data)
HEARTBEAT_S  = 10       # reconnect if WS silent this long
MIN_PUSH_S   = 5        # min seconds between git pushes — floor to prevent push storms
INTRABAR_S   = 1        # recompute every second intrabar

# 1m-bar bands — period in bars (1 bar = 1 minute)
BANDS = {
    'subharm': (6,  22),
    'carrier': (15, 45),
    'macro':   (40, 130),
}
BAND_MIN = {'subharm': 50, 'carrier': 80, 'macro': 200}

# 1s-bar bands — period in bars (1 bar = 1 second)
# Same wave structures but at 1s resolution for intrabar leading signal
BANDS_1S = {
    'fast_sub':     (6,   22),    # 6-22s  swings
    'fast_carrier': (15,  60),    # 15-60s swings
    'fast_macro':   (60, 180),    # 1-3min swings
}
BAND_MIN_1S = {'fast_sub': 30, 'fast_carrier': 60, 'fast_macro': 150}


# ── Logging (file only — no terminal spam blocking WebSocket) ─────────────────

_log_fh = None

def _log(msg: str):
    global _log_fh
    line = f"[{time.strftime('%H:%M:%S')}] {msg}\n"
    if _log_fh is None:
        try:
            _log_fh = open(LOG_FILE, 'a', buffering=1)
        except Exception:
            pass
    if _log_fh:
        try:
            _log_fh.write(line)
        except Exception:
            pass

def _print(msg: str):
    """Print to terminal AND log. Use sparingly — only key events."""
    print(f"[pipeline] {msg}", flush=True)
    _log(msg)


# ── Tick buffer (memmap ring) ─────────────────────────────────────────────────

def _buf_create() -> tuple:
    buf = np.memmap(str(TICK_BIN), dtype='float64', mode='w+', shape=(RING_SIZE,))
    buf[:] = 0.0
    buf.flush()
    TICK_IDX.write_bytes(np.int64(0).tobytes())
    return buf, 0

def _buf_open() -> tuple:
    buf = np.memmap(str(TICK_BIN), dtype='float64', mode='r+', shape=(RING_SIZE,))
    idx = int(np.frombuffer(TICK_IDX.read_bytes(), dtype='int64')[0])
    return buf, idx

def buf_init() -> tuple:
    if TICK_BIN.exists() and TICK_IDX.exists():
        try:
            b, i = _buf_open()
            n = min(i, RING_SIZE)
            _print(f"Resumed tick buffer — {n} prices")
            return b, i
        except Exception:
            pass
    _print("Creating fresh tick buffer")
    return _buf_create()

def buf_write(buf: np.memmap, idx: int, price: float) -> int:
    buf[idx % RING_SIZE] = price
    buf.flush()
    new_idx = idx + 1
    TICK_IDX.write_bytes(np.int64(new_idx).tobytes())
    return new_idx

def buf_read(buf: np.memmap, idx: int, n: int) -> np.ndarray:
    avail = min(idx, RING_SIZE)
    n = min(n, avail)
    if n == 0:
        return np.array([], dtype=float)
    end   = idx % RING_SIZE
    start = (idx - n) % RING_SIZE
    if start < end:
        return buf[start:end].copy()
    return np.concatenate([buf[start:], buf[:end]])


# ── Layer 2: scipy wave decomposition (runs in thread pool) ──────────────────

def _bandpass(prices: np.ndarray, lo_p: int, hi_p: int) -> np.ndarray:
    nyq = 0.5
    lo  = np.clip(1.0 / hi_p, 1e-4, nyq * 0.48)
    hi  = np.clip(1.0 / lo_p, lo * 1.05, nyq * 0.98)
    try:
        b, a     = butter(N=4, Wn=[lo / nyq, hi / nyq], btype='band')
        min_len  = 3 * max(len(b), len(a)) + 1
        if len(prices) < min_len:
            return prices - prices.mean()
        return filtfilt(b, a, prices)
    except Exception:
        return prices - prices.mean()

def _hilbert_stats(filtered: np.ndarray) -> dict:
    """
    Hilbert decomposition: phase_norm = real(analytic)/|analytic| → -1..+1.
    -1 = trough (signal minimum), +1 = peak (signal maximum).
    velocity = d(phase_norm)/dt via numpy.gradient.
    """
    null = {'amp': 0.0, 'phase': 0.0, 'vel': 0.0, 'dir': 0}
    if len(filtered) < 10:
        return null
    try:
        analytic   = sp_hilbert(filtered)
        env        = np.abs(analytic)
        phase_norm = np.real(analytic) / (env + 1e-10)
        vel        = np.gradient(phase_norm)
        amp   = float(env[-1])
        ph    = float(np.clip(phase_norm[-1], -1.0, 1.0))
        v     = float(vel[-1])
        d     = int(np.sign(v)) if amp > 0.5 and abs(v) > amp * 0.002 else 0
        return {'amp': round(amp, 2), 'phase': round(ph, 4),
                'vel': round(v, 6), 'dir': d}
    except Exception:
        return null

def decompose_prices(prices: np.ndarray) -> dict:
    """Full Hilbert wave decomposition on 1m bars. Pure math — no I/O."""
    waves = {}
    for band, (lo_p, hi_p) in BANDS.items():
        if len(prices) < BAND_MIN[band]:
            waves[band] = {'amp': 0.0, 'phase': 0.0, 'vel': 0.0, 'dir': 0}
            continue
        waves[band] = _hilbert_stats(_bandpass(prices, lo_p, hi_p))
    return waves

def decompose_1s(prices: np.ndarray) -> dict:
    """Fast Hilbert decomposition on 1s bars — intrabar leading signal."""
    waves = {}
    for band, (lo_p, hi_p) in BANDS_1S.items():
        if len(prices) < BAND_MIN_1S[band]:
            waves[band] = {'amp': 0.0, 'phase': 0.0, 'vel': 0.0, 'dir': 0}
            continue
        waves[band] = _hilbert_stats(_bandpass(prices, lo_p, hi_p))
    return waves

def _alignment(waves: dict) -> float:
    W = {'subharm': 0.25, 'carrier': 0.40, 'macro': 0.35}
    s, t = 0.0, 0.0
    for b, w in W.items():
        wv = waves.get(b, {})
        if wv.get('amp', 0) > 1.0:
            s += wv.get('dir', 0) * w
            t += w
    return round(s / t, 4) if t else 0.0

def build_stats(waves: dict, bar_count: int, cur_price: float) -> dict:
    c  = waves.get('carrier', {})
    sh = waves.get('subharm', {})
    mc = waves.get('macro',   {})
    tgt   = cur_price + c.get('amp', 0) * (1.0 - c.get('phase', 0)) / 2.0
    align = _alignment(waves)
    score = round(float(np.clip(align + mc.get('dir', 0) * 0.3, -1, 1)), 4)
    snr   = c.get('amp', 0) / max(sh.get('amp', 1), 1) * 10
    a     = abs(score)
    tier  = 1 if a > 0.7 else 2 if a > 0.5 else 3 if a > 0.35 else 4 if a > 0.2 else 5
    def trend(d): return 'up' if d > 0 else 'down' if d < 0 else 'neutral'
    return {
        'bars_live':            bar_count,
        'wf_tgt_primary':       round(tgt, 2),
        'wf_carrier_phase':     c.get('phase', 0.0),
        'wf_carrier_direction': c.get('dir',   0),
        'wf_carrier_velocity':  c.get('vel',   0.0),
        'wf_carrier_amp':       c.get('amp',   0.0),
        'wf_subharm_phase':     sh.get('phase', 0.0),
        'wf_subharm_direction': sh.get('dir',   0),
        'wf_subharm_velocity':  sh.get('vel',   0.0),
        'wf_subharm_amp':       sh.get('amp',   0.0),
        'wf_macro_phase':       mc.get('phase', 0.0),
        'wf_macro_direction':   mc.get('dir',   0),
        'wf_macro_amp':         mc.get('amp',   0.0),
        'last_score':           score,
        'threshold':            0.12,
        'last_tier':            tier,
        'res_alignment':        abs(align),
        'res_dissonance':       bool(align < -0.3),
        'htf_at_sup':           False,
        'htf_reversal':         False,
        'htf_fk':               False,
        'last_cav_active':      False,
        'last_snr':             round(snr, 1),
        'res_tf_1m':   round(sh.get('dir', 0) * 0.8, 3),
        'res_tf_5m':   round(c.get('dir',  0) * 0.7, 3),
        'res_tf_15m':  round(c.get('dir',  0) * 0.6, 3),
        'res_tf_1h':   round(mc.get('dir', 0) * 0.9, 3),
        'res_tf_4h':   round(mc.get('dir', 0) * 1.0, 3),
        'htf_trend_1m':  trend(sh.get('dir', 0)),
        'htf_trend_5m':  trend(c.get('dir',  0)),
        'htf_trend_15m': trend(c.get('dir',  0)),
        'htf_trend_1h':  trend(mc.get('dir', 0)),
        'htf_trend_4h':  trend(mc.get('dir', 0)),
    }

def process_waves(buf: np.memmap, idx: int, bars: list,
                  cur_price: float, sec_bars: list = None) -> dict | None:
    """Layer 2 worker — pure computation, called from thread pool."""
    closes = np.array([b['close'] for b in bars], dtype=float)
    if cur_price > 0:
        closes = np.append(closes, cur_price)
    if len(closes) < 10:
        return None

    # 1m decomposition — carrier/macro context
    waves = decompose_prices(closes)
    stats = build_stats(waves, len(bars), cur_price)

    # 1s decomposition — intrabar leading signal (sub/carrier at 1s resolution)
    if sec_bars and len(sec_bars) >= BAND_MIN_1S['fast_sub']:
        sec_closes = np.array([b['c'] for b in sec_bars], dtype=float)
        fast = decompose_1s(sec_closes)
        # Attach fast wave stats — leading indicator for sub/carrier turns
        stats['fast_sub_phase']     = fast.get('fast_sub',     {}).get('phase', 0.0)
        stats['fast_sub_dir']       = fast.get('fast_sub',     {}).get('dir',   0)
        stats['fast_sub_vel']       = fast.get('fast_sub',     {}).get('vel',   0.0)
        stats['fast_sub_amp']       = fast.get('fast_sub',     {}).get('amp',   0.0)
        stats['fast_carrier_phase'] = fast.get('fast_carrier', {}).get('phase', 0.0)
        stats['fast_carrier_dir']   = fast.get('fast_carrier', {}).get('dir',   0)
        stats['fast_carrier_amp']   = fast.get('fast_carrier', {}).get('amp',   0.0)
        stats['fast_macro_phase']   = fast.get('fast_macro',   {}).get('phase', 0.0)
        stats['fast_macro_dir']     = fast.get('fast_macro',   {}).get('dir',   0)
    else:
        stats['fast_sub_phase'] = stats['fast_sub_dir'] = 0
        stats['fast_carrier_phase'] = stats['fast_carrier_dir'] = 0
        stats['fast_macro_phase']   = stats['fast_macro_dir']   = 0

    # Keep sec_bars local (gzip cache) — strip from git payload to save bandwidth
    stats['sec_bars_n'] = len(sec_bars) if sec_bars else 0  # just the count
    wave_payload = {'waves': waves, 'stats': stats, 'ts': time.time()}
    tmp = WAVE_FILE.with_suffix('.tmp.gz')
    with gzip.open(tmp, 'wt', compresslevel=6) as f:
        json.dump(wave_payload, f, separators=(',', ':'))
    tmp.rename(WAVE_FILE)
    return stats


# ── Terminal display ─────────────────────────────────────────────────────────

_R = '\033[0m'
_BOLD = '\033[1m'
_DIM  = '\033[2m'
def _g(t): return f'\033[92m{t}{_R}'   # green
def _r(t): return f'\033[91m{t}{_R}'   # red
def _y(t): return f'\033[93m{t}{_R}'   # yellow
def _c(t): return f'\033[96m{t}{_R}'   # cyan
def _m(t): return f'\033[95m{t}{_R}'   # magenta
def _d(t): return f'\033[2m{t}{_R}'    # dim

def _arr(d):
    return _g('↑') if d > 0 else _r('↓') if d < 0 else _d('→')

def _tc(t):
    return _g(t) if t == 'up' else _r(t) if t == 'down' else _d(t)

def display_signal(stats: dict, cur_price: float,
                   bids: list, asks: list, ob: dict, slide: dict = None):
    s      = stats
    score  = s.get('last_score', 0)
    tier   = s.get('last_tier', 5)
    align  = s.get('res_alignment', 0)
    c_ph   = s.get('wf_carrier_phase', 0)
    c_dir  = s.get('wf_carrier_direction', 0)
    c_amp  = s.get('wf_carrier_amp', 0)
    sh_dir = s.get('wf_subharm_direction', 0)
    mc_dir = s.get('wf_macro_direction', 0)
    mc_ph  = s.get('wf_macro_phase', 0)
    mc_amp = s.get('wf_macro_amp', 0)
    bar_n  = s.get('bars_live', 0)

    at_peak   = c_ph >  0.75
    at_trough = c_ph < -0.75
    if at_peak:    ph_lbl = _r(f'{_BOLD}PEAK{_R}')
    elif at_trough: ph_lbl = _g(f'{_BOLD}TROUGH{_R}')
    else:           ph_lbl = _y('MID')

    sc_str = (_g if score > 0.2 else _r if score < -0.2 else _d)(f'{score:+.3f}')
    mc_head = mc_amp * (1.0 - mc_ph) / 2.0

    t1  = s.get('htf_trend_1m',  'neutral')
    t5  = s.get('htf_trend_5m',  'neutral')
    t15 = s.get('htf_trend_15m', 'neutral')
    t1h = s.get('htf_trend_1h',  'neutral')
    t4h = s.get('htf_trend_4h',  'neutral')

    # OB line
    ba_str = ''
    if asks and bids:
        best_a = asks[0][0]
        best_b = bids[0][0]
        ba_str = f'ask={_r(f"${best_a:.2f}")} bid={_g(f"${best_b:.2f}")} spread={best_a-best_b:.2f}'

    af = ob.get('ask_fill', 0); ap = ob.get('ask_pull', 0)
    bf = ob.get('bid_fill', 0); bp = ob.get('bid_pull', 0)
    ask_lbl = (_r('REAL') if af > ap * 1.5 else _g('FAKE') if ap > af * 1.5
               else _y('MIX')) if (af + ap) > 0 else _d('—')
    bid_lbl = (_g('REAL') if bf > bp * 1.5 else _r('FAKE') if bp > bf * 1.5
               else _y('MIX')) if (bf + bp) > 0 else _d('—')
    ob_wall = f'ask_wall:{ask_lbl}(f{af}/p{ap}) bid_wall:{bid_lbl}(f{bf}/p{bp})'

    # Fast 1s
    f_sd  = s.get('fast_sub_dir', 0)
    f_sa  = s.get('fast_sub_amp', 0.0)
    f_cd  = s.get('fast_carrier_dir', 0)
    f_md  = s.get('fast_macro_dir', 0)
    fast_str = (f'fast1s: sub{_arr(f_sd)} amp={f_sa:.1f} | '
                f'car{_arr(f_cd)} | mac{_arr(f_md)}') if f_sa > 0.5 else ''

    # Entry/exit annotation
    sig = ''
    fast_lead = f_sd > 0 and f_sa > 0.5 and (at_trough or c_ph < -0.5)
    if fast_lead:
        sig = _g('  *** ENTRY FORMING — 1s sub leading')
    elif at_trough and sh_dir > 0:
        sig = _g('  ** ENTRY — trough + sub rising')
    elif at_peak and score < -0.05 and mc_head < c_amp * 0.15:
        sig = _r('  !! EXHAUSTION — downside bias')

    # Sliding window line — shows real/synthetic slot balance and rolling OHLCV
    slide_str = ''
    if slide:
        real_n = slide.get('real_slots', 0)
        synth_n = 60 - real_n
        fill_pct = real_n / 60 * 100
        bar_filled = '█' * (real_n // 6) + '░' * (10 - real_n // 6)
        slide_h = slide.get('high', 0); slide_l = slide.get('low', 0)
        slide_rng = slide_h - slide_l
        slide_str = (f"[1m~]  [{bar_filled}] {real_n}real/{synth_n}syn "
                     f"O={slide.get('open',0):.2f} H={slide_h:.2f} "
                     f"L={slide_l:.2f} C={slide.get('close',0):.2f} "
                     f"rng=${slide_rng:.2f}")

    print(
        f"[wave] {_c(bar_n)} | {ph_lbl} | score={sc_str} | "
        f"c_ph={c_ph:+.2f}{_arr(c_dir)} sub{_arr(sh_dir)} mc{_arr(mc_dir)} | "
        f"align={align:.2f} T{tier} | mc_head=${mc_head:.0f} | ${cur_price:,.2f}"
        + (f"\n{slide_str}" if slide_str else '')
        + f"\n[ob]   {ba_str} | {ob_wall}"
        f"\n[tf]   1m:{_tc(t1)} 5m:{_tc(t5)} 15m:{_tc(t15)} 1h:{_tc(t1h)} 4h:{_tc(t4h)}"
        + (f"\n[fast] {fast_str}" if fast_str else '')
        + (sig if sig else ''),
        flush=True
    )


# ── Layer 3: git publisher (runs in thread pool) ──────────────────────────────

def _git(*args) -> bool:
    r = subprocess.run(['git'] + list(args), capture_output=True,
                       cwd=str(REPO), timeout=20)
    return r.returncode == 0

def publish(stats: dict):
    """Layer 3 worker — write compact JSON + git push. Blocking; called in executor."""
    # Strip sec_bars from git payload — raw bars not needed on cloud side
    pub_stats = {k: v for k, v in stats.items() if k != 'sec_bars'}
    payload = {'ts': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
               'stats': pub_stats}
    tmp = OUT_FILE.with_suffix('.tmp.json')
    tmp.write_text(json.dumps(payload, separators=(',', ':')))
    tmp.rename(OUT_FILE)
    _git('fetch', 'origin', 'data/live')
    _git('add', 'physics_live_results.json')
    msg = f"bar={stats.get('bars_live',0)} ${stats.get('wf_tgt_primary',0):.0f}"
    if _git('commit', '-m', msg):
        _git('push', 'origin', 'HEAD:data/live')
        c = stats
        _log(f"pushed bar={c.get('bars_live',0)} "
             f"c_ph={c.get('wf_carrier_phase',0):+.2f} "
             f"score={c.get('last_score',0):+.3f}")


# ── Raw tick archiver ─────────────────────────────────────────────────────────

def _gzip_file(path: Path):
    """Compress a plain JSONL file to .jsonl.gz, then delete the original."""
    gz = path.with_suffix('.gz')
    try:
        with open(path, 'rb') as fi, gzip.open(gz, 'wb', compresslevel=6) as fo:
            while True:
                chunk = fi.read(65536)
                if not chunk:
                    break
                fo.write(chunk)
        path.unlink()
        _log(f"archived {path.name} → {gz.name} ({gz.stat().st_size//1024}KB)")
    except Exception as e:
        _log(f"gzip_file error {path}: {e}")


# ── Layer 1: WebSocket capture ────────────────────────────────────────────────

class Capture:

    def __init__(self, publish_enabled: bool = True):
        self.buf, self.idx  = buf_init()
        self.bars: list     = self._load_bars()
        self.sec_bars: list = []   # ring of completed 1s OHLCV bars
        self.bids: list     = []
        self.asks: list     = []
        self.cur_price      = 0.0
        self.last_push      = 0.0
        self.last_process   = 0.0
        self._running       = True
        self._last_msg      = time.time()
        self._publish       = publish_enabled
        self._pool          = ThreadPoolExecutor(max_workers=2,
                                                  thread_name_prefix='pipeline')
        self._processing    = False
        self._pushing       = False
        # 1-second OHLCV aggregator — accumulates trades, seals each second
        self._sec_ts:    int   = 0      # current second bucket (unix second)
        self._sec_o:     float = 0.0
        self._sec_h:     float = 0.0
        self._sec_l:     float = 0.0
        self._sec_c:     float = 0.0
        self._sec_vol:   float = 0.0
        self._sec_n:     int   = 0
        # Sliding 60-slot 1s window — bootstrap with 59 copies of last bar,
        # real seconds fill in one by one until the exchange data falls off.
        self._slide: deque = deque(maxlen=60)
        self._slide_real: int = 0   # count of real 1s slots in window
        self._init_slide()

        # Order book fill/pull tracker
        self._prev_asks: dict = {}   # price_str → qty from last depth snapshot
        self._prev_bids: dict = {}
        self._ob_stats:  dict = {'ask_fill': 0, 'ask_pull': 0,
                                  'bid_fill': 0, 'bid_pull': 0}
        # Raw tick archiver — daily rotating JSONL, buffered writes
        RAW_DIR.mkdir(exist_ok=True)
        self._raw_fh    = None
        self._raw_day:  str  = ''
        self._raw_buf:  list = []
        self._raw_open(time.strftime('%Y%m%d', time.gmtime()))

    def _load_bars(self) -> list:
        try:
            with gzip.open(BARS_FILE, 'rt') as f:
                bars = json.load(f)[-MAX_BARS:]
            if len(bars) >= 50:
                _print(f"Loaded {len(bars)} closed bars from cache")
                return bars
        except Exception:
            pass
        # Cache empty or cold — bootstrap from Binance REST API
        return self._bootstrap_bars()

    def _bootstrap_bars(self) -> list:
        """Fetch last MAX_BARS 1m klines from Binance REST to warm up waves."""
        import urllib.request
        try:
            url = (f"https://api.binance.us/api/v3/klines"
                   f"?symbol=BTCUSDC&interval=1m&limit={MAX_BARS}")
            with urllib.request.urlopen(url, timeout=10) as r:
                raw = json.loads(r.read())
            bars = [{
                'ts':        int(k[0]),
                'open':      float(k[1]),
                'high':      float(k[2]),
                'low':       float(k[3]),
                'close':     float(k[4]),
                'volume':    float(k[5]),
                'taker_buy': float(k[9]),
            } for k in raw[:-1]]   # drop last (open/partial bar)
            _print(f"Bootstrapped {len(bars)} bars from Binance REST")
            self.bars = bars
            self._save_bars()
            return bars
        except Exception as e:
            _print(f"Bootstrap failed ({e}) — starting cold")
            return []

    def _save_bars(self):
        tmp = BARS_FILE.with_suffix('.tmp.gz')
        with gzip.open(tmp, 'wt', compresslevel=6) as f:
            json.dump(self.bars[-MAX_BARS:], f, separators=(',', ':'))
        tmp.rename(BARS_FILE)

    def _save_sec(self):
        tmp = SEC_FILE.with_suffix('.tmp.gz')
        with gzip.open(tmp, 'wt', compresslevel=6) as f:
            json.dump(self.sec_bars[-MAX_SEC_BARS:], f, separators=(',', ':'))
        tmp.rename(SEC_FILE)

    def _init_slide(self):
        """Pre-fill sliding window with 59 synthetic 1s slots from last closed bar.
        Real seconds replace them one by one — after 59s the window is 100% real.
        Mimic the compression phase: data density present from second 1."""
        self._slide.clear()
        self._slide_real = 0
        if not self.bars:
            return
        last = self.bars[-1]
        c = last['close']
        vol_per = last['volume'] / 59.0
        slot = {'o': c, 'h': c, 'l': c, 'c': c, 'vol': vol_per, 'real': False}
        for _ in range(59):
            self._slide.append(slot)

    def _slide_ohlcv(self) -> dict | None:
        """Compute rolling OHLCV from the 60-slot sliding window."""
        w = list(self._slide)
        if not w:
            return None
        return {
            'open':   w[0]['o'],
            'high':   max(s['h'] for s in w),
            'low':    min(s['l'] for s in w),
            'close':  w[-1]['c'],
            'volume': sum(s['vol'] for s in w),
            'real_slots': self._slide_real,   # how many are from real 1s data
        }

    def _raw_open(self, day: str):
        """Open a new daily raw tick file, gzip previous day's file in background."""
        self._raw_flush()
        if self._raw_fh:
            self._raw_fh.close()
            # Gzip the just-closed day's file if it exists
            old = RAW_DIR / f"BTCUSDC_{self._raw_day}.jsonl"
            if old.exists():
                self._pool.submit(_gzip_file, old)
        path = RAW_DIR / f"BTCUSDC_{day}.jsonl"
        self._raw_fh  = open(path, 'a', buffering=8192)
        self._raw_day = day
        _log(f"raw ticks → {path.name}")

    def _raw_flush(self):
        """Flush buffered raw tick rows to disk."""
        if self._raw_buf and self._raw_fh:
            self._raw_fh.writelines(self._raw_buf)
            self._raw_fh.flush()
            self._raw_buf.clear()

    # ── Handlers (called from asyncio — MUST NOT BLOCK) ──────────────────────

    def _seal_sec(self):
        """Finalize the current 1s bucket, append to sec_bars ring + sliding window."""
        if self._sec_n == 0:
            return
        slot = {
            'ts': self._sec_ts,
            'o':  round(self._sec_o, 2), 'h': round(self._sec_h, 2),
            'l':  round(self._sec_l, 2), 'c': round(self._sec_c, 2),
            'vol': round(self._sec_vol, 6), 'n': self._sec_n,
        }
        self.sec_bars.append(slot)
        self.sec_bars = self.sec_bars[-MAX_SEC_BARS:]
        # Feed real 1s data into the sliding window — oldest synthetic slot drops off
        dropped = self._slide[0] if self._slide else None
        self._slide.append({'o': slot['o'], 'h': slot['h'], 'l': slot['l'],
                            'c': slot['c'], 'vol': slot['vol'], 'real': True})
        if dropped is not None and not dropped['real']:
            pass   # a synthetic slot fell off — real_slots count doesn't change
        self._slide_real = sum(1 for s in self._slide if s['real'])
        self._sec_n = 0

    def on_trade(self, data: dict):
        price  = float(data['p'])
        qty    = float(data.get('q', 0.0))
        ts_ms  = int(data.get('T', time.time() * 1000))
        sec    = ts_ms // 1000
        self.cur_price = price
        self.idx = buf_write(self.buf, self.idx, price)

        # Raw tick archiver — [ts_ms, price*100_int, qty*10000_int, side]
        # m=True → maker is buyer → sell aggressor (side=0), m=False → buy (side=1)
        side = 0 if data.get('m', True) else 1
        self._raw_buf.append(
            f'[{ts_ms},{int(price * 100)},{int(qty * 10000)},{side}]\n'
        )
        if len(self._raw_buf) >= RAW_FLUSH:
            self._raw_flush()

        # Day rollover — gzip yesterday's file in background
        day = time.strftime('%Y%m%d', time.gmtime(sec))
        if day != self._raw_day:
            self._raw_open(day)

        # Seal previous second bucket when second rolls over
        if sec != self._sec_ts and self._sec_ts != 0:
            self._seal_sec()
            self._trigger_process()   # new 1s bar = recompute opportunity

        if self._sec_n == 0:
            self._sec_ts = sec; self._sec_o = price; self._sec_h = price
            self._sec_l  = price
        else:
            self._sec_h = max(self._sec_h, price)
            self._sec_l = min(self._sec_l, price)
        self._sec_c   = price
        self._sec_vol += qty
        self._sec_n   += 1

    def on_depth(self, data: dict):
        new_asks = {p: float(q) for p, q in data.get('asks', [])}
        new_bids = {p: float(q) for p, q in data.get('bids', [])}
        px = self.cur_price or 0.0

        # Levels that vanished from the snapshot
        for p_str, qty in self._prev_asks.items():
            if p_str not in new_asks and qty > 0.01:
                if px >= float(p_str):           # price crossed it → takers filled
                    self._ob_stats['ask_fill'] += 1
                else:                            # price never reached → maker pulled
                    self._ob_stats['ask_pull'] += 1
        for p_str, qty in self._prev_bids.items():
            if p_str not in new_bids and qty > 0.01:
                if px <= float(p_str):
                    self._ob_stats['bid_fill'] += 1
                else:
                    self._ob_stats['bid_pull'] += 1

        self._prev_asks = new_asks
        self._prev_bids = new_bids
        self.bids = [[float(p), float(q)] for p, q in data.get('bids', [])]
        self.asks = [[float(p), float(q)] for p, q in data.get('asks', [])]

    def on_kline(self, k: dict):
        self.cur_price = float(k['c'])
        self.idx = buf_write(self.buf, self.idx, self.cur_price)

        if not k.get('x', False):
            return

        bar = {
            'ts': int(k['t']), 'open': float(k['o']), 'high': float(k['h']),
            'low': float(k['l']), 'close': float(k['c']),
            'volume': float(k['v']), 'taker_buy': float(k['V']),
        }
        self.bars.append(bar)
        self.bars = self.bars[-MAX_BARS:]
        self._seal_sec()      # seal any partial 1s bucket at bar boundary
        self._init_slide()    # reset sliding window: 59 copies of closed bar
        self._raw_flush()     # guaranteed flush at each bar close
        self._save_bars()
        self._save_sec()
        n = len(self.bars)
        _print(f"BAR {n}  ${self.cur_price:.2f}  vol={bar['volume']:.3f}")

        # Periodic GC — prevent Termux OOM kill
        if n % GC_EVERY_BARS == 0:
            gc.collect()
        if n % MEM_LOG_BARS == 0:
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            _log(f"mem rss={rss}KB  bars={n}  sec_bars={len(self.sec_bars)}")

        # Bar close → trigger process + publish (bypass debounce)
        self._trigger_process(force_publish=True)

    # ── Layer 2 trigger (non-blocking — hands work to thread pool) ───────────

    def _trigger_process(self, force_publish: bool = False):
        if not _SCIPY:
            return
        now = time.time()
        if not force_publish and now - self.last_process < INTRABAR_S:
            return
        if self._processing:
            return
        self._processing = True
        self.last_process = now

        bars_snap  = list(self.bars)
        sec_snap   = list(self.sec_bars)
        buf_snap   = self.buf
        idx_snap   = self.idx
        slide_ohlcv = self._slide_ohlcv()
        # Rolling close from sliding window beats raw last-tick noise once
        # at least one real second has landed in the window.
        price_snap = (slide_ohlcv['close'] if slide_ohlcv and self._slide_real >= 1
                      else self.cur_price)
        slide_snap  = slide_ohlcv   # passed to display for intrabar OHLCV
        bids_snap  = list(self.bids)
        asks_snap  = list(self.asks)
        ob_snap    = dict(self._ob_stats)
        do_publish = self._publish and (force_publish or
                     (now - self.last_push >= MIN_PUSH_S))
        if force_publish:
            self._ob_stats = {'ask_fill': 0, 'ask_pull': 0,
                              'bid_fill': 0, 'bid_pull': 0}

        loop = asyncio.get_event_loop()

        def _work():
            try:
                stats = process_waves(buf_snap, idx_snap, bars_snap,
                                      price_snap, sec_snap)
                return stats
            except Exception as e:
                _log(f"process error: {e}")
                return None
            finally:
                self._processing = False

        def _done(fut):
            stats = fut.result()
            if stats:
                display_signal(stats, price_snap, bids_snap, asks_snap, ob_snap,
                               slide_snap)
            if stats and do_publish and not self._pushing:
                self._pushing = True
                self.last_push = time.time()
                pub_fut = loop.run_in_executor(self._pool, publish, stats)
                pub_fut.add_done_callback(lambda f: setattr(self, '_pushing', False))

        fut = loop.run_in_executor(self._pool, _work)
        fut.add_done_callback(_done)

    # ── Heartbeat monitor ─────────────────────────────────────────────────────

    async def _heartbeat(self, ws):
        while self._running:
            await asyncio.sleep(3)
            age = time.time() - self._last_msg
            if age > HEARTBEAT_S:
                _print(f"Heartbeat timeout ({age:.0f}s) — reconnecting")
                await ws.close()
                return

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def run(self):
        backoff = 1
        while self._running:
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.ws_connect(WS_URL, receive_timeout=30,
                                               heartbeat=20) as ws:
                        backoff = 1
                        self._last_msg = time.time()
                        _print("Connected")
                        hb = asyncio.create_task(self._heartbeat(ws))
                        async for msg in ws:
                            if not self._running:
                                break
                            self._last_msg = time.time()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    d  = json.loads(msg.data)
                                    st = d.get('stream', '')
                                    da = d.get('data',   {})
                                    if 'aggTrade' in st:
                                        self.on_trade(da)
                                        self._trigger_process()
                                    elif '@kline' in st:
                                        self.on_kline(da.get('k', {}))
                                    elif '@depth' in st:
                                        self.on_depth(da)
                                except Exception as e:
                                    _log(f"parse: {e}")
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                break
                        hb.cancel()
            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self._running:
                    break
                _print(f"Error: {e} — retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

        self._pool.shutdown(wait=False)
        _print("Stopped")

    def stop(self):
        self._running = False


# ── Entry point ───────────────────────────────────────────────────────────────

async def _main(publish_enabled: bool):
    cap  = Capture(publish_enabled=publish_enabled)
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, cap.stop)
    await cap.run()


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--capture', action='store_true',
                    help='Layer 1 only: capture ticks, no git push')
    ap.add_argument('--process', action='store_true',
                    help='Layers 2+3: read existing memmap, push only')
    args = ap.parse_args()

    if args.process:
        # Standalone processor: read memmap, run waves, push, exit
        buf, idx = buf_init()
        bars = json.loads(BARS_FILE.read_text()) if BARS_FILE.exists() else []
        prices = np.array([b['close'] for b in bars], dtype=float)
        waves  = decompose_prices(prices)
        stats  = build_stats(waves, len(bars), float(prices[-1]) if len(prices) else 0)
        publish(stats)
        _print(f"Processed {len(prices)} bars, pushed")
    else:
        pub = not args.capture
        asyncio.run(_main(publish_enabled=pub))
