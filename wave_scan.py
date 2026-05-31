#!/usr/bin/env python3
"""
wave_scan.py — Intrabar wave engine scan.

4-state signal framework (micro phase + KdV direction):
  PEAK-REV    micro at peak,   KdV flips  -1  → SHORT
  PEAK-CONT   micro at peak,   KdV holds  +1  → LONG  (requires MTF alignment)
  TROUGH-REV  micro at trough, KdV flips  +1  → LONG
  TROUGH-CONT micro at trough, KdV holds  -1  → SHORT

ALL thresholds derive from the last 30 completed candles' value distributions.
Nothing is fixed except SUSTAIN_S (a timing floor, not a market threshold).
At each minute: peak/trough thresholds come from the session's own phase
distribution, score gate from the session's own score distribution, OBI gate
from the session's OBI distribution, KdV gates from the session's KdV
balance distribution, alignment gate from the session's alignment distribution.

Usage:
    python wave_scan.py              # last 96 minutes
    python wave_scan.py --all        # entire file
    python wave_scan.py --mins 60    # last N minutes
"""

import argparse, collections, datetime, gzip, json, subprocess, sys, os
import numpy as np
sys.path.insert(0, os.path.dirname(__file__))

from physics import fusion, waveform as WF, resonance as RES, htf
from physics.signals import HydraulicAccumulator

REPO         = os.path.dirname(__file__)
SUSTAIN_S    = 1    # fire on first qualifying second — predict, not follow
SUSTAIN_FLIP = 1    # when KdV flips, 1 confirmed second is enough (the flip IS confirmation)
PEAK_PH      =  0.75  # fallback per-TF threshold (used only when composite unavailable)
TROUGH_PH    = -0.75
# Composite multi-TF phase weights — same as physics/resonance._TF_WEIGHTS.
# composite = Σ(w_tf * phase_tf) ranges -1..+1.
# +1.0 means EVERY timeframe has its wave at the crest simultaneously.
# -1.0 means EVERY timeframe has its wave at the trough simultaneously.
# The adaptive peak_ph/trough_ph thresholds from _thresholds() are stored against
# composite values so they self-calibrate to the composite scale over time.
_COMP_W = {'1m': 0.05, '5m': 0.10, '15m': 0.20, '1h': 0.30, '4h': 0.35}
MIN_SHELF_RANGE = 40.0   # min $ gap between range floor and ceiling to qualify
SHELF_TOUCH_PCT = 0.0008 # within 0.08% of shelf level = "touching it"

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; W='\033[97m'; Z='\033[0m'

_EPS = 0.001   # minimum non-zero floor for all computed scores/imbalances


# ── Data loading ──────────────────────────────────────────────────────────────

_TMP = str(__import__('pathlib').Path.home() / '.trendrider')  # pipeline data dir


def _fetch_raw():
    """Load raw tick data.
    Priority: /tmp/trendrider/ (collector deque snapshot) → git show (remote).
    /tmp/ is tmpfs (RAM) — no flash writes, no Android backup.
    """
    local = os.path.join(_TMP, 'BTCUSDT_LIVE.jsonl.gz')
    if os.path.exists(local):
        with gzip.open(local, 'rt') as f:
            return f.read().strip().split('\n')
    r = subprocess.run(
        ['git','show','origin/data/raw:data/raw/BTCUSDT_LIVE.jsonl.gz'],
        capture_output=True, cwd=REPO)
    if not r.stdout:
        print("ERROR: no data — start collector or ensure origin/data/raw exists")
        sys.exit(1)
    return gzip.decompress(r.stdout).decode().strip().split('\n')


def _load_candles(fname):
    """Load candle file from ~/.trendrider/ (backfill_local target) then git."""
    local = os.path.join(_TMP, fname)
    if os.path.exists(local):
        try:
            with gzip.open(local, 'rb') as f:
                klines = json.loads(f.read().decode())
            return [{'ts': int(k[0]), 'open': float(k[1]), 'high': float(k[2]),
                     'low': float(k[3]), 'close': float(k[4]),
                     'volume': float(k[5]), 'taker_buy': float(k[6])}
                    for k in klines]
        except Exception:
            pass
    r = subprocess.run(
        ['git','show', f'origin/data/raw:data/raw/{fname}'],
        capture_output=True, cwd=REPO)
    if not r.stdout:
        return []
    try:
        klines = json.loads(gzip.decompress(r.stdout).decode())
        return [{'ts': int(k[0]), 'open': float(k[1]), 'high': float(k[2]),
                 'low': float(k[3]), 'close': float(k[4]),
                 'volume': float(k[5]), 'taker_buy': float(k[6])}
                for k in klines]
    except Exception:
        return []


def _fetch_candles_1m():
    """Load backfilled 1m candles, merging live session bars written by scan_live."""
    base = _load_candles('BTCUSDT_1m.json.gz')

    # Live session candles (bar dicts, not klines) — check local file first, then git
    live_bars = []
    live_local = os.path.join(REPO, 'data', 'raw', 'BTCUSDT_1m_live.json.gz')
    if os.path.exists(live_local):
        try:
            with gzip.open(live_local, 'rb') as _lf:
                live_bars = json.loads(_lf.read().decode())
        except Exception:
            pass
    if not live_bars:
        try:
            r = subprocess.run(
                ['git', 'show', 'origin/data/signals:data/raw/BTCUSDT_1m_live.json.gz'],
                capture_output=True, cwd=REPO)
            if r.stdout:
                live_bars = json.loads(gzip.decompress(r.stdout).decode())
        except Exception:
            pass

    if live_bars:
        base_ts = {b['ts'] for b in base}
        for b in live_bars:
            if b['ts'] not in base_ts:
                base.append(b)
                base_ts.add(b['ts'])
        base.sort(key=lambda b: b['ts'])

    return base


def _fetch_candles_1h():
    """Load backfilled 1h candles."""
    return _load_candles('BTCUSDT_1h.json.gz')


def _quick_ts(ln: str) -> int:
    """Extract timestamp from raw JSON line without full json.loads — fast path."""
    try:
        # Lines are: ["T",ts,...] or ["D",ts,...]
        # Find the second comma (after record type) and grab the number
        i = ln.index(',', 2) + 1
        j = ln.index(',', i)
        return int(ln[i:j])
    except Exception:
        return 0


def _extend_s1(s1_by_sec: dict, ob_by_sec: dict, raw_lines: list, seed_close=None):
    """
    Parse new raw lines and extend s1_by_sec / ob_by_sec in place.
    Called on subsequent incremental scan passes — no full rebuild needed.
    seed_close: last known close from candle history — used to anchor OB-only
    seconds when no trade has been seen yet in the live stream.
    """
    trade_by_sec = collections.defaultdict(list)
    for ln in raw_lines:
        if not ln: continue
        try:
            rec = json.loads(ln)
        except Exception:
            continue
        if rec[0] == 'T':
            sec = rec[1] // 1000
            trade_by_sec[sec].append((rec[2]/100, rec[3]/10000, rec[4]))
        elif rec[0] == 'D':
            sec = rec[1] // 1000
            ob_by_sec[sec] = ([(p/100, q/10000) for p,q in rec[2][:20]],
                              [(p/100, q/10000) for p,q in rec[3][:20]])

    last_close = seed_close   # candle close anchors price before first live trade
    last_ob    = None
    if s1_by_sec:
        last_key   = max(s1_by_sec.keys())
        last_close = s1_by_sec[last_key]['close']
        last_ob    = s1_by_sec[last_key]['ob']

    all_new = sorted(set(list(trade_by_sec.keys()) + list(ob_by_sec.keys())))
    for sec in all_new:
        trades = trade_by_sec.get(sec, [])
        if sec in s1_by_sec:
            # Accumulate any new trades that arrived after the bar was first created.
            # This completes partial bars (first D record fires before all T records
            # for that second have been collected).
            existing = s1_by_sec[sec]
            if trades:
                prices = [t[0] for t in trades]
                existing['high']     = max(existing['high'],   max(prices))
                existing['low']      = min(existing['low'],    min(prices))
                existing['close']    = prices[-1]
                existing['volume']  += sum(t[1] for t in trades)
                existing['taker_buy'] += sum(t[1] for t in trades if t[2] == 1)
                last_close = existing['close']
            if sec in ob_by_sec:
                existing['ob'] = ob_by_sec[sec]
                last_ob = ob_by_sec[sec]
            continue
        if trades:
            prices = [t[0] for t in trades]
            vols   = [t[1] for t in trades]
            tb     = sum(t[1] for t in trades if t[2] == 1)
            o,h,l,c = prices[0], max(prices), min(prices), prices[-1]
            vol = sum(vols); last_close = c
        else:
            if last_close is None: continue
            o=h=l=c=last_close; vol=0.0; tb=0.0
        if sec in ob_by_sec:
            last_ob = ob_by_sec[sec]
        s1_by_sec[sec] = {'ts': sec*1000, 'open':o, 'high':h, 'low':l,
                          'close':c, 'volume':vol, 'taker_buy':tb,
                          'ob': last_ob or ([], [])}


def _agg(bars, period_ms):
    """Aggregate 1m (or any) bars into a higher timeframe by period_ms."""
    by_period = collections.defaultdict(list)
    for b in bars:
        by_period[(b['ts'] // period_ms) * period_ms].append(b)
    result = []
    for p in sorted(by_period):
        sl = by_period[p]
        result.append({
            'ts':        p,
            'open':      sl[0]['open'],
            'high':      max(b['high'] for b in sl),
            'low':       min(b['low']  for b in sl),
            'close':     sl[-1]['close'],
            'volume':    sum(b['volume'] for b in sl),
            'taker_buy': sum(b['taker_buy'] for b in sl),
        })
    return result


def _build_tfs(raw_lines):
    trade_by_sec = collections.defaultdict(list)
    ob_by_sec    = {}

    for ln in raw_lines:
        if not ln: continue
        rec = json.loads(ln)
        if rec[0] == 'T':
            _, ts, p_c, q_u, side = rec
            sec = ts // 1000
            trade_by_sec[sec].append((p_c/100, q_u/10000, side))
        elif rec[0] == 'D':
            _, ts, bids, asks = rec
            sec = ts // 1000
            ob_by_sec[sec] = ([(p/100,q/10000) for p,q in bids[:20]],
                              [(p/100,q/10000) for p,q in asks[:20]])

    all_secs = sorted(set(list(trade_by_sec.keys()) + list(ob_by_sec.keys())))
    if not all_secs:
        return [], [], [], [], [], [], {}

    # Seed last_close from candle history so OB-only seconds before the first
    # live trade are included, not skipped.  Without this the session only
    # starts at the first trade, leaving the OB dark until then.
    _hist = _fetch_candles_1m()
    first_live_ms = all_secs[0] * 1000
    _prior = [c for c in _hist if c['ts'] < first_live_ms]
    seed_close = _prior[-1]['close'] if _prior else None

    s1 = []; last_close = seed_close; last_ob = None
    for sec in range(all_secs[0], all_secs[-1]+1):
        trades = trade_by_sec.get(sec, [])
        if trades:
            prices = [t[0] for t in trades]
            vols   = [t[1] for t in trades]
            tb     = sum(t[1] for t in trades if t[2]==1)
            o,h,l,c = prices[0],max(prices),min(prices),prices[-1]
            vol = sum(vols); last_close = c
        else:
            if last_close is None: continue
            o=h=l=c=last_close; vol=0.0; tb=0.0
        if sec in ob_by_sec:
            last_ob = ob_by_sec[sec]
        s1.append({'ts':sec*1000,'open':o,'high':h,'low':l,'close':c,
                   'volume':vol,'taker_buy':tb,
                   'ob':last_ob or ([],[])})

    tf1m  = _agg(s1, 60_000)
    tf5m  = _agg(s1, 300_000)
    tf15m = _agg(s1, 900_000)
    tf1h  = _agg(s1, 3_600_000)
    tf4h  = _agg(s1, 14_400_000)

    # Merge backfilled 1m candles into gaps (raw tick data always wins)
    # Need enough history for 5m ATR (15 bars × 5m = 75m) + 15m ATR (15 × 15m = 225m)
    # Load 400 bars — covers 5m ATR warmup from the start of any 96m session window
    raw_1m_ts = {b['ts'] for b in tf1m}
    candles   = _fetch_candles_1m()[-400:]
    filled    = 0
    for c in candles:
        bucket = (c['ts'] // 60_000) * 60_000
        if bucket not in raw_1m_ts:
            tf1m.append({'ts': bucket, 'open': c['open'], 'high': c['high'],
                         'low': c['low'], 'close': c['close'],
                         'volume': c['volume'], 'taker_buy': c['taker_buy']})
            raw_1m_ts.add(bucket)
            filled += 1

    if filled:
        tf1m.sort(key=lambda b: b['ts'])
        print(f"  +{filled} 1m candle bars merged")
        tf5m  = _agg(tf1m, 300_000)
        tf15m = _agg(tf1m, 900_000)
        tf1h  = _agg(tf1m, 3_600_000)
        tf4h  = _agg(tf1m, 14_400_000)

    return s1, tf1m, tf5m, tf15m, tf1h, tf4h, ob_by_sec


# ── Adaptive thresholds ───────────────────────────────────────────────────────

def _thresholds(hist_scores, hist_phases, hist_obi, hist_kdv_bals, hist_aligns,
                window=30):
    """
    Derive all signal thresholds from the last `window` completed candles.
    Every value comes from the actual session distribution — nothing fixed.

    Returns: (thresh, peak_ph, trough_ph, obi_conf, gate_rev, gate_cont, align_cont)
    """
    def _pct(arr, p, lo=None, hi=None):
        if len(arr) < 3:
            return None
        v = float(np.percentile(arr[-window:], p))
        if lo is not None: v = max(lo, v)
        if hi is not None: v = min(hi, v)
        return v

    sc = hist_scores;   ph = hist_phases
    ob = hist_obi;      kd = hist_kdv_bals;  al = hist_aligns

    # Score gate: 55th percentile of absolute scores (above-median signal strength)
    thresh     = _pct(sc, 55, lo=0.05)      or 0.12

    # OBI confirmation: 55th percentile of absolute OBI seen in session
    obi_conf   = _pct([abs(x) for x in ob], 55, lo=0.08) or 0.20

    # KdV balance gates: session percentiles
    gate_rev   = _pct(kd, 30, lo=0.5)       or 1.5   # lighter for reversals
    gate_cont  = _pct(kd, 60, lo=1.0)       or 4.0   # stricter for continuations

    # MTF alignment gate: 40th percentile of session alignment
    align_cont = _pct(al, 40, lo=0.40, hi=0.95) or 0.65

    # Phase thresholds are structural constants (position on -1..+1 cycle), not data-derived
    peak_ph   = PEAK_PH
    trough_ph = TROUGH_PH

    return thresh, peak_ph, trough_ph, obi_conf, gate_rev, gate_cont, align_cont


# ── OB metric helpers (used by KnifeDecayBuffer) ──────────────────────────────

def _ob_obi5(bids, asks):
    bv = sum(q for _,q in bids[:5]); av = sum(q for _,q in asks[:5])
    if not bv+av: return _EPS
    v = (bv-av)/(bv+av)
    return v if v != 0.0 else _EPS

def _ob_conc(bids, asks, mid, w=None):
    w = (mid * 0.0004) if w is None else w   # 0.04% of price
    bv = sum(q for p,q in bids if abs(p-mid)<=w)
    av = sum(q for p,q in asks if abs(p-mid)<=w)
    if not bv+av: return _EPS
    v = (bv-av)/(bv+av)
    return v if v != 0.0 else _EPS

def _ob_spr(bids, asks):
    return asks[0][0]-bids[0][0] if bids and asks else 99.0

def _ob_gravity(bids, asks, mid):
    """
    Locate the nearest significant bid/ask walls and volume-weighted
    centres of gravity.  All return values are $ distances from mid.

    Wall = first level (scanning outward from mid) whose volume meets or
    exceeds the per-side average.  When no single level dominates (uniform
    thin book), the wall falls back to the CoG so we never falsely report
    a tight floor on a uniformly empty book.

    bid_wall_dist large = air gap: the next sell order travels that far
    before landing on real bids.  This bridges the gap between "OBI looks
    positive" and "there is actually a floor here."
    """
    def _side(levels):
        if not levels:
            return float('inf'), float('inf')
        vols  = [q for _, q in levels]
        total = sum(vols)
        if total == 0:
            return float('inf'), float('inf')
        avg = total / len(vols)
        cog = sum(abs(p - mid) * q for p, q in levels) / total
        wall = float('inf')
        for price, qty in levels:     # sorted closest-to-mid first
            if qty >= avg:
                wall = abs(price - mid)
                break
        if wall == float('inf'):
            wall = cog                # no dominant level → use centre of mass
        return wall, cog

    bid_wd,  bid_cog  = _side(bids)
    ask_wd,  ask_cog  = _side(asks)
    return bid_wd, ask_wd, bid_cog, ask_cog


def _atr_ratio(bars, window=14):
    """ATR + up/down direction ratio for per-TF profitability assessment.
    Returns (atr, up_ratio, up_pnl) in dollars.
    up_pnl = ATR × up_ratio = expected upward component of the range.
    A $80 ATR with 0.25 up_ratio → $20 up_pnl: profitable on that TF.
    """
    if len(bars) < window + 1:
        return _EPS, 0.5, _EPS
    trs, ups, dns = [], [], []
    for i in range(-window, 0):
        b  = bars[i]; pc = bars[i-1]['close']
        h  = b.get('high', b['close']); l = b.get('low', b['close'])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        ups.append(max(0.0, h - pc))
        dns.append(max(0.0, pc - l))
    atr    = sum(trs) / len(trs)
    tot    = sum(ups) + sum(dns)
    up_rat = sum(ups) / tot if tot > 0 else 0.5
    return max(atr, _EPS), up_rat, max(atr * up_rat, _EPS)


def _ob_room_above(asks, price, min_dist):
    """Check if the nearest significant ask wall is >= min_dist above price.
    Significant = first ask level at or above average per-level volume.
    Returns (has_room: bool, wall_dist: float, note: str).
    """
    if not asks:
        return True, float('inf'), 'no-wall'
    vols = [q for _, q in asks]
    avg  = sum(vols) / len(vols)
    wall = next((p for p, q in asks if q >= avg), asks[-1][0])
    dist = wall - price
    return dist >= min_dist, dist, f'wall@{wall:.0f}(+{dist:.0f})'


def _detect_shelves(bars, min_bars=3, range_pct=0.0005):
    """Find consolidation zones — runs where 1m close range stays within range_pct.

    Returns list of shelf dicts sorted by level ascending:
      level     float  — mean close price of the shelf
      bars      int    — minutes spent at this level
      vol       float  — total volume traded while shelving
      buy_ratio float  — taker_buy / total vol (>0.5 = mostly bought, longs sitting here)
      broken    str    — 'up'|'down'|'open' (open = price still in zone)
      ts_start  int    — epoch ms of first bar in shelf
      ts_end    int    — epoch ms of last bar in shelf
    """
    if len(bars) < min_bars:
        return []
    shelves = []
    i = 0
    while i < len(bars):
        j = i
        base = bars[i]['close']
        while j + 1 < len(bars):
            nxt_closes = [bars[k]['close'] for k in range(i, j+2)]
            rng = (max(nxt_closes) - min(nxt_closes)) / base if base else 1.0
            if rng > range_pct:
                break
            j += 1
        span = j - i + 1
        if span >= min_bars:
            sl = bars[i:j+1]
            closes = [b['close'] for b in sl]
            level = sum(closes) / len(closes)
            vol   = sum(b['volume'] for b in sl)
            tb    = sum(b.get('taker_buy', b['volume'] * 0.5) for b in sl)
            buy_r = tb / vol if vol > 0 else 0.5
            # Determine break direction from the bar after the shelf (if available)
            if j + 1 < len(bars):
                after = bars[j+1]['close']
                broken = 'up' if after > level * (1 + range_pct) else \
                         'down' if after < level * (1 - range_pct) else 'open'
            else:
                broken = 'open'
            shelves.append({
                'level':     level,
                'bars':      span,
                'vol':       vol,
                'buy_ratio': buy_r,
                'broken':    broken,
                'ts_start':  sl[0]['ts'],
                'ts_end':    sl[-1]['ts'],
            })
        i = j + 1
    return sorted(shelves, key=lambda s: s['level'])


def _shelf_context(shelves, price):
    """Return nearest shelf above and below current price as annotation strings."""
    below = [s for s in shelves if s['level'] < price]
    above = [s for s in shelves if s['level'] > price]
    parts = []
    if below:
        s = below[-1]          # closest below
        dist = price - s['level']
        bias = 'L' if s['buy_ratio'] >= 0.55 else ('S' if s['buy_ratio'] <= 0.45 else '~')
        # L = longs trapped below (potential sellers on return), S = shorts
        parts.append(f"sup↓${s['level']:,.0f}({s['bars']}bar,{bias},{dist:.0f}Δ)")
    if above:
        s = above[0]           # closest above
        dist = s['level'] - price
        bias = 'L' if s['buy_ratio'] >= 0.55 else ('S' if s['buy_ratio'] <= 0.45 else '~')
        parts.append(f"res↑${s['level']:,.0f}({s['bars']}bar,{bias},{dist:.0f}Δ)")
    return '  '.join(parts)


def _range_signal(shelves, price, last_range_state):
    """Detect ranging oscillation signals.

    Ranging = price is bracketed by an open support shelf below AND an open
    resistance shelf above, with a gap >= MIN_SHELF_RANGE dollars.

    Returns (direction, shelf_sup, shelf_res, range_dollar) or (0, None, None, 0).
    direction: +1 = at support → LONG, -1 = at resistance → SHORT, 0 = no signal.
    last_range_state: 'sup'|'res'|None — prevents re-firing same side until
    price crosses to the other level.
    """
    open_below = [s for s in shelves if s['level'] < price and s['broken'] == 'open']
    open_above = [s for s in shelves if s['level'] > price and s['broken'] == 'open']
    if not open_below or not open_above:
        return 0, None, None, 0

    sup = open_below[-1]   # nearest below
    res = open_above[0]    # nearest above
    span = res['level'] - sup['level']
    if span < MIN_SHELF_RANGE:
        return 0, None, None, 0

    # Touch: price within SHELF_TOUCH_PCT of the shelf level, AND closer to that
    # shelf than the other.  Mutual exclusion prevents simultaneous touch of both
    # sides when the range is narrow relative to the touch threshold.
    dist_sup = abs(price - sup['level'])
    dist_res = abs(price - res['level'])
    at_sup = dist_sup / price <= SHELF_TOUCH_PCT and dist_sup < dist_res
    at_res = dist_res / price <= SHELF_TOUCH_PCT and dist_res < dist_sup

    if at_sup and last_range_state != 'sup':
        return +1, sup, res, span
    if at_res and last_range_state != 'res':
        return -1, sup, res, span
    return 0, None, None, 0


def _wall_absorption(bids, asks, price, closed_1m, b5, b15, is_long, n=14):
    """
    Compare the nearest significant OB wall to average taker-directional
    completed volume at each timeframe.

    A wall is not a true barrier if the market's typical throughput at a
    higher timeframe exceeds it — the wall gets eaten in one bar.
    Only completed (taker) orders count; pulled/cancelled orders are ignored.

    is_long=True  → look at ask wall above price, compare to avg taker_buy
    is_long=False → look at bid wall below price, compare to avg taker_sell

    Returns (tf_clears, wall_vol, wall_price, note)
      tf_clears: '15m'/'5m'/'1m'/None — highest TF whose avg flow >= wall_vol
    """
    # Locate nearest significant wall
    if is_long:
        levels = asks
    else:
        levels = list(reversed(bids)) if bids else []  # closest bid below price

    wall_price = wall_vol = None
    if levels:
        vols = [q for _, q in levels]
        avg  = sum(vols) / len(vols) if vols else 0.0
        for p, q in levels:
            if q >= avg:
                wall_price, wall_vol = p, q
                break
        if wall_price is None:
            wall_price, wall_vol = levels[-1][0], levels[-1][1]

    if not wall_vol:
        return 'no-wall', 0.0, price, 'no-wall'

    # Average taker-directional completed volume per bar at each TF
    def _avg(bars):
        if not bars: return 0.0
        recent = bars[-n:]
        if is_long:
            vals = [b.get('taker_buy', b['volume'] * 0.5) for b in recent]
        else:
            vals = [b['volume'] - b.get('taker_buy', b['volume'] * 0.5) for b in recent]
        return sum(vals) / len(vals) if vals else 0.0

    avg_1m  = _avg(closed_1m)
    avg_5m  = _avg(b5)
    avg_15m = _avg(b15)

    dist = abs(wall_price - price)
    sign = '+' if is_long else '-'
    note = (f'w={wall_vol:.3f}@{wall_price:.0f}({sign}{dist:.0f})'
            f'|1m={avg_1m:.3f},5m={avg_5m:.3f},15m={avg_15m:.3f}')

    # Highest TF whose typical flow can absorb the wall in one bar
    tf_clears = None
    if   avg_15m >= wall_vol: tf_clears = '15m'
    elif avg_5m  >= wall_vol: tf_clears = '5m'
    elif avg_1m  >= wall_vol: tf_clears = '1m'

    return tf_clears, wall_vol, wall_price, note


# ── KnifeDecayBuffer ──────────────────────────────────────────────────────────

class KnifeDecayBuffer:
    """
    Falling-knife momentum-decay detector.

    Detects cascade exhaustion by tracking successive swing lows:
      - Each new low reached with LESS downward velocity  → knife losing energy
      - Each bounce between lows reaches HIGHER             → buyers pushing harder
      - OBI stays MORE positive at each successive low      → book absorbing
      - Spread TIGHTENS across the pattern                  → MMs stepping in

    Phase transitions are price-level driven (not velocity thresholds):
      FLAT    → FALLING  when price drops MIN_DROP  from rolling peak
      FALLING → BOUNCE   when price rises MIN_BOUNCE from swing trough
      BOUNCE  → FALLING  when price drops MIN_DROP  from bounce peak

    OB floor-lock layer (runs continuously, independent of phase):
      CONC > 0 after being negative   →  bids just reloaded near price
      SPR < 1.0 sustained             →  spread locked (anchor forming)
      Combined: FLOOR_LOCK = entry signal

    States:
      NEUTRAL (0) — no pattern
      KNIFE   (1) — falling, watching for lows
      DEC?    (2) — 2 lows, velocity decaying
      DECAY   (3) — 3+ lows, confirmed decay + bounce expansion
      FLR?    (4) — DECAY + OB beginning to confirm
      FLOOR   (5) — all signals aligned: high-confidence TROUGH-REV
    """
    NEUTRAL=0; KNIFE=1; DWATCH=2; DCONF=3; FWATCH=4; FLOCK=5
    LABELS  = {0:'NEUT', 1:'KNIFE', 2:'DEC?', 3:'DECAY', 4:'FLR?', 5:'FLOOR'}
    _SCHAR  = [' ',' ','D','D','F','F']
    _CCHAR  = ['-','-','?','!','?','!']

    # Drop/bounce thresholds are derived dynamically from rolling price std
    # and spread — see _vol_thresh().  These multipliers are the only tunables.
    DROP_STD_K   = 1.5   # min_drop   = max(std * 1.5,  spread * 2)
    BOUNCE_STD_K = 0.75  # min_bounce = max(std * 0.75, spread * 1)
    VEL_WIN_MS  = 5000   # ms rolling window for velocity at swing lows
    MAX_LOWS    = 5      # keep this many recent swing lows
    TIMEOUT_S   = 1200   # seconds quiet → reset pattern
    _FLOW_WIN_MS = 30_000  # ms rolling window for bid/ask net flow

    def __init__(self):
        self._reset()

    # ── internal ────────────────────────────────────────────────────────────

    def _reset(self):
        self.state          = self.NEUTRAL
        self.phase          = 'FLAT'    # FLAT | FALLING | BOUNCE
        self.peak           = None      # rolling price high
        self.trough         = None      # current swing-low candidate
        self.bounce_pk      = None      # rolling high during current bounce
        self._low_snap      = None      # (ts_ms, px, vel, obi, conc, spr) at trough
        self.lows           = []        # confirmed swing lows
        self.px_buf         = []        # [(ts_ms, price)] rolling, for velocity
        self.conc_pos       = False     # CONC currently positive
        self.conc_flip      = False     # CONC just flipped positive this snapshot
        self.spr_cnt        = 0         # consecutive ticks near min spread
        self._spr_min       = None      # rolling minimum spread seen
        self.last_ts        = None
        self._prev_bids_map = None      # {price: qty} from previous D snapshot
        self._prev_asks_map = None
        self._flow_buf      = []        # [(ts_ms, bid_net_delta, ask_net_delta)]

    def _vol_thresh(self, mid, spr):
        """
        Derive min_drop and min_bounce from actual market conditions.
        Uses rolling 60s price std (noise level) and current spread (tick size).
        Both scale automatically: tight quiet book → tiny thresholds;
        volatile trending market → larger thresholds.
        """
        if len(self.px_buf) >= 5:
            prices = np.array([p for _, p in self.px_buf[-60:]])
            std = float(prices.std())
        else:
            std = 0.0
        spr_eff    = max(spr, mid * 0.00001) if mid > 0 else 0.01
        min_drop   = max(std * self.DROP_STD_K,   spr_eff * 2.0)
        min_bounce = max(std * self.BOUNCE_STD_K, spr_eff * 1.0)
        return min_drop, min_bounce

    def _vel(self):
        """$/s over VEL_WIN_MS rolling window. Negative = falling."""
        if len(self.px_buf) < 2: return 0.0
        t1  = self.px_buf[-1][0]
        win = [(t,p) for t,p in self.px_buf if t >= t1 - self.VEL_WIN_MS]
        if len(win) < 2: return 0.0
        dt  = (win[-1][0] - win[0][0]) / 1000.0
        return (win[-1][1] - win[0][1]) / dt if dt > 0.1 else 0.0

    def _update_flows(self, ts_ms, bids, asks):
        """Track bid/ask net delta between consecutive OB snapshots."""
        cur_b = {p: q for p, q in bids}
        cur_a = {p: q for p, q in asks}
        if self._prev_bids_map is not None:
            bid_delta = sum(cur_b.get(p,0) - self._prev_bids_map.get(p,0)
                            for p in set(cur_b) | set(self._prev_bids_map))
            ask_delta = sum(cur_a.get(p,0) - self._prev_asks_map.get(p,0)
                            for p in set(cur_a) | set(self._prev_asks_map))
            self._flow_buf.append((ts_ms, bid_delta, ask_delta))
            cutoff = ts_ms - self._FLOW_WIN_MS
            self._flow_buf = [(t,b,a) for t,b,a in self._flow_buf if t >= cutoff]
        self._prev_bids_map = cur_b
        self._prev_asks_map = cur_a

    def _flow_rates(self):
        """(bid_net_BTC/s, ask_net_BTC/s) over _FLOW_WIN_MS rolling window.
        Positive bid = buyers accumulating. Negative ask = sellers pulling."""
        if len(self._flow_buf) < 3: return _EPS, _EPS
        span = (self._flow_buf[-1][0] - self._flow_buf[0][0]) / 1000.0
        if span < 2.0: return _EPS, _EPS
        return (sum(b for _,b,_ in self._flow_buf) / span,
                sum(a for _,_,a in self._flow_buf) / span)

    def _register_low(self):
        """Commit current trough candidate as a confirmed swing low."""
        if self._low_snap is None: return
        ts_ms, px, vel, obi, conc, spr = self._low_snap
        bid_r, ask_r = self._flow_rates()
        self.lows.append(dict(ts=ts_ms, px=px, vel=vel,
                              obi=obi, conc=conc, spr=spr, bounce_hi=None,
                              bid_flow=bid_r, ask_flow=ask_r))
        if len(self.lows) > self.MAX_LOWS: self.lows.pop(0)
        self._low_snap = None

    def _set_bounce_hi(self, price):
        """Track bounce high in the most recent swing low."""
        if self.lows:
            prev = self.lows[-1]['bounce_hi']
            if prev is None or price > prev:
                self.lows[-1]['bounce_hi'] = price

    def _decay_score(self):
        """0→1: strength of momentum-decay pattern across confirmed lows."""
        ls = self.lows
        if len(ls) < 2: return _EPS

        v = [abs(l['vel']) for l in ls]
        vel = sum(v[i] < v[i-1] for i in range(1, len(v))) / max(1, len(v)-1)

        b = [l['bounce_hi'] - l['px']
             for l in ls[:-1] if l.get('bounce_hi') is not None]
        bou = (sum(b[i] > b[i-1] for i in range(1, len(b)))
               / max(1, len(b)-1)) if len(b) >= 2 else 0.5

        o = [l['obi'] for l in ls]
        obi = sum(o[i] > o[i-1] for i in range(1, len(o))) / max(1, len(o)-1)

        s = [l['spr'] for l in ls]
        sps = sum(s[i] < s[i-1] for i in range(1, len(s))) / max(1, len(s)-1)

        return vel*0.40 + bou*0.25 + obi*0.25 + sps*0.10

    def _flow_walking_score(self):
        """0→1: sellers pulling + buyers accumulating on re-test of prior low."""
        bid_r, ask_r = self._flow_rates()
        sc = 0.0
        # Bid net positive: buyers accumulating (not just bouncing off the level)
        if bid_r > 0.00020: sc += 0.10
        if bid_r > 0.00050: sc += 0.05
        # Ask net negative: sellers cancelling faster than they're adding
        if ask_r < -0.00020: sc += 0.10
        if ask_r < -0.00050: sc += 0.05
        # Both together: coordinated rotation — the primary signal
        if bid_r > 0.00010 and ask_r < -0.00010: sc += 0.10
        # Compare to flows at last confirmed swing low (re-test improvement)
        if len(self.lows) >= 1:
            ref = self.lows[-1]
            if bid_r > ref.get('bid_flow', 0): sc += 0.05   # buyers more aggressive
            if ask_r < ref.get('ask_flow', 0): sc += 0.05   # sellers more withdrawn
        return min(1.0, sc)

    def _floor_score(self, conc, spr, mid=None, bids=None, asks=None):
        """0→1: OB confirmation that a floor is forming at current price."""
        vel = self._vel()
        sc = 0.0
        if conc > 0.05:         sc += 0.15
        if conc > 0.30:         sc += 0.10
        if self.conc_flip and vel > -1.0: sc += 0.20
        # Spread tightness relative to rolling minimum (not a fixed dollar gate)
        spr_min = self._spr_min if self._spr_min is not None else spr
        if spr < spr_min * 1.10: sc += 0.10   # near tightest seen
        if spr < spr_min * 1.03: sc += 0.10   # essentially at minimum
        if self.spr_cnt >= 3:   sc += 0.10
        if self.spr_cnt >= 8:   sc += 0.05
        sc += self._flow_walking_score() * 0.30
        # Bid gravity: WHERE is the nearest real bid wall?
        # Positive OBI means nothing if the wall is $40 below mid — any sell
        # order just teleports through the air gap to where bids actually are.
        if bids and asks and mid:
            spr_eff = max(spr, mid * 0.00001) if mid > 0 else 0.01
            bid_wd, ask_wd, bid_cog, _ = _ob_gravity(bids, asks, mid)
            if   bid_wd < spr_eff * 5:   sc += 0.20   # wall right here
            elif bid_wd < spr_eff * 15:  sc += 0.10   # wall nearby
            elif bid_wd > spr_eff * 30:  sc -= 0.15   # air gap — penalise
            # Bulk of bid liquidity close to mid
            if   bid_cog < spr_eff * 10: sc += 0.08
            elif bid_cog < spr_eff * 25: sc += 0.04
            # Sellers retreated (ask wall far) → floor has room to hold
            if ask_wd > spr_eff * 15:    sc += 0.05
        return min(1.0, max(_EPS, sc))

    # ── public ──────────────────────────────────────────────────────────────

    def update(self, ts_ms, price, bids, asks):
        """
        Process one sub-second OB snapshot.
        Returns (state_int, decay_score, floor_score, label_str).
        """
        mid = (bids[0][0]+asks[0][0])/2.0 if bids and asks else price
        obi = _ob_obi5(bids, asks)
        cnc = _ob_conc(bids, asks, mid)
        spr = _ob_spr(bids, asks)

        # Timeout reset (before updating state)
        if self.last_ts and (ts_ms - self.last_ts) > self.TIMEOUT_S * 1000:
            self._reset()
        self.last_ts = ts_ms

        # Rolling price buffer (for velocity)
        self.px_buf.append((ts_ms, price))
        if len(self.px_buf) > 600: self.px_buf = self.px_buf[-300:]
        vel = self._vel()

        # Bid/ask net flow deltas (from consecutive OB snapshots)
        self._update_flows(ts_ms, bids, asks)

        # SPR tracking: tight = below rolling min spread (book is locking up)
        self._spr_min = spr if self._spr_min is None else min(self._spr_min, spr)
        spr_tight = self._spr_min * 1.2   # within 20% of tightest seen
        if spr < spr_tight: self.spr_cnt += 1
        else:               self.spr_cnt = max(0, self.spr_cnt - 2)

        # CONC flip detection (negative → positive)
        was_pos        = self.conc_pos
        self.conc_pos  = cnc > 0
        self.conc_flip = (not was_pos) and self.conc_pos

        # ── Phase / swing-low machine ────────────────────────────────────
        min_drop, min_bounce = self._vol_thresh(mid, spr)

        if self.phase == 'FLAT':
            if self.peak is None or price > self.peak:
                self.peak = price
            if self.peak - price >= min_drop:
                self.phase     = 'FALLING'
                self.trough    = price
                self._low_snap = (ts_ms, price, vel, obi, cnc, spr)

        elif self.phase == 'FALLING':
            if price < self.trough:
                self.trough    = price
                self._low_snap = (ts_ms, price, vel, obi, cnc, spr)
            if price - self.trough >= min_bounce:
                # Bounce confirmed: register the trough as a swing low
                self._register_low()
                self.phase     = 'BOUNCE'
                self.bounce_pk = price
                self._set_bounce_hi(price)

        elif self.phase == 'BOUNCE':
            if self.bounce_pk is None or price > self.bounce_pk:
                self.bounce_pk = price
            self._set_bounce_hi(price)
            # New down leg: drop min_drop from bounce peak
            if self.bounce_pk - price >= min_drop:
                self.phase     = 'FALLING'
                self.peak      = self.bounce_pk   # reset reference high
                self.trough    = price
                self._low_snap = (ts_ms, price, vel, obi, cnc, spr)
                self.bounce_pk = None

        # ── State machine ────────────────────────────────────────────────

        ds  = self._decay_score()
        fos = self._floor_score(cnc, spr, mid=mid, bids=bids, asks=asks)
        n   = len(self.lows)

        if self.phase == 'FLAT' and n == 0:
            new_st = self.NEUTRAL
        elif n == 0:
            new_st = self.KNIFE
        elif n == 1:
            new_st = self.KNIFE
        elif n >= 3 and ds >= 0.55:
            new_st = self.DCONF
        elif n >= 2 and ds >= 0.35:
            new_st = self.DWATCH
        else:
            new_st = self.KNIFE

        # Upgrade to floor states when OB confirms
        if new_st >= self.DWATCH:
            if   fos >= 0.65: new_st = self.FLOCK
            elif fos >= 0.30: new_st = self.FWATCH

        self.state = new_st
        return self.state, ds, fos, self.LABELS[self.state]

    @classmethod
    def compact(cls, state, ds, fos):
        """4-char display string: state-char + conf-char + decay-digit + floor-digit."""
        if state == cls.NEUTRAL: return 'N---'
        sc = cls._SCHAR[state]; cc = cls._CCHAR[state]
        return f"{sc}{cc}{min(9,int(ds*10))}{min(9,int(fos*10))}"


def _build_decay_states(raw_lines, t_start_s, t_end_s):
    """
    Pre-pass: run KnifeDecayBuffer over the full raw OB stream.
    Returns {unix_sec: (state_int, decay_score, floor_score, label)} for every
    second in [t_start_s, t_end_s].  Pre-buffers 120s before t_start_s for warmup.
    """
    buf      = KnifeDecayBuffer()
    states   = {}
    last_px  = None
    pre      = t_start_s - 120

    recs = []
    for ln in raw_lines:
        if not ln: continue
        try: r = json.loads(ln)
        except Exception: continue
        if r[0] not in ('T', 'D'): continue
        if r[1] // 1000 < pre or r[1] // 1000 > t_end_s: continue
        recs.append(r)
    recs.sort(key=lambda r: r[1])

    for r in recs:
        ts_ms = r[1]
        if r[0] == 'T':
            last_px = r[2] / 100
        elif r[0] == 'D':
            bids = [(p/100, q/10000) for p,q in r[2][:20]]
            asks = [(p/100, q/10000) for p,q in r[3][:20]]
            mid  = (bids[0][0]+asks[0][0])/2.0 if bids and asks else last_px
            if mid is None: continue
            px   = last_px if last_px is not None else mid
            st, ds, fos, lbl = buf.update(ts_ms, px, bids, asks)
            sec = ts_ms // 1000
            if t_start_s <= sec <= t_end_s:
                states[sec] = (st, ds, fos, lbl)

    return states


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ph(v):
    if v >= 0.75:  return 'PK'
    if v >= 0.35:  return '+½'
    if v <= -0.75: return 'TR'
    if v <= -0.35: return '-½'
    return ' 0'

def _ds(d): return {1:'▲',-1:'▼',0:'─'}.get(d,'─')
def _ts(s): return datetime.datetime.utcfromtimestamp(s).strftime('%H:%M:%S')
def _tm(s): return datetime.datetime.utcfromtimestamp(s).strftime('%H:%M')

def _bars2arr(bars):
    c = np.array([b['close']                             for b in bars], dtype=float)
    o = np.array([b['open']                              for b in bars], dtype=float)
    v = np.array([b['volume']                            for b in bars], dtype=float)
    t = np.array([b.get('taker_buy', b['volume']*0.5)   for b in bars], dtype=float)
    return c, o, v, t

def _micro_state(phase, peak_ph, trough_ph):
    if phase >= peak_ph:   return 'PEAK'
    if phase <= trough_ph: return 'TROUGH'
    return 'MID'

def _sig_type(state, direction):
    if state == 'PEAK':
        return 'PEAK-REV' if direction < 0 else 'PEAK-CONT'
    if state == 'TROUGH':
        return 'TROUGH-REV' if direction > 0 else 'TROUGH-CONT'
    return 'MID'

# Per-TF gate parameters.
# Higher TFs require deeper phase extremes and larger amplitude-to-price waves
# because each bar represents more accumulated market force.
# min_amp_pct: wave amplitude_sum must be ≥ this fraction of current price.
#   5m  → ≥0.10% of price (≈$73 on $73k BTC) — real 5m swing, not 1m noise
#   15m → ≥0.20%          (≈$147) — 15m move
#   1h  → ≥0.40%          (≈$293) — hourly structure
#   4h  → ≥0.80%          (≈$587) — daily block
_TF_GATES = {
    # phase threshold: same 0.75 base as 1m, steps up only for higher TFs.
    #   WF normalizes phase to -1..+1 regardless of TF, so the detection
    #   threshold is comparable — the RANGE gate is what makes TFs vastly different.
    # min_amp_pct: price H-L range in the window as % of price, scales with TF.
    #   5m  → 0.10% (~$73)  — must show real 5m swing, not 1m micro-jitter
    #   15m → 0.20% (~$147) — 15m structural move
    #   1h  → 0.40% (~$293) — hourly block
    #   4h  → 0.80% (~$587) — daily structure
    '5m':  {'min_bars': 10, 'peak_ph': 0.75, 'trough_ph': -0.75, 'min_amp_pct': 0.0010},
    '15m': {'min_bars':  7, 'peak_ph': 0.75, 'trough_ph': -0.75, 'min_amp_pct': 0.0020},
    '1h':  {'min_bars':  5, 'peak_ph': 0.78, 'trough_ph': -0.78, 'min_amp_pct': 0.0040},
    '4h':  {'min_bars':  3, 'peak_ph': 0.82, 'trough_ph': -0.82, 'min_amp_pct': 0.0080},
}


def _wf_phase(bars, n=30):
    """Return WF micro-component phase for bars[-n:].  0.0 on any failure."""
    if len(bars) < 10:
        return 0.0
    try:
        closes = np.array([b['close'] for b in bars[-n:]], dtype=float)
        return WF.run(closes, entry=float(closes[-1]),
                      direction=1)['components'].get('micro', {}).get('phase', 0.0)
    except Exception:
        return 0.0


def _observe_structural(price, obi, kdv_up, kdv_down, micro_bars, res_dir=0,
                        bars_5m=None, bars_1m=None):
    """
    Window = the timeframe = 60 1s-bars (1m).
    Prior context from the last closed 1m candle (bars_1m[-1]).

    PEAK/TROUGH = ORDER BOOK REVERSAL at the local 1m high/low.
    OB-based signals: wall proximity, spread compression/explosion, side loading.
    KdV + OBI are secondary (both exist on 100% of bars — no trades required).

    CONT_UP/DOWN = genuine new extreme (>= prior candle) + no reversal.
    RANGE_HI/LO  = lower-high / higher-low (< prior candle) + no reversal.

    Returns: 'PEAK' | 'TROUGH' | 'CONT_UP' | 'CONT_DOWN' | 'RANGE_HI' | 'RANGE_LO' | 'MID'
    """
    m = len(micro_bars)
    if m < 15:
        return 'MID'

    closes = [b['close'] for b in micro_bars]

    n1 = min(60, m)  # 1m window = THE timeframe

    hi_s     = max(closes[-n1:])
    lo_s     = min(closes[-n1:])
    range_1m = hi_s - lo_s

    if range_1m < price * 0.0003:
        return 'MID'

    # ── Prior 1m candle reference ─────────────────────────────────────────────
    if bars_1m and len(bars_1m) >= 1:
        prior    = bars_1m[-1]
        prior_hi = prior['high']
        prior_lo = prior['low']
        v_pr     = (prior['close'] - prior['open']) / 60.0
    else:
        n_half   = n1 // 2
        v_pr     = (closes[-n_half] - closes[-n1]) / max(n1 - n_half, 1) if n1 > n_half else 0.0
        prior_hi = hi_s
        prior_lo = lo_s

    # ── Structural position ───────────────────────────────────────────────────
    at_hi  = price >= hi_s
    at_lo  = price <= lo_s
    new_hi = hi_s >= prior_hi
    new_lo = lo_s <= prior_lo

    # ── OBI thresholds ───────────────────────────────────────────────────────
    obi_pos     = obi >  0.05;  obi_neg     = obi < -0.05
    obi_str_pos = obi >  0.20;  obi_str_neg = obi < -0.20

    # ── Velocity (pure price — no trades needed) ─────────────────────────────
    v_now       = (closes[-1] - closes[-n1]) / (n1 - 1) if n1 > 1 else 0.0
    vel_cont_up = v_now > 0.02 and v_pr > 0.02
    vel_cont_dn = v_now < -0.02 and v_pr < -0.02
    vel_up      = v_now >  0.10
    vel_dn      = v_now < -0.10

    # ── Order book signals (available on 100% of bars) ───────────────────────
    def _ob_stats(bar):
        bids, asks = bar.get('ob', ([], []))
        if not bids or not asks:
            return None
        best_bid = bids[0][0];  best_ask = asks[0][0]
        spread   = best_ask - best_bid
        bid_tot  = sum(q for _, q in bids)
        ask_tot  = sum(q for _, q in asks)
        big_bid  = max(bids, key=lambda x: x[1])
        big_ask  = max(asks, key=lambda x: x[1])
        return {
            'spread':      spread,
            'bid_tot':     bid_tot,     'ask_tot':     ask_tot,
            'big_bid_qty': big_bid[1],  'big_bid_px':  big_bid[0],
            'big_ask_qty': big_ask[1],  'big_ask_px':  big_ask[0],
        }

    cur  = _ob_stats(micro_bars[-1])
    prev = _ob_stats(micro_bars[-min(10, m)])
    if cur is None:
        return 'MID'

    # Wall = large resting order within ~$15 of price at $75k
    WALL_QTY  = 0.15
    WALL_DIST = price * 0.0002

    ask_wall_at_price = (cur['big_ask_qty'] >= WALL_QTY and
                         0 <= cur['big_ask_px'] - price <= WALL_DIST)
    bid_wall_at_price = (cur['big_bid_qty'] >= WALL_QTY and
                         0 <= price - cur['big_bid_px'] <= WALL_DIST)

    # Spread compression / explosion vs recent baseline
    recent_spreads = []
    for b in micro_bars[-n1:]:
        st = _ob_stats(b)
        if st is not None:
            recent_spreads.append(st['spread'])
    if len(recent_spreads) > 5:
        avg_spread = sum(recent_spreads[:-5]) / len(recent_spreads[:-5])
    elif recent_spreads:
        avg_spread = recent_spreads[0]
    else:
        avg_spread = 10.0

    spread_compressed = cur['spread'] < avg_spread * 0.15 and cur['spread'] < 1.0
    spread_exploding  = cur['spread'] > avg_spread * 2.0  and cur['spread'] > 5.0

    # Side loading: total qty jumped or thinned vs 10 bars ago
    asks_loaded  = prev is not None and cur['ask_tot'] > prev['ask_tot'] * 1.25
    bids_loaded  = prev is not None and cur['bid_tot'] > prev['bid_tot'] * 1.25
    asks_thinned = prev is not None and cur['ask_tot'] < prev['ask_tot'] * 0.75
    bids_thinned = prev is not None and cur['bid_tot'] < prev['bid_tot'] * 0.75

    if at_hi:
        # ── PEAK: OB reversal signals at local high ───────────────────────────
        # spread_compressed without OBI fires everywhere in thin books — require
        # matching OBI direction. asks_loaded requires obi_neg confirmation.
        reversal = (ask_wall_at_price or
                    (asks_loaded and obi_neg) or
                    (spread_compressed and obi_neg) or
                    spread_exploding or
                    (kdv_down and not obi_str_pos))
        if reversal:
            return 'PEAK'

        # No reversal — classify by whether this high extends beyond prior candle
        if new_hi:
            # ── CONT_UP: genuine new high + trend continues ────────────────
            if not kdv_down:
                if obi_pos and (bids_loaded or asks_thinned):   return 'CONT_UP'
                if vel_cont_up and obi_pos:                      return 'CONT_UP'
                if vel_up and not ask_wall_at_price:             return 'CONT_UP'
        else:
            # ── RANGE_HI: lower-high + no reversal ────────────────────────
            if obi_neg:  return 'RANGE_HI'

    if at_lo:
        # ── TROUGH: OB reversal signals at local low ──────────────────────────
        # spread_compressed without OBI fires everywhere — require matching OBI.
        # bids_loaded requires obi_pos (knife-catching bids alone don't reverse).
        reversal = (bid_wall_at_price or
                    (bids_loaded and obi_pos) or
                    (spread_compressed and obi_pos) or
                    spread_exploding or
                    (kdv_up and not obi_str_neg))
        if reversal:
            return 'TROUGH'

        # No reversal — classify by whether this low extends beyond prior candle
        if new_lo:
            # ── CONT_DOWN: genuine new low + trend continues ───────────────
            # OBI stays positive in thin-book downtrends (limit buyers stack bids
            # while market sellers hit them) — use velocity as the primary signal.
            if not kdv_up:
                if obi_neg and (asks_loaded or bids_thinned):   return 'CONT_DOWN'
                if vel_cont_dn and obi_neg:                      return 'CONT_DOWN'
                if vel_dn and not bid_wall_at_price:             return 'CONT_DOWN'
        else:
            # ── RANGE_LO: higher-low + no reversal ────────────────────────
            if obi_pos:  return 'RANGE_LO'

    return 'MID'


def _tf_phase_state(bars, tf_label):
    """Run WF on TF bar array using TF-specific gates.
    Returns (state, phase, range_pct, wf_dir, fail_reason).
    range_pct = (high - low) / price over the window — actual price swing as %.
    state='MID' means not at phase extreme or window price range too small for TF.
    """
    g = _TF_GATES[tf_label]
    if len(bars) < g['min_bars']:
        return 'MID', 0.0, 0.0, 0, f'bars={len(bars)}<{g["min_bars"]}'
    window  = bars[-30:]
    closes  = np.array([b['close'] for b in window], dtype=float)
    highs   = np.array([b.get('high', b['close']) for b in window], dtype=float)
    lows    = np.array([b.get('low',  b['close']) for b in window], dtype=float)
    rng_pct = (highs.max() - lows.min()) / closes[-1] if closes[-1] > 0 else 0.0
    try:
        wf     = WF.run(closes, entry=closes[-1], direction=1)
        comp   = wf['components']
        itf    = wf['interference']
        phase  = comp.get('micro', {}).get('phase', 0.0)
        wf_dir = itf.get('direction', 0)
        state  = _micro_state(phase, g['peak_ph'], g['trough_ph'])
        if state != 'MID' and rng_pct < g['min_amp_pct']:
            return 'MID', phase, rng_pct, wf_dir, \
                   f'rng={rng_pct:.4f}<{g["min_amp_pct"]}'
        return state, phase, rng_pct, wf_dir, ''
    except Exception as e:
        return 'MID', 0.0, 0.0, 0, f'err:{e}'


def _gates(stype, score, kdv_bal, kdv_dir,
           thresh, gate_rev, gate_cont, obi_conf, align_cont,
           obi_pressure, align, res_dir, sig_dir,
           at_sup, kdv_flipped, sc_gate=None, at_res=False):
    """Returns (pass: bool, reason_or_confirm: str).

    REV signals: mark every peak/trough — gates annotate confidence, not suppress.
    Only hard blocks: score below noise floor, or kdv_bal=0 (no soliton data yet).
    HTF levels, OBI direction, kdv direction = confirmation bonuses, not requirements.
    """
    if not kdv_flipped:
        effective_thresh = sc_gate if sc_gate is not None else thresh
        if abs(score) < effective_thresh:
            return False, f'score={score:+.3f}<{effective_thresh:.3f}'

    if stype in ('PEAK-REV', 'TROUGH-REV'):
        # Hard minimum: soliton must have computed (kdv_bal=0 = cold start, no data)
        if kdv_bal < 0.3:
            return False, f'kdv-bal={kdv_bal:.2f}<0.3(cold)'

        # Annotate what's present — none of these block detection
        kdv_matches  = (sig_dir < 0 and kdv_dir <= -1) or (sig_dir > 0 and kdv_dir >= 1)
        ob_confirms  = (stype == 'TROUGH-REV' and obi_pressure >=  obi_conf) or \
                       (stype == 'PEAK-REV'   and obi_pressure <= -obi_conf)
        htf_confirms = (stype == 'TROUGH-REV' and at_sup) or \
                       (stype == 'PEAK-REV'   and at_res)

        if   kdv_flipped:   confirm = 'kdv-flip'
        elif kdv_matches:   confirm = f'kdv={kdv_dir:+d}'
        elif htf_confirms:  confirm = 'htf-lvl'
        elif ob_confirms:   confirm = f'ob={obi_pressure:+.2f}'
        else:               confirm = 'detect'   # phase+score only — still a valid mark
        return True, confirm

    if stype in ('PEAK-CONT', 'TROUGH-CONT'):
        if kdv_bal < gate_cont:
            return False, f'kdv-bal={kdv_bal:.1f}<{gate_cont:.1f}'
        if align < align_cont:
            return False, f'align={align:.2f}<{align_cont:.2f}'
        if res_dir != 0 and res_dir != sig_dir:
            return False, f'res-dir={res_dir}≠{sig_dir}'
        return True, ''

    return False, 'mid-state'


def _divergence_pattern(hist_signed, hist_dk_ds, hist_dk_fos, stype, window=12):
    """
    Predict reversals by scanning indicator history as a mini chart.

    TROUGH-REV: signed score still negative but making higher lows (becoming less
                negative) over the last `window` candles, with decay or floor score
                improving in the second half of the window → fall losing energy.
    PEAK-REV:   signed score still positive but making lower highs → momentum fading.

    Returns (bool, description_str).
    """
    n = len(hist_signed)
    if n < window:
        return False, ''

    recent = hist_signed[-window:]
    half   = window // 2

    if stype == 'TROUGH-REV':
        if recent[-1] >= 0:          # score already flipped positive — normal path
            return False, ''

        lo_early = min(recent[:half])
        lo_late  = min(recent[half:])
        if lo_late <= lo_early + 0.02:   # need clear improvement (higher low)
            return False, ''

        # Physical confirmation: decay or floor score must be improving
        ds_ok = fos_ok = False
        if len(hist_dk_ds) >= window:
            ds_early = max(hist_dk_ds[-window:-half]) if half else 0.0
            ds_late  = max(hist_dk_ds[-half:])
            ds_ok    = ds_late > ds_early + 0.05
        if len(hist_dk_fos) >= window:
            fos_late = max(hist_dk_fos[-half:])
            fos_ok   = fos_late > 0.25

        if not (ds_ok or fos_ok):
            return False, ''

        desc = (f'patt({lo_early:+.2f}→{lo_late:+.2f})'
                + ('+ds' if ds_ok else '') + ('+fos' if fos_ok else ''))
        return True, desc

    elif stype == 'PEAK-REV':
        if recent[-1] <= 0:          # score already flipped negative — normal path
            return False, ''

        hi_early = max(recent[:half])
        hi_late  = max(recent[half:])
        if hi_early - hi_late < 0.05:   # need meaningful drop in score peaks
            return False, ''

        desc = f'patt({hi_early:+.2f}→{hi_late:+.2f})'
        return True, desc

    return False, ''


# ── Main scan ─────────────────────────────────────────────────────────────────

def _preseed(session_tf1m, ob_by_sec):
    """
    Pre-populate distribution histories from the session's own 1m bars.
    Used when there are no prior-session history bars (e.g. first session of the day).
    Runs a silent physics pass — no signals, just stats collection.
    """
    hist_s=[]; hist_ph=[]; hist_ob=[]; hist_kd=[]; hist_al=[]
    seed_bars = []
    seed_accum = HydraulicAccumulator()
    for b in session_tf1m:
        seed_bars.append(b)
        if len(seed_bars) < 15: continue
        closes, opens_a, volumes, taker_buy = _bars2arr(seed_bars[-200:])
        try:
            prices = (closes + opens_a) / 2.0
            sec    = b['ts'] // 1000
            bids, asks = ob_by_sec.get(sec, ([], []))
            fus    = fusion.run(prices, opens_a, closes, volumes, taker_buy,
                                seed_accum, bids, asks)
            f_sc   = fus['score']
            kd_bal = abs(fus['soliton'].get('balance', 0.0))
            obi_r  = fus.get('obi', {})
            obi_p  = (obi_r.get('obi', 0.0) if isinstance(obi_r, dict) else float(obi_r or 0.0))
            wf     = WF.run(closes, entry=closes[-1], direction=1 if f_sc >= 0 else -1)
            mph    = wf['components'].get('micro', {}).get('phase', 0.0)
            hist_s.append(abs(f_sc)); hist_ph.append(mph)
            hist_ob.append(obi_p);   hist_kd.append(kd_bal)
            hist_al.append(0.5)
        except Exception:
            pass
    return hist_s, hist_ph, hist_ob, hist_kd, hist_al


def scan(mins_limit=96, session_idx=0, signals_only=False):
    if not signals_only: print("Fetching raw data...", flush=True)
    # If the collector's tmpfs snapshot exists, read directly — no git fetch needed.
    # git fetch origin data/raw is ONLY for the remote-only case (no local collector).
    # IMPORTANT: never fetch data/raw on the phone — that would make historical
    # blobs reachable in .git/objects and cause permanent local storage bloat.
    tmpfs_raw = os.path.join(_TMP, 'BTCUSDT_LIVE.jsonl.gz')
    if not os.path.exists(tmpfs_raw):
        subprocess.run(['git','fetch','origin','data/raw'], capture_output=True, cwd=REPO)
    raw = _fetch_raw()
    if not signals_only: print(f"Raw lines: {len(raw)}")

    if not signals_only: print("Building timeframes...", flush=True)
    s1, tf1m, tf5m, tf15m, tf1h, tf4h, ob_by_sec = _build_tfs(raw)

    s1_by_sec = {b['ts']//1000: b for b in s1}
    s1_secs   = sorted(s1_by_sec.keys())

    # Session detection uses TRADE timestamps (forward-filled s1 has no gaps).
    # Session boundary = gap > 30 min between actual trades (not forward-fill).
    trade_secs = sorted(sec for sec in s1_secs if s1_by_sec[sec]['volume'] > 0)
    if not trade_secs:
        if not signals_only:
            print("No trades in raw data yet — waiting for first trade")
        return
    trade_gaps = sorted(
        [i for i in range(1, len(trade_secs))
         if trade_secs[i] - trade_secs[i-1] > 1800],   # >30 min = new session
        reverse=True   # newest gap first
    )

    if not trade_gaps:
        # No session gaps — treat all data as one continuous session
        t_start = trade_secs[0]
        t_end   = trade_secs[-1]
    elif session_idx < len(trade_gaps):
        t_start = trade_secs[trade_gaps[session_idx]]
        t_end   = trade_secs[trade_gaps[session_idx - 1] - 1] if session_idx > 0 \
                  else trade_secs[-1]
    elif session_idx == len(trade_gaps):
        # Earliest session: start of data to just before the oldest gap
        t_start = trade_secs[0]
        t_end   = trade_secs[trade_gaps[-1] - 1]
    else:
        t_start = trade_secs[0]
        t_end   = trade_secs[-1]

    # Include all s1 seconds within the session's trade-active window
    live_secs = [s for s in s1_secs if t_start <= s <= t_end]

    if mins_limit:
        cutoff = live_secs[-1] - mins_limit * 60
        live_secs = [s for s in live_secs if s >= cutoff]

    live_start_min = (live_secs[0] // 60) * 60
    history_1m     = [b for b in tf1m if b['ts']//1000 < live_start_min]

    if not signals_only:
        print(f"Live session: {_ts(live_secs[0])} → {_ts(live_secs[-1])} UTC  "
              f"({len(live_secs)}s,  {len(history_1m)} history bars)")
        print("Computing knife-decay states...", flush=True)
    knife_states = _build_decay_states(raw, live_secs[0], live_secs[-1])
    if not signals_only: print(f"  {len(knife_states)} decay snapshots")

    live_by_min = collections.defaultdict(list)
    for sec in live_secs:
        live_by_min[(sec // 60) * 60].append(sec)

    closed_1m       = list(history_1m)
    accum           = HydraulicAccumulator()
    prev_kdv_global = 0
    sig_count       = 0
    session_shelves = []   # updated each minute from closed_1m


    # Per-closed-candle history for adaptive thresholds
    hist_scores  = []   # abs(score) at minute end
    hist_phases  = []   # micro phase at minute end
    hist_obi     = []   # raw OBI at minute end
    hist_kdv_bals= []   # KdV balance at minute end
    hist_aligns  = []   # MTF alignment at minute end
    hist_signed  = []   # signed score — pattern engine (divergence detection)
    hist_dk_ds_a = []   # decay score per minute
    hist_dk_fos_a= []   # floor score per minute

    # Pre-seed distributions when no prior history (first session of the day)
    if len(history_1m) < 15:
        session_tf1m = [b for b in tf1m
                        if live_secs[0]//60*60 <= b['ts']//1000 <= live_secs[-1]//60*60]
        ps, pp, po, pk, pa = _preseed(session_tf1m, ob_by_sec)
        hist_scores   = ps; hist_phases = pp
        hist_obi      = po; hist_kdv_bals = pk; hist_aligns = pa
        hist_signed   = [0.0] * len(ps)   # preseed has no sign info; pattern engine warms up live
        hist_dk_ds_a  = [0.0] * len(ps)
        hist_dk_fos_a = [0.0] * len(ps)
        if not signals_only:
            print(f"Pre-seeded from {len(ps)} session bars (no prior history)")

    # Per-TF phase state for edge-detected multi-TF signals
    last_tf_states = {'5m': 'MID', '15m': 'MID', '1h': 'MID', '4h': 'MID'}
    last_1m_state  = 'MID'
    last_range_state = None    # 'sup'|'res' — last ranging side fired
    # Price-level dedup for all four structural labels.
    # Each label fires only when price reaches a genuinely new level in its
    # direction — prevents same-price rapid re-fires when OBI/TR oscillates.
    # On reversal (TROUGH/PEAK) the opposite-direction trackers reset so the
    # next continuation move fires fresh from whatever level it starts.
    _last_cont_up_px  = -float('inf')  # CONT_UP: fires when price > this
    _last_cont_dn_px  =  float('inf')  # CONT_DOWN: fires when price < this
    _last_peak_px     = -float('inf')  # PEAK: fires when price > this
    _last_trough_px   =  float('inf')  # TROUGH: fires when price < this
    _last_range_hi_px =  float('inf')  # RANGE_HI: fires when price < this (lower-high)
    _last_range_lo_px = -float('inf')  # RANGE_LO: fires when price > this (higher-low)

    # Micro layer: rolling 1s window across minute boundaries
    micro_window  = []   # list of 1s bar dicts
    micro_accum   = HydraulicAccumulator()

    if signals_only:
        print(f"\n{C}━━━  {_ts(live_secs[0])} → {_ts(live_secs[-1])} UTC  ━━━{Z}")
    else:
        print(f"\n{C}━━━ WAVE ENGINE  {_ts(live_secs[0])} → {_ts(live_secs[-1])} UTC ━━━{Z}")
        print(f"{W}[wave] TIME   PRICE       SCORE  D  μ  sh  ca  ma  "
              f"1m 5m 15 1h 4h  KdV  OBI   mSC    mPH  itype  STATE  DECAY  NOTES{Z}\n")

    for min_sec in sorted(live_by_min.keys()):
        secs_in_min = sorted(live_by_min[min_sec])

        b5  = [b for b in tf5m  if b['ts']//1000 <= min_sec][-30:]
        b15 = [b for b in tf15m if b['ts']//1000 <= min_sec][-20:]
        b1h = [b for b in tf1h  if b['ts']//1000 <= min_sec][-12:]
        b4h = [b for b in tf4h  if b['ts']//1000 <= min_sec][-6:]

        # Thresholds from completed candle distributions
        thresh, peak_ph, trough_ph, obi_conf, gate_rev, gate_cont, align_cont = \
            _thresholds(hist_scores, hist_phases, hist_obi, hist_kdv_bals, hist_aligns)

        # HTF/MTF context uses only closed bars — stable for the whole minute.
        # Compute once before the inner loop so gate checks fire immediately
        # on each second without waiting for the minute to complete.
        _htf_price = closed_1m[-1]['close'] if closed_1m else 0.0
        htf_ctx = htf.regime(_htf_price, b1h, b4h, closed_1m[-30:], b5, b15)
        at_sup  = htf_ctx.get('at_support',    False)
        at_res  = htf_ctx.get('at_resistance', False)
        try:
            res_out    = RES.resonance(closed_1m[-30:], b5, b15, b1h, b4h)
            res_dir    = res_out.get('direction',  0)
            align      = res_out.get('alignment',  0.0)
            dissonance = res_out.get('dissonance', False)
        except Exception:
            res_dir=0; align=0.0; dissonance=False

        # TF phases for composite — stable each minute (closed bars only).
        # micro_phase (1m) varies per-second and is blended in the inner loop.
        _tf_ph_5m  = _wf_phase(b5)
        _tf_ph_15m = _wf_phase(b15)
        _tf_ph_1h  = _wf_phase(b1h)
        _tf_ph_4h  = _wf_phase(b4h)

        p_opens=[]; p_closes=[]; p_vols=[]; p_tb=[]

        sustain_count    = 0
        prev_sb          = 0
        prev_kdv_min     = prev_kdv_global
        prev_itype       = None

        kdv_flipped_up   = False
        kdv_flipped_down = False

        kdv_flips     = []
        wh_secs       = []
        itype_changes = []
        final         = {}
        best_obi_long  = 0.0
        best_obi_short = 0.0

        min_dk_state = 0; min_dk_ds = 0.0; min_dk_fos = 0.0; min_dk_lbl = 'NEUT'

        for sec in secs_in_min:
            bar1s = s1_by_sec.get(sec)
            if bar1s is None: continue

            p_opens.append(bar1s['open'])
            p_closes.append(bar1s['close'])
            p_vols.append(bar1s['volume'])
            p_tb.append(bar1s.get('taker_buy', bar1s['volume']*0.5))

            p_close = p_closes[-1]
            partial = {'open':p_opens[0],'high':max(p_closes),'low':min(p_closes),
                       'close':p_close,'volume':sum(p_vols),'taker_buy':sum(p_tb)}
            window = closed_1m[-200:] + [partial]
            if len(window) < 15: continue

            closes, opens_a, volumes, taker_buy = _bars2arr(window)
            bids, asks = ob_by_sec.get(sec, ([], []))

            # ── Knife-decay state lookup ──────────────────────────────────────
            dk = knife_states.get(sec, (0, 0.0, 0.0, 'NEUT'))
            if dk[0] > min_dk_state:
                min_dk_state = dk[0]; min_dk_ds = dk[1]
                min_dk_fos   = dk[2]; min_dk_lbl = dk[3]

            # ── Micro layer: 1s rolling window physics ───────────────────────
            micro_window.append(bar1s)
            if len(micro_window) > 60: micro_window = micro_window[-60:]
            micro_sc = 0.0; micro_ph = 0.0; micro_kdv = 0
            if len(micro_window) >= 15:
                mc = np.array([b['close']  for b in micro_window], dtype=float)
                mo = np.array([b['open']   for b in micro_window], dtype=float)
                mv = np.array([b['volume'] for b in micro_window], dtype=float)
                mt = np.array([b.get('taker_buy', b['volume']*0.5)
                               for b in micro_window], dtype=float)
                try:
                    mp   = (mc + mo) / 2.0
                    mfus = fusion.run(mp, mo, mc, mv, mt, micro_accum, [], [])
                    micro_sc  = mfus['score']
                    micro_kdv = mfus['soliton']['direction']
                    mwf       = WF.run(mc, entry=mc[-1],
                                       direction=1 if micro_sc >= 0 else -1)
                    micro_ph  = mwf['components'].get('micro',{}).get('phase', 0.0)
                except Exception:
                    pass

            try:
                prices = (closes + opens_a) / 2.0
                fus    = fusion.run(prices, opens_a, closes, volumes, taker_buy,
                                    accum, bids, asks)
                kdv      = fus['soliton']['direction']
                kdv_bal  = abs(fus['soliton'].get('balance', 0.0))
                wh       = fus['water_hammer']['detected']
                f_sc     = fus['score']
                obi_raw  = fus.get('obi', {})
                obi_p    = (obi_raw.get('obi', 0.0)
                            if isinstance(obi_raw, dict) else float(obi_raw or 0.0))
            except Exception:
                kdv=0; kdv_bal=0.0; wh=False; f_sc=0.0; obi_p=0.0

            # Detect KdV flip NOW (before score gate) so the reduced gate
            # applies on the same second the flip occurs, not one second late
            kdv_just_flipped = (kdv != 0 and kdv != prev_kdv_min and prev_kdv_min != 0)
            if kdv_just_flipped:
                kdv_flips.append((sec, kdv))
                if kdv > 0: kdv_flipped_up   = True
                if kdv < 0: kdv_flipped_down = True
            prev_kdv_min = kdv if kdv != 0 else prev_kdv_min

            # When KdV has flipped this minute, the flip is structural confirmation.
            # Only require score direction (sign), not magnitude — gate_rev on balance
            # provides the strength filter. Non-flip bars still need score ≥ thresh.
            flip_active = kdv_flipped_up or kdv_flipped_down
            if flip_active:
                sb = int(np.sign(f_sc)) if abs(f_sc) > 0.01 else 0
                sc_gate = 0.01  # direction-only; gate at minute-end uses kdv_bal
            else:
                sc_gate = thresh
                sb = (1 if f_sc >= thresh else -1 if f_sc <= -thresh else 0)

            # Track extremes for gate check
            if obi_p > best_obi_long:  best_obi_long  = obi_p
            if obi_p < best_obi_short: best_obi_short = obi_p

            wf_direction = sb if sb != 0 else (1 if f_sc >= 0 else -1)
            wf   = WF.run(closes, entry=p_close, direction=wf_direction)
            comp = wf['components']
            itf  = wf['interference']
            tgts = wf.get('targets', {})

            micro_phase = comp.get('micro', {}).get('phase', 0.0)
            wf_dir      = itf['direction']
            itype       = itf['type'][:4]

            if sb != 0 and sb == prev_sb:
                sustain_count += 1
            elif sb != 0:
                sustain_count = 1
            else:
                sustain_count = 0
            prev_sb = sb

            if wh and not wh_secs:
                wh_secs.append(sec)

            if itype != prev_itype and prev_itype is not None:
                itype_changes.append((sec, prev_itype, itype))
            prev_itype = itype

            sustain_needed = SUSTAIN_FLIP if (kdv_flipped_up or kdv_flipped_down) else SUSTAIN_S

            # Composite multi-TF phase — kept as context in the observation, not the trigger.
            composite_phase = (
                _COMP_W['1m']  * micro_phase  +
                _COMP_W['5m']  * _tf_ph_5m    +
                _COMP_W['15m'] * _tf_ph_15m   +
                _COMP_W['1h']  * _tf_ph_1h    +
                _COMP_W['4h']  * _tf_ph_4h
            )

            # ── Pattern observation — order flow patterns at price extremes ───
            obs_label = _observe_structural(
                p_close, obi_p, kdv_flipped_up, kdv_flipped_down,
                micro_window, res_dir=res_dir, bars_5m=b5,
                bars_1m=closed_1m)

            if obs_label == 'MID':
                last_1m_state = 'MID'
            else:
                # All four labels use price-level dedup: each fires only when
                # price reaches a genuinely new level in its structural direction.
                # This removes same-price oscillation noise without blocking any
                # legitimate new extreme.  On each reversal (TROUGH/PEAK) the
                # opposite-side trackers reset so the next leg starts fresh.
                _fire_obs = False
                if obs_label == 'CONT_UP':
                    if p_close > _last_cont_up_px:
                        _fire_obs = True
                        _last_cont_up_px  = p_close
                        _last_peak_px     = p_close        # PEAK can't fire at this same level
                        _last_range_hi_px =  float('inf')  # trending up — range ref invalid
                        _last_range_lo_px = -float('inf')
                elif obs_label == 'CONT_DOWN':
                    if p_close < _last_cont_dn_px:
                        _fire_obs = True
                        _last_cont_dn_px  = p_close
                        _last_trough_px   = p_close        # TROUGH can't fire at this same level
                        _last_range_hi_px =  float('inf')  # trending down — range ref invalid
                        _last_range_lo_px = -float('inf')
                elif obs_label == 'PEAK':
                    if p_close > _last_peak_px:
                        _fire_obs = True
                        _last_peak_px     = p_close
                        _last_trough_px   =  float('inf')  # reset opposite
                        _last_cont_dn_px  =  float('inf')  # down leg starts fresh after peak
                        _last_cont_up_px  = p_close        # CONT_UP must exceed this peak
                        _last_range_hi_px = p_close        # next RANGE_HI must be below this peak
                elif obs_label == 'TROUGH':
                    if p_close < _last_trough_px:
                        _fire_obs = True
                        _last_trough_px   = p_close
                        _last_peak_px     = -float('inf')  # reset opposite
                        _last_cont_up_px  = -float('inf')  # up leg starts fresh after trough
                        _last_cont_dn_px  = p_close        # CONT_DOWN must go below this trough
                        _last_range_lo_px = p_close        # next RANGE_LO must be above this trough
                elif obs_label == 'RANGE_HI':
                    if p_close < _last_range_hi_px:        # lower-high confirms range
                        _fire_obs = True
                        _last_range_hi_px = p_close
                        _last_cont_up_px  = p_close        # CONT_UP must exceed this range high
                elif obs_label == 'RANGE_LO':
                    if p_close > _last_range_lo_px:        # higher-low confirms range
                        _fire_obs = True
                        _last_range_lo_px = p_close
                        _last_cont_dn_px  = p_close        # CONT_DOWN must go below this range low

                if _fire_obs:
                    last_1m_state = obs_label
                    obs_dir   = +1 if obs_label in ('TROUGH', 'CONT_UP', 'RANGE_LO') else -1
                    side_o    = 'LONG' if obs_dir > 0 else 'SHORT'
                    arrow_o   = '▲' if obs_dir > 0 else '▼'
                    c_o       = G if obs_dir > 0 else R
                    bar_o     = '━' * 50

                    _n1o    = min(60, len(micro_window))
                    vol3_o  = sum(b['volume'] for b in micro_window[-_n1o:])
                    tb3_o   = sum(b.get('taker_buy', b['volume']*0.5) for b in micro_window[-_n1o:])
                    tb_r_o  = tb3_o / vol3_o if vol3_o > 1e-8 else 0.5
                    tb_pct_o = f"taker {tb_r_o*100:.0f}% (1m)"
                    kdv_ev_o = ('kdv↑' if kdv_flipped_up else
                                'kdv↓' if kdv_flipped_down else
                                f'kdv={kdv_bal:+.1f}')
                    dk_o     = KnifeDecayBuffer.compact(min_dk_state, min_dk_ds, min_dk_fos)
                    shelf_o  = _shelf_context(session_shelves, p_close)
                    t1mo= htf_ctx.get('trend_1m','ne')[:2]
                    t5mo= htf_ctx.get('trend_5m','ne')[:2]
                    t15o= htf_ctx.get('trend_15m','ne')[:2]
                    t1ho= htf_ctx.get('trend_1h','ne')[:2]
                    t4ho= htf_ctx.get('trend_4h','ne')[:2]
                    def _tr2o(t): return {'up':'↑','do':'↓','ne':'─'}.get(t[:2],'─')
                    mtf_o = (f"1m{_tr2o(t1mo)}  5m{_tr2o(t5mo)}  15m{_tr2o(t15o)}"
                             f"  1h{_tr2o(t1ho)}  4h{_tr2o(t4ho)}")

                    # Range tightening metric — only for RANGE_HI/RANGE_LO
                    range_tight_o = ''
                    if obs_label in ('RANGE_HI', 'RANGE_LO'):
                        _mw_o  = micro_window
                        _mm    = len(_mw_o)
                        _nc1   = min(60, _mm)
                        _cls_o = [b['close'] for b in _mw_o]
                        _r1_o  = max(_cls_o[-_nc1:]) - min(_cls_o[-_nc1:])  # 1m range
                        _r2_o  = max(_cls_o)         - min(_cls_o)           # 2m range
                        if _r2_o > 1e-8:
                            _ratio = _r1_o / _r2_o
                            _tight_pct = int((1.0 - _ratio) * 100)
                            _tight_str = f"tight +{_tight_pct}%" if _tight_pct >= 0 else f"expand +{-_tight_pct}%"
                            range_tight_o = f"range: {_tight_str}  (1m=${_r1_o:.2f}  2m=${_r2_o:.2f})"

                    sig_count += 1
                    notes_inline = (f"[{sig_count}] {obs_label} {side_o} @ {_ts(sec)}"
                                    f"  ${p_close:,.2f}")
                    print(f"\n{c_o}{bar_o}")
                    print(f"  {arrow_o}  {side_o}  ·  {obs_label:<20}  [{sig_count}]")
                    print(f"     {_ts(sec)}  ·  ${p_close:>10,.2f}")
                    print(f"     {tb_pct_o}  ·  obi={obi_p:+.3f}  ·  {kdv_ev_o}")
                    print(f"     cph={composite_phase:+.3f}  ·  {mtf_o}")
                    if range_tight_o: print(f"     {range_tight_o}")
                    if dk_o:    print(f"     {dk_o}")
                    if shelf_o: print(f"     {shelf_o}")
                    print(f"{bar_o}{Z}\n")

            # ── Ranging oscillation signal ─────────────────────────────────────
            rng_dir, rng_sup, rng_res, rng_span = _range_signal(
                session_shelves, p_close, last_range_state)
            if rng_dir != 0:
                side_r  = 'LONG' if rng_dir > 0 else 'SHORT'
                arrow_r = '▲' if rng_dir > 0 else '▼'
                c_r     = G if rng_dir > 0 else R
                bar_r   = '━' * 50
                sup_bias = ('L' if rng_sup['buy_ratio'] >= 0.55
                            else 'S' if rng_sup['buy_ratio'] <= 0.45 else '~')
                res_bias = ('L' if rng_res['buy_ratio'] >= 0.55
                            else 'S' if rng_res['buy_ratio'] <= 0.45 else '~')
                # Shelf bias confirms the bounce when sup=L (longs defend) or res=S (shorts defend)
                touched_bias = sup_bias if rng_dir > 0 else res_bias
                conf_r = 'hi-conf' if (rng_dir > 0 and sup_bias == 'L') or \
                                      (rng_dir < 0 and res_bias == 'S') else \
                         'lo-conf' if (rng_dir > 0 and sup_bias == 'S') or \
                                      (rng_dir < 0 and res_bias == 'L') else 'mid'
                t1mr= htf_ctx.get('trend_1m','ne')[:2]; t5mr= htf_ctx.get('trend_5m','ne')[:2]
                t15r= htf_ctx.get('trend_15m','ne')[:2]; t1hr= htf_ctx.get('trend_1h','ne')[:2]
                t4hr= htf_ctx.get('trend_4h','ne')[:2]
                def _tr2r(t): return {'up':'↑','do':'↓','ne':'─'}.get(t[:2],'─')
                mtf_r = (f"1m{_tr2r(t1mr)}  5m{_tr2r(t5mr)}  15m{_tr2r(t15r)}"
                         f"  1h{_tr2r(t1hr)}  4h{_tr2r(t4hr)}")
                sig_count += 1
                print(f"\n{c_r}{bar_r}")
                print(f"  {arrow_r}  {side_r}  ·  {'RANGE '+side_r:<20}  [{sig_count}]  {conf_r}")
                print(f"     {_ts(sec)}  ·  ${p_close:>10,.2f}")
                print(f"     range ${rng_span:.0f}"
                      f"  ·  sup ${rng_sup['level']:,.2f}({sup_bias},{rng_sup['bars']}bar)"
                      f"  →  res ${rng_res['level']:,.2f}({res_bias},{rng_res['bars']}bar)")
                print(f"     {mtf_r}")
                print(f"{bar_r}{Z}\n")
                last_range_state = 'sup' if rng_dir > 0 else 'res'
            elif last_range_state is not None and session_shelves:
                # Midpoint crossing: once price crosses mid, same side can fire again
                open_b_r = [s for s in session_shelves
                            if s['level'] < p_close and s['broken'] == 'open']
                open_a_r = [s for s in session_shelves
                            if s['level'] > p_close and s['broken'] == 'open']
                if open_b_r and open_a_r:
                    mid_r = (open_b_r[-1]['level'] + open_a_r[0]['level']) / 2
                    if (last_range_state == 'sup' and p_close > mid_r) or \
                       (last_range_state == 'res' and p_close < mid_r):
                        last_range_state = None

            final = {'sec':sec,'price':p_close,'score':f_sc,'wf_dir':wf_dir,
                     'kdv':kdv,'kdv_bal':kdv_bal,'wh':wh,'itype':itype,
                     'comp':comp,'itf':itf,'sb':sb,'micro_phase':micro_phase,
                     'composite_phase':composite_phase,
                     'sustain':sustain_count,'obi':obi_p,
                     'micro_sc':micro_sc,'micro_ph':micro_ph,'micro_kdv':micro_kdv}

        if not final:
            for b in tf1m:
                if b['ts']//1000 == min_sec:
                    closed_1m.append(b); break
            continue

        # Record completed-minute stats for next minute's thresholds
        hist_scores.append(abs(final['score']))
        hist_phases.append(final['composite_phase'])   # composite calibrates threshold to multi-TF scale
        hist_obi.append(final['obi'])
        hist_kdv_bals.append(final['kdv_bal'])
        hist_aligns.append(align)
        hist_signed.append(final['score'])    # signed — for divergence pattern
        hist_dk_ds_a.append(min_dk_ds)
        hist_dk_fos_a.append(min_dk_fos)

        session_shelves = _detect_shelves(closed_1m[-60:])

        # ── Output ───────────────────────────────────────────────────────────
        comp_f = final['comp']
        mi  = comp_f.get('micro',{});  sh_ = comp_f.get('subharm',{})
        ca  = comp_f.get('carrier',{}); ma  = comp_f.get('macro',{})
        price  = final['price']; score = final['score']
        wf_dir = final['wf_dir']; kdv  = final['kdv']
        itype  = final['itype']; sb    = final['sb']
        obi_f  = final['obi']

        t1m = htf_ctx.get('trend_1m', 'ne')[:2]
        t5m = htf_ctx.get('trend_5m', 'ne')[:2]
        t15 = htf_ctx.get('trend_15m','ne')[:2]
        t1h = htf_ctx.get('trend_1h', 'ne')[:2]
        t4h = htf_ctx.get('trend_4h', 'ne')[:2]

        state_now = _micro_state(final.get('composite_phase', final['micro_phase']), peak_ph, trough_ph)

        notes = []
        # notes_inline is set inside the inner loop when a signal fires or is blocked
        if 'notes_inline' in dir() and notes_inline:
            notes.append(notes_inline)
            notes_inline = ''

        for fs, fd in kdv_flips:
            notes.append(f"KdV→{fd:+d}@{_ts(fs)}")
            prev_kdv_global = fd

        for ws in wh_secs:
            notes.append(f"WH@{_ts(ws)}")

        for cs, ot, nt in itype_changes:
            if nt == 'dest': notes.append(f"→DEST@{_ts(cs)}")
            elif nt == 'cons': notes.append(f"→CONS@{_ts(cs)}")

        if at_sup: notes.append('SUP')
        if at_res: notes.append('RES')
        if htf_ctx.get('reversal_setup'): notes.append('REVERSAL')
        if htf_ctx.get('falling_knife'):  notes.append('FALL-KNIFE')
        if dissonance: notes.append('DISS')

        note_str = '  '.join(notes)
        kdv_str  = f"{kdv:+d}" if kdv != 0 else " 0"
        obi_str  = f"{obi_f:+.2f}"

        col = Y if notes else Z
        comp_ph_f   = final.get('composite_phase', 0.0)
        state_now_f = _micro_state(comp_ph_f, peak_ph, trough_ph)
        state_col = (C if state_now_f == 'PEAK' else
                     G if state_now_f == 'TROUGH' else W)

        msc_f  = final.get('micro_sc',  0.0)
        mph_f  = final.get('micro_ph',  0.0)
        msc_str = f"{msc_f:>+6.3f}" if msc_f != 0.0 else "  ---  "
        mph_str = _ph(mph_f)
        cph_str = f"{comp_ph_f:+.3f}"   # composite phase shown in wave table
        dec_str = KnifeDecayBuffer.compact(min_dk_state, min_dk_ds, min_dk_fos)
        dec_col = (G if min_dk_state >= KnifeDecayBuffer.FLOCK  else
                   C if min_dk_state >= KnifeDecayBuffer.DWATCH else
                   Y if min_dk_state >= KnifeDecayBuffer.KNIFE  else Z)

        t1m = htf_ctx.get('trend_1m', 'ne')[:2]
        t5m = htf_ctx.get('trend_5m', 'ne')[:2]
        t15 = htf_ctx.get('trend_15m','ne')[:2]
        t1h = htf_ctx.get('trend_1h', 'ne')[:2]
        t4h = htf_ctx.get('trend_4h', 'ne')[:2]

        if not signals_only:
            print(f"{col}[wave] {_tm(min_sec)}  ${price:>9,.2f}  {score:>+7.4f} {_ds(wf_dir)}  "
                  f"{_ph(mi.get('phase',0)):>2} {_ph(sh_.get('phase',0)):>2} "
                  f"{_ph(ca.get('phase',0)):>2} {_ph(ma.get('phase',0)):>2}  "
                  f"{t1m} {t5m} {t15} {t1h} {t4h}  "
                  f"{kdv_str}  {obi_str}  {msc_str}  {mph_str}  {cph_str}  {itype:4}  "
                  f"{state_col}{state_now_f:6}{col}  {dec_col}{dec_str}{col}  {note_str}{Z}")

        # ── Per-TF independent phase detection ───────────────────────────
        # ── Per-TF independent phase detection ───────────────────────────
        tf_bars_map = [('5m', b5), ('15m', b15), ('1h', b1h), ('4h', b4h)]
        tf_sb     = final.get('score', 0.0)
        tf_sb_dir = 1 if tf_sb >= 0 else -1
        for tf_lbl, tf_bars in tf_bars_map:
            tf_state_now, tf_phase, tf_score, tf_wf_dir, tf_fail = \
                _tf_phase_state(tf_bars, tf_lbl)
            tf_state_prev = last_tf_states[tf_lbl]
            # Always update prev so we track transitions even through gated bars
            if tf_state_now != 'MID':
                last_tf_states[tf_lbl] = tf_state_now
            if tf_state_now == 'MID':
                if not tf_fail:
                    last_tf_states[tf_lbl] = 'MID'
                continue                           # gated out or not at extreme
            if tf_state_now == tf_state_prev:
                continue                           # no edge — already in this state
            # Edge: transitioned into PEAK or TROUGH, passed TF-specific gate
            tf_stype = _sig_type(tf_state_now, tf_wf_dir if tf_wf_dir != 0 else tf_sb_dir)
            tf_dir   = 1 if tf_stype in ('TROUGH-REV', 'PEAK-CONT') else -1
            tf_entry = final.get('price', 0.0)
            sig_count += 1
            tf_cl, _, _, _ = _wall_absorption(
                bids, asks, tf_entry, closed_1m, b5, b15, tf_dir > 0)
            wa_tag  = f'wa={tf_cl}' if tf_cl else 'wa=X'
            g       = _TF_GATES[tf_lbl]
            tf_conf = (f'ph={tf_phase:+.3f}(≥{g["peak_ph"]:.2f})'
                       f'  rng={tf_score*100:.3f}%(≥{g["min_amp_pct"]*100:.2f}%)'
                       f'  {wa_tag}')
            tf_arrow = '▲' if tf_dir > 0 else '▼'
            tf_side  = 'LONG' if tf_dir > 0 else 'SHORT'
            tf_c     = G if tf_dir > 0 else R
            tf_bar   = '━' * 50
            print(f"\n{tf_c}{tf_bar}")
            print(f"  [{tf_lbl}] {tf_arrow}  {tf_side}  ·  {tf_stype:<20}  [{sig_count}]")
            print(f"     {_ts(min_sec)}  ·  ${tf_entry:>10,.2f}")
            print(f"     {tf_conf}")
            tf_shelf_ann = _shelf_context(session_shelves, tf_entry)
            if tf_shelf_ann: print(f"     {tf_shelf_ann}")
            print(f"{tf_bar}{Z}\n")

        if kdv != 0: prev_kdv_global = kdv

        for b in tf1m:
            if b['ts']//1000 == min_sec:
                closed_1m.append(b); break

    # Final threshold state
    thresh_f, pk_f, tr_f, obi_f2, gr_f, gc_f, al_f = \
        _thresholds(hist_scores, hist_phases, hist_obi, hist_kdv_bals, hist_aligns)
    print(f"\n{C}━━━ {sig_count} signal{'s' if sig_count!=1 else ''} ━━━{Z}")
    if not signals_only:
        print(f"Final thresholds (from {len(hist_scores)} closed candles):")
        print(f"  score≥{thresh_f:.3f}  peak_ph≥{pk_f:.2f}  trough_ph≤{tr_f:.2f}")
        print(f"  obi_conf≥{obi_f2:.2f}  kdv_rev≥{gr_f:.1f}  kdv_cont≥{gc_f:.1f}  align≥{al_f:.2f}")
        print(f"\nKey: STATE = micro position (PEAK/TROUGH/MID)  OBI = order book imbalance")
        print(f"     All thresholds derived from completed candle value distributions")
        print(f"     SUSTAIN_S={SUSTAIN_S}s is the only fixed constant")


# ── Incremental scan state ────────────────────────────────────────────────────

class ScanState:
    """Persists between incremental scan calls — holds session context."""
    __slots__ = ('closed_1m', 'accum', 'micro_accum', 'micro_window',
                 'hist_scores', 'hist_phases', 'hist_obi', 'hist_kdv_bals',
                 'hist_aligns', 'hist_signed_scores', 'hist_dk_ds', 'hist_dk_fos',
                 'prev_kdv_global', 'sig_count',
                 'last_sec', 'tf1m', 'tf5m', 'tf15m', 'tf1h', 'tf4h',
                 'tf1h_seed',
                 's1_by_sec', 'ob_by_sec', 'knife_buf', 'signals',
                 'appended_min_ts', 'last_align', 'last_res_dir', 'last_htf_sup',
                 'last_tf_states', 'last_1m_state',
                 'last_cont_up_px', 'last_cont_dn_px',
                 'last_peak_px', 'last_trough_px',
                 'last_range_hi_px', 'last_range_lo_px',
                 'last_range_state', 'session_shelves')

    def __init__(self):
        from physics.signals import HydraulicAccumulator
        self.closed_1m        = []
        self.accum            = HydraulicAccumulator()
        self.micro_accum      = HydraulicAccumulator()
        self.micro_window     = []
        self.hist_scores      = []
        self.hist_phases      = []
        self.hist_obi         = []
        self.hist_kdv_bals    = []
        self.hist_aligns      = []
        self.hist_signed_scores = []   # signed score per closed minute (pattern engine)
        self.hist_dk_ds         = []   # decay score per closed minute
        self.hist_dk_fos        = []   # floor score per closed minute
        self.prev_kdv_global  = 0
        self.sig_count        = 0
        self.last_sec         = 0
        self.tf1m = self.tf5m = self.tf15m = self.tf1h = self.tf4h = []
        self.tf1h_seed        = []   # historical 1h candles — base layer for 1h/4h
        self.s1_by_sec        = {}
        self.ob_by_sec        = {}
        self.knife_buf        = KnifeDecayBuffer()
        self.signals          = []   # list of signal dicts emitted so far
        self.appended_min_ts  = set()
        self.last_align       = 0.0
        self.last_res_dir     = 0
        self.last_htf_sup     = False
        self.last_tf_states   = {'5m': 'MID', '15m': 'MID', '1h': 'MID', '4h': 'MID'}
        self.last_1m_state    = 'MID'
        self.last_cont_up_px  = -float('inf')
        self.last_cont_dn_px  =  float('inf')
        self.last_peak_px     = -float('inf')
        self.last_trough_px   =  float('inf')
        self.last_range_hi_px =  float('inf')   # RANGE_HI: fires when price < this
        self.last_range_lo_px = -float('inf')   # RANGE_LO: fires when price > this
        self.last_range_state = None   # 'sup'|'res' — last ranging side fired
        self.session_shelves  = []


_LIVE_MINS = 2    # minutes of tick-by-tick on first call (history uses 1m candles)


def _seed_state_from_candles(state: ScanState, tf1m: list, ob_by_sec: dict,
                             raw_lines=None):
    """
    Pre-populate state from closed 1m bars using vectorized rolling physics.
    One numpy pass over the last 250 candles instead of 200 fusion.run() calls.
    knife_buf is seeded from the actual raw T/D stream (sub-second resolution)
    so _vel() has real data points within its 5s window.
    """
    from physics.signals import rolling_physics, rolling_score
    if len(tf1m) < 2:
        return
    state.closed_1m = list(tf1m[:-1])
    seed_bars = state.closed_1m[-30:]
    print(f'[seed] {len(tf1m)} candle bars loaded, seeding from last {len(seed_bars)}', flush=True)
    if len(seed_bars) < 30:
        print(f'[seed] BAIL — need 30, got {len(seed_bars)}', flush=True)
        return

    # Load historical 1h candles as the base layer for 1h/4h depth
    h1 = _fetch_candles_1h()
    if h1:
        state.tf1h_seed = h1
    _update_htf(state)
    print(f'[seed] HTF: {len(state.tf5m)}x5m  {len(state.tf15m)}x15m  '
          f'{len(state.tf1h)}x1h  {len(state.tf4h)}x4h'
          f'  (1h-seed={len(h1)})', flush=True)

    c, o, v, t = _bars2arr(seed_bars)
    prices = (c + o) / 2.0

    # Vectorized: all rolling indicators in one numpy pass
    ph = rolling_physics(prices, c, o, v, t)

    # OBI per bar from cached OB snapshots
    obi_arr = np.zeros(len(seed_bars))
    for i, b in enumerate(seed_bars):
        bids, asks = ob_by_sec.get(b['ts'] // 1000, ([], []))
        if bids and asks:
            bv = sum(q for _, q in bids[:5])
            av = sum(q for _, q in asks[:5])
            obi_arr[i] = (bv - av) / (bv + av) if (bv + av) else 0.0

    score_arr = rolling_score(ph, obi_arr, t, v)

    # Populate threshold histories from bar 20 onward (window warmup)
    for i in range(20, len(seed_bars)):
        sc = score_arr[i]
        if np.isnan(sc):
            continue
        kd = ph['kdv_bal'][i]
        state.hist_scores.append(float(abs(sc)))
        state.hist_phases.append(0.0)          # micro phase seeded at 0; live data refines
        state.hist_obi.append(float(obi_arr[i]))
        state.hist_kdv_bals.append(0.0 if np.isnan(kd) else float(abs(kd)))
        state.hist_aligns.append(0.5)
        state.hist_signed_scores.append(float(sc))  # signed — used by pattern engine
        state.hist_dk_ds.append(0.0)                # dk not available from candles
        state.hist_dk_fos.append(0.0)

    # Seed knife buffer in two passes:
    # Pass 1 — direct swing-low injection from candle closes.
    #   Running the state machine over candles is unreliable: _vol_thresh scales
    #   thresholds with std of 1m closes (~$200-500 for BTC), so min_drop ends
    #   up $300+ and very few FALLING→BOUNCE cycles complete.  Instead detect
    #   local minima directly (11-bar window) and inject them into buf.lows so
    #   ds > 0 from the first update after restart.
    # Pass 2 — raw T/D stream (when available): replays sub-second ticks so
    #   _vel() has real data and OB metrics are current.  buf.last_ts is cleared
    #   between passes to prevent the 20-min timeout from wiping pass-1 state.
    buf = state.knife_buf
    closes  = [float(b['close']) for b in seed_bars]
    ts_list = [b['ts']           for b in seed_bars]
    n_bars  = len(closes)
    WIN     = 5   # local-min half-width (11-bar window)

    raw_low_idx = []
    for i in range(WIN, n_bars - WIN):
        if closes[i] == min(closes[i - WIN : i + WIN + 1]):
            raw_low_idx.append(i)

    injected = []
    for k, i in enumerate(raw_low_idx):
        next_i    = raw_low_idx[k + 1] if k + 1 < len(raw_low_idx) else n_bars
        bounce_hi = max(closes[i:next_i]) if next_i > i + 1 else None

        # Velocity from consecutive 1m closes ($/s)
        vel = (closes[i] - closes[i - 1]) / 60.0 if i > 0 else 0.0

        # OBI proxy from taker_buy volume (candle-derived)
        bar_i = seed_bars[i]
        vol_i = bar_i.get('volume', 0.0)
        tb_i  = bar_i.get('taker_buy', vol_i * 0.5)
        obi   = (2 * tb_i - vol_i) / vol_i if vol_i > 0 else _EPS

        # Override with live OB snapshot if available for this second
        b2, a2 = ob_by_sec.get(ts_list[i] // 1000, ([], []))
        if b2 and a2:
            bv = sum(q for _, q in b2[:5]); av = sum(q for _, q in a2[:5])
            if bv + av:
                obi = (bv - av) / (bv + av)

        injected.append(dict(ts=ts_list[i], px=closes[i], vel=vel,
                             obi=obi, conc=0.0, spr=_EPS,
                             bounce_hi=bounce_hi, bid_flow=0.0, ask_flow=0.0))

    buf.lows = injected[-buf.MAX_LOWS:]

    # Set phase / peak / trough from recent candle action
    if n_bars >= 3:
        recent = closes[-20:]
        pk     = max(recent)
        last   = closes[-1]
        if pk - last > 0:
            buf.phase     = 'FALLING'
            buf.peak      = pk
            buf.trough    = last
            buf._low_snap = (ts_list[-1], last, 0.0, 0.0, 0.0, 0.0)
        else:
            buf.phase     = 'BOUNCE'
            buf.bounce_pk = last
            buf.peak      = max(closes[-50:]) if n_bars >= 50 else pk

    # Seed px_buf for velocity window (last 120 candle closes)
    start_px = max(0, n_bars - 120)
    buf.px_buf  = [(ts_list[i], closes[i]) for i in range(start_px, n_bars)]
    buf.last_ts = ts_list[-1] if ts_list else None

    if raw_lines:
        recs = []
        for ln in raw_lines:
            if not ln: continue
            try: r = json.loads(ln)
            except Exception: continue
            if r[0] not in ('T', 'D'): continue
            recs.append(r)
        recs.sort(key=lambda r: r[1])
        if recs:
            buf.last_ts = None   # bridge candle→live gap; skip timeout reset
            last_px = None
            for r in recs:
                ts_ms = r[1]
                if r[0] == 'T':
                    last_px = r[2] / 100
                elif r[0] == 'D':
                    bids = [(p/100, q/10000) for p,q in r[2][:20]]
                    asks = [(p/100, q/10000) for p,q in r[3][:20]]
                    mid  = (bids[0][0]+asks[0][0])/2.0 if bids and asks else last_px
                    if mid is None: continue
                    px = last_px if last_px is not None else mid
                    buf.update(ts_ms, px, bids, asks)


def _update_htf(state: 'ScanState'):
    """
    Re-derive 5m/15m from closed_1m (fine, live layer).
    1h/4h: blend historical 1h seed with live-derived 1h tail so both TFs
    have meaningful depth from session start rather than waiting hours.
    Live-derived bars always win at overlap (same ts = more accurate OHLC).
    """
    bars = state.closed_1m
    if not bars:
        return
    state.tf5m  = _agg(bars, 300_000)
    state.tf15m = _agg(bars, 900_000)
    live_1h = _agg(bars, 3_600_000)
    if state.tf1h_seed:
        live_ts    = {b['ts'] for b in live_1h}
        merged_1h  = [b for b in state.tf1h_seed if b['ts'] not in live_ts] + live_1h
        state.tf1h = sorted(merged_1h, key=lambda b: b['ts'])
    else:
        state.tf1h = live_1h
    state.tf4h = _agg(state.tf1h, 14_400_000)


def _append_closed_1m(state: 'ScanState', s1_by_sec: dict, min_sec: int):
    """Build a complete 1m bar from all per-second bars in s1_by_sec for min_sec."""
    secs = sorted(s for s in s1_by_sec if min_sec <= s < min_sec + 60)
    if not secs:
        return
    bars = [s1_by_sec[s] for s in secs]
    state.closed_1m.append({
        'ts':        min_sec * 1000,
        'open':      bars[0]['open'],
        'high':      max(b['high'] for b in bars),
        'low':       min(b['low']  for b in bars),
        'close':     bars[-1]['close'],
        'volume':    sum(b['volume']    for b in bars),
        'taker_buy': sum(b['taker_buy'] for b in bars),
    })
    state.appended_min_ts.add(min_sec)
    _update_htf(state)


def scan_incremental(state: ScanState, from_sec: int = 0,
                     signals_only: bool = True,
                     raw_lines: list = None,
                     seed_bars: list = None) -> list:
    """
    Incremental scan — fast live scanner.

    First call: seeds state from exchange bars passed in directly.
    Subsequent calls: processes new seconds from the raw deque.
    Returns list of new signal dicts emitted this call.
    """
    raw = raw_lines if raw_lines is not None else _fetch_raw()

    if not state.last_sec:
        import time as _t
        now_s = int(_t.time())
        tf1m  = seed_bars or []
        if not tf1m:
            return []
        _seed_state_from_candles(state, tf1m, {}, raw_lines=None)
        state.s1_by_sec = {}
        state.ob_by_sec = {}
        state.last_sec  = now_s - 1
        return []
    else:
        # Subsequent calls: only parse lines newer than last processed second
        cutoff_ms = state.last_sec * 1000
        new_raw   = [ln for ln in raw if ln
                     and (ln[2] == 'T' or ln[2] == 'D')
                     and _quick_ts(ln) > cutoff_ms]
        if not new_raw:
            return []
        # Append new 1s bars to existing s1_by_sec
        s1_by_sec = state.s1_by_sec
        ob_by_sec = state.ob_by_sec
        # Seed close from last candle so OB-only seconds get a valid price anchor
        seed_close = state.closed_1m[-1]['close'] if state.closed_1m else None
        _extend_s1(s1_by_sec, ob_by_sec, new_raw, seed_close=seed_close)
        start_at  = state.last_sec + 1

    s1_secs  = sorted(s1_by_sec.keys())
    if not s1_secs:
        return []
    # Exclude the most-recent (in-progress) second — it hasn't closed yet.
    # A second is confirmed only when a newer second has started, guaranteeing
    # all its trades are accumulated. Phase machine runs on the full bar close,
    # not a partial sub-second snapshot.
    max_confirmed = s1_secs[-1] - 1 if len(s1_secs) > 1 else -1
    new_secs = [s for s in s1_secs if s >= start_at and s <= max_confirmed]
    if not new_secs:
        return []
    tf1m  = state.tf1m;  tf5m  = state.tf5m
    tf15m = state.tf15m; tf1h  = state.tf1h; tf4h = state.tf4h

    # Group new seconds by minute
    by_min = collections.defaultdict(list)
    for s in new_secs:
        by_min[(s // 60) * 60].append(s)

    new_signals = []

    for min_sec in sorted(by_min.keys()):
        secs_in_min = sorted(by_min[min_sec])

        b5  = [b for b in tf5m  if b['ts']//1000 <= min_sec][-30:]
        b15 = [b for b in tf15m if b['ts']//1000 <= min_sec][-20:]
        b1h = [b for b in tf1h  if b['ts']//1000 <= min_sec][-12:]
        b4h = [b for b in tf4h  if b['ts']//1000 <= min_sec][-6:]

        thresh, peak_ph, trough_ph, obi_conf, gate_rev, gate_cont, align_cont = \
            _thresholds(state.hist_scores, state.hist_phases, state.hist_obi,
                        state.hist_kdv_bals, state.hist_aligns)

        # HTF/MTF context — computed once per minute batch from closed bars only.
        # Moving this before the inner loop lets gate checks fire per-second.
        _htf_px = state.closed_1m[-1]['close'] if state.closed_1m else 0.0
        htf_ctx = htf.regime(_htf_px, b1h, b4h, state.closed_1m[-30:], b5, b15)
        at_sup  = htf_ctx.get('at_support',    False)
        at_res  = htf_ctx.get('at_resistance', False)
        try:
            res_out    = RES.resonance(state.closed_1m[-30:], b5, b15, b1h, b4h)
            res_dir    = res_out.get('direction', 0)
            align      = res_out.get('alignment', 0.0)
            dissonance = res_out.get('dissonance', False)
        except Exception:
            res_dir=0; align=0.0; dissonance=False
        state.last_align   = align
        state.last_res_dir = res_dir
        state.last_htf_sup = at_sup

        # TF phases for composite — stable each minute (closed bars only)
        _tf_ph_5m_i  = _wf_phase(b5)
        _tf_ph_15m_i = _wf_phase(b15)
        _tf_ph_1h_i  = _wf_phase(b1h)
        _tf_ph_4h_i  = _wf_phase(b4h)

        p_opens=[]; p_closes=[]; p_vols=[]; p_tb=[]
        sustain_count=0; prev_sb=0
        prev_kdv_min=state.prev_kdv_global
        kdv_flipped_up=False; kdv_flipped_down=False
        final={}
        best_obi_long=0.0; best_obi_short=0.0
        min_dk_state=0; min_dk_ds=0.0; min_dk_fos=0.0; min_dk_lbl='NEUT'

        for sec in secs_in_min:
            bar1s = s1_by_sec.get(sec)
            if bar1s is None: continue

            p_opens.append(bar1s['open'])
            p_closes.append(bar1s['close'])
            p_vols.append(bar1s['volume'])
            p_tb.append(bar1s.get('taker_buy', bar1s['volume']*0.5))

            p_close = p_closes[-1]
            partial = {'open':p_opens[0],'high':max(p_closes),'low':min(p_closes),
                       'close':p_close,'volume':sum(p_vols),'taker_buy':sum(p_tb)}
            window = state.closed_1m[-30:] + [partial]
            if len(window) < 10: continue

            closes_a, opens_a, volumes_a, taker_buy_a = _bars2arr(window)
            bids, asks = ob_by_sec.get(sec, ([], []))

            # Knife-decay via stateful buffer
            ts_ms = sec * 1000
            last_px = p_close
            dk_st, dk_ds, dk_fos, dk_lbl = state.knife_buf.update(
                ts_ms, last_px, bids, asks)
            if dk_st > min_dk_state:
                min_dk_state=dk_st; min_dk_ds=dk_ds
                min_dk_fos=dk_fos; min_dk_lbl=dk_lbl

            # Micro layer
            state.micro_window.append(bar1s)
            if len(state.micro_window) > 60:
                state.micro_window = state.micro_window[-60:]
            micro_sc=0.0; micro_ph=0.0; micro_kdv=0
            if len(state.micro_window) >= 15:
                mc = np.array([b['close']  for b in state.micro_window], dtype=float)
                mo = np.array([b['open']   for b in state.micro_window], dtype=float)
                mv = np.array([b['volume'] for b in state.micro_window], dtype=float)
                mt = np.array([b.get('taker_buy', b['volume']*0.5)
                               for b in state.micro_window], dtype=float)
                try:
                    mp   = (mc + mo) / 2.0
                    mfus = fusion.run(mp, mo, mc, mv, mt, state.micro_accum, [], [])
                    micro_sc  = mfus['score']
                    micro_kdv = mfus['soliton']['direction']
                    mwf = WF.run(mc, entry=mc[-1],
                                 direction=1 if micro_sc >= 0 else -1)
                    micro_ph = mwf['components'].get('micro',{}).get('phase', 0.0)
                except Exception:
                    pass

            try:
                prices_a = (closes_a + opens_a) / 2.0
                fus  = fusion.run(prices_a, opens_a, closes_a, volumes_a,
                                  taker_buy_a, state.accum, bids, asks)
                kdv      = fus['soliton']['direction']
                kdv_bal  = abs(fus['soliton'].get('balance', 0.0))
                wh       = fus['water_hammer']['detected']
                f_sc     = fus['score']
                obi_raw  = fus.get('obi', {})
                obi_p    = (obi_raw.get('obi', 0.0)
                            if isinstance(obi_raw, dict) else float(obi_raw or 0.0))
            except Exception:
                kdv=0; kdv_bal=0.0; wh=False; f_sc=0.0; obi_p=0.0

            kdv_just_flipped = (kdv != 0 and kdv != prev_kdv_min and prev_kdv_min != 0)
            if kdv_just_flipped:
                if kdv > 0: kdv_flipped_up   = True
                if kdv < 0: kdv_flipped_down = True
            prev_kdv_min = kdv if kdv != 0 else prev_kdv_min

            flip_active = kdv_flipped_up or kdv_flipped_down
            if flip_active:
                sb = int(np.sign(f_sc)) if abs(f_sc) > 0.01 else 0
                sc_gate = 0.01
            else:
                sc_gate = thresh
                sb = (1 if f_sc >= thresh else -1 if f_sc <= -thresh else 0)

            if obi_p > best_obi_long:  best_obi_long  = obi_p
            if obi_p < best_obi_short: best_obi_short = obi_p

            wf_direction = sb if sb != 0 else (1 if f_sc >= 0 else -1)
            try:
                wf   = WF.run(closes_a, entry=p_close, direction=wf_direction)
                comp = wf['components']
                itf  = wf['interference']
                tgts = wf.get('targets', {})
                micro_phase = comp.get('micro', {}).get('phase', 0.0)
            except Exception:
                comp={}; itf={'direction':0,'type':'cons'}; tgts={}; micro_phase=0.0

            if sb != 0 and sb == prev_sb:
                sustain_count += 1
            elif sb != 0:
                sustain_count = 1
            else:
                sustain_count = 0
            prev_sb = sb

            sustain_needed = SUSTAIN_FLIP if (kdv_flipped_up or kdv_flipped_down) else SUSTAIN_S

            composite_phase_i = (
                _COMP_W['1m']  * micro_phase   +
                _COMP_W['5m']  * _tf_ph_5m_i   +
                _COMP_W['15m'] * _tf_ph_15m_i  +
                _COMP_W['1h']  * _tf_ph_1h_i   +
                _COMP_W['4h']  * _tf_ph_4h_i
            )
            # ── Pattern observation — order flow patterns at price extremes ───
            # composite_phase_i kept for display only, not for state management.
            # _observe_structural uses the rolling micro_window — no minute resets.
            obs_label_i = _observe_structural(
                p_close, obi_p, kdv_flipped_up, kdv_flipped_down,
                state.micro_window, res_dir=res_dir, bars_5m=b5,
                bars_1m=state.closed_1m)

            if obs_label_i == 'MID':
                state.last_1m_state = 'MID'
            else:
                _fire_i = False
                if obs_label_i == 'CONT_UP':
                    if p_close > state.last_cont_up_px:
                        _fire_i = True
                        state.last_cont_up_px  = p_close
                        state.last_peak_px     = p_close        # PEAK can't fire at this same level
                        state.last_range_hi_px =  float('inf')  # trending — range ref invalid
                        state.last_range_lo_px = -float('inf')
                elif obs_label_i == 'CONT_DOWN':
                    if p_close < state.last_cont_dn_px:
                        _fire_i = True
                        state.last_cont_dn_px  = p_close
                        state.last_trough_px   = p_close        # TROUGH can't fire at this same level
                        state.last_range_hi_px =  float('inf')  # trending — range ref invalid
                        state.last_range_lo_px = -float('inf')
                elif obs_label_i == 'PEAK':
                    if p_close > state.last_peak_px:
                        _fire_i = True
                        state.last_peak_px     = p_close
                        state.last_trough_px   =  float('inf')  # reset opposite
                        state.last_cont_dn_px  =  float('inf')  # down leg starts fresh after peak
                        state.last_cont_up_px  = p_close        # CONT_UP must exceed this peak
                        state.last_range_hi_px = p_close        # next RANGE_HI must be below this peak
                elif obs_label_i == 'TROUGH':
                    if p_close < state.last_trough_px:
                        _fire_i = True
                        state.last_trough_px   = p_close
                        state.last_peak_px     = -float('inf')  # reset opposite
                        state.last_cont_up_px  = -float('inf')  # up leg starts fresh after trough
                        state.last_cont_dn_px  = p_close        # CONT_DOWN must go below this trough
                        state.last_range_lo_px = p_close        # next RANGE_LO must be above this trough
                elif obs_label_i == 'RANGE_HI':
                    if p_close < state.last_range_hi_px:        # lower-high confirms range
                        _fire_i = True
                        state.last_range_hi_px = p_close
                        state.last_cont_up_px  = p_close        # CONT_UP must exceed this range high
                elif obs_label_i == 'RANGE_LO':
                    if p_close > state.last_range_lo_px:        # higher-low confirms range
                        _fire_i = True
                        state.last_range_lo_px = p_close
                        state.last_cont_dn_px  = p_close        # CONT_DOWN must go below this range low

                if _fire_i:
                    state.last_1m_state = obs_label_i
                    obs_dir_i  = +1 if obs_label_i in ('TROUGH', 'CONT_UP', 'RANGE_LO') else -1
                    side_oi    = 'LONG' if obs_dir_i > 0 else 'SHORT'
                    arrow_oi   = '▲' if obs_dir_i > 0 else '▼'
                    c_oi       = G if obs_dir_i > 0 else R
                    bar_oi     = '━' * 50

                    _n1oi   = min(60, len(state.micro_window))
                    vol3_oi = sum(b['volume'] for b in state.micro_window[-_n1oi:])
                    tb3_oi  = sum(b.get('taker_buy', b['volume']*0.5) for b in state.micro_window[-_n1oi:])
                    tb_r_oi = tb3_oi / vol3_oi if vol3_oi > 1e-8 else 0.5
                    kdv_ev_oi = ('kdv↑' if kdv_flipped_up else
                                 'kdv↓' if kdv_flipped_down else
                                 f'kdv={kdv_bal:+.1f}')
                    shelf_oi = _shelf_context(state.session_shelves, p_close)
                    t1moi= htf_ctx.get('trend_1m','ne')[:2]
                    t5moi= htf_ctx.get('trend_5m','ne')[:2]
                    t15oi= htf_ctx.get('trend_15m','ne')[:2]
                    t1hoi= htf_ctx.get('trend_1h','ne')[:2]
                    t4hoi= htf_ctx.get('trend_4h','ne')[:2]
                    def _tr2oi(t): return {'up':'↑','do':'↓','ne':'─'}.get(t[:2],'─')
                    mtf_oi = (f"1m{_tr2oi(t1moi)}  5m{_tr2oi(t5moi)}  15m{_tr2oi(t15oi)}"
                              f"  1h{_tr2oi(t1hoi)}  4h{_tr2oi(t4hoi)}")

                    # Range tightening metric — only for RANGE_HI/RANGE_LO
                    range_tight_oi = ''
                    if obs_label_i in ('RANGE_HI', 'RANGE_LO'):
                        _mw_oi  = state.micro_window
                        _mm_oi  = len(_mw_oi)
                        _nc1_oi = min(60, _mm_oi)
                        _cls_oi = [b['close'] for b in _mw_oi]
                        _r1_oi  = max(_cls_oi[-_nc1_oi:]) - min(_cls_oi[-_nc1_oi:])
                        _r2_oi  = max(_cls_oi)            - min(_cls_oi)
                        if _r2_oi > 1e-8:
                            _ratio_oi   = _r1_oi / _r2_oi
                            _tight_pct_oi = int((1.0 - _ratio_oi) * 100)
                            _tight_str_oi = (f"tight +{_tight_pct_oi}%"
                                             if _tight_pct_oi >= 0
                                             else f"expand +{-_tight_pct_oi}%")
                            range_tight_oi = (f"range: {_tight_str_oi}"
                                              f"  (1m=${_r1_oi:.2f}  2m=${_r2_oi:.2f})")

                    state.sig_count += 1
                    sig_i = {
                        'n':      state.sig_count,
                        'dir':    obs_dir_i,
                        'label':  obs_label_i,
                        'time':   _ts(sec),
                        'price':  p_close,
                        'taker':  round(tb_r_oi, 3),
                        'obi':    round(obi_p, 3),
                        'kdv':    kdv_ev_oi,
                        'cph':    round(composite_phase_i, 3),
                        'min_sec': min_sec,
                    }
                    state.signals.append(sig_i)
                    new_signals.append(sig_i)

                    if signals_only:
                        print(f"\n{c_oi}{bar_oi}")
                        print(f"  {arrow_oi}  {side_oi}  ·  {obs_label_i:<20}  [{state.sig_count}]")
                        print(f"     {_ts(sec)}  ·  ${p_close:>10,.2f}")
                        print(f"     taker {tb_r_oi*100:.0f}%  ·  obi={obi_p:+.3f}  ·  {kdv_ev_oi}")
                        print(f"     cph={composite_phase_i:+.3f}  ·  {mtf_oi}")
                        if range_tight_oi: print(f"     {range_tight_oi}")
                        if shelf_oi: print(f"     {shelf_oi}")
                    print(f"{bar_oi}{Z}\n")

            # ── Ranging oscillation signal ─────────────────────────────────────
            rng_dir_i, rng_sup_i, rng_res_i, rng_span_i = _range_signal(
                state.session_shelves, p_close, state.last_range_state)
            if rng_dir_i != 0:
                side_ri  = 'LONG' if rng_dir_i > 0 else 'SHORT'
                arrow_ri = '▲' if rng_dir_i > 0 else '▼'
                c_ri     = G if rng_dir_i > 0 else R
                bar_ri   = '━' * 50
                sup_bias_i = ('L' if rng_sup_i['buy_ratio'] >= 0.55
                              else 'S' if rng_sup_i['buy_ratio'] <= 0.45 else '~')
                res_bias_i = ('L' if rng_res_i['buy_ratio'] >= 0.55
                              else 'S' if rng_res_i['buy_ratio'] <= 0.45 else '~')
                conf_ri = 'hi-conf' if (rng_dir_i > 0 and sup_bias_i == 'L') or \
                                       (rng_dir_i < 0 and res_bias_i == 'S') else \
                          'lo-conf' if (rng_dir_i > 0 and sup_bias_i == 'S') or \
                                       (rng_dir_i < 0 and res_bias_i == 'L') else 'mid'
                t1mri= htf_ctx.get('trend_1m','ne')[:2]; t5mri= htf_ctx.get('trend_5m','ne')[:2]
                t15ri= htf_ctx.get('trend_15m','ne')[:2]; t1hri= htf_ctx.get('trend_1h','ne')[:2]
                t4hri= htf_ctx.get('trend_4h','ne')[:2]
                def _tr2ri(t): return {'up':'↑','do':'↓','ne':'─'}.get(t[:2],'─')
                mtf_ri = (f"1m{_tr2ri(t1mri)}  5m{_tr2ri(t5mri)}  15m{_tr2ri(t15ri)}"
                          f"  1h{_tr2ri(t1hri)}  4h{_tr2ri(t4hri)}")
                state.sig_count += 1
                if signals_only:
                    print(f"\n{c_ri}{bar_ri}")
                    print(f"  {arrow_ri}  {side_ri}  ·  {'RANGE '+side_ri:<20}  [{state.sig_count}]  {conf_ri}")
                    print(f"     {_ts(sec)}  ·  ${p_close:>10,.2f}")
                    print(f"     range ${rng_span_i:.0f}"
                          f"  ·  sup ${rng_sup_i['level']:,.2f}({sup_bias_i},{rng_sup_i['bars']}bar)"
                          f"  →  res ${rng_res_i['level']:,.2f}({res_bias_i},{rng_res_i['bars']}bar)")
                    print(f"     {mtf_ri}")
                    print(f"{bar_ri}{Z}\n")
                rng_sig_i = {
                    'type': 'RANGE-' + side_ri,
                    'dir':  rng_dir_i,
                    'entry': p_close,
                    'time':  _ts(sec),
                    'span':  rng_span_i,
                    'sup':   rng_sup_i['level'],
                    'res':   rng_res_i['level'],
                    'conf':  conf_ri,
                }
                state.signals.append(rng_sig_i)
                new_signals.append(rng_sig_i)
                state.last_range_state = 'sup' if rng_dir_i > 0 else 'res'
            elif state.last_range_state is not None and state.session_shelves:
                open_b_ri = [s for s in state.session_shelves
                             if s['level'] < p_close and s['broken'] == 'open']
                open_a_ri = [s for s in state.session_shelves
                             if s['level'] > p_close and s['broken'] == 'open']
                if open_b_ri and open_a_ri:
                    mid_ri = (open_b_ri[-1]['level'] + open_a_ri[0]['level']) / 2
                    if (state.last_range_state == 'sup' and p_close > mid_ri) or \
                       (state.last_range_state == 'res' and p_close < mid_ri):
                        state.last_range_state = None

            final = {'sec':sec,'price':p_close,'score':f_sc,'kdv':kdv,
                     'kdv_bal':kdv_bal,'obi':obi_p,'micro_phase':micro_phase,
                     'composite_phase':composite_phase_i,
                     'micro_sc':micro_sc,'micro_ph':micro_ph,'micro_kdv':micro_kdv}

        if not final:
            if (s1_secs[-1] >= min_sec + 60
                    and min_sec not in state.appended_min_ts):
                _append_closed_1m(state, s1_by_sec, min_sec)
                state.session_shelves = _detect_shelves(state.closed_1m[-60:])
            continue

        state.hist_scores.append(abs(final['score']))
        state.hist_phases.append(final['composite_phase'])   # composite calibrates threshold
        state.hist_obi.append(final['obi'])
        state.hist_kdv_bals.append(final['kdv_bal'])
        state.hist_aligns.append(align)
        state.hist_signed_scores.append(final['score'])
        state.hist_dk_ds.append(min_dk_ds)
        state.hist_dk_fos.append(min_dk_fos)

        state.session_shelves = _detect_shelves(state.closed_1m[-60:])

        kdv_f = final.get('kdv', 0)
        if kdv_f != 0: state.prev_kdv_global = kdv_f

        # ── Per-TF independent phase detection ───────────────────────────
        tf_bars_map_i  = [('5m', b5), ('15m', b15), ('1h', b1h), ('4h', b4h)]
        tf_sb_i        = final.get('score', 0.0)
        tf_sb_dir_i    = 1 if tf_sb_i >= 0 else -1
        bids_i, asks_i = ob_by_sec.get(s1_secs[-1], ([], []))
        for tf_lbl_i, tf_bars_i in tf_bars_map_i:
            tf_state_now_i, tf_phase_i, tf_score_i, tf_wf_dir_i, tf_fail_i = \
                _tf_phase_state(tf_bars_i, tf_lbl_i)
            tf_state_prev_i = state.last_tf_states[tf_lbl_i]
            if tf_state_now_i != 'MID':
                state.last_tf_states[tf_lbl_i] = tf_state_now_i
            if tf_state_now_i == 'MID':
                if not tf_fail_i:
                    state.last_tf_states[tf_lbl_i] = 'MID'
                continue
            if tf_state_now_i == tf_state_prev_i:
                continue
            tf_stype_i = _sig_type(tf_state_now_i,
                                   tf_wf_dir_i if tf_wf_dir_i != 0 else tf_sb_dir_i)
            tf_dir_i   = 1 if tf_stype_i in ('TROUGH-REV', 'PEAK-CONT') else -1
            tf_entry_i = final.get('price', 0.0)
            state.sig_count += 1
            tf_cl_i, _, _, _ = _wall_absorption(
                bids_i, asks_i, tf_entry_i, state.closed_1m, b5, b15, tf_dir_i > 0)
            wa_tag_i  = f'wa={tf_cl_i}' if tf_cl_i else 'wa=X'
            g_i       = _TF_GATES[tf_lbl_i]
            tf_conf_i = (f'ph={tf_phase_i:+.3f}(≥{g_i["peak_ph"]:.2f})'
                         f'  rng={tf_score_i*100:.3f}%(≥{g_i["min_amp_pct"]*100:.2f}%)'
                         f'  {wa_tag_i}')
            tf_sig = {
                'n':      state.sig_count,
                'dir':    tf_dir_i,
                'stype':  tf_stype_i,
                'tf':     tf_lbl_i,
                'time':   _ts(min_sec),
                'price':  tf_entry_i,
                'confirm':tf_conf_i,
                'min_sec':min_sec,
            }
            state.signals.append(tf_sig)
            new_signals.append(tf_sig)
            if signals_only:
                tf_arrow_i = '▲' if tf_dir_i > 0 else '▼'
                tf_side_i  = 'LONG' if tf_dir_i > 0 else 'SHORT'
                tf_c_i     = G if tf_dir_i > 0 else R
                tf_bar_i   = '━' * 50
                print(f"\n{tf_c_i}{tf_bar_i}")
                print(f"  [{tf_lbl_i}] {tf_arrow_i}  {tf_side_i}  ·  {tf_stype_i:<20}  [{state.sig_count}]")
                print(f"     {_ts(min_sec)}  ·  ${tf_entry_i:>10,.2f}")
                print(f"     {tf_conf_i}")
                tf_shelf_ann_i = _shelf_context(state.session_shelves, tf_entry_i)
                if tf_shelf_ann_i: print(f"     {tf_shelf_ann_i}")
                print(f"{tf_bar_i}{Z}\n")

        if (s1_secs[-1] >= min_sec + 60
                and min_sec not in state.appended_min_ts):
            _append_closed_1m(state, s1_by_sec, min_sec)
            state.session_shelves = _detect_shelves(state.closed_1m[-60:])

    if new_secs:
        state.last_sec = new_secs[-1]

    return new_signals


if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Intrabar wave engine scan')
    p.add_argument('--all',     action='store_true', help='scan entire file')
    p.add_argument('--mins',    type=int, default=96, help='last N minutes (default 96)')
    p.add_argument('--session', type=int, default=0,  help='session to scan (0=latest, 1=previous, …)')
    p.add_argument('--signals', action='store_true',  help='clean signal cards only (no per-minute ticker)')
    args = p.parse_args()
    scan(mins_limit=None if args.all else args.mins,
         session_idx=args.session,
         signals_only=args.signals)
