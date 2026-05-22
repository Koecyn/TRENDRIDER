"""
Higher-timeframe regime context — entry filter, not a signal generator.

Uses pre-fetched 1m / 5m / 10m / 1h / 4h bars (seeded at startup via REST,
updated live) to determine price position within the full wave structure:

  continuation_setup  — HTF uptrend, not approaching resistance
  reversal_setup      — HTF downtrend, price at or near support
  falling_knife       — downtrend, price NOT at support AND short TFs not
                        showing counter-trend momentum → block entries

All five timeframes contribute to bias (shorter TFs get lower weight).
Short-term (5m/10m) showing counter-trend momentum prevents falling_knife
even when HTF is bearish, allowing reversal entries at support.
"""

import numpy as np
from . import config as C


# ─────────────────────────────────────────────────────────────────────────────

def _trend(highs: np.ndarray, lows: np.ndarray) -> str:
    """Compare first-third vs last-third: up / down / neutral."""
    if len(highs) < 4:
        return 'neutral'
    n = max(len(highs) // 3, 1)
    if (np.max(highs[-n:]) > np.max(highs[:n]) and
            np.min(lows[-n:]) > np.min(lows[:n])):
        return 'up'
    if (np.max(highs[-n:]) < np.max(highs[:n]) and
            np.min(lows[-n:]) < np.min(lows[:n])):
        return 'down'
    return 'neutral'


def _levels(bars: list, n: int = 20):
    """Recent swing high and swing low over last n bars."""
    b = bars[-n:] if len(bars) >= n else bars
    h = float(np.max([x['high'] for x in b]))
    l = float(np.min([x['low']  for x in b]))
    return h, l


# ─────────────────────────────────────────────────────────────────────────────

def regime(price: float,
           bars_1h: list, bars_4h: list,
           bars_1m: list = None,
           bars_5m: list = None,
           bars_15m: list = None) -> dict:
    """
    Compute HTF context across all available timeframes.

    Timeframe weights for bias (normalized to weights actually available):
      1m → 0.05   5m → 0.10   15m → 0.15   1h → 0.30   4h → 0.40

    Returns:
      trend_1m / trend_5m / trend_15m / trend_1h / trend_4h
      support / resistance  nearest key levels (all TFs combined)
      at_support / at_resistance  bool (within HTF_LEVEL_TOL)
      reversal_setup        HTF downtrend + at support
      continuation_setup    HTF uptrend + not at resistance
      falling_knife         HTF down, NOT at support, AND short TFs not
                            showing counter-trend (5m/10m both not up)
      bias                  float  -1 (bearish) … +1 (bullish)
    """
    tol = C.HTF_LEVEL_TOL

    ctx = {
        'trend_1m':           'neutral',
        'trend_5m':           'neutral',
        'trend_15m':          'neutral',
        'trend_1h':           'neutral',
        'trend_4h':           'neutral',
        'support':            0.0,
        'resistance':         0.0,
        'at_support':         False,
        'at_resistance':      False,
        'reversal_setup':     False,
        'continuation_setup': False,
        'falling_knife':      False,
        'bias':               0.0,
    }

    sup = res = None
    bias_raw   = 0.0
    total_w    = 0.0
    s_map      = {'up': 1.0, 'neutral': 0.0, 'down': -1.0}

    tf_specs = [
        (bars_1m,   0.05, '1m'),
        (bars_5m,   0.10, '5m'),
        (bars_15m,  0.15, '15m'),
        (bars_1h,   0.30, '1h'),
        (bars_4h,   0.40, '4h'),
    ]

    for bars, w, label in tf_specs:
        if not bars or len(bars) < 4:
            continue
        h = np.array([b['high'] for b in bars])
        l = np.array([b['low']  for b in bars])
        trend = _trend(h, l)
        ctx[f'trend_{label}'] = trend
        r, s = _levels(bars)
        if sup is None or s > sup: sup = s
        if res is None or r < res: res = r
        bias_raw += s_map[trend] * w
        total_w  += w

    ctx['support']    = float(sup) if sup else 0.0
    ctx['resistance'] = float(res) if res else 0.0

    if sup and sup > 0:
        ctx['at_support']    = abs(price - sup) / sup < tol
    if res and res > 0:
        ctx['at_resistance'] = abs(price - res) / res < tol

    t1h, t4h = ctx['trend_1h'], ctx['trend_4h']
    t5m, t15m = ctx['trend_5m'], ctx['trend_15m']

    htf_down = (t1h == 'down' or t4h == 'down')
    htf_up   = (t1h == 'up'   or t4h == 'up')

    # Short-term counter-trend momentum — prevents falling_knife block
    stf_up   = (t5m == 'up' or t15m == 'up')

    ctx['reversal_setup']     = htf_down and ctx['at_support']
    ctx['continuation_setup'] = htf_up and not ctx['at_resistance']
    # Falling knife only when HTF down, not at support, AND shorter TFs
    # aren't showing a counter-trend bounce yet
    ctx['falling_knife']      = htf_down and not ctx['at_support'] and not stf_up

    bias = bias_raw / total_w if total_w > 0 else 0.0
    if ctx['at_support']:    bias += 0.3
    if ctx['at_resistance']: bias -= 0.3
    ctx['bias'] = float(np.clip(bias, -1.0, 1.0))

    return ctx


# ─────────────────────────────────────────────────────────────────────────────

def allows_entry(sig: dict, ctx: dict) -> tuple:
    """
    Gate a fusion signal against HTF context.

    Returns (allowed: bool, reason: str).
    Falls back to True when ctx is None (no HTF data available).
    """
    if ctx is None:
        return True, ''

    # Falling knife: HTF down, not at support, short TFs not reversing → skip
    if ctx['falling_knife']:
        return False, (f"falling knife — trend down, not at support, "
                       f"5m/10m not turning up (bias={ctx['bias']:.2f})")

    # Reversal: downtrend at support.
    # Require short-TF momentum turning (5m or 15m not still down) — entering
    # long while every timeframe is pointing down is fighting the full trend.
    if ctx['reversal_setup']:
        t5m  = ctx.get('trend_5m',  'down')
        t15m = ctx.get('trend_15m', 'down')
        stf_turning = (t5m != 'down' or t15m != 'down')
        if not stf_turning:
            return False, (f"reversal setup but short TFs still down "
                           f"(5m={t5m} 15m={t15m}) — wait for turn")

        wh  = sig.get('water_hammer', {})
        cvd = sig.get('cvd', {})
        obi = sig.get('obi', {})
        confirmed = (
            wh.get('detected') or
            (cvd.get('strong') and cvd.get('divergence', 0) > 0) or
            obi.get('direction', 0) == 1
        )
        if not confirmed:
            return False, 'reversal setup but no wave confirmation (WH/CVD/OBI)'
        return True, 'reversal at HTF support'

    return True, ''
