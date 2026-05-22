"""
Higher-timeframe regime context — entry filter, not a signal generator.

Uses pre-fetched 1h / 4h bars (seeded at startup via REST, updated live)
to determine price position within the larger wave structure:

  continuation_setup  — HTF uptrend, not approaching resistance
  reversal_setup      — HTF downtrend, price at or near support
  falling_knife       — downtrend, price NOT at support → block entries

Keeps physics/signals.py and physics/fusion.py untouched.
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

def regime(price: float, bars_1h: list, bars_4h: list) -> dict:
    """
    Compute HTF context from pre-fetched 1h and 4h bars.

    Returns:
      trend_1h / trend_4h  'up' | 'down' | 'neutral'
      support / resistance  nearest key levels
      at_support / at_resistance  bool (within HTF_LEVEL_TOL)
      reversal_setup        downtrend + at support
      continuation_setup    uptrend + not at resistance
      falling_knife         downtrend + NOT at support → block
      bias                  float  -1 (bearish) … +1 (bullish)
    """
    tol = C.HTF_LEVEL_TOL

    ctx = {
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

    for bars, w_trend, label in [(bars_1h, 0.4, '1h'), (bars_4h, 0.6, '4h')]:
        if len(bars) < 4:
            continue
        h = np.array([b['high'] for b in bars])
        l = np.array([b['low']  for b in bars])
        ctx[f'trend_{label}'] = _trend(h, l)
        r, s = _levels(bars)
        if sup is None or s > sup: sup = s
        if res is None or r < res: res = r

    ctx['support']    = float(sup) if sup else 0.0
    ctx['resistance'] = float(res) if res else 0.0

    if sup and sup > 0:
        ctx['at_support']    = abs(price - sup) / sup < tol
    if res and res > 0:
        ctx['at_resistance'] = abs(price - res) / res < tol

    t1, t4 = ctx['trend_1h'], ctx['trend_4h']

    ctx['reversal_setup']     = (t1 == 'down' or t4 == 'down') and ctx['at_support']
    ctx['continuation_setup'] = (t1 == 'up'   or t4 == 'up')   and not ctx['at_resistance']
    ctx['falling_knife']      = (t1 == 'down' or t4 == 'down') and not ctx['at_support']

    s_map = {'up': 1.0, 'neutral': 0.0, 'down': -1.0}
    bias  = s_map[t1] * 0.4 + s_map[t4] * 0.6
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

    # Falling knife: downtrend and NOT at support → skip
    if ctx['falling_knife']:
        return False, f"falling knife — trend down, not at support (bias={ctx['bias']:.2f})"

    # Reversal: downtrend at support — require water hammer OR CVD confirmation
    if ctx['reversal_setup']:
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
