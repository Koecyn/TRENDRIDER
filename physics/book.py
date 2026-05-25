"""
L20 order book depth analyzer.

Computes structural features from the L20 snapshot every 100ms:
  curve_shape      — Gini concentration of qty across levels
  weighted_asymmetry — USD-weighted bid/ask pressure ratio
  void_score       — fraction of levels with near-zero qty (easy to push through)
  replenishment_rate — how much new liquidity appeared since last snapshot
  analyze          — composite global_pressure in [-1, +1]

Positive global_pressure = bid-heavy (accumulation / global buying pressure).
Negative global_pressure = ask-heavy (distribution / global selling pressure).

Called from LiveEngine._handle_depth() at 100ms cadence — must be lightweight.
"""

import numpy as np


def curve_shape(levels: list) -> float:
    """Gini coefficient of qty distribution (0 = flat/spread, 1 = all in one level)."""
    if not levels:
        return 0.0
    qtys = np.array([q for _, q in levels], dtype=float)
    total = qtys.sum()
    if total == 0:
        return 0.0
    qtys = np.sort(qtys)
    n    = len(qtys)
    idx  = np.arange(1, n + 1)
    return float((2.0 * np.sum(idx * qtys) / (n * total)) - (n + 1.0) / n)


def weighted_asymmetry(bids: list, asks: list) -> float:
    """
    USD-weighted imbalance across all levels.
    +1 = all USD on bid side, -1 = all USD on ask side.
    """
    if not bids or not asks:
        return 0.0
    bid_usd = sum(p * q for p, q in bids)
    ask_usd = sum(p * q for p, q in asks)
    total   = bid_usd + ask_usd
    if total == 0:
        return 0.0
    return float((bid_usd - ask_usd) / total)


def void_score(levels: list, threshold_frac: float = 0.05) -> float:
    """
    Fraction of levels with qty below threshold_frac × mean qty.
    High score = thin book = price moves easily through these levels.
    """
    if not levels:
        return 0.0
    qtys   = np.array([q for _, q in levels], dtype=float)
    mean_q = qtys.mean()
    if mean_q == 0:
        return 1.0
    return float(np.sum(qtys < threshold_frac * mean_q) / len(qtys))


def replenishment_rate(prev: list, curr: list) -> float:
    """
    Proportion of current book qty that is NEW since the previous snapshot.
    1 = entirely fresh liquidity (fast churn), 0 = book unchanged.
    """
    if not prev or not curr:
        return 0.0
    prev_map  = {p: q for p, q in prev}
    new_qty   = sum(max(0.0, q - prev_map.get(p, 0.0)) for p, q in curr)
    total_curr = sum(q for _, q in curr)
    if total_curr == 0:
        return 0.0
    return float(min(new_qty / total_curr, 1.0))


def analyze(bids: list, asks: list,
            prev_bids: list = None, prev_asks: list = None) -> dict:
    """
    Composite L20 structural analysis.

    Weights:
      50% asymmetry     — USD imbalance (strongest real-time signal)
      25% void_signal   — thin asks = easy upside / thin bids = easy downside
      15% rr_signal     — bids refilling faster = aggressive accumulation
      10% gini_signal   — concentrated asks vs flat bids = distribution

    Returns global_pressure in [-1, +1].
    """
    if not bids or not asks:
        return {
            'global_pressure': 0.0, 'asymmetry': 0.0,
            'bid_gini': 0.0,        'ask_gini':  0.0,
            'bid_void': 0.0,        'ask_void':  0.0,
            'bid_rr':   0.0,        'ask_rr':    0.0,
        }

    asym  = weighted_asymmetry(bids, asks)
    b_gin = curve_shape(bids)
    a_gin = curve_shape(asks)
    b_vd  = void_score(bids)
    a_vd  = void_score(asks)
    b_rr  = replenishment_rate(prev_bids or [], bids)
    a_rr  = replenishment_rate(prev_asks or [], asks)

    # Positive = bullish signal
    void_signal = a_vd  - b_vd   # thin asks  > thin bids → path up is easy
    rr_signal   = b_rr  - a_rr   # bids refilling faster → accumulation
    gini_signal = b_gin - a_gin   # concentrated bids vs flat asks → support wall

    gp = np.clip(
        0.50 * asym + 0.25 * void_signal + 0.15 * rr_signal + 0.10 * gini_signal,
        -1.0, 1.0,
    )

    return {
        'global_pressure': float(gp),
        'asymmetry':       float(asym),
        'bid_gini':        float(b_gin),
        'ask_gini':        float(a_gin),
        'bid_void':        float(b_vd),
        'ask_void':        float(a_vd),
        'bid_rr':          float(b_rr),
        'ask_rr':          float(a_rr),
    }
