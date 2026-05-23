"""
Multi-timeframe wave resonance engine.

Separates the raw price stream into independent wave fronts per timeframe.
Each TF runs its own KdV soliton — SGF + nonlinearity/dispersion decomposition.
Combining them reveals resonance (TFs aligned → big move coming) vs dissonance
(macro TFs opposing the signal → stay flat or trade tiny).

TF weights (normalized to what's available):
  1m → 0.05   5m → 0.10   15m → 0.20   1h → 0.30   4h → 0.35

Dissonance declared only when 1h or 4h KdV direction conflicts with the
weighted majority — short-TF noise never triggers dissonance alone.

Returns:
  score        float  weighted soliton score, −1.0 … +1.0
  direction    int    +1 / −1 / 0 dominant wave direction
  alignment    float  0.0 (fully opposed) … 1.0 (fully aligned)
  tf_scores    dict   per-TF signed soliton score
  tf_dirs      dict   per-TF wave direction
  dominant_tf  str    TF with greatest weighted score contribution
  dissonance   bool   macro TF (1h/4h) conflicts with majority direction
"""

import numpy as np
from .signals import kdv_soliton
from . import config as C

_TF_WEIGHTS = {
    '1m':  0.05,
    '5m':  0.10,
    '15m': 0.20,
    '1h':  0.30,
    '4h':  0.35,
}

_MIN_BARS = C.SOLITON_WIN + 5   # 25 bars minimum per TF


def _tf_score(bars: list) -> tuple:
    """
    Run KdV soliton on a TF bar list. Returns (score, direction, balance).
    score ∈ [−1, +1], signed by wave direction.
    Unconfirmed solitons contribute a 0.3-weighted directional lean.
    """
    if not bars or len(bars) < _MIN_BARS:
        return 0.0, 0, 0.0

    prices = np.array([b['close'] for b in bars], dtype=float)
    sol = kdv_soliton(prices)

    mean_p = float(np.mean(prices[-C.SOLITON_WIN:])) if len(prices) >= C.SOLITON_WIN else float(np.mean(prices))
    amplitude_pct = sol['amplitude'] / (mean_p + 1e-10)

    # tanh(balance × amplitude_pct × 10) maps typical values to [−1, +1]
    raw = sol['balance'] * amplitude_pct * 10.0
    mag = float(np.tanh(raw))

    if sol['detected']:
        direction = sol['direction']
        score = mag * direction
    else:
        # Directional lean without full soliton — price relative to window open
        direction = (int(np.sign(prices[-1] - prices[-C.SOLITON_WIN]))
                     if len(prices) >= C.SOLITON_WIN else 0)
        score = mag * 0.3 * direction if direction != 0 else 0.0

    return float(score), direction, float(sol['balance'])


def resonance(bars_1m: list, bars_5m: list, bars_15m: list,
              bars_1h: list, bars_4h: list) -> dict:
    """
    Compute multi-TF wave resonance across all available timeframes.

    Parameters: lists of bar dicts with at least a 'close' key.
    Any TF with < _MIN_BARS is skipped (weight reallocated to others).

    Returns dict: score, direction, alignment, tf_scores, tf_dirs,
                  dominant_tf, dissonance
    """
    tf_map = {
        '1m':  bars_1m  or [],
        '5m':  bars_5m  or [],
        '15m': bars_15m or [],
        '1h':  bars_1h  or [],
        '4h':  bars_4h  or [],
    }

    tf_scores = {}
    tf_dirs   = {}

    for label, bars in tf_map.items():
        s, d, _ = _tf_score(bars)
        tf_scores[label] = round(float(s), 4)
        tf_dirs[label]   = d

    # Weighted score — skip TFs that lack data
    total_w        = 0.0
    weighted_score = 0.0
    dominant_tf    = '1h'
    dominant_abs   = 0.0

    for label, w in _TF_WEIGHTS.items():
        bars = tf_map[label]
        if len(bars) < _MIN_BARS:
            continue
        s = tf_scores[label]
        weighted_score += s * w
        total_w        += w
        if abs(s) * w > dominant_abs:
            dominant_abs = abs(s) * w
            dominant_tf  = label

    if total_w > 0:
        weighted_score /= total_w

    direction = int(np.sign(weighted_score)) if abs(weighted_score) > 0.05 else 0

    # Alignment: weighted fraction of available TFs pointing same direction
    if direction == 0:
        alignment = 0.5
    else:
        aligned_w = sum(
            w for label, w in _TF_WEIGHTS.items()
            if len(tf_map[label]) >= _MIN_BARS and tf_dirs[label] == direction
        )
        alignment = aligned_w / total_w if total_w > 0 else 0.5

    # Dissonance: macro TF (1h or 4h) KdV direction opposes the majority
    dissonance = False
    if direction != 0:
        for macro in ('1h', '4h'):
            macro_dir = tf_dirs.get(macro, 0)
            if macro_dir != 0 and macro_dir != direction and len(tf_map[macro]) >= _MIN_BARS:
                dissonance = True
                break

    return {
        'score':       round(float(weighted_score), 4),
        'direction':   direction,
        'alignment':   round(float(alignment), 3),
        'tf_scores':   tf_scores,
        'tf_dirs':     tf_dirs,
        'dominant_tf': dominant_tf,
        'dissonance':  dissonance,
    }
