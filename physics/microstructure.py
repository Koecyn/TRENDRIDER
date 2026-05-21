"""
Real market microstructure signals.

  CVD  — Cumulative Volume Delta (taker aggression)
  OBI  — Order Book Imbalance (weighted)

These are the 40% microstructure layer of the fusion score.
Taker data comes from Binance kline field k[9] (takerBuyBaseAssetVolume).
"""

import numpy as np
from . import config as C


# ─────────────────────────────────────────────────────────────────────────────
# CUMULATIVE VOLUME DELTA
# ─────────────────────────────────────────────────────────────────────────────

def compute_cvd(taker_buy: np.ndarray, total_volume: np.ndarray) -> np.ndarray:
    """
    delta[i] = taker_buy[i] - taker_sell[i]
             = 2 * taker_buy[i] - total_volume[i]
    CVD = cumulative sum of delta — shows net aggressor pressure.
    """
    delta = 2.0 * taker_buy - total_volume
    return np.cumsum(delta)


def cvd_signal(prices: np.ndarray, cvd: np.ndarray) -> dict:
    """
    Divergence between price slope and CVD slope.
    Price up + CVD down → distribution (bearish).
    Price down + CVD up → accumulation (bullish).

    Returns: divergence value, signal direction for scoring
    """
    w = C.CVD_WINDOW if hasattr(C, 'CVD_WINDOW') else 10
    w = min(w, len(prices), len(cvd))
    if w < 2:
        return {'divergence': 0.0, 'direction': 0, 'strong': False}

    eps      = 1e-10
    p_slope  = float(prices[-1] - prices[-w])
    c_slope  = float(cvd[-1] - cvd[-w])
    p_norm   = p_slope / (abs(p_slope) + eps)
    c_norm   = c_slope / (abs(c_slope) + eps)
    div      = p_norm - c_norm   # >0 = price up, CVD down = bearish divergence

    strong   = abs(div) > C.CVD_DIV
    # Scoring direction: divergence opposes price → fade price direction
    direction = int(-np.sign(div)) if strong else 0

    return {'divergence': float(div), 'direction': direction, 'strong': strong}


def cvd_from_taker_ratio(taker_buy: np.ndarray, total_volume: np.ndarray,
                          window: int = 5) -> float:
    """
    OBI proxy when order book unavailable.
    taker_buy_ratio > 0.5 → aggressive buyers dominate → positive OBI proxy.
    Returns value in [-1, +1].
    """
    w = min(window, len(taker_buy), len(total_volume))
    if w < 1:
        return 0.0
    buy   = float(np.sum(taker_buy[-w:]))
    total = float(np.sum(total_volume[-w:])) + 1e-10
    ratio = buy / total        # 0 → 1
    return (ratio - 0.5) * 2  # → −1 to +1


# ─────────────────────────────────────────────────────────────────────────────
# ORDER BOOK IMBALANCE
# ─────────────────────────────────────────────────────────────────────────────

def obi(bids: list, asks: list) -> float:
    """
    Weighted Order Book Imbalance.
    Levels closer to mid carry more weight (1 / distance).
    Returns value in [−1, +1].
      +1 = all bids (max buy pressure)
      −1 = all asks (max sell pressure)
    """
    if not bids or not asks:
        return 0.0

    mid = (bids[0][0] + asks[0][0]) / 2.0

    bid_w = sum(q / (abs(p - mid) + 1.0) for p, q in bids)
    ask_w = sum(q / (abs(p - mid) + 1.0) for p, q in asks)
    total = bid_w + ask_w + 1e-10

    return float((bid_w - ask_w) / total)


def obi_signal(bids: list, asks: list,
               darcy_Q: float,
               taker_buy: np.ndarray = None,
               total_vol: np.ndarray = None) -> dict:
    """
    Combined OBI signal with Darcy modulation.
    Thick book + high OBI = blocked (don't trust).
    Thin book + high OBI = high conviction directional.

    Returns: obi_value, contribution (to micro score), direction
    """
    if bids and asks:
        obi_val = obi(bids, asks)
    elif taker_buy is not None and total_vol is not None:
        obi_val = cvd_from_taker_ratio(taker_buy, total_vol)
    else:
        return {'obi': 0.0, 'contribution': 0.0, 'direction': 0}

    if abs(obi_val) < C.OBI_SIGNAL:
        return {'obi': float(obi_val), 'contribution': 0.0, 'direction': 0}

    contrib   = float(np.sign(obi_val) * min(darcy_Q * 0.1, 0.15))
    direction = int(np.sign(obi_val))
    return {'obi': float(obi_val), 'contribution': contrib, 'direction': direction}
