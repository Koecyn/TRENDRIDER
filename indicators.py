"""
Multi-timeframe indicator library — all outputs are price-normalized (dimensionless).
"""

import numpy as np


# ── Rolling helpers ──────────────────────────────────────────────────────────

def _ema(arr: np.ndarray, span: int) -> np.ndarray:
    alpha = 2.0 / (span + 1)
    out = np.full(len(arr), np.nan, dtype=float)
    first_valid = np.where(~np.isnan(arr))[0]
    if len(first_valid) == 0:
        return out
    out[first_valid[0]] = arr[first_valid[0]]
    for i in range(first_valid[0] + 1, len(arr)):
        v = arr[i] if not np.isnan(arr[i]) else out[i - 1]
        out[i] = alpha * v + (1 - alpha) * out[i - 1]
    return out


def _rolling_min(arr: np.ndarray, window: int) -> np.ndarray:
    out = np.full(len(arr), np.nan, dtype=float)
    for i in range(window - 1, len(arr)):
        out[i] = arr[i - window + 1 : i + 1].min()
    return out


# ── Momentum (normalized, dimensionless) ─────────────────────────────────────

def momentum_pct(close: np.ndarray, fast: int = 5, slow: int = 20) -> np.ndarray:
    """
    EMA ribbon difference normalized by price → dimensionless momentum.
    ~0 = neutral, positive = uptrend, negative = downtrend.
    Range roughly ±0.01 for typical crypto 1m data.
    """
    ema_f = _ema(close, fast)
    ema_s = _ema(close, slow)
    diff = (ema_f - ema_s) / (close + 1e-12)
    return diff


def momentum_slope_pct(close: np.ndarray, fast: int = 5, slow: int = 20,
                       smooth: int = 3) -> np.ndarray:
    """
    1st derivative of normalized momentum → velocity.
    Positive → accelerating up; negative → decelerating / reversing.
    """
    mom = momentum_pct(close, fast, slow)
    slope = np.gradient(mom)
    return _ema(slope, smooth)


def roc(close: np.ndarray, period: int = 5) -> np.ndarray:
    """Price rate-of-change over `period` bars (fraction, not %)."""
    out = np.full(len(close), np.nan, dtype=float)
    out[period:] = (close[period:] - close[:-period]) / (close[:-period] + 1e-12)
    return out


# ── Volume / exhaustion ──────────────────────────────────────────────────────

def volume_exhaustion(volume: np.ndarray, window: int = 20) -> np.ndarray:
    """vol / rolling_mean_vol. <0.6 exhaustion, >1.8 spike."""
    n = len(volume)
    mean_vol = np.array([volume[max(0, i - window + 1): i + 1].mean()
                         for i in range(n)])
    return volume / (mean_vol + 1e-12)


def vwap_rolling(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                 volume: np.ndarray, window: int = 20) -> np.ndarray:
    """Rolling VWAP (window bars)."""
    tp = (high + low + close) / 3.0
    num = np.convolve(tp * volume, np.ones(window), "full")[: len(close)]
    den = np.convolve(volume, np.ones(window), "full")[: len(close)]
    vwap = num / (den + 1e-12)
    vwap[: window - 1] = np.nan
    return vwap


# ── Multi-timeframe resampling ───────────────────────────────────────────────

def resample_ohlcv(close: np.ndarray, high: np.ndarray, low: np.ndarray,
                   volume: np.ndarray, tf_bars: int):
    """
    Downsample 1m bars to a higher TF. Each output bar carries the
    LAST COMPLETED higher-TF bar's values → zero lookahead.
    """
    n = len(close)
    tf_close = np.full(n, np.nan)
    tf_high  = np.full(n, np.nan)
    tf_low   = np.full(n, np.nan)
    tf_vol   = np.full(n, np.nan)

    last_c = last_h = last_l = last_v = np.nan
    for i in range(n):
        if i % tf_bars == tf_bars - 1:
            s = i - tf_bars + 1
            last_c = close[i]
            last_h = high[s: i + 1].max()
            last_l = low[s: i + 1].min()
            last_v = volume[s: i + 1].sum()
        tf_close[i] = last_c
        tf_high[i]  = last_h
        tf_low[i]   = last_l
        tf_vol[i]   = last_v

    return tf_close, tf_high, tf_low, tf_vol


# ── Stop-loss helper ─────────────────────────────────────────────────────────

def trailing_low(low: np.ndarray, window: int) -> np.ndarray:
    return _rolling_min(low, window)
