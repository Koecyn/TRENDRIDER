"""
Causal indicator library — 5-second bar resolution.
All outputs price-normalized or dimensionless. Zero lookahead.
"""

import numpy as np


# ── Rolling helpers ───────────────────────────────────────────────────────────

def _ema(arr: np.ndarray, span: int) -> np.ndarray:
    alpha = 2.0 / (span + 1)
    out   = np.full(len(arr), np.nan, dtype=float)
    fv    = np.where(~np.isnan(arr))[0]
    if not len(fv): return out
    out[fv[0]] = arr[fv[0]]
    for i in range(fv[0] + 1, len(arr)):
        v = arr[i] if not np.isnan(arr[i]) else out[i-1]
        out[i] = alpha * v + (1-alpha) * out[i-1]
    return out

def _rolling_min(arr: np.ndarray, w: int) -> np.ndarray:
    out = np.full(len(arr), np.nan)
    for i in range(w-1, len(arr)):
        out[i] = arr[i-w+1:i+1].min()
    return out

def _rolling_mean(arr: np.ndarray, w: int) -> np.ndarray:
    """Causal rolling mean (no min_periods fill)."""
    out = np.full(len(arr), np.nan)
    cs  = np.cumsum(arr)
    out[w-1:] = (cs[w-1:] - np.concatenate([[0], cs[:-w]])[: len(cs)-w+1]) / w
    return out

def _rolling_std(arr: np.ndarray, w: int) -> np.ndarray:
    out = np.full(len(arr), np.nan)
    for i in range(w-1, len(arr)):
        out[i] = arr[i-w+1:i+1].std()
    return out


# ── RSI ───────────────────────────────────────────────────────────────────────

def rsi(close: np.ndarray, period: int = 14) -> np.ndarray:
    delta    = np.diff(close, prepend=close[0])
    gain     = np.maximum(delta, 0.0)
    loss     = np.maximum(-delta, 0.0)
    avg_gain = _ema(gain, 2*period - 1)
    avg_loss = _ema(loss, 2*period - 1)
    rs       = avg_gain / (avg_loss + 1e-12)
    return 100.0 - 100.0 / (1.0 + rs)


# ── Bollinger Bands ───────────────────────────────────────────────────────────

def bollinger(close: np.ndarray, window: int = 30, n_std: float = 2.0):
    """
    Returns (bb_mid, bb_lower, bb_upper, bb_pct_b, bb_std).
    bb_pct_b: 0=lower band, 0.5=mid, 1=upper band.
    bb_std  : rolling std (used for stop placement).
    """
    mid   = _rolling_mean(close, window)
    std   = _rolling_std(close, window)
    lower = mid - n_std * std
    upper = mid + n_std * std
    width = upper - lower + 1e-12
    pct_b = (close - lower) / width
    return mid, lower, upper, pct_b, std


# ── Momentum (normalized) ─────────────────────────────────────────────────────

def momentum_pct(close: np.ndarray, fast: int = 5, slow: int = 20) -> np.ndarray:
    ema_f = _ema(close, fast)
    ema_s = _ema(close, slow)
    return (ema_f - ema_s) / (close + 1e-12)

def momentum_slope_pct(close: np.ndarray, fast: int = 5, slow: int = 20,
                       smooth: int = 2) -> np.ndarray:
    mom = momentum_pct(close, fast, slow)
    return _ema(np.gradient(mom), smooth)


# ── Volume ────────────────────────────────────────────────────────────────────

def volume_exhaustion(volume: np.ndarray, window: int = 60) -> np.ndarray:
    n = len(volume)
    mv = np.array([volume[max(0,i-window+1):i+1].mean() for i in range(n)])
    return volume / (mv + 1e-12)

def vwap_rolling(high, low, close, volume, window: int = 60) -> np.ndarray:
    tp  = (high + low + close) / 3.0
    num = np.convolve(tp*volume, np.ones(window), "full")[:len(close)]
    den = np.convolve(volume,   np.ones(window), "full")[:len(close)]
    v   = num / (den + 1e-12)
    v[:window-1] = np.nan
    return v


# ── Multi-timeframe resampling ────────────────────────────────────────────────

def resample_ohlcv(close, high, low, volume, tf_bars: int):
    """Zero-lookahead resampling: each bar carries last COMPLETED TF bar."""
    n  = len(close)
    tc = np.full(n, np.nan); th = np.full(n, np.nan)
    tl = np.full(n, np.nan); tv = np.full(n, np.nan)
    lc = lh = ll = lv = np.nan
    for i in range(n):
        if i % tf_bars == tf_bars-1:
            s = i - tf_bars + 1
            lc = close[i]; lh = high[s:i+1].max()
            ll = low[s:i+1].min(); lv = volume[s:i+1].sum()
        tc[i], th[i], tl[i], tv[i] = lc, lh, ll, lv
    return tc, th, tl, tv


# ── Stop ──────────────────────────────────────────────────────────────────────

def trailing_low(low: np.ndarray, window: int) -> np.ndarray:
    return _rolling_min(low, window)
