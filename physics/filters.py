"""
Savitzky-Golay noise filter.

SGF preserves peak amplitude and phase — moving averages flatten both.
All physics equations must run on clean prices, never raw.
"""

import numpy as np
try:
    from scipy.signal import savgol_filter as _sgf
    _SCIPY = True
except ImportError:
    _SCIPY = False

from . import config as C


def sgf(prices: np.ndarray, window: int = None, polyorder: int = None) -> np.ndarray:
    """
    Apply Savitzky-Golay filter.  Returns clean (filtered) array same length as input.
    Falls back to centered moving average if scipy unavailable.
    """
    w = window or C.SGF_WINDOW
    p = polyorder or C.SGF_POLYORD
    arr = np.asarray(prices, dtype=float)
    if len(arr) < w:
        return arr.copy()
    if _SCIPY:
        return _sgf(arr, window_length=w, polyorder=p)
    # Fallback: centered MA
    half = w // 2
    out  = np.empty_like(arr)
    for i in range(len(arr)):
        lo, hi = max(0, i - half), min(len(arr), i + half + 1)
        out[i] = arr[lo:hi].mean()
    return out


def noise(prices: np.ndarray, clean: np.ndarray = None) -> np.ndarray:
    """Return noise residual: raw − filtered."""
    c = clean if clean is not None else sgf(prices)
    return np.asarray(prices, dtype=float) - c


def snr(prices: np.ndarray) -> float:
    """
    Signal-to-noise ratio of the price series.
    > 5 → reliable;  2-5 → moderate;  < 2 → stand aside.
    """
    arr = np.asarray(prices, dtype=float)
    c   = sgf(arr)
    n   = arr - c
    return float(np.sqrt(np.mean(c**2)) / (np.sqrt(np.mean(n**2)) + 1e-10))
