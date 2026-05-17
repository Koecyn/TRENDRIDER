"""
Synthetic OHLCV data generator mimicking real crypto market regimes.
Produces 1-minute bars with:
  - Trending macro periods (uptrend / downtrend / sideways)
  - Mean-reverting micro noise with fat tails
  - Volume clustering around price turning points
  - Spread simulation for maker/taker execution
"""

import numpy as np
import pandas as pd
from scipy.stats import t as student_t


def _regime_schedule(n_bars: int, rng: np.random.Generator) -> np.ndarray:
    """Return per-bar regime labels: 1=uptrend, -1=downtrend, 0=sideways."""
    regimes = []
    remaining = n_bars
    while remaining > 0:
        regime = rng.choice([1, -1, 0], p=[0.45, 0.35, 0.20])
        length = int(rng.integers(60, 300))  # 1–5 h blocks
        length = min(length, remaining)
        regimes.extend([regime] * length)
        remaining -= length
    return np.array(regimes[:n_bars])


def _vol_cluster(n: int, base_vol: float, rng: np.random.Generator) -> np.ndarray:
    """GARCH(1,1)-like volatility clustering."""
    h = np.empty(n)
    h[0] = base_vol ** 2
    alpha, beta, omega = 0.08, 0.88, base_vol ** 2 * (1 - 0.08 - 0.88)
    eps = student_t.rvs(df=5, size=n, random_state=rng)
    returns = np.empty(n)
    for i in range(n):
        h[i] = omega + alpha * (returns[i - 1] ** 2 if i > 0 else 0) + beta * h[i - 1]
        returns[i] = np.sqrt(max(h[i], 1e-12)) * eps[i]
    return returns, np.sqrt(h)


def generate_1m_ohlcv(
    n_bars: int = 20_000,
    seed: int = 42,
    start_price: float = 100.0,   # $100 unit price keeps int-size viable
    base_vol: float = 0.0012,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    regimes = _regime_schedule(n_bars, rng)
    returns, vols = _vol_cluster(n_bars, base_vol, rng)

    # Macro drift per regime
    drift = regimes * 0.00015
    log_prices = np.cumsum(returns + drift)
    closes = start_price * np.exp(log_prices)

    # Intrabar OHLC from close using vol proxy
    spread_half = vols * closes * 0.5
    opens = np.empty(n_bars)
    opens[0] = start_price
    opens[1:] = closes[:-1]

    highs = np.maximum(opens, closes) + np.abs(rng.normal(0, spread_half * 0.8, n_bars))
    lows = np.minimum(opens, closes) - np.abs(rng.normal(0, spread_half * 0.8, n_bars))

    # Volume: higher at turning points and trend accelerations
    base_vol_amt = 500.0
    vol_mult = 1.0 + 3.0 * np.abs(returns) / (vols + 1e-12)
    # Extra volume spike at local turning points (momentum reversals)
    momentum = np.convolve(returns, np.ones(5) / 5, mode="same")
    reversal_signal = np.abs(np.gradient(momentum))
    reversal_signal = reversal_signal / (reversal_signal.max() + 1e-12)
    volume = base_vol_amt * vol_mult * (1 + 4.0 * reversal_signal)
    volume = np.maximum(volume + rng.normal(0, base_vol_amt * 0.1, n_bars), 10.0)

    idx = pd.date_range("2024-01-01", periods=n_bars, freq="1min")
    df = pd.DataFrame(
        {
            "Open": opens,
            "High": highs,
            "Low": lows,
            "Close": closes,
            "Volume": volume,
        },
        index=idx,
    )
    # Spread for maker/taker simulation (in price units, full spread)
    df["Spread"] = vols * closes * 0.4
    return df


if __name__ == "__main__":
    df = generate_1m_ohlcv()
    df.to_csv("data/synthetic_1m.csv")
    print(f"Generated {len(df):,} bars  |  price range: {df['Close'].min():.0f}–{df['Close'].max():.0f}")
    print(df.tail())
