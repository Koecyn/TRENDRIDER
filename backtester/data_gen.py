"""
Synthetic OHLCV — 5-second bars, 21-day dataset.
Realistic BTC microstructure:
  - 50/25/25 regime (up/down/flat) — balanced for mean-reversion
  - Drift ~3.5%/day uptrend (down from runaway 25%/day)
  - GARCH(1,1) volatility clustering + Student-t fat tails
  - Volume spikes at turning points
  - start_price = $1.00, cash = $10 → 9-10 integer units per trade
"""

import numpy as np
import pandas as pd
from scipy.stats import t as student_t


def _regime_schedule(n_bars: int, rng: np.random.Generator) -> np.ndarray:
    """50% uptrend, 25% downtrend, 25% sideways — balanced for mean-rev."""
    regimes, remaining = [], n_bars
    while remaining > 0:
        regime = rng.choice([1, -1, 0], p=[0.50, 0.25, 0.25])
        bars   = int(rng.integers(60, 600))   # 5–50 min blocks
        bars   = min(bars, remaining)
        regimes.extend([regime] * bars)
        remaining -= bars
    return np.array(regimes[:n_bars])


def _garch(n: int, base_vol: float, rng: np.random.Generator):
    alpha, beta = 0.08, 0.88
    omega = base_vol**2 * (1.0 - alpha - beta)
    h = np.empty(n); h[0] = base_vol**2
    eps  = student_t.rvs(df=5, size=n, random_state=rng)
    rets = np.empty(n)
    for i in range(n):
        if i > 0: h[i] = omega + alpha*rets[i-1]**2 + beta*h[i-1]
        rets[i] = np.sqrt(max(h[i], 1e-16)) * eps[i]
    return rets, np.sqrt(h)


def generate_5s_ohlcv(
    n_bars: int = 362_880,       # 21 days × 86400 / 5
    seed: int = 42,
    start_price: float = 1.0,
    base_vol: float = 0.00010,   # per 5s bar → ~1.3% daily vol (realistic)
) -> pd.DataFrame:

    rng     = np.random.default_rng(seed)
    regimes = _regime_schedule(n_bars, rng)
    rets, vols = _garch(n_bars, base_vol, rng)

    # ~3.5%/day uptrend drift (was 25%/day — now realistic)
    drift      = regimes * 0.000002
    log_prices = np.cumsum(rets + drift)
    closes     = start_price * np.exp(log_prices)

    spread_half = vols * closes * 0.25
    opens       = np.empty(n_bars)
    opens[0]    = start_price
    opens[1:]   = closes[:-1]
    highs = np.maximum(opens, closes) + np.abs(rng.normal(0, spread_half*0.5, n_bars))
    lows  = np.minimum(opens, closes) - np.abs(rng.normal(0, spread_half*0.5, n_bars))

    base_v   = 200.0
    vol_mult = 1.0 + 4.0 * np.abs(rets) / (vols + 1e-14)
    mom      = np.convolve(rets, np.ones(5)/5, mode="same")
    reversal = np.abs(np.gradient(mom)); reversal /= reversal.max() + 1e-14
    volume   = base_v * vol_mult * (1 + 5.0*reversal)
    volume   = np.maximum(volume + rng.normal(0, base_v*0.05, n_bars), 1.0)

    idx = pd.date_range("2024-01-01", periods=n_bars, freq="5s")
    return pd.DataFrame(
        {"Open": opens, "High": highs, "Low": lows,
         "Close": closes, "Volume": volume},
        index=idx,
    )


if __name__ == "__main__":
    import os; os.makedirs("data", exist_ok=True)
    df = generate_5s_ohlcv()
    df.to_csv("data/synthetic_5s.csv")
    print(f"{len(df):,} bars | {len(df)/17280:.1f} days | "
          f"price {df['Close'].min():.4f}–{df['Close'].max():.4f}")
