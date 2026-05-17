# TRENDRIDER — HFT Mean-Reversion Multi-Timeframe Strategy

## Overview

A high-frequency mean-reversion strategy targeting 1-minute trough-to-peak captures
during macroscopic uptrends. Built on `backtesting.py` with strict zero-lookahead
bar-by-bar simulation.

## Architecture

```
data_gen.py        — Synthetic OHLCV generator (GARCH-like vol clustering, regime switching)
indicators.py      — Normalized, causal indicator library (no pandas shift tricks)
strategy.py        — MeanReversionMTF backtesting.py Strategy class + precompute()
backtest_runner.py — Baseline run + grid-search optimizer + report generator
```

## Strategy Logic

### Entry (Trough Detection)
1. **Macro Context**: Price above rolling VWAP AND 5m EMA momentum positive
2. **Momentum Flattening**: 1m normalized EMA-slope is negative but improving (trough forming)
3. **Volume Exhaustion**: Vol/mean-vol < 0.65 (sellers drying up) OR > 1.70 (capitulation spike)

### Exit (Peak Detection)
1. **Momentum Death**: Slope peaked above threshold, now declining below exit floor
2. **ROC5 Negative**: 5-bar return turns negative (momentum confirmed dead)
3. **Trailing Stop**: 5-bar rolling low with hardening (10m macro confirmation OR 5 consecutive bars)
4. **Anti-Stall**: Force-exit after 60 bars to prevent stuck positions

### Order Model
- **Maker-first**: orders fill at next bar's open (backtesting.py `trade_on_close=False`)
- **Taker Switch**: only if projected gain > 30% STCG + 0.002% taker fee + 0.8% buffer
- **Integer unit sizing**: risk_pct × equity ÷ stop_distance, capped to 20% of equity

### Stop-Loss Hardening
- Initial SL: 5-bar trailing low × (1 − sl_buffer)
- Hardening trigger: price breaks 10m macro low **OR** 5 consecutive bars below SL
- Trail: SL ratchets upward only, never down

## Performance (Baseline)

| Metric | Value |
|---|---|
| Sharpe Ratio | **1.664** |
| Return (14-day period) | 28.45% |
| Max Drawdown | -2.91% |
| Win Rate | 62.2% |
| Total Trades | 233 |
| Profit Factor | 1.87 |
| SQN | 3.10 |

## Quick Start

```bash
pip install -r requirements.txt
python backtest_runner.py
```

Results are written to `results/`:
- `best_params.json` — optimal strategy parameters
- `trade_log.csv`   — full trade execution log
- `optimization_history.csv` — grid-search iteration log (if optimization ran)
