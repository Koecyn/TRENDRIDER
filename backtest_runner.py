"""
Backtest runner with iterative parameter optimization.
Runs the strategy, checks Sharpe Ratio, then tunes thresholds until
Sharpe > 1.5 or the iteration budget is exhausted.
"""

import os, sys, json, warnings, itertools
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.dirname(__file__))

from data_gen import generate_1m_ohlcv
from strategy import MeanReversionMTF, precompute
from backtesting import Backtest


# ─────────────────────────────────────────────────────────────────────────────
# Parameters
# ─────────────────────────────────────────────────────────────────────────────

INDICATOR_PARAMS = dict(
    mom_fast=5,
    mom_slow=20,
    vol_window=20,
    vwap_window=20,
)

# Columns passed to backtesting.py (must match precompute output names)
BT_COLS = [
    "Open", "High", "Low", "Close", "Volume",
    "mom_pct", "mom_slope", "roc5", "vol_exhaust", "vwap",
    "trail_sl", "low_5m", "low_10m", "macro_up",
]

DEFAULT_STRATEGY_PARAMS = dict(
    mom_flat_thresh  = -0.0002,
    mom_peak_thresh  =  0.0001,
    mom_exit_thresh  = -0.00005,
    vol_exhaust_lo   =  0.65,
    vol_exhaust_hi   =  1.70,
    sl_buffer        =  0.0005,
    risk_pct         =  0.01,
    max_hold_bars    =  60,
    taker_win_min    =  0.008,
)

# Grid to search over (only the most impactful thresholds)
PARAM_GRID = {
    "mom_flat_thresh":  [-0.0005, -0.0003, -0.0002, -0.0001, -0.00005],
    "mom_exit_thresh":  [-0.0002, -0.0001, -0.00005, 0.0],
    "vol_exhaust_lo":   [0.50, 0.65, 0.80],
    "vol_exhaust_hi":   [1.4, 1.6, 1.8, 2.2],
    "max_hold_bars":    [40, 60, 90],
}


# ─────────────────────────────────────────────────────────────────────────────
# Single backtest
# ─────────────────────────────────────────────────────────────────────────────

def run_backtest(df: pd.DataFrame, strategy_params: dict,
                 cash: float = 100_000,
                 commission: float = 0.0001) -> tuple:
    bt = Backtest(
        df,
        MeanReversionMTF,
        cash=cash,
        commission=commission,
        exclusive_orders=True,
        trade_on_close=False,   # fills at next open → maker delay approximation
    )
    stats = bt.run(**strategy_params)
    return stats, bt


# ─────────────────────────────────────────────────────────────────────────────
# Grid search optimizer
# ─────────────────────────────────────────────────────────────────────────────

def iterative_optimize(df: pd.DataFrame, target_sharpe: float = 1.5,
                       max_iters: int = 120) -> tuple:
    best_sharpe = -np.inf
    best_stats  = None
    best_params = None
    history     = []

    keys   = list(PARAM_GRID.keys())
    combos = list(itertools.product(*[PARAM_GRID[k] for k in keys]))
    cap    = min(len(combos), max_iters)
    print(f"  Grid: {len(combos)} combos, running {cap}")

    for i, combo in enumerate(combos[:cap]):
        trial = DEFAULT_STRATEGY_PARAMS.copy()
        for k, v in zip(keys, combo):
            trial[k] = v

        try:
            stats, _ = run_backtest(df, trial)
            sr = float(stats.get("Sharpe Ratio", float("nan")))
            if np.isnan(sr):
                sr = -np.inf
        except Exception:
            sr = -np.inf
            stats = {}

        n_trades = stats.get("# Trades", 0) if stats is not None else 0
        wr       = stats.get("Win Rate [%]", 0.0) if stats is not None else 0.0
        history.append(dict(iter=i, sharpe=sr, n_trades=n_trades, win_rate=wr, **trial))

        if sr > best_sharpe:
            best_sharpe = sr
            best_stats  = stats
            best_params = trial.copy()
            tag = "  ★"
        else:
            tag = ""

        if (i + 1) % 20 == 0 or sr >= target_sharpe:
            print(f"  [{i+1:3d}/{cap}] SR={sr:+.4f} | trades={n_trades} | "
                  f"wr={wr:.1f}%{tag} | best={best_sharpe:+.4f}")

        if best_sharpe >= target_sharpe:
            print(f"\n  TARGET {target_sharpe} reached at iteration {i+1}!")
            break

    return best_stats, best_params, history


# ─────────────────────────────────────────────────────────────────────────────
# Reporting
# ─────────────────────────────────────────────────────────────────────────────

METRIC_KEYS = [
    "Start", "End", "Duration",
    "Equity Final [$]", "Equity Peak [$]",
    "Return [%]", "Buy & Hold Return [%]",
    "Return (Ann.) [%]", "Volatility (Ann.) [%]",
    "Sharpe Ratio", "Sortino Ratio", "Calmar Ratio",
    "Max. Drawdown [%]", "Avg. Drawdown [%]",
    "Max. Drawdown Duration", "Avg. Drawdown Duration",
    "# Trades", "Win Rate [%]",
    "Best Trade [%]", "Worst Trade [%]",
    "Avg. Trade [%]", "Max. Trade Duration",
    "Avg. Trade Duration", "Profit Factor",
    "Expectancy [%]", "SQN",
]


def print_performance_table(stats):
    print("\n" + "═" * 58)
    print("  PERFORMANCE METRICS")
    print("═" * 58)
    for k in METRIC_KEYS:
        v = stats.get(k, "N/A")
        if isinstance(v, float):
            print(f"  {k:<38} {v:>12.4f}")
        else:
            print(f"  {k:<38} {str(v):>12}")
    print("═" * 58)


def print_trade_log(stats, max_rows: int = 40):
    trades = stats.get("_trades")
    if trades is None or len(trades) == 0:
        print("\n  No trades recorded.")
        return
    df = trades[["EntryTime", "ExitTime", "EntryPrice", "ExitPrice",
                  "PnL", "ReturnPct", "Duration"]].head(max_rows).copy()
    df["PnL"]       = df["PnL"].map(lambda x: f"${x:,.2f}")
    df["ReturnPct"] = df["ReturnPct"].map(lambda x: f"{x*100:.3f}%")
    print("\n" + "─" * 90)
    print(f"  TRADE EXECUTION LOG  (showing first {min(len(trades), max_rows)} of {len(trades)} trades)")
    print("─" * 90)
    print(df.to_string(index=True))
    print("─" * 90)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    os.makedirs("data",    exist_ok=True)
    os.makedirs("results", exist_ok=True)

    print("=" * 60)
    print("  HFT MEAN-REVERSION MULTI-TIMEFRAME BACKTEST  ")
    print("=" * 60)

    # ── 1. Data ───────────────────────────────────────────────────────────
    data_path = "data/synthetic_1m.csv"
    if os.path.exists(data_path):
        print(f"\n[1] Loading cached data: {data_path}")
        raw = pd.read_csv(data_path, index_col=0, parse_dates=True)
    else:
        print("\n[1] Generating synthetic 1m OHLCV (20 000 bars) …")
        raw = generate_1m_ohlcv(n_bars=20_000, seed=42)
        raw.to_csv(data_path)
    print(f"    Bars: {len(raw):,}  |  price {raw['Close'].min():.0f}–{raw['Close'].max():.0f}")

    # ── 2. Pre-compute indicators ─────────────────────────────────────────
    print("\n[2] Pre-computing indicators …")
    df = precompute(raw.copy(), INDICATOR_PARAMS)
    df = df.dropna(subset=["vwap", "trail_sl", "mom_slope"])
    print(f"    Valid bars: {len(df):,}")
    df_bt = df[BT_COLS].copy()

    # ── 3. Baseline ───────────────────────────────────────────────────────
    print("\n[3] Baseline run …")
    base_stats, _ = run_backtest(df_bt, DEFAULT_STRATEGY_PARAMS)
    sr_base = float(base_stats.get("Sharpe Ratio", float("nan")))
    n_base  = base_stats.get("# Trades", 0)
    print(f"    Sharpe={sr_base:.4f}  |  Trades={n_base}")

    # ── 4. Optimize if needed ─────────────────────────────────────────────
    TARGET = 1.5
    if sr_base >= TARGET:
        print(f"\n[4] Baseline already meets target Sharpe {TARGET}.")
        best_stats, best_params = base_stats, DEFAULT_STRATEGY_PARAMS.copy()
    else:
        print(f"\n[4] Sharpe {sr_base:.4f} < {TARGET} → grid search …")
        best_stats, best_params, history = iterative_optimize(df_bt, TARGET)

        if best_stats is None:
            print("  No valid runs found. Check indicator setup.")
            return

        hist_df = pd.DataFrame(history)
        hist_df.to_csv("results/optimization_history.csv", index=False)

        sr_best = float(best_stats.get("Sharpe Ratio", float("nan")))
        print(f"\n  Best Sharpe after optimization: {sr_best:.4f}")
        if sr_best < TARGET:
            print(f"  NOTE: Target {TARGET} not reached. Reporting best achieved.")

    # ── 5. Final run ──────────────────────────────────────────────────────
    print("\n[5] Final backtest …")
    final_stats, final_bt = run_backtest(df_bt, best_params)

    # ── 6. Output ─────────────────────────────────────────────────────────
    print_performance_table(final_stats)
    print_trade_log(final_stats)

    # Save artefacts
    with open("results/best_params.json", "w") as f:
        json.dump({"indicator_params": INDICATOR_PARAMS,
                   "strategy_params": best_params}, f, indent=2)

    trades_df = final_stats.get("_trades")
    if trades_df is not None and len(trades_df) > 0:
        trades_df.to_csv("results/trade_log.csv")
        print(f"\n  Trade log → results/trade_log.csv  ({len(trades_df)} trades)")

    print("\n[SUMMARY]")
    print(f"  Sharpe Ratio : {final_stats.get('Sharpe Ratio','N/A'):.4f}")
    print(f"  Return       : {final_stats.get('Return [%]','N/A'):.2f}%")
    print(f"  Max Drawdown : {final_stats.get('Max. Drawdown [%]','N/A'):.2f}%")
    print(f"  Total Trades : {final_stats.get('# Trades','N/A')}")
    print(f"  Win Rate     : {final_stats.get('Win Rate [%]','N/A'):.1f}%")
    print(f"  Best params  → results/best_params.json")

    return final_stats, best_params


if __name__ == "__main__":
    main()
