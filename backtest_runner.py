"""
Backtest runner — Bollinger + RSI mean-reversion, 5-second bars.
Long-only: buy BTC dips, sell BTC we own on recovery.
$10 starting balance. Sharpe target: 2.0. Full trade log output.
"""

import os, sys, json, warnings, itertools
import numpy as np, pandas as pd

warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.dirname(__file__))

from data_gen import generate_5s_ohlcv
from strategy  import MeanReversionMTF, precompute, BT_COLS
from backtesting import Backtest


# ─────────────────────────────────────────────────────────────────────────────
# Parameters
# ─────────────────────────────────────────────────────────────────────────────

INDICATOR_PARAMS = dict(
    rsi_period  = 14,   # RSI (14 × 5s = 70s)
    bb_window   = 15,   # Bollinger (15 × 5s = 75s) — short for many touches
    bb_nstd     = 1.5,  # 1.5σ bands — 3:1 R:R with 0.5σ stop
    mom_fast    = 3,    # EMA fast (15s) — tighter momentum read
    mom_slow    = 12,   # EMA slow (60s)
    vol_window  = 60,   # volume window (5 min)
    vwap_window = 60,   # VWAP (5 min)
)

DEFAULT_STRATEGY_PARAMS = dict(
    rsi_entry      = 42.0,   # long: oversold / short: >58
    rsi_exit       = 58.0,   # long: overbought / short: <42
    bb_entry_pct   = 0.20,   # band proximity zone
    bb_stop_mult   = 0.5,    # stop 0.5σ beyond band → 3:1 R:R
    max_hold_bars  = 6,      # 30s max hold — very fast recycling
    taker_win_min  = 0.005,
)

PARAM_GRID = {
    "rsi_entry":     [38, 42, 45, 50],
    "rsi_exit":      [52, 56, 60, 65],
    "bb_entry_pct":  [0.15, 0.20, 0.25, 0.30],
    "bb_stop_mult":  [0.3, 0.5, 0.7, 1.0],
    "max_hold_bars": [4, 6, 8, 12],
}


# ─────────────────────────────────────────────────────────────────────────────
# Backtest runner
# ─────────────────────────────────────────────────────────────────────────────

def run_backtest(df, strategy_params, cash=10.0, commission=0.00001):
    # Maker fee = 0%, taker fee = 0.002% (0.00002).
    # Entries are maker (0 fee), exits mix taker/maker.
    # Conservative estimate: all exits taker → round-trip 0.002% → per side 0.001%
    # backtesting.py charges commission on each fill, so use 0.00001 (≈ 0.001% / side).
    bt = Backtest(df, MeanReversionMTF, cash=cash,
                  commission=commission, exclusive_orders=True,
                  trade_on_close=False)
    stats = bt.run(**strategy_params)
    return stats, bt


# ─────────────────────────────────────────────────────────────────────────────
# Grid optimizer
# ─────────────────────────────────────────────────────────────────────────────

def iterative_optimize(df, target_sharpe=2.0, max_iters=150):
    best_sharpe = -np.inf
    best_stats  = None
    best_params = None
    history     = []
    keys   = list(PARAM_GRID.keys())
    combos = list(itertools.product(*[PARAM_GRID[k] for k in keys]))
    cap    = min(len(combos), max_iters)
    print(f"  Grid: {len(combos)} combos, sampling {cap}")

    for i, combo in enumerate(combos[:cap]):
        trial = DEFAULT_STRATEGY_PARAMS.copy()
        for k, v in zip(keys, combo): trial[k] = v
        try:
            stats, _ = run_backtest(df, trial)
            sr = float(stats.get("Sharpe Ratio", float("nan")))
            if np.isnan(sr): sr = -np.inf
        except Exception: sr = -np.inf; stats = {}
        n  = stats.get("# Trades", 0) if stats is not None else 0
        wr = stats.get("Win Rate [%]", 0.0) if stats is not None else 0.0
        history.append(dict(iter=i, sharpe=sr, n_trades=n, win_rate=wr, **trial))
        tag = ""
        if sr > best_sharpe:
            best_sharpe = sr; best_stats = stats; best_params = trial.copy(); tag = "  ★"
        if (i+1) % 20 == 0 or sr >= target_sharpe:
            print(f"  [{i+1:3d}/{cap}] SR={sr:+.4f} | trades={n} "
                  f"| wr={wr:.1f}%{tag} | best={best_sharpe:+.4f}")
        if best_sharpe >= target_sharpe:
            print(f"\n  ✓ Target Sharpe {target_sharpe} reached at iter {i+1}!")
            break
    return best_stats, best_params, history


# ─────────────────────────────────────────────────────────────────────────────
# Reporting
# ─────────────────────────────────────────────────────────────────────────────

METRIC_KEYS = [
    "Start", "End", "Duration",
    "Equity Final [$]", "Equity Peak [$]",
    "Return [%]", "Buy & Hold Return [%]",
    "Sharpe Ratio", "Sortino Ratio", "Calmar Ratio",
    "Max. Drawdown [%]", "Avg. Drawdown [%]",
    "Max. Drawdown Duration", "Avg. Drawdown Duration",
    "# Trades", "Win Rate [%]",
    "Best Trade [%]", "Worst Trade [%]",
    "Avg. Trade [%]", "Avg. Trade Duration",
    "Profit Factor", "Expectancy [%]", "SQN",
]


def print_performance_table(stats):
    print("\n" + "═"*62)
    print("  PERFORMANCE METRICS")
    print("═"*62)
    for k in METRIC_KEYS:
        v = stats.get(k, "N/A")
        if isinstance(v, float): print(f"  {k:<42} {v:>12.4f}")
        else:                    print(f"  {k:<42} {str(v):>12}")
    print("═"*62)


def print_trade_log(stats, days: float):
    """Print ALL trades grouped by day, plus full CSV."""
    trades = stats.get("_trades")
    if trades is None or len(trades) == 0:
        print("\n  No trades."); return

    df = trades.copy()
    df["Date"] = pd.to_datetime(df["EntryTime"]).dt.date

    # ── Daily summary ─────────────────────────────────────────────────────
    daily = df.groupby("Date").agg(
        trades  =("PnL", "count"),
        wins    =("PnL", lambda x: (x > 0).sum()),
        losses  =("PnL", lambda x: (x <= 0).sum()),
        win_rate=("PnL", lambda x: f"{(x > 0).mean()*100:.1f}%"),
        total_pnl=("PnL", lambda x: f"${x.sum():+.4f}"),
        avg_dur =("Duration", lambda x: str(x.mean()).split('.')[0]),
    )
    print(f"\n{'─'*72}")
    print(f"  DAILY SUMMARY  ({len(df)} total trades, {days:.1f} days)")
    print(f"{'─'*72}")
    print(daily.to_string())
    print(f"{'─'*72}")

    # ── Full per-trade log (sample: first 80 + last 20) ───────────────────
    cols = ["EntryTime","ExitTime","EntryPrice","ExitPrice","PnL","ReturnPct","Duration"]
    disp = df[cols].copy()
    disp["PnL"]       = disp["PnL"].map(lambda x: f"${x:+.5f}")
    disp["ReturnPct"] = disp["ReturnPct"].map(lambda x: f"{x*100:+.4f}%")

    print(f"\n{'─'*95}")
    print(f"  TRADE LOG — first 80 trades")
    print(f"{'─'*95}")
    print(disp.head(80).to_string(index=True))
    if len(df) > 100:
        print(f"\n  ... {len(df)-100} middle trades in results/trade_log.csv ...\n")
        print(f"  LAST 20 TRADES:")
        print(disp.tail(20).to_string(index=True))
    print(f"{'─'*95}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    os.makedirs("data",    exist_ok=True)
    os.makedirs("results", exist_ok=True)

    print("="*65)
    print("  HFT MEAN-REVERSION  |  5s bars  |  BB+RSI LONG  |  $10")
    print("="*65)

    # ── 1. Data ───────────────────────────────────────────────────────────
    data_path = "data/synthetic_5s_21d.csv"
    if os.path.exists(data_path):
        print(f"\n[1] Loading {data_path}")
        raw = pd.read_csv(data_path, index_col=0, parse_dates=True)
    else:
        print("\n[1] Generating 5s synthetic OHLCV (21 days = 362,880 bars)…")
        raw = generate_5s_ohlcv(n_bars=362_880, seed=42)
        raw.to_csv(data_path)
    days = (raw.index[-1] - raw.index[0]).total_seconds() / 86400
    print(f"    {len(raw):,} bars | {days:.1f} days | "
          f"price {raw['Close'].min():.4f}–{raw['Close'].max():.4f}")

    # ── 2. Indicators ─────────────────────────────────────────────────────
    print("\n[2] Pre-computing indicators…")
    df = precompute(raw.copy(), INDICATOR_PARAMS)
    df = df.dropna(subset=["bb_mid","rsi","sma60"])
    print(f"    Valid bars: {len(df):,}")
    df_bt = df[BT_COLS].copy()

    # Signal scan — long-only (BB + RSI + momentum already turning up)
    bb_pct = df["bb_pct"].values
    rsi_v  = df["rsi"].values
    mom_v  = df["mom_slope"].values
    ep     = DEFAULT_STRATEGY_PARAMS["bb_entry_pct"]
    re     = DEFAULT_STRATEGY_PARAMS["rsi_entry"]
    sigs   = (bb_pct < ep) & (rsi_v < re) & (mom_v > 0)
    print(f"    Entry signals: {sigs.sum():,} ({sigs.sum()/days:.0f}/day)")

    # ── 3. Baseline ───────────────────────────────────────────────────────
    print("\n[3] Baseline run…")
    base_stats, _ = run_backtest(df_bt, DEFAULT_STRATEGY_PARAMS)
    sr_base = float(base_stats.get("Sharpe Ratio", float("nan")))
    n_base  = base_stats.get("# Trades", 0)
    wr_base = base_stats.get("Win Rate [%]", 0.0)
    print(f"    Sharpe={sr_base:.4f} | Trades={n_base} ({n_base/days:.0f}/day)"
          f" | Win Rate={wr_base:.1f}%")

    # ── 4. Optimize ───────────────────────────────────────────────────────
    TARGET = 2.0
    if sr_base >= TARGET:
        print(f"\n[4] Baseline meets Sharpe {TARGET}.")
        best_stats, best_params = base_stats, DEFAULT_STRATEGY_PARAMS.copy()
    else:
        print(f"\n[4] Sharpe {sr_base:.4f} < {TARGET} → grid search…")
        best_stats, best_params, history = iterative_optimize(df_bt, TARGET)
        if best_stats is None:
            print("  No valid result."); return
        pd.DataFrame(history).to_csv("results/optimization_history.csv", index=False)
        sr_best = float(best_stats.get("Sharpe Ratio", float("nan")))
        print(f"\n  Best Sharpe: {sr_best:.4f}")
        if sr_best < TARGET:
            print(f"  Note: Reporting best achieved ({sr_best:.4f})")

    # ── 5. Final run ──────────────────────────────────────────────────────
    print("\n[5] Final backtest…")
    final_stats, _ = run_backtest(df_bt, best_params)

    # ── 6. Output ─────────────────────────────────────────────────────────
    print_performance_table(final_stats)
    print_trade_log(final_stats, days)

    # Save
    with open("results/best_params.json", "w") as f:
        json.dump({"indicator_params": INDICATOR_PARAMS,
                   "strategy_params":  best_params}, f, indent=2)
    tdf = final_stats.get("_trades")
    if tdf is not None and len(tdf) > 0:
        tdf.to_csv("results/trade_log.csv")

    n_f  = final_stats.get("# Trades", 0)
    tpd  = n_f / days

    print("\n" + "═"*65)
    print("  FINAL SUMMARY")
    print("═"*65)
    print(f"  Starting Balance : $10.00")
    print(f"  Final Equity     : ${final_stats.get('Equity Final [$]',0):.4f}")
    print(f"  Total Return     : {final_stats.get('Return [%]',0):.2f}%")
    print(f"  Sharpe Ratio     : {final_stats.get('Sharpe Ratio',0):.4f}")
    print(f"  Sortino Ratio    : {final_stats.get('Sortino Ratio',0):.4f}")
    print(f"  Max Drawdown     : {final_stats.get('Max. Drawdown [%]',0):.2f}%")
    print(f"  Total Trades     : {n_f}  ({tpd:.0f}/day)")
    print(f"  Win Rate         : {final_stats.get('Win Rate [%]',0):.1f}%")
    print(f"  Profit Factor    : {final_stats.get('Profit Factor',0):.3f}")
    print(f"  SQN              : {final_stats.get('SQN',0):.3f}")
    print(f"  Full trade log   : results/trade_log.csv")
    print("═"*65)

    return final_stats, best_params


if __name__ == "__main__":
    main()
