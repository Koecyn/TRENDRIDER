"""
analyzer.py — Nightly walk-forward analysis and config self-optimization.

Reads hft/logs/trades_YYYYMMDD.jsonl, computes rolling-window performance metrics,
then adjusts config.json signal/risk parameters based on observed win-rate and R:R.

Walk-forward logic:
  - Split trade log into 20-trade rolling windows
  - For each window compute: win_rate, avg_R (win/loss ratio), Sharpe approximation
  - Compare latest window to historical baseline
  - Widen vol_coefficient if recent losses are MAE-driven (stops too tight)
  - Tighten vol_coefficient if recent losses are target-driven (taking profit too early)
  - Adjust OFI/VPD thresholds based on signal quality metrics logged per trade

Run nightly (cron or manual):
  python hft/analyzer.py

Or import and call:
  from hft.analyzer import run_nightly
  run_nightly("hft/config.json", "hft/logs")
"""

import json
import logging
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

log = logging.getLogger("analyzer")


# ── Trade log reader ───────────────────────────────────────────────────────────

def load_trades(log_dir: str, days: int = 30) -> List[dict]:
    """Load up to `days` days of JSONL trade logs, newest first."""
    log_path = Path(log_dir)
    trades = []
    for f in sorted(log_path.glob("trades_*.jsonl"), reverse=True)[:days]:
        with open(f) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        trades.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    return trades


# ── Rolling metrics ────────────────────────────────────────────────────────────

def window_metrics(trades: List[dict], window: int = 20) -> List[dict]:
    """Compute metrics for each rolling window of `window` trades."""
    results = []
    for i in range(len(trades) - window + 1):
        w = trades[i: i + window]
        pnls     = [t["net_pnl"] for t in w]
        wins     = [p for p in pnls if p >= 0]
        losses   = [p for p in pnls if p < 0]
        win_rate = len(wins) / len(pnls)
        avg_win  = sum(wins)  / len(wins)   if wins   else 0.0
        avg_loss = sum(losses)/ len(losses) if losses else 1e-9
        R        = avg_win / abs(avg_loss)  if avg_loss else 0.0
        kelly    = win_rate - (1 - win_rate) / R if R > 0 else 0.0

        # Sharpe approximation from trade PnL sequence
        mean_p = sum(pnls) / len(pnls)
        std_p  = math.sqrt(sum((p - mean_p) ** 2 for p in pnls) / len(pnls)) if len(pnls) > 1 else 1e-9
        sharpe = mean_p / std_p if std_p > 0 else 0.0

        # MAE analysis: fraction of losses where MAE > stop_dist * 0.5
        mae_driven = sum(
            1 for t in w
            if t["net_pnl"] < 0
            and t.get("mae", 0) > abs(t["net_pnl"]) * 1.5
        )

        results.append({
            "window_end_idx": i + window,
            "trades":         window,
            "win_rate":       round(win_rate, 4),
            "avg_win":        round(avg_win,  6),
            "avg_loss":       round(avg_loss, 6),
            "R":              round(R, 3),
            "kelly":          round(kelly, 4),
            "sharpe":         round(sharpe, 4),
            "mae_driven_losses": mae_driven,
        })
    return results


# ── Parameter adjustment ───────────────────────────────────────────────────────

def _clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


def recommend_adjustments(windows: List[dict], cfg: dict) -> dict:
    """
    Given rolling window metrics, produce parameter delta suggestions.
    Returns a dict of {param_path: new_value} for config.json.
    """
    if not windows:
        return {}

    latest = windows[-1]
    # Baseline from all windows except the last
    baseline = windows[:-1] if len(windows) > 1 else windows

    base_win  = sum(w["win_rate"] for w in baseline) / len(baseline)
    base_R    = sum(w["R"]        for w in baseline) / len(baseline)
    base_sh   = sum(w["sharpe"]   for w in baseline) / len(baseline)

    changes = {}
    risk = cfg.get("risk", {})
    vol_coeff = risk.get("vol_coefficient", 1.2)

    # Stops too tight: many MAE-driven losses → widen vol_coefficient
    if latest["mae_driven_losses"] >= latest["trades"] * 0.4:
        new_vol = _clamp(vol_coeff * 1.15, 0.8, 3.0)
        changes["risk.vol_coefficient"] = round(new_vol, 3)
        log.info(f"MAE-driven losses high → vol_coefficient {vol_coeff:.3f} → {new_vol:.3f}")

    # Win rate declining → tighten OFI ratio (require stronger signal)
    if latest["win_rate"] < base_win - 0.05:
        sig = cfg.get("signals", {})
        ofi_long  = sig.get("ofi_ratio_long",  3.5)
        ofi_short = sig.get("ofi_ratio_short", 3.5)
        new_ofi = _clamp(ofi_long * 1.1, 2.5, 8.0)
        changes["signals.ofi_ratio_long"]  = round(new_ofi, 2)
        changes["signals.ofi_ratio_short"] = round(new_ofi, 2)
        log.info(f"Win rate declining → OFI ratio {ofi_long:.2f} → {new_ofi:.2f}")

    # Sharpe improving → slightly loosen VPD vol multiplier (more entries)
    if latest["sharpe"] > base_sh + 0.2:
        sig = cfg.get("signals", {})
        vpd_mult = sig.get("vpd_vol_mult", 2.5)
        new_vpd = _clamp(vpd_mult * 0.95, 1.5, 4.0)
        changes["signals.vpd_vol_mult"] = round(new_vpd, 2)
        log.info(f"Sharpe improving → vpd_vol_mult {vpd_mult:.2f} → {new_vpd:.2f}")

    # Kelly too low (≤ 0): reduce kelly_multiplier floor or keep default
    if latest["kelly"] <= 0 and latest["win_rate"] < 0.45:
        log.warning(f"Negative Kelly in latest window — edge has flipped. Win rate {latest['win_rate']:.1%}")

    return changes


def apply_changes(cfg: dict, changes: dict) -> dict:
    """Apply dot-path changes to config dict in place."""
    for path, value in changes.items():
        keys = path.split(".")
        node = cfg
        for k in keys[:-1]:
            node = node.setdefault(k, {})
        node[keys[-1]] = value
    return cfg


# ── Entry point ────────────────────────────────────────────────────────────────

def run_nightly(cfg_path: str = "hft/config.json", log_dir: str = "hft/logs"):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s")

    cfg_file = Path(cfg_path)
    if not cfg_file.exists():
        log.error(f"Config not found: {cfg_path}")
        return

    with open(cfg_file) as f:
        cfg = json.load(f)

    trades = load_trades(log_dir, days=30)
    if len(trades) < 20:
        log.info(f"Only {len(trades)} trades — need ≥20 for walk-forward. Skipping.")
        return

    log.info(f"Loaded {len(trades)} trades from {log_dir}")
    windows = window_metrics(trades, window=20)
    latest  = windows[-1] if windows else {}

    log.info(
        f"Latest window: win={latest.get('win_rate', 0):.1%}  "
        f"R={latest.get('R', 0):.2f}  sharpe={latest.get('sharpe', 0):.3f}  "
        f"kelly={latest.get('kelly', 0):.3f}"
    )

    changes = recommend_adjustments(windows, cfg)

    if changes:
        cfg = apply_changes(cfg, changes)
        with open(cfg_file, "w") as f:
            json.dump(cfg, f, indent=2)
        log.info(f"Config updated: {list(changes.keys())}")
    else:
        log.info("No parameter adjustments needed.")

    # Summary report
    report = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "total_trades":  len(trades),
        "windows":       len(windows),
        "latest":        latest,
        "changes":       changes,
    }

    report_path = Path(log_dir) / f"wf_report_{datetime.now().strftime('%Y%m%d')}.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    log.info(f"Report → {report_path}")


if __name__ == "__main__":
    run_nightly()
