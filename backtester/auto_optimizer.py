#!/usr/bin/env python3
"""
auto_optimizer.py — Autonomous backtest optimization loop.

Reads bt_results.json from data/live branch, analyzes metrics, decides
the next parameter change, edits strategy_downtrend.py if needed, writes
a new bt_trigger.json, and pushes — then waits for bt_watcher to run it.

Loops forever until Sharpe ≥ 1.6 with ≥ 10 trades, then writes STOP.

Run on Termux alongside bt_watcher:
  python backtester/auto_optimizer.py

Rules:
  - NEVER change EMA_CROSS or EMA_PULLBACK signal conditions
  - Only tune P dict values or add/remove structural gates
  - One change per iteration (controlled experiments)
"""

import json, os, re, subprocess, sys, time
from datetime import datetime
from pathlib import Path

REPO        = Path(__file__).resolve().parent.parent
CODE_BRANCH = "claude/hft-mean-reversion-strategy-pJ4YU"
DATA_BRANCH = "data/live"
STRATEGY    = REPO / "backtester" / "DOWNTREND" / "strategy_downtrend.py"
TRIGGER_F   = REPO / "bt_trigger.json"
POLL_S      = 25          # seconds between data/live checks
TARGET_SHARPE = 2.5     # stop here — 1.6 is floor, 2.5 is goal
MIN_TRADES    = 10

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; B='\033[1m'; Z='\033[0m'

def ts():
    return datetime.now().strftime('%H:%M:%S')

def log(msg, col=Z):
    print(f"{col}[optimizer {ts()}] {msg}{Z}", flush=True)

def git(*args, input=None):
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    return subprocess.run(["git"]+list(args), cwd=REPO,
                          capture_output=True, text=True, input=input, env=env)

# ── Fetch latest results from data/live ───────────────────────────────────────

def fetch_results():
    git("fetch", "origin", DATA_BRANCH, "-q")
    r = git("show", f"origin/{DATA_BRANCH}:bt_results.json")
    if r.returncode != 0:
        return None
    try:
        return json.loads(r.stdout)
    except Exception:
        return None

# ── Parse diagnostic lines from output_tail ───────────────────────────────────

def parse_diag(output_tail: list) -> dict:
    text = "\n".join(output_tail)
    def num(pat):
        m = re.search(pat, text)
        return float(m.group(1)) if m else 0.0
    return {
        "slope_pct":    num(r"EMA200 slope blocked:\s+\d+\s+\(([0-9.]+)%\)"),
        "below_e200":   num(r"Price < EMA200:\s+\d+\s+\(([0-9.]+)%\)"),
        "no_golden":    num(r"EMA50 < EMA200.*?:\s+\d+\s+\(([0-9.]+)%\)"),
        "adx_pct":      num(r"ADX <.*?blocked:\s+\d+\s+\(([0-9.]+)%\)"),
        "bias_pct":     num(r"Bias1m.*?bearish.*?:\s+\d+\s+\(([0-9.]+)%\)"),
        "rsi_pct":      num(r"RSI/pullback blocked:\s+\d+\s+\(([0-9.]+)%\)"),
        "cross_fired":  num(r"EMA_CROSS fired:\s+(\d+)"),
        "pullback_fired": num(r"EMA_PULLBACK fired:\s+(\d+)"),
        "avg_ratio":    num(r"Avg RNG_\d+m / ATR_1m ratio:\s+([0-9.]+)"),
    }

# ── Read current P dict values from strategy file ─────────────────────────────

def read_param(name: str) -> str:
    """Return raw value string for P[name] from strategy file."""
    m = re.search(rf"'{name}'\s*:\s*([0-9.]+)", STRATEGY.read_text())
    return m.group(1) if m else None

def set_param(name: str, value):
    """Update a single P dict value in strategy_downtrend.py."""
    text = STRATEGY.read_text()
    new_text = re.sub(
        rf"('{name}'\s*:\s*)[0-9.]+",
        rf"\g<1>{value}",
        text,
    )
    if new_text == text:
        log(f"  WARNING: param {name} not found in P dict", Y)
        return False
    STRATEGY.write_text(new_text)
    log(f"  SET P['{name}'] = {value}", G)
    return True

# ── Decision engine ───────────────────────────────────────────────────────────

def decide(stats: dict, diag: dict, history: list) -> dict:
    """
    Return {'param': name, 'value': v, 'reason': str}
    or {'gate': 'remove_golden', 'reason': str}
    or {'action': 'none', 'reason': str}
    """
    trades    = int(stats.get("trades", 0))
    sharpe    = stats.get("sharpe", -99)
    win_rate  = stats.get("win_rate", 0) / 100.0
    ret       = stats.get("return_pct", -99)

    adx_min   = float(read_param("adxMin")   or 22)
    min_bias  = float(read_param("minBias")  or 0.05)
    rsi_ob    = float(read_param("rsiOB")    or 65)
    atr_tp    = float(read_param("atrTp")    or 2.8)
    atr_stop  = float(read_param("atrStop")  or 1.0)
    tgt_win   = float(read_param("targetWindow") or 10)

    # ── Zero trades: filters too aggressive ─────────────────────────────────
    if trades == 0:
        # Golden cross gate likely blocked everything in March death-cross
        if diag["no_golden"] > 70:
            return {"gate": "remove_golden",
                    "reason": f"EMA50<EMA200 blocked {diag['no_golden']:.1f}% of bars — removing golden cross gate"}
        if diag["adx_pct"] > 50 and adx_min > 18:
            return {"param": "adxMin", "value": max(18, adx_min - 2),
                    "reason": f"ADX gate blocking {diag['adx_pct']:.1f}% — loosen adxMin {adx_min}→{max(18, adx_min-2)}"}
        if diag["slope_pct"] > 80:
            return {"param": "emaSlopeN", "value": 240,
                    "reason": "EMA200 slope gate too restrictive (>80%) — shorten to 4hr lookback"}
        return {"action": "none", "reason": "no trades but cause unclear — waiting next result"}

    # ── Too many trades, all losing ──────────────────────────────────────────
    if trades > 40 and win_rate < 0.38:
        # Win rate critically low — need stronger filters
        if rsi_ob > 55:
            new_rsi = rsi_ob - 5
            return {"param": "rsiOB", "value": new_rsi,
                    "reason": f"win={win_rate:.0%} too low — tighten rsiOB {rsi_ob}→{new_rsi}"}
        if adx_min < 28:
            new_adx = adx_min + 2
            return {"param": "adxMin", "value": new_adx,
                    "reason": f"win={win_rate:.0%} too low — tighten adxMin {adx_min}→{new_adx}"}
        if min_bias < 0.15:
            new_bias = round(min_bias + 0.03, 2)
            return {"param": "minBias", "value": new_bias,
                    "reason": f"win={win_rate:.0%} too low — tighten minBias {min_bias}→{new_bias}"}

    # ── Win rate OK but Sharpe still negative (R:R problem) ─────────────────
    if win_rate >= 0.48 and sharpe < 0:
        if atr_tp < 5.0:
            new_tp = round(atr_tp * 1.25, 2)
            return {"param": "atrTp", "value": new_tp,
                    "reason": f"win={win_rate:.0%} good but sharpe={sharpe:.2f} — widen target atrTp {atr_tp}→{new_tp}"}
        if tgt_win < 20:
            new_win = int(tgt_win * 1.5)
            return {"param": "targetWindow", "value": new_win,
                    "reason": f"target window too narrow — increase targetWindow {tgt_win}→{new_win}"}

    # ── Moderate win rate, moderate sharpe — tighten stops ─────────────────
    if 0.40 <= win_rate < 0.48 and sharpe < 0.5:
        if atr_stop > 0.7:
            new_stop = round(atr_stop - 0.15, 2)
            return {"param": "atrStop", "value": new_stop,
                    "reason": f"tighten stop atrStop {atr_stop}→{new_stop} to improve R:R"}

    # ── Sharpe positive — keep pushing toward 2.5 ───────────────────────────
    if 0 < sharpe < TARGET_SHARPE:
        # Try widening target first
        if atr_tp < 8.0:
            new_tp = round(atr_tp * 1.2, 2)
            return {"param": "atrTp", "value": new_tp,
                    "reason": f"sharpe={sharpe:.2f} → push atrTp {atr_tp}→{new_tp}"}
        # Then try wider target window
        if tgt_win < 30:
            new_win = int(tgt_win + 5)
            return {"param": "targetWindow", "value": new_win,
                    "reason": f"sharpe={sharpe:.2f} → widen targetWindow {tgt_win}→{new_win}"}
        # Tighten stop to improve R:R ratio
        if atr_stop > 0.5:
            new_stop = round(atr_stop - 0.1, 2)
            return {"param": "atrStop", "value": new_stop,
                    "reason": f"sharpe={sharpe:.2f} → tighten atrStop {atr_stop}→{new_stop}"}
        # Tighten ADX to get only strongest trends
        if adx_min < 32 and trades > 15:
            new_adx = adx_min + 2
            return {"param": "adxMin", "value": new_adx,
                    "reason": f"sharpe={sharpe:.2f} → tighten adxMin {adx_min}→{new_adx} (quality over quantity)"}

    return {"action": "none",
            "reason": f"sharpe={sharpe:.2f} win={win_rate:.0%} trades={trades} — holding current params"}

# ── Apply a gate removal (edit strategy source) ──────────────────────────────

def remove_golden_cross_gate():
    """Comment out the EMA50 > EMA200 gate block."""
    text = STRATEGY.read_text()
    old = (
        "        # Golden cross gate: EMA50 must be above EMA200.\n"
        "        # When EMA50 < EMA200 we are in a death-cross bear regime — no longs.\n"
        "        e50 = self.ema50[-1]\n"
        "        if e50 < e200:\n"
        "            self._d_no_golden += 1\n"
        "            return\n"
    )
    if old not in text:
        log("Golden cross gate block not found — may already be removed", Y)
        return False
    new = (
        "        # Golden cross gate removed — March 2026 was in death-cross all month\n"
        "        # e50 = self.ema50[-1]\n"
        "        # if e50 < e200:\n"
        "        #     self._d_no_golden += 1\n"
        "        #     return\n"
    )
    STRATEGY.write_text(text.replace(old, new))
    log("Removed golden cross gate from strategy", G)
    return True

# ── Push new trigger ──────────────────────────────────────────────────────────

def push_trigger(iter_id: str, message: str, is_stop=False):
    if is_stop:
        payload = {"id": iter_id, "command": "STOP",
                   "bash": "", "message": message}
    else:
        payload = {
            "id": iter_id,
            "command": "RUN",
            "bash": (
                "git fetch origin claude/hft-mean-reversion-strategy-pJ4YU && "
                "git reset --hard origin/claude/hft-mean-reversion-strategy-pJ4YU && "
                "python backtester/DOWNTREND/strategy_downtrend.py "
                "--csv backtester/btc_mar30.csv --balance 100 "
                "--min-bias 0.05 --adx-min 22 --atr-stop 1.0 "
                "--target-window 10 --max-hold 60"
            ),
            "message": message,
        }
    TRIGGER_F.write_text(json.dumps(payload, indent=2))

    # Commit strategy + trigger together
    files = [str(TRIGGER_F.relative_to(REPO)), str(STRATEGY.relative_to(REPO))]
    git("add", *files)
    r = git("commit", "-m", f"auto-optimizer: {message}")
    if r.returncode != 0 and "nothing to commit" not in r.stdout + r.stderr:
        log(f"commit failed: {r.stderr.strip()}", R)
        return False

    for attempt in range(4):
        r = git("push", "-u", "origin", CODE_BRANCH)
        if r.returncode == 0:
            log(f"Pushed trigger {iter_id}", G)
            return True
        wait = 2 ** (attempt + 1)
        log(f"Push failed (attempt {attempt+1}) — retry in {wait}s", Y)
        time.sleep(wait)

    log("Push failed after 4 attempts", R)
    return False

# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    log(f"Auto-optimizer starting — target Sharpe ≥ {TARGET_SHARPE}", G)
    log(f"Polling data/live every {POLL_S}s", C)

    last_id  = None
    iter_num = 22          # next iter to write
    history  = []          # list of stats dicts for trend analysis

    while True:
        results = fetch_results()

        if results is None:
            log("No bt_results.json yet — waiting...", Y)
            time.sleep(POLL_S)
            continue

        rid = results.get("trigger_id", "")

        # Skip if same result as last time
        if rid == last_id:
            time.sleep(POLL_S)
            continue

        last_id = rid
        stats   = results.get("stats", {})
        tail    = results.get("output_tail", [])
        diag    = parse_diag(tail)

        sharpe = stats.get("sharpe", -99)
        trades = int(stats.get("trades", 0))
        win    = stats.get("win_rate", 0)
        ret    = stats.get("return_pct", -99)
        history.append({**stats, "iter": rid})

        log(
            f"{B}[{rid}]{Z}  sharpe={C}{sharpe:+.3f}{Z}  "
            f"win={win:.1f}%  trades={trades}  ret={ret:+.2f}%",
            G if sharpe > 0 else R
        )
        log(
            f"  diag → adx_blocked={diag['adx_pct']:.1f}%  "
            f"slope={diag['slope_pct']:.1f}%  "
            f"below_e200={diag['below_e200']:.1f}%  "
            f"no_golden={diag['no_golden']:.1f}%  "
            f"cross_fired={int(diag['cross_fired'])}"
        )

        # ── MILESTONE ─────────────────────────────────────────────────────────
        if sharpe >= 1.6 and trades >= MIN_TRADES:
            log(f"MILESTONE: Sharpe={sharpe:.3f} ≥ 1.6 — pushing toward 2.5", G)

        # ── SUCCESS ──────────────────────────────────────────────────────────
        if sharpe >= TARGET_SHARPE and trades >= MIN_TRADES:
            log(f"TARGET REACHED — Sharpe={sharpe:.3f} ≥ {TARGET_SHARPE} ({trades} trades)", G)
            push_trigger(f"iter{iter_num}_SUCCESS", f"Sharpe={sharpe:.3f} ≥ 2.5 — DONE", is_stop=True)
            log("STOP trigger pushed. bt_watcher will halt.", G)
            break

        # ── DECIDE NEXT ACTION ────────────────────────────────────────────────
        decision = decide(stats, diag, history)
        log(f"  decision → {decision.get('reason','?')}", C)

        iter_num += 1
        iter_id   = f"iter{iter_num}_{datetime.now().strftime('%H%M%S')}"

        if decision.get("gate") == "remove_golden":
            remove_golden_cross_gate()
        elif decision.get("param"):
            set_param(decision["param"], decision["value"])
        else:
            log("  No code change — re-running same params to confirm result", Y)

        push_trigger(iter_id, decision.get("reason", "auto-optimizer step"))
        log(f"  → iter{iter_num} trigger pushed — waiting for bt_watcher...", C)
        time.sleep(POLL_S)


if __name__ == "__main__":
    main()
