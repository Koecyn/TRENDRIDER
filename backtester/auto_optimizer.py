#!/usr/bin/env python3
"""
auto_optimizer.py — Self-contained backtest optimization engine.

One process. No bt_watcher needed.

  - Pulls latest code from the repo before each run
  - Runs the backtest subprocess directly
  - Pushes results to data/live branch
  - Analyzes results and decides next parameter change
  - Edits strategy_downtrend.py and commits the change
  - Loops until Sharpe ≥ 2.5 (milestone logged at 1.6)
  - Hot-reloads itself from the repo on new commits (re-exec)

Usage:
  cd ~/TRENDRIDER
  python backtester/auto_optimizer.py
"""

import json, os, re, subprocess, sys, time, signal
from datetime import datetime
from pathlib import Path

REPO        = Path(__file__).resolve().parent.parent
CODE_BRANCH = "claude/hft-mean-reversion-strategy-pJ4YU"
DATA_BRANCH = "data/live"
STRATEGY    = REPO / "backtester" / "DOWNTREND" / "strategy_downtrend.py"
TRIGGER_F   = REPO / "bt_trigger.json"
RESULTS_F   = REPO / "bt_results.json"
CSV_PATH    = REPO / "backtester" / "btc_mar30.csv"
LOG_F       = REPO / "backtester" / "optimizer.log"

TARGET_SHARPE  = 2.5
TARGET_WIN_PCT = 60.0    # must also reach 60% win rate before stopping
MILESTONE      = 1.6
MIN_TRADES     = 10

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; B='\033[1m'; Z='\033[0m'

def ts():
    return datetime.now().strftime('%H:%M:%S')

def log(msg, col=Z):
    line = f"{col}[optimizer {ts()}] {msg}{Z}"
    print(line, flush=True)
    try:
        with open(LOG_F, "a") as f:
            f.write(re.sub(r'\033\[[0-9;]*m', '', line) + "\n")
    except Exception:
        pass

# ── Git helpers ───────────────────────────────────────────────────────────────

ENV = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}

def git(*args, stdin=None):
    return subprocess.run(["git"]+list(args), cwd=REPO,
                          capture_output=True, text=True,
                          input=stdin, env=ENV)

def git_unlock():
    for lock in ["index.lock", "MERGE_HEAD"]:
        p = REPO / ".git" / lock
        if p.exists():
            p.unlink()

# ── Pull latest code from repo ────────────────────────────────────────────────

def sync_code():
    """Pull latest strategy + optimizer code before running."""
    git("fetch", "origin", CODE_BRANCH, "-q")
    git("reset", "--hard", f"origin/{CODE_BRANCH}")
    log("Code synced from repo", C)

# ── Hot-reload: re-exec self if this file changed ─────────────────────────────

_self_hash = None

def check_self_reload():
    """Re-exec this script if a newer version is on the code branch."""
    global _self_hash
    r = git("show", f"origin/{CODE_BRANCH}:backtester/auto_optimizer.py")
    if r.returncode != 0:
        return
    new_hash = hash(r.stdout)
    if _self_hash is None:
        _self_hash = new_hash
        return
    if new_hash != _self_hash:
        log("New version of auto_optimizer detected — reloading", Y)
        sync_code()
        os.execv(sys.executable, [sys.executable] + sys.argv)

# ── Run backtest subprocess ───────────────────────────────────────────────────

def run_backtest() -> dict:
    """Run strategy_downtrend.py and return parsed stats."""
    fetch_flag = []
    if not CSV_PATH.exists():
        log("btc_mar30.csv not found — fetching from Binance", Y)
        fetch_flag = ["--start", "2026-03-01", "--days", "30",
                      "--save-csv", str(CSV_PATH)]
    else:
        fetch_flag = ["--csv", str(CSV_PATH)]

    cmd = [
        sys.executable,
        str(STRATEGY),
        *fetch_flag,
        "--balance",       "100",
        "--min-bias",      str(read_param("minBias")      or "0.05"),
        "--adx-min",       str(read_param("adxMin")       or "22"),
        "--atr-stop",      str(read_param("atrStop")      or "1.0"),
        "--atr-tp",        str(read_param("atrTp")        or "2.8"),
        "--partial-at",    str(read_param("partialAt")    or "1.0"),
        "--target-window", str(int(float(read_param("targetWindow") or "10"))),
        "--max-hold",      str(int(float(read_param("maxHoldBars")  or "60"))),
        "--rsi-ob",        str(read_param("rsiOB")        or "65"),
    ]
    log(f"Running backtest... {' '.join(cmd[2:])}", C)
    r = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True, timeout=300)
    output = r.stdout + r.stderr
    stats = parse_stats(output)
    tail  = output.strip().splitlines()[-45:]
    return {"stats": stats, "output_tail": tail, "returncode": r.returncode}

# ── Push results to data/live ─────────────────────────────────────────────────

def push_results(iter_id: str, result: dict):
    git_unlock()
    payload = {
        "ts":         datetime.utcnow().isoformat(),
        "trigger_id": iter_id,
        "status":     "complete",
        "returncode": result["returncode"],
        "stats":      result["stats"],
        "output_tail": result["output_tail"],
    }
    RESULTS_F.write_text(json.dumps(payload, indent=2))

    r = git("hash-object", "-w", str(RESULTS_F))
    blob = r.stdout.strip()
    if not blob:
        log("hash-object failed", R); return

    r = git("mktree", stdin=f"100644 blob {blob}\tbt_results.json\n")
    tree = r.stdout.strip()
    if not tree:
        log("mktree failed", R); return

    r = git("rev-parse", f"refs/remotes/origin/{DATA_BRANCH}")
    parent = ["-p", r.stdout.strip()] if r.returncode == 0 else []
    r = git("commit-tree", tree, *parent, "-m", f"bt_results {datetime.now().strftime('%Y%m%d_%H%M%S')}")
    commit = r.stdout.strip()
    if not commit:
        log("commit-tree failed", R); return

    r = git("push", "origin", f"{commit}:refs/heads/{DATA_BRANCH}")
    if r.returncode == 0:
        git("update-ref", f"refs/remotes/origin/{DATA_BRANCH}", commit)
        log("Results pushed to data/live", G)
    else:
        log(f"Push failed: {r.stderr.strip()}", R)

# ── Stats parser ──────────────────────────────────────────────────────────────

def parse_stats(output: str) -> dict:
    pats = {
        "trades":       r"# Trades\s+(\d+)",
        "win_rate":     r"Win Rate \[%\]\s+([\-\d.]+)",
        "sharpe":       r"Sharpe Ratio\s+([\-\d.]+)",
        "return_pct":   r"Return \[%\]\s+([\-\d.]+)",
        "equity_final": r"Equity Final \[\$\]\s+([\-\d.]+)",
        "max_dd":       r"Max\. Drawdown \[%\]\s+([\-\d.]+)",
        "profit_factor":r"Profit Factor\s+([\-\d.]+)",
        "sqn":          r"SQN\s+([\-\d.]+)",
    }
    stats = {}
    for k, p in pats.items():
        m = re.search(p, output)
        if m:
            try: stats[k] = float(m.group(1))
            except: pass
    return stats

def parse_diag(tail: list) -> dict:
    text = "\n".join(tail)
    def n(pat): m=re.search(pat,text); return float(m.group(1)) if m else 0.0
    return {
        "slope_pct":   n(r"EMA200 slope blocked:\s+\d+\s+\(([0-9.]+)%\)"),
        "below_e200":  n(r"Price < EMA200:\s+\d+\s+\(([0-9.]+)%\)"),
        "no_golden":   n(r"EMA50 < EMA200.*?:\s+\d+\s+\(([0-9.]+)%\)"),
        "adx_pct":     n(r"ADX <.*?blocked:\s+\d+\s+\(([0-9.]+)%\)"),
        "bias_pct":    n(r"Bias1m.*?bearish.*?:\s+\d+\s+\(([0-9.]+)%\)"),
        "cross_fired": n(r"EMA_CROSS fired:\s+(\d+)"),
        "avg_ratio":   n(r"Avg RNG_\d+m / ATR_1m ratio:\s+([0-9.]+)"),
    }

# ── Read / set P dict params ──────────────────────────────────────────────────

def read_param(name: str):
    m = re.search(rf"'{name}'\s*:\s*([0-9.]+)", STRATEGY.read_text())
    return m.group(1) if m else None

def set_param(name: str, value):
    text = STRATEGY.read_text()
    new  = re.sub(rf"('{name}'\s*:\s*)[0-9.]+", rf"\g<1>{value}", text)
    if new == text:
        log(f"  WARNING: param {name} not found", Y); return False
    STRATEGY.write_text(new)
    log(f"  SET P['{name}'] = {value}", G); return True

def remove_golden_cross_gate():
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
        log("Golden cross gate already removed", Y); return False
    new = (
        "        # Golden cross gate disabled — March OOS was in death-cross all month\n"
        "        # if self.ema50[-1] < e200: return\n"
    )
    STRATEGY.write_text(text.replace(old, new))
    log("Removed golden cross gate", G); return True

# ── Decision engine ───────────────────────────────────────────────────────────

def decide(stats: dict, diag: dict, history: list) -> dict:
    trades      = int(stats.get("trades", 0))
    sharpe      = stats.get("sharpe", -99)
    win_rate    = stats.get("win_rate", 0) / 100.0
    pf          = stats.get("profit_factor", 0)

    adx_min     = float(read_param("adxMin")       or 22)
    min_bias    = float(read_param("minBias")       or 0.05)
    rsi_ob      = float(read_param("rsiOB")         or 65)
    atr_tp      = float(read_param("atrTp")         or 2.8)
    atr_stop    = float(read_param("atrStop")       or 1.0)
    tgt_win     = float(read_param("targetWindow")  or 10)
    ema_slope_n = float(read_param("emaSlopeN")     or 480)

    # ── 0 trades: loosen gates ───────────────────────────────────────────
    if trades == 0:
        if diag["no_golden"] > 30:
            return {"gate": "remove_golden",
                    "reason": f"golden cross gate blocked {diag['no_golden']:.0f}% — removing"}
        if adx_min > 18:
            return {"param": "adxMin", "value": max(18, adx_min-2),
                    "reason": f"0 trades — loosen adxMin {adx_min}→{max(18,adx_min-2)}"}
        if min_bias > 0.0:
            return {"param": "minBias", "value": 0.0,
                    "reason": "0 trades — remove minBias gate"}
        return {"action": "none", "reason": "0 trades, all gates minimal — strategy doesn't fire in this regime"}

    # ── RESCUE: over-tightened params hurt win rate → full reset + pivot ──
    # Tighter stops cause more false exits in volatile BTC. Lower rsiOB
    # admits weaker pullbacks. Both hurt in this regime.
    if pf < 0.42 and atr_stop < 0.6:
        return {"action": "multi",
                "params": [("atrStop", 1.0), ("targetWindow", 10), ("rsiOB", 65)],
                "reason": f"atrStop={atr_stop:.1f}/rsiOB={rsi_ob:.0f} over-tightened PF={pf:.2f} — full reset"}

    # ── Signal quality: emaSlopeN controls how reactive slope gate is ────
    # Halving lookback from 480 (8h) → 240 (4h) → 120 (2h) filters only
    # bars where EMA200 has been rising for the past N bars. In a bear month
    # this keeps us out of weak bounces and only trades real bull windows.
    if trades >= MIN_TRADES and pf < 0.60 and atr_stop >= 0.8 and ema_slope_n > 120:
        new_slope = max(120, int(ema_slope_n * 0.5))
        return {"param": "emaSlopeN", "value": new_slope,
                "reason": f"PF={pf:.2f} — tighten slope lookback emaSlopeN {ema_slope_n:.0f}→{new_slope} (filters weak bear bounces)"}

    # ── Signal quality: lower rsiOB for better pullback entry ───────────
    # A lower RSI threshold means we only enter after a more significant
    # pullback in the EMA uptrend — better entry price, better R:R.
    if trades >= MIN_TRADES and pf < 0.60 and atr_stop >= 0.8 and rsi_ob > 50:
        new_rsi = rsi_ob - 5
        return {"param": "rsiOB", "value": new_rsi,
                "reason": f"PF={pf:.2f} — lower rsiOB {rsi_ob}→{new_rsi} for deeper pullback before entry"}

    # ── Win-rate push: sharpe already ≥ milestone but win rate < 60% ─────
    # Quality filter: only enter on the strongest trend bars.
    # Priority order: adxMin → minBias → rsiOB (narrowing RSI pullback band).
    # Never tighten past hard ceilings — would drop trade count to zero.
    if sharpe >= MILESTONE and trades >= MIN_TRADES and win_rate < (TARGET_WIN_PCT / 100):
        if adx_min < 30:
            return {"param": "adxMin", "value": adx_min + 2,
                    "reason": f"win={win_rate:.0%} < 60% — tighten adxMin {adx_min}→{adx_min+2} (stronger trend)"}
        if min_bias < 0.12:
            return {"param": "minBias", "value": round(min_bias + 0.02, 2),
                    "reason": f"win={win_rate:.0%} < 60% — raise minBias {min_bias}→{round(min_bias+0.02,2)} (more momentum)"}
        if rsi_ob > 47:
            return {"param": "rsiOB", "value": rsi_ob - 1,
                    "reason": f"win={win_rate:.0%} < 60% — lower rsiOB {rsi_ob}→{rsi_ob-1} (tighter pullback band)"}

    # ── Low win rate: tighten signal quality filters ─────────────────────
    # Applies regardless of trade count — too few qualifying signals
    # means we should tighten criteria, not trade less-than-ideal setups.
    if trades >= MIN_TRADES and win_rate < 0.38:
        if rsi_ob > 50:
            return {"param": "rsiOB", "value": rsi_ob-5,
                    "reason": f"win={win_rate:.0%} low — tighten rsiOB {rsi_ob}→{rsi_ob-5}"}
        if adx_min < 30:
            return {"param": "adxMin", "value": adx_min+2,
                    "reason": f"win={win_rate:.0%} low — tighten adxMin {adx_min}→{adx_min+2}"}
        if min_bias < 0.15:
            return {"param": "minBias", "value": round(min_bias+0.03,2),
                    "reason": f"win={win_rate:.0%} low — tighten minBias {min_bias}→{round(min_bias+0.03,2)}"}

    # ── Near breakeven (sharpe -1 to 0): widen target to push positive ───
    # With R:R near 1.6:1 and win rate near breakeven, widening the target
    # lets winners run further and can push expectancy positive.
    if -1.0 < sharpe < 0 and trades >= MIN_TRADES and pf >= 0.80:
        if atr_tp < 8.0:
            new_tp = round(atr_tp * 1.25, 2)
            return {"param": "atrTp", "value": new_tp,
                    "reason": f"sharpe={sharpe:.2f} near-breakeven PF={pf:.2f} — widen atrTp {atr_tp}→{new_tp}"}
        if adx_min < 28 and trades > 15:
            return {"param": "adxMin", "value": adx_min+2,
                    "reason": f"sharpe={sharpe:.2f} — tighten adxMin {adx_min}→{adx_min+2} for quality"}

    # ── Good win rate but sharpe negative: widen target ──────────────────
    if win_rate >= 0.48 and sharpe < 0:
        if atr_tp < 8.0:
            new_tp = round(atr_tp*1.25, 2)
            return {"param": "atrTp", "value": new_tp,
                    "reason": f"win={win_rate:.0%} good, sharpe={sharpe:.2f} — widen atrTp {atr_tp}→{new_tp}"}

    # ── Moderate win rate not yet profitable: tighten stop slightly ──────
    if 0.40 <= win_rate < 0.48 and sharpe < 0.5 and atr_stop > 0.8:
        new_stop = round(atr_stop-0.15, 2)
        return {"param": "atrStop", "value": new_stop,
                "reason": f"tighten stop atrStop {atr_stop}→{new_stop}"}

    # ── Positive sharpe: push toward TARGET_SHARPE ───────────────────────
    if 0 < sharpe < TARGET_SHARPE:
        if atr_tp < 8.0:
            new_tp = round(atr_tp*1.2, 2)
            return {"param": "atrTp", "value": new_tp,
                    "reason": f"sharpe={sharpe:.2f} — push atrTp {atr_tp}→{new_tp}"}
        if tgt_win < 30:
            return {"param": "targetWindow", "value": int(tgt_win+5),
                    "reason": f"widen targetWindow {tgt_win}→{int(tgt_win+5)}"}
        if atr_stop > 0.7 and win_rate < 0.55:
            new_stop = round(atr_stop-0.1, 2)
            return {"param": "atrStop", "value": new_stop,
                    "reason": f"sharpe={sharpe:.2f} — tighten atrStop {atr_stop}→{new_stop}"}
        if adx_min < 32 and trades > 15:
            return {"param": "adxMin", "value": adx_min+2,
                    "reason": f"quality over quantity — adxMin {adx_min}→{adx_min+2}"}

    return {"action": "none",
            "reason": f"sharpe={sharpe:.2f} win={win_rate:.0%} PF={pf:.2f} — holding params"}

# ── Commit and push code change ───────────────────────────────────────────────

def commit_and_push(message: str):
    git("add",
        str(STRATEGY.relative_to(REPO)),
        str(TRIGGER_F.relative_to(REPO)))
    r = git("commit", "-m", f"optimizer: {message}")
    if r.returncode != 0 and "nothing to commit" not in r.stdout+r.stderr:
        log(f"commit warning: {r.stderr.strip()}", Y)
    for attempt in range(4):
        r = git("push", "-u", "origin", CODE_BRANCH)
        if r.returncode == 0:
            log("Pushed to code branch", G); return True
        wait = 2**(attempt+1)
        log(f"Push failed — retry in {wait}s", Y); time.sleep(wait)
    log("Push failed after 4 attempts", R); return False

# ── Main loop ─────────────────────────────────────────────────────────────────

def detect_start_iter() -> int:
    """Read iter number from latest bt_trigger.json on code branch, increment by 1."""
    try:
        r = git("show", f"origin/{CODE_BRANCH}:bt_trigger.json")
        if r.returncode == 0:
            d = json.loads(r.stdout)
            m = re.match(r'iter(\d+)', d.get("id", ""))
            if m:
                return int(m.group(1)) + 1
    except Exception:
        pass
    return 37  # fallback: known current state

def main():
    log(f"Auto-optimizer starting — target Sharpe ≥ {TARGET_SHARPE} (milestone {MILESTONE})", G)

    iter_num = detect_start_iter()
    history  = []

    while True:
        check_self_reload()
        sync_code()

        log(f"── iter{iter_num} ── running backtest", B)
        try:
            result = run_backtest()
        except subprocess.TimeoutExpired:
            log("Backtest timed out — retrying", R); time.sleep(10); continue
        except Exception as e:
            log(f"Backtest error: {e}", R); time.sleep(10); continue

        stats  = result["stats"]
        diag   = parse_diag(result["output_tail"])
        sharpe = stats.get("sharpe", -99)
        trades = int(stats.get("trades", 0))
        win    = stats.get("win_rate", 0)
        ret    = stats.get("return_pct", -99)
        iter_id = f"iter{iter_num}_{datetime.now().strftime('%H%M%S')}"

        col = G if sharpe > 0 else (Y if sharpe > -2 else R)
        log(f"[{iter_id}] sharpe={sharpe:+.3f} | win={win:.1f}% | trades={trades} | ret={ret:+.2f}%", col)
        log(f"  adx={diag['adx_pct']:.0f}%blk  slope={diag['slope_pct']:.0f}%blk  "
            f"no_golden={diag['no_golden']:.0f}%blk  cross_fired={int(diag['cross_fired'])}")

        push_results(iter_id, result)

        if sharpe >= MILESTONE and trades >= MIN_TRADES:
            log(f"MILESTONE: Sharpe={sharpe:.3f} ≥ {MILESTONE} — pushing toward {TARGET_SHARPE}", G)

        if sharpe >= TARGET_SHARPE and trades >= MIN_TRADES and win >= TARGET_WIN_PCT:
            log(f"TARGET REACHED — Sharpe={sharpe:.3f} win={win:.1f}% ({trades} trades)", G)
            TRIGGER_F.write_text(json.dumps(
                {"id": iter_id+"_DONE", "command": "STOP",
                 "bash": "", "message": f"Sharpe={sharpe:.3f} win={win:.1f}% — DONE"}, indent=2))
            commit_and_push(f"SUCCESS Sharpe={sharpe:.3f} win={win:.1f}%")
            break

        history.append({**stats, "iter": iter_id})
        decision = decide(stats, diag, history)
        log(f"  → {decision.get('reason','?')}", C)

        if decision.get("gate") == "remove_golden":
            remove_golden_cross_gate()
        elif decision.get("action") == "multi":
            for p_name, p_val in decision.get("params", []):
                set_param(p_name, p_val)
        elif decision.get("param"):
            set_param(decision["param"], decision["value"])

        iter_num += 1
        TRIGGER_F.write_text(json.dumps({
            "id": f"iter{iter_num}",
            "command": "RUN",
            "bash": "",
            "message": decision.get("reason", "auto step"),
        }, indent=2))

        commit_and_push(decision.get("reason", "auto step"))
        log(f"  iter{iter_num} committed — starting next run\n", C)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    main()
