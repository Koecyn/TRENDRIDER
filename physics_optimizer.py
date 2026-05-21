#!/usr/bin/env python3
"""
physics_optimizer.py — Self-contained optimization loop for the physics engine.

Mirrors the existing auto_optimizer.py pattern:
  - Reads  physics_trigger.json    (code branch)
  - Runs   physics backtest in-process
  - Writes physics_results.json    → data/live branch
  - Edits  physics/config.py       → code branch
  - Pushes both, then loops

Usage (Termux):
  cd ~/TRENDRIDER
  python physics_optimizer.py

Hot-reload: if physics_optimizer.py changes on the remote branch,
the process re-execs itself so new optimizer logic takes effect immediately.
"""

import json, os, re, subprocess, sys, time, hashlib
from datetime import datetime
from pathlib import Path

REPO        = Path(__file__).resolve().parent
CODE_BRANCH = "claude/hft-mean-reversion-strategy-pJ4YU"
DATA_BRANCH = "data/live"
CONFIG_F    = REPO / "physics" / "config.py"
TRIGGER_F   = REPO / "physics_trigger.json"
RESULTS_F   = REPO / "physics_results.json"
SELF_F      = Path(__file__).resolve()
LOG_F       = REPO / "physics_optimizer.log"

# ── Optimization targets ──────────────────────────────────────────────────────
TARGET_SHARPE   = 1.6
TARGET_WIN_PCT  = 60.0    # 60% — not negotiable
TARGET_TRADES   = 30      # minimum trades over test window
TIER1_WIN_TGT   = 78.0    # Tier 1 mechanical target
TIER2_WIN_TGT   = 65.0    # Tier 2 target

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; B='\033[1m'; Z='\033[0m'

def ts():
    return datetime.now().strftime('%H:%M:%S')

def log(msg, col=Z):
    line = f"{col}[physics {ts()}] {msg}{Z}"
    print(line, flush=True)
    try:
        with open(LOG_F, 'a') as f:
            f.write(re.sub(r'\033\[[0-9;]*m', '', line) + '\n')
    except Exception:
        pass

ENV = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}

def git(*args, stdin=None):
    return subprocess.run(
        ["git"] + list(args), cwd=REPO,
        capture_output=True, text=True, env=ENV
    )


# ── Parameter read/write ──────────────────────────────────────────────────────

def read_param(name: str):
    """Read a numeric param from physics/config.py."""
    m = re.search(rf"^{name}\s*=\s*(-?[0-9.]+)", CONFIG_F.read_text(), re.MULTILINE)
    return m.group(1) if m else None

def set_param(name: str, value):
    """Overwrite a numeric param in physics/config.py."""
    text = CONFIG_F.read_text()
    new  = re.sub(rf"(^{name}\s*=\s*)-?[0-9.]+", rf"\g<1>{value}", text, flags=re.MULTILINE)
    if new == text:
        log(f"  WARNING: param {name} not found in config.py", Y)
        return False
    CONFIG_F.write_text(new)
    return True


# ── Git helpers ───────────────────────────────────────────────────────────────

def fetch_pull():
    git("stash")
    git("fetch", "origin", CODE_BRANCH)
    r = git("rebase", f"origin/{CODE_BRANCH}")
    if r.returncode != 0:
        log(f"Rebase conflict — resetting to origin: {r.stderr[:200]}", R)
        git("rebase", "--abort")
        git("reset", "--hard", f"origin/{CODE_BRANCH}")
    git("stash", "pop")

def commit_and_push(message: str):
    git("add", str(CONFIG_F.relative_to(REPO)), str(TRIGGER_F.relative_to(REPO)))
    git("commit", "-m", message)
    for attempt, wait in enumerate([0, 2, 4, 8, 16]):
        time.sleep(wait)
        r = git("push", "-u", "origin", CODE_BRANCH)
        if r.returncode == 0:
            return
        log(f"Push failed (attempt {attempt+1}): {r.stderr[:100]}", Y)
    log("All push attempts failed", R)

def push_results():
    """Write physics_results.json to data/live branch."""
    rel = str(RESULTS_F.relative_to(REPO))
    # Stash, switch to data branch, write, commit, push, switch back
    git("stash")
    git("fetch", "origin", DATA_BRANCH)
    r = git("checkout", "-B", DATA_BRANCH, f"origin/{DATA_BRANCH}")
    if r.returncode != 0:
        git("checkout", "-b", DATA_BRANCH)
    import shutil
    shutil.copy(str(RESULTS_F), str(RESULTS_F))   # already written by caller
    git("add", rel)
    git("commit", "--allow-empty", "-m", f"physics results {ts()}")
    for attempt, wait in enumerate([0, 2, 4, 8, 16]):
        time.sleep(wait)
        r = git("push", "-u", "origin", DATA_BRANCH, "--force")
        if r.returncode == 0:
            break
    git("checkout", CODE_BRANCH)
    git("stash", "pop")


# ── Self hot-reload ───────────────────────────────────────────────────────────

def _self_hash() -> str:
    return hashlib.md5(SELF_F.read_bytes()).hexdigest()

_STARTUP_HASH = _self_hash()

def check_reload():
    git("fetch", "origin", CODE_BRANCH)
    r = git("show", f"origin/{CODE_BRANCH}:{SELF_F.name}")
    if r.returncode == 0:
        remote_hash = hashlib.md5(r.stdout.encode()).hexdigest()
        if remote_hash != _STARTUP_HASH:
            log("optimizer changed on remote — re-execing", Y)
            git("reset", "--hard", f"origin/{CODE_BRANCH}")
            os.execv(sys.executable, [sys.executable, str(SELF_F)] + sys.argv[1:])


# ── Run backtest ──────────────────────────────────────────────────────────────

def run_backtest() -> dict:
    """Import physics package fresh and run backtest. Returns summary dict."""
    # Flush module cache so config changes take effect
    for key in list(sys.modules.keys()):
        if key.startswith('physics'):
            del sys.modules[key]

    from physics import data as D, backtester as BT, config as CF

    log("Fetching data…", C)
    d = D.get_data(cache_path=str(REPO / "physics_data_cache.csv"),
                   refresh=False)
    log(f"  {len(d['prices'])} bars loaded", C)

    log("Running backtest…", C)
    result = BT.run(d, initial_balance=CF.INITIAL_BAL, long_only=True)

    summary = result.summary()
    log(f"  n={summary['n_trades']}  sharpe={summary['sharpe']:.3f}"
        f"  win={summary['win_rate']:.1f}%  dd={summary['max_dd']:.2f}%", C)
    for tier in [1, 2, 3]:
        ts_ = result.tier_stats(tier)
        if ts_['n'] > 0:
            log(f"  Tier {tier}: n={ts_['n']}  win={ts_['win_rate']:.1f}%  "
                f"sharpe={ts_['sharpe']:.3f}", C)

    return summary


# ── Optimization decision ─────────────────────────────────────────────────────

def decide(stats: dict) -> dict:
    """
    Single most impactful parameter change based on current results.
    Priority:
      0 trades         → lower LONG_THRESHOLD (let more signals through)
      Tier1 win < 70%  → lower MACH_SHOCK (more water hammer confirmations)
      Tier2 win < 55%  → adjust SOLITON_BAL
      Sharpe < 0       → loosen VISC_BASE (lower friction = more flow signals)
      Too few trades   → lower LONG_THRESHOLD
      Low overall win  → raise SOLITON_BAL (stricter soliton quality)
      Low sharpe       → tune KDV_ALPHA
    """
    n         = stats.get('n_trades', 0)
    sharpe    = stats.get('sharpe', -99)
    win       = stats.get('win_rate', 0)
    t1_n      = stats.get('t1_n', 0)
    t1_win    = stats.get('t1_win_rate', 0)
    t2_n      = stats.get('t2_n', 0)
    t2_win    = stats.get('t2_win_rate', 0)

    thresh    = float(read_param('LONG_THRESHOLD') or 0.12)
    mach      = float(read_param('MACH_SHOCK') or 1.5)
    sol_bal   = float(read_param('SOLITON_BAL') or 0.5)
    kdv_alpha = float(read_param('KDV_ALPHA') or 0.1)
    visc      = float(read_param('VISC_BASE') or 0.02)

    # No trades at all — lower entry threshold
    if n == 0:
        new = round(max(0.06, thresh - 0.02), 3)
        return {'param': 'LONG_THRESHOLD', 'value': new,
                'reason': f'0 trades — lower threshold {thresh}→{new}'}

    # Too few trades overall
    if n < TARGET_TRADES and thresh > 0.07:
        new = round(max(0.07, thresh - 0.01), 3)
        return {'param': 'LONG_THRESHOLD', 'value': new,
                'reason': f'n={n} < {TARGET_TRADES} — lower threshold {thresh}→{new}'}

    # Tier 1 win rate too low — lower Mach threshold to get cleaner confirmations
    if t1_n >= 3 and t1_win < 70.0 and mach > 1.0:
        new = round(max(1.0, mach - 0.1), 2)
        return {'param': 'MACH_SHOCK', 'value': new,
                'reason': f'Tier1 win={t1_win:.1f}% < 70% — tighten MACH_SHOCK {mach}→{new}'}

    # Tier 2 win too low — stricter soliton
    if t2_n >= 5 and t2_win < 55.0 and sol_bal < 0.9:
        new = round(min(0.9, sol_bal + 0.1), 2)
        return {'param': 'SOLITON_BAL', 'value': new,
                'reason': f'Tier2 win={t2_win:.1f}% — tighten SOLITON_BAL {sol_bal}→{new}'}

    # Negative sharpe — reduce friction so Darcy signals are more active
    if sharpe < 0 and visc > 0.005:
        new = round(max(0.005, visc * 0.8), 4)
        return {'param': 'VISC_BASE', 'value': new,
                'reason': f'sharpe={sharpe:.2f} — reduce VISC_BASE {visc}→{new}'}

    # Overall win rate too low — increase soliton quality gate
    if win < TARGET_WIN_PCT and n >= TARGET_TRADES and sol_bal < 0.8:
        new = round(min(0.8, sol_bal + 0.05), 2)
        return {'param': 'SOLITON_BAL', 'value': new,
                'reason': f'win={win:.1f}% < {TARGET_WIN_PCT}% — raise SOLITON_BAL {sol_bal}→{new}'}

    # Sharpe positive but below target — tune nonlinearity sensitivity
    if 0 < sharpe < TARGET_SHARPE and kdv_alpha < 0.25:
        new = round(min(0.25, kdv_alpha + 0.02), 3)
        return {'param': 'KDV_ALPHA', 'value': new,
                'reason': f'sharpe={sharpe:.3f} — push KDV_ALPHA {kdv_alpha}→{new}'}

    return {'action': 'hold',
            'reason': f'sharpe={sharpe:.3f} win={win:.1f}% n={n} — no change'}


# ── Main loop ─────────────────────────────────────────────────────────────────

def next_iter_id() -> str:
    try:
        cur = json.loads(TRIGGER_F.read_text()).get('id', 'p0')
        n   = int(re.search(r'\d+', cur.lstrip('p')).group()) + 1
    except Exception:
        n = 1
    return f"p{n}"


def main():
    log(f"Physics optimizer starting — target Sharpe≥{TARGET_SHARPE} "
        f"Win≥{TARGET_WIN_PCT}% Trades≥{TARGET_TRADES}", G)

    # Initial trigger if missing
    if not TRIGGER_F.exists():
        TRIGGER_F.write_text(json.dumps({
            'id': 'p0', 'command': 'RUN', 'bash': '', 'message': 'initial run'
        }, indent=2))

    iter_count = 0

    while True:
        iter_count += 1
        check_reload()
        fetch_pull()

        trigger = json.loads(TRIGGER_F.read_text())
        if trigger.get('command') == 'STOP':
            log(f"STOP command received: {trigger.get('message', '')}", G)
            break

        log(f"=== iter {trigger['id']} ===", B)

        # Run backtest
        try:
            stats = run_backtest()
        except Exception as e:
            log(f"Backtest error: {e}", R)
            import traceback; traceback.print_exc()
            time.sleep(30)
            continue

        # Write results to file (push_results will commit to data/live)
        n         = stats.get('n_trades', 0)
        sharpe    = stats.get('sharpe', 0)
        win       = stats.get('win_rate', 0)
        ret       = stats.get('equity_final', 0)

        result_data = {
            'ts':         datetime.utcnow().isoformat(),
            'trigger_id': trigger['id'],
            'status':     'complete',
            'stats':      stats,
        }
        RESULTS_F.write_text(json.dumps(result_data, indent=2))
        push_results()

        col = G if sharpe >= TARGET_SHARPE else (Y if sharpe > 0 else R)
        log(f"[{trigger['id']}] sharpe={sharpe:+.3f} | win={win:.1f}% | "
            f"trades={n} | equity={ret:.2f}", col)

        # Check target
        if sharpe >= TARGET_SHARPE and n >= TARGET_TRADES and win >= TARGET_WIN_PCT:
            log(f"TARGET REACHED — Sharpe={sharpe:.3f} Win={win:.1f}% ({n} trades)", G)
            TRIGGER_F.write_text(json.dumps({
                'id':      next_iter_id(),
                'command': 'STOP',
                'bash':    '',
                'message': f"Sharpe={sharpe:.3f} Win={win:.1f}% — DONE",
            }, indent=2))
            commit_and_push(f"physics SUCCESS: Sharpe={sharpe:.3f} win={win:.1f}%")
            break

        # Decide and apply parameter change
        decision = decide(stats)
        reason   = decision.get('reason', '')

        if decision.get('action') == 'hold':
            log(f"  Holding params — {reason}", C)
        else:
            param, value = decision['param'], decision['value']
            log(f"  → {param} = {value}   ({reason})", Y)
            set_param(param, value)

        # Write new trigger
        new_id = next_iter_id()
        TRIGGER_F.write_text(json.dumps({
            'id':      new_id,
            'command': 'RUN',
            'bash':    '',
            'message': reason,
        }, indent=2))

        commit_msg = f"{new_id}: {reason[:80]}"
        commit_and_push(commit_msg)

        log(f"Sleeping 5s before next iteration…", C)
        time.sleep(5)


if __name__ == '__main__':
    main()
