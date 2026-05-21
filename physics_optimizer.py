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

REPO           = Path(__file__).resolve().parent
CODE_BRANCH    = "claude/hft-mean-reversion-strategy-pJ4YU"
DATA_BRANCH    = "data/live"
CONFIG_F       = REPO / "physics" / "config.py"
TRIGGER_F      = REPO / "physics_trigger.json"
RESULTS_F      = REPO / "physics_results.json"
LIVE_RESULTS_F = REPO / "physics_live_results.json"   # written by physics_live.py
SELF_F         = Path(__file__).resolve()
LOG_F          = REPO / "physics_optimizer.log"

LIVE_STALE_SECS = 600   # live results older than this → wait for fresh data

# ── Optimization targets ──────────────────────────────────────────────────────
TARGET_SHARPE   = 2.5
TARGET_WIN_PCT  = 60.0    # 60% — not negotiable
TARGET_TPH      = 5.0     # trades per hour
MIN_TPH         = 2.0     # below this → open the gate
MAX_TPH         = 10.0    # above this → too much noise
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
    # Save results content before switching branches
    results_content = RESULTS_F.read_text()
    git("stash")
    git("fetch", "origin", DATA_BRANCH)
    r = git("checkout", "-B", DATA_BRANCH, f"origin/{DATA_BRANCH}")
    if r.returncode != 0:
        git("checkout", "-b", DATA_BRANCH)
    # Re-write the file on the data branch (stash hid it)
    RESULTS_F.write_text(results_content)
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


# ── Live stats reader ─────────────────────────────────────────────────────────

def read_live_stats() -> dict | None:
    """
    Read stats written by physics_live.py from the data/live branch.
    Reads via 'git show' to avoid depending on the local working-tree file,
    which gets deleted whenever the live engine switches back to CODE_BRANCH
    after pushing results.
    """
    from datetime import datetime
    try:
        git("fetch", "origin", DATA_BRANCH)
        r = git("show", f"origin/{DATA_BRANCH}:physics_live_results.json")
        if r.returncode != 0:
            log("physics_live_results.json not on data/live yet — waiting", Y)
            return None
        raw   = json.loads(r.stdout)
        ts_str = raw.get('ts', '')
        # Parse timestamp robustly — handle naive and aware forms
        from datetime import timezone
        # Handle naive timestamps (no tz suffix) and aware ones ('Z' or '+HH:MM')
        if ts_str.endswith('Z'):
            ts_str = ts_str[:-1] + '+00:00'
        try:
            written = datetime.fromisoformat(ts_str)
            if written.tzinfo is None:          # naive → treat as UTC
                written = written.replace(tzinfo=timezone.utc)
        except Exception:
            written = datetime.fromtimestamp(0, tz=timezone.utc)
        age_s = (datetime.now(timezone.utc) - written).total_seconds()
        if age_s > LIVE_STALE_SECS:
            log(f"Live results stale ({age_s:.0f}s old) — waiting for physics_live.py", Y)
            return None
        stats = raw.get('stats', {})
        n   = stats.get('n_trades', 0)
        log(f"Live stats: n={n}  bars={stats.get('bars_live',0)}"
            f"  score={stats.get('last_score',0):+.4f}"
            f"  snr={stats.get('last_snr',0):.2f}"
            f"  sharpe={stats.get('sharpe',0):.3f}"
            f"  win={stats.get('win_rate',0):.1f}%"
            f"  tph={stats.get('trades_per_hr',0):.2f}"
            f"  age={age_s:.0f}s", C)
        return stats
    except Exception as e:
        log(f"Live stats read error: {e}", R)
        return None


# ── Optimization decision ─────────────────────────────────────────────────────

def decide(stats: dict) -> dict:
    """
    Physics-informed tuning priority:
      1. MAE_ATR_FALLBACK  — stop must be outside 1m noise (target 3.0)
      2. TIER1_WH_STRENGTH — only genuine trough reflections qualify (target 2.0)
      3. SOLITON_BAL       — ascending wave must be self-reinforcing (target 0.92)
      4. SNR_MIN           — filter noise bars (ceiling 4.0)
      5. CVD_DIV           — divergence gate (ceiling 0.55 — avoid starving signals)
      6. Trade frequency   — LONG_THRESHOLD ± 0.01 to stay in 2-10 tph band
      7. KDV_ALPHA         — amplify edge once win≥60%
    """
    n      = stats.get('n_trades', 0)
    sharpe = stats.get('sharpe', -99)
    win    = stats.get('win_rate', 0)
    tph    = stats.get('trades_per_hr', 0)

    thresh    = float(read_param('LONG_THRESHOLD') or 0.12)
    sol_bal   = float(read_param('SOLITON_BAL') or 0.5)
    kdv_alpha = float(read_param('KDV_ALPHA') or 0.1)
    visc      = float(read_param('VISC_BASE') or 0.02)
    snr_min   = float(read_param('SNR_MIN') or 2.0)
    cvd_div   = float(read_param('CVD_DIV') or 0.30)
    mae_atr   = float(read_param('MAE_ATR_FALLBACK') or 1.5)
    wh_str    = float(read_param('TIER1_WH_STRENGTH') or 0.5)

    # ── Priority 1: stop distance — 1m BTC noise stops trades before wave develops
    if mae_atr < 3.0:
        new = round(min(3.0, mae_atr + 0.5), 1)
        return {'param': 'MAE_ATR_FALLBACK', 'value': new,
                'reason': f'stops too tight for 1m — MAE_ATR {mae_atr}→{new}'}

    # ── Priority 2: water hammer trough gate — only genuine reflections
    if wh_str < 2.0:
        new = round(min(2.0, wh_str + 0.5), 2)
        return {'param': 'TIER1_WH_STRENGTH', 'value': new,
                'reason': f'WH noise trough filter — strength {wh_str}→{new}'}

    # ── Priority 3: soliton wave quality — ascending phase only
    if sol_bal < 0.92 and win < 40.0:
        new = round(min(0.92, sol_bal + 0.03), 2)
        return {'param': 'SOLITON_BAL', 'value': new,
                'reason': f'win={win:.1f}% — tighten ascending wave gate {sol_bal}→{new}'}

    # CVD over ceiling — roll back first (blocks both frequency and win rate)
    if cvd_div > 0.55:
        new = 0.55
        return {'param': 'CVD_DIV', 'value': new,
                'reason': f'CVD_DIV={cvd_div} over ceiling — roll back to {new}'}

    # No trades at all — open the gate
    if n == 0:
        new = round(max(0.06, thresh - 0.02), 3)
        return {'param': 'LONG_THRESHOLD', 'value': new,
                'reason': f'0 trades — lower threshold {thresh}→{new}'}

    # Trade frequency too low — try threshold first, then SNR if threshold at floor
    if tph < MIN_TPH:
        if thresh > 0.08:
            new = round(max(0.08, thresh - 0.01), 3)
            return {'param': 'LONG_THRESHOLD', 'value': new,
                    'reason': f'tph={tph:.1f} < {MIN_TPH} — lower threshold {thresh}→{new}'}
        if snr_min > 3.0:
            new = round(max(3.0, snr_min - 0.25), 2)
            return {'param': 'SNR_MIN', 'value': new,
                    'reason': f'tph={tph:.1f} threshold floored — lower SNR_MIN {snr_min}→{new}'}

    # Too many trades — noise flooding in, raise threshold
    if tph > MAX_TPH and thresh < 0.22:
        new = round(min(0.22, thresh + 0.01), 3)
        return {'param': 'LONG_THRESHOLD', 'value': new,
                'reason': f'tph={tph:.1f} > {MAX_TPH} — raise threshold {thresh}→{new}'}

    # Win still bad — raise SNR (cap at 4.0 to avoid killing all signals)
    if win < 30.0 and snr_min < 4.0:
        new = round(min(4.0, snr_min + 0.25), 2)
        return {'param': 'SNR_MIN', 'value': new,
                'reason': f'win={win:.1f}% terrible — raise SNR_MIN {snr_min}→{new}'}

    # Win below target — tighten CVD gate (hard ceiling 0.55)
    if win < TARGET_WIN_PCT and cvd_div < 0.55:
        new = round(min(0.55, cvd_div + 0.05), 2)
        return {'param': 'CVD_DIV', 'value': new,
                'reason': f'win={win:.1f}% < {TARGET_WIN_PCT}% — raise CVD_DIV {cvd_div}→{new}'}

    # Negative sharpe — reduce friction
    if sharpe < 0 and visc > 0.005:
        new = round(max(0.005, visc * 0.8), 4)
        return {'param': 'VISC_BASE', 'value': new,
                'reason': f'sharpe={sharpe:.3f} — reduce VISC_BASE {visc}→{new}'}

    # Good win rate, below sharpe target — amplify nonlinearity
    if win >= TARGET_WIN_PCT and 0 < sharpe < TARGET_SHARPE and kdv_alpha < 0.25:
        new = round(min(0.25, kdv_alpha + 0.02), 3)
        return {'param': 'KDV_ALPHA', 'value': new,
                'reason': f'sharpe={sharpe:.3f} win={win:.1f}% — push KDV_ALPHA {kdv_alpha}→{new}'}

    return {'action': 'hold',
            'reason': f'sharpe={sharpe:.3f} win={win:.1f}% tph={tph:.1f} — holding'}


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
        f"Win≥{TARGET_WIN_PCT}% TPH≥{TARGET_TPH}", G)
    log("Mode: LIVE WebSocket stats — physics_live.py must be running in parallel", C)

    # Initial trigger if missing
    if not TRIGGER_F.exists():
        TRIGGER_F.write_text(json.dumps({
            'id': 'p0', 'command': 'RUN', 'bash': '', 'message': 'initial run'
        }, indent=2))

    while True:
        check_reload()
        fetch_pull()

        trigger = json.loads(TRIGGER_F.read_text())
        if trigger.get('command') == 'STOP':
            log(f"STOP command received: {trigger.get('message', '')}", G)
            break

        log(f"=== iter {trigger['id']} ===", B)

        # Read live stats from physics_live.py WebSocket engine
        stats = read_live_stats()
        if stats is None:
            log("No fresh live stats — ensure physics_live.py is running:", Y)
            log("  python physics_live.py", Y)
            time.sleep(30)
            continue

        n_live = stats.get('n_trades', 0)
        if n_live < 5:
            log(f"Only {n_live} live trades so far — need ≥5 for reliable decisions, waiting 60s…", Y)
            time.sleep(60)
            continue

        sharpe = stats.get('sharpe', 0)
        win    = stats.get('win_rate', 0)
        tph    = stats.get('trades_per_hr', 0)

        col = G if sharpe >= TARGET_SHARPE else (Y if sharpe > 0 else R)
        log(f"[{trigger['id']}] sharpe={sharpe:+.3f} | win={win:.1f}% | "
            f"trades={n_live} ({tph:.1f}/hr)", col)

        # Check target
        if sharpe >= TARGET_SHARPE and tph >= MIN_TPH and win >= TARGET_WIN_PCT:
            log(f"TARGET REACHED — Sharpe={sharpe:.3f} Win={win:.1f}% ({n_live} trades)", G)
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

        # Write new trigger and push config change to code branch
        # physics_live.py will git-reset --hard to pick up new params in ≤30s
        new_id = next_iter_id()
        TRIGGER_F.write_text(json.dumps({
            'id':      new_id,
            'command': 'RUN',
            'bash':    '',
            'message': reason,
        }, indent=2))
        commit_and_push(f"{new_id}: {reason[:80]}")

        # Wait for live engine to hot-reload new params (30s) + accumulate trades (60s)
        log(f"Params pushed — waiting 90s for live engine to reload and generate fresh stats…", C)
        time.sleep(90)


if __name__ == '__main__':
    main()
