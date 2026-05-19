#!/usr/bin/env python3
"""
bt_watcher.py — poll-and-rerun backtester bridge.

Same two-branch architecture as watcher.py:
  CODE_BRANCH — Claude pushes bt_trigger.json here  (bt_watcher reads only)
  DATA_BRANCH — bt_watcher pushes bt_results.json   (Claude reads only)

Workflow:
  1. On start: read bt_trigger.json, run the specified backtest command
  2. After each run: parse stats, push bt_results.json to DATA_BRANCH
  3. Poll CODE_BRANCH every POLL_S seconds for a new bt_trigger.json
  4. On new trigger id: re-run with the updated command / params
  5. STOP command: exit cleanly

Claude writes bt_trigger.json to CODE_BRANCH.
bt_watcher pushes bt_results.json to DATA_BRANCH after every run.

Usage:
  cd /path/to/TRENDRIDER
  python backtester/bt_watcher.py

bt_trigger.json format:
  {
    "id":      "20260519_150000",
    "command": "RUN",
    "bash":    "python backtester/strategy_bt.py --csv btc_30d.csv --balance 100",
    "message": "optional note"
  }

Commands:
  RUN / RELOAD  — stop any running backtest, re-run with new bash
  STOP          — exit bt_watcher
"""

import os, sys, re, json, subprocess, time, threading
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

# ── Config ────────────────────────────────────────────────────────────────────
REPO_DIR    = Path(__file__).parent.parent.resolve()
CODE_BRANCH = "claude/hft-mean-reversion-strategy-pJ4YU"
DATA_BRANCH = "data/live"
GITHUB_USER = "Koecyn"
TRIGGER_F   = REPO_DIR / "bt_trigger.json"
RESULTS_F   = REPO_DIR / "bt_results.json"
LOG_F       = REPO_DIR / "backtester" / "bt_run.log"
POLL_S      = 20

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; B='\033[1m'; Z='\033[0m'

def ts():
    return datetime.now().strftime('%H:%M:%S')

def log(msg, col=Z):
    print(f"{col}[bt_watcher {ts()}] {msg}{Z}", flush=True)

# ── Git helpers (identical pattern to watcher.py) ─────────────────────────────
def git(args, stdin=None):
    return subprocess.run(
        ["git"] + args, cwd=REPO_DIR,
        capture_output=True, text=True, input=stdin)

def git_unlock():
    for lock in ["index.lock", "MERGE_HEAD", "CHERRY_PICK_HEAD"]:
        p = REPO_DIR / ".git" / lock
        if p.exists():
            p.unlink()
            log(f"Removed stale {lock}", Y)

def inject_token():
    token = os.getenv("GITHUB_TOKEN", "").strip()
    if not token:
        return
    r = git(["remote", "get-url", "origin"])
    url = r.stdout.strip()
    import re as _re
    url = _re.sub(r"https://[^@]*@", "https://", url)
    if url.startswith("https://"):
        git(["remote", "set-url", "origin",
             url.replace("https://", f"https://{GITHUB_USER}:{token}@", 1)])

def fetch_trigger():
    """Pull latest bt_trigger.json from CODE_BRANCH without touching working tree."""
    git(["fetch", "origin", CODE_BRANCH, "-q"])
    r = git(["show", f"origin/{CODE_BRANCH}:bt_trigger.json"])
    if r.returncode == 0:
        try:
            data = json.loads(r.stdout)
            TRIGGER_F.write_text(r.stdout)
            return data
        except Exception:
            pass
    return None

def read_trigger():
    try:
        if TRIGGER_F.exists():
            return json.loads(TRIGGER_F.read_text())
    except Exception:
        pass
    return None

def push_results(payload: dict):
    """Push bt_results.json to DATA_BRANCH using git plumbing — no checkout needed."""
    git_unlock()
    RESULTS_F.write_text(json.dumps(payload, indent=2))

    r = git(["hash-object", "-w", str(RESULTS_F)])
    blob = r.stdout.strip()
    if not blob:
        log("hash-object failed", R); return

    r = git(["mktree"], stdin=f"100644 blob {blob}\tbt_results.json\n")
    tree = r.stdout.strip()
    if not tree:
        log("mktree failed", R); return

    r = git(["rev-parse", f"refs/remotes/origin/{DATA_BRANCH}"])
    parent_args = ["-p", r.stdout.strip()] if r.returncode == 0 else []

    msg = f"bt_results {datetime.now().strftime('%Y%m%d_%H%M%S')}"
    r = git(["commit-tree", tree] + parent_args + ["-m", msg])
    commit = r.stdout.strip()
    if not commit:
        log("commit-tree failed", R); return

    r = git(["push", "origin", f"{commit}:refs/heads/{DATA_BRANCH}"])
    if r.returncode == 0:
        git(["update-ref", f"refs/remotes/origin/{DATA_BRANCH}", commit])
        log("Results pushed to data/live", G)
    else:
        log(f"push failed: {r.stderr.strip()}", R)

# ── Stats parser ──────────────────────────────────────────────────────────────
def parse_stats(output: str) -> dict:
    """Extract key metrics from backtesting.py stdout."""
    stats = {}
    patterns = {
        'trades':         r'# Trades\s+(\d+)',
        'win_rate':       r'Win Rate \[%\]\s+([\d.]+)',
        'sharpe':         r'Sharpe Ratio\s+([\-\d.]+)',
        'sortino':        r'Sortino Ratio\s+([\-\d.]+)',
        'return_pct':     r'Return \[%\]\s+([\-\d.]+)',
        'equity_final':   r'Equity Final \[\$\]\s+([\-\d.]+)',
        'max_dd':         r'Max\. Drawdown \[%\]\s+([\-\d.]+)',
        'profit_factor':  r'Profit Factor\s+([\-\d.]+)',
        'avg_trade_pct':  r'Avg\. Trade \[%\]\s+([\-\d.]+)',
        'sqn':            r'SQN\s+([\-\d.]+)',
        'exposure':       r'Exposure Time \[%\]\s+([\-\d.]+)',
    }
    for key, pat in patterns.items():
        m = re.search(pat, output)
        if m:
            try:
                stats[key] = float(m.group(1))
            except ValueError:
                pass

    # Diagnostic lines (optional, present in both strategy files)
    m = re.search(r'Avg ATR bias.*?:\s*([\d.]+)', output)
    if m:
        stats['atr_bias'] = float(m.group(1))

    m = re.search(r'EMA_CROSS fired:\s*(\d+)', output)
    if m:
        stats['cross_signals'] = int(m.group(1))

    m = re.search(r'EMA_PULLBACK fired:\s*(\d+)', output)
    if m:
        stats['pullback_signals'] = int(m.group(1))

    m = re.search(r'ADX.*?blocked.*?(\d+)', output)
    if m:
        stats['adx_blocked'] = int(m.group(1))

    m = re.search(r'EMA bear.*?(\d+)\s+\(([\d.]+)%\)', output)
    if m:
        stats['ema_bear_pct'] = float(m.group(2))

    m = re.search(r'Bias.*?too bearish.*?(\d+)\s+\(([\d.]+)%\)', output)
    if m:
        stats['bias_blocked_pct'] = float(m.group(2))

    return stats

# ── Backtest runner ───────────────────────────────────────────────────────────
class Runner:
    def __init__(self):
        self._proc   = None
        self._thread = None
        self._output = []
        self._lock   = threading.Lock()

    def _stream(self):
        try:
            for line in self._proc.stdout:
                sys.stdout.write(line); sys.stdout.flush()
                with self._lock:
                    self._output.append(line)
                try:
                    LOG_F.parent.mkdir(parents=True, exist_ok=True)
                    with open(LOG_F, "a") as f:
                        f.write(line)
                except Exception:
                    pass
        except Exception:
            pass

    def running(self):
        return self._proc is not None and self._proc.poll() is None

    def stop(self):
        if not self.running():
            return
        log(f"Stopping PID {self._proc.pid}…", Y)
        try:
            self._proc.terminate()
            self._proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait()
        if self._thread:
            self._thread.join(timeout=5)
        self._proc = None

    def run(self, bash_cmd: str) -> dict:
        """Run bash_cmd synchronously, stream output, return result dict."""
        self.stop()
        log(f"Running: {bash_cmd}", C)
        with self._lock:
            self._output = []

        header = (f"\n{'='*60}\n"
                  f"[{datetime.now().isoformat()}] {bash_cmd}\n"
                  f"{'='*60}\n")
        sys.stdout.write(header); sys.stdout.flush()

        self._proc = subprocess.Popen(
            bash_cmd, shell=True, cwd=REPO_DIR,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            bufsize=1, universal_newlines=True)

        self._thread = threading.Thread(target=self._stream, daemon=True)
        self._thread.start()
        self._proc.wait()
        self._thread.join(timeout=10)

        with self._lock:
            full_output = "".join(self._output)

        rc    = self._proc.returncode
        stats = parse_stats(full_output)
        self._proc = None

        log(f"Done (rc={rc})  trades={stats.get('trades','?')}  "
            f"sharpe={stats.get('sharpe','?')}  "
            f"return={stats.get('return_pct','?')}%", G if rc == 0 else R)
        return {
            "returncode": rc,
            "output_tail": full_output.strip().splitlines()[-40:],
            "stats":       stats,
        }

# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    log("bt_watcher starting", B)
    log(f"Code: {CODE_BRANCH}  →  Data: {DATA_BRANCH}", C)
    log(f"Polling every {POLL_S}s after each run  |  Ctrl+C to stop", C)

    inject_token()
    if git(["fetch", "origin", CODE_BRANCH]).returncode != 0:
        log("Cannot reach remote — check GITHUB_TOKEN in .env", R); sys.exit(1)

    runner  = Runner()
    last_id = None

    trigger = fetch_trigger() or read_trigger()
    if not trigger or not trigger.get("bash"):
        log("No bt_trigger.json found — waiting for one to be pushed", Y)
        log("Push a bt_trigger.json to CODE_BRANCH to start a run.", C)
    else:
        log(f"Initial trigger: {trigger['bash']}", C)

    try:
        while True:
            # If we have a trigger and it's new, run it
            if trigger and trigger.get("id") != last_id:
                cmd     = trigger.get("command", "RUN").upper()
                bash    = trigger.get("bash", "")
                msg     = trigger.get("message", "")
                new_id  = trigger.get("id")

                if msg:
                    log(f"Message: {msg}", C)

                if cmd == "STOP":
                    log("STOP command received — exiting", R)
                    push_results({
                        "ts": datetime.now().isoformat(),
                        "trigger_id": new_id,
                        "status": "stopped",
                        "bash": bash,
                        "stats": {},
                        "output_tail": [],
                    })
                    break

                if cmd in ("RUN", "RELOAD") and bash:
                    result = runner.run(bash)
                    push_results({
                        "ts":         datetime.now().isoformat(),
                        "trigger_id": new_id,
                        "bash":       bash,
                        "status":     "complete" if result["returncode"] == 0 else "error",
                        "returncode": result["returncode"],
                        "stats":      result["stats"],
                        "output_tail": result["output_tail"],
                    })
                    last_id = new_id

            log(f"Polling for new trigger in {POLL_S}s…", Z)
            time.sleep(POLL_S)

            new_trigger = fetch_trigger()
            if new_trigger:
                trigger = new_trigger
            elif not trigger:
                trigger = read_trigger()

    except KeyboardInterrupt:
        log("\nCtrl+C — stopping", Y)
        runner.stop()

    log("bt_watcher done.", B)


if __name__ == "__main__":
    main()
