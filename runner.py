#!/usr/bin/env python3
"""
runner.py — Termux trading loop with GitHub command bridge

How it works:
  1. Runs live_trader.py as a subprocess (Ctrl+C sent to it for graceful shutdown)
  2. Every POLL_S seconds: git pull, check trigger.json for new commands
  3. On RELOAD: stop trader gracefully, pull new code, restart
  4. On STOP:   stop trader, exit runner
  5. After each session: write live_data.json, git push
  6. Runner itself: Ctrl+C exits cleanly

trigger.json written by Claude (commands to Termux):
  { "version": "YYYYMMDD_HHMMSS", "command": "RELOAD|STOP|STATUS",
    "message": "why", "branch": "..." }

live_data.json written by runner (data to Claude):
  { "ts": "...", "price": ..., "regime": "...", "trades": ...,
    "pnl": ..., "sharpe": ..., "indicators": {...} }

Ctrl+C stops everything cleanly.
"""
import os, sys, time, json, subprocess, signal, math
from pathlib import Path
from datetime import datetime

REPO_DIR   = Path(__file__).parent.resolve()
BRANCH     = "claude/hft-mean-reversion-strategy-pJ4YU"
TRIGGER_F  = REPO_DIR / "trigger.json"
DATA_F     = REPO_DIR / "live_data.json"
POLL_S     = 20      # seconds between GitHub checks
MAX_WAIT_S = 50      # max seconds to wait for trader graceful shutdown

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; B='\033[1m'; Z='\033[0m'

def log(msg, col=Z):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"{col}[runner {ts}] {msg}{Z}", flush=True)

# ── Git helpers ───────────────────────────────────────────────────────────────
def git_pull():
    r = subprocess.run(
        ["git", "pull", "origin", BRANCH, "--ff-only"],
        cwd=REPO_DIR, capture_output=True, text=True)
    if r.returncode != 0:
        log(f"git pull failed: {r.stderr.strip()}", R)
        return False
    if "Already up to date" not in r.stdout:
        log(f"Pulled: {r.stdout.strip()}", G)
    return True

def git_push_data():
    subprocess.run(["git", "add", str(DATA_F)], cwd=REPO_DIR, capture_output=True)
    r = subprocess.run(
        ["git", "commit", "-m",
         f"live_data {datetime.now().strftime('%Y%m%d_%H%M%S')}"],
        cwd=REPO_DIR, capture_output=True, text=True)
    if "nothing to commit" in r.stdout + r.stderr:
        return
    r2 = subprocess.run(
        ["git", "push", "origin", BRANCH],
        cwd=REPO_DIR, capture_output=True, text=True)
    if r2.returncode == 0:
        log("live_data.json pushed", G)
    else:
        log(f"push failed: {r2.stderr.strip()}", R)

# ── Trigger / data helpers ────────────────────────────────────────────────────
def read_trigger():
    try:
        if TRIGGER_F.exists():
            return json.loads(TRIGGER_F.read_text())
    except Exception:
        pass
    return None

def write_live_data(data: dict):
    try:
        DATA_F.write_text(json.dumps(data, indent=2))
    except Exception as e:
        log(f"write live_data failed: {e}", R)

def build_trader():
    p1 = (REPO_DIR / "live_trader_p1.py").read_text()
    p2 = (REPO_DIR / "live_trader_p2.py").read_text()
    (REPO_DIR / "live_trader.py").write_text(p1 + "\n" + p2)
    log("live_trader.py rebuilt", C)

# ── Trader subprocess ─────────────────────────────────────────────────────────
class Trader:
    def __init__(self):
        self.proc    = None
        self.logfile = None
        self.logpath = REPO_DIR / "trader_output.log"

    def start(self):
        build_trader()
        self.logfile = open(self.logpath, "a")
        self.proc = subprocess.Popen(
            [sys.executable, str(REPO_DIR / "live_trader.py")],
            cwd=REPO_DIR,
            stdout=self.logfile,
            stderr=self.logfile)
        log(f"Trader started  PID {self.proc.pid}  "
            f"log → {self.logpath.name}", G)

    def stop(self):
        if not self.is_running():
            return
        log(f"Stopping trader PID {self.proc.pid} (Ctrl+C → wait {MAX_WAIT_S}s)…", Y)
        self.proc.send_signal(signal.SIGINT)
        try:
            self.proc.wait(timeout=MAX_WAIT_S)
            log("Trader stopped cleanly", G)
        except subprocess.TimeoutExpired:
            log("Timeout — killing trader", R)
            self.proc.kill()
        finally:
            if self.logfile:
                self.logfile.close()
                self.logfile = None
            self.proc = None

    def is_running(self):
        return self.proc is not None and self.proc.poll() is None

# ── Live data snapshot (parsed from log tail) ─────────────────────────────────
def parse_live_data(logpath: Path, last_known: dict) -> dict:
    """Extract last known state from trader log. Falls back to last_known."""
    data = dict(last_known)
    data["ts"] = datetime.now().isoformat()
    if not logpath.exists():
        return data
    try:
        lines = logpath.read_text().splitlines()[-200:]
        for line in reversed(lines):
            # Pull price from log lines like: BAR  $78,045.60  UPTREND ...
            if "BAR " in line and "$" in line:
                parts = line.split()
                for p in parts:
                    if p.startswith("$"):
                        try:
                            data["price"] = float(p.replace("$","").replace(",",""))
                            break
                        except ValueError:
                            pass
                for i, p in enumerate(parts):
                    if p in ("UPTREND","DOWNTREND","RANGING","COMPRESS","UNKNOWN"):
                        data["regime"] = p
                break
        # Sharpe from trade results
        pnls = []
        for line in lines:
            if "↳ sold" in line and "PnL" in line:
                try:
                    idx = line.index("PnL") + 4
                    val = float(line[idx:].split()[0].replace("+",""))
                    pnls.append(val)
                except Exception:
                    pass
        if len(pnls) >= 2:
            arr  = __import__("numpy").array(pnls)
            mean = float(arr.mean())
            std  = float(arr.std()) if arr.std() > 0 else 1e-9
            data["sharpe"]  = round(mean / std * math.sqrt(len(pnls)), 3)
            data["trades"]  = len(pnls)
            data["pnl"]     = round(float(arr.sum()), 6)
            data["wins"]    = int((arr > 0).sum())
    except Exception:
        pass
    return data

# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    log(f"Runner starting  repo={REPO_DIR}  branch={BRANCH}", B)
    log(f"Polling every {POLL_S}s  |  Ctrl+C to stop", C)

    trader      = Trader()
    last_ver    = None
    live_data   = {"ts": "", "price": 0, "regime": "UNKNOWN",
                   "trades": 0, "wins": 0, "pnl": 0.0, "sharpe": 0.0}
    stop_flag   = False

    try:
        git_pull()
        trader.start()

        while not stop_flag:
            time.sleep(POLL_S)

            # Restart if trader died unexpectedly
            if not trader.is_running():
                log("Trader exited unexpectedly — restarting in 5s", R)
                time.sleep(5)
                git_pull()
                trader.start()
                continue

            # Update + push live data
            live_data = parse_live_data(trader.logpath, live_data)
            write_live_data(live_data)
            sharpe = live_data.get("sharpe", 0)
            log(f"price=${live_data.get('price',0):,.2f}  "
                f"regime={live_data.get('regime','?')}  "
                f"trades={live_data.get('trades',0)}  "
                f"pnl={live_data.get('pnl',0):+.5f}  "
                f"sharpe={sharpe:.3f}", C)

            # Auto-STOP when Sharpe target reached
            if sharpe >= 1.8 and live_data.get("trades", 0) >= 5:
                log(f"Sharpe {sharpe:.3f} ≥ 1.8 — sending STOP", G)
                TRIGGER_F.write_text(json.dumps({
                    "version":  datetime.now().strftime("%Y%m%d_%H%M%S"),
                    "command":  "STOP",
                    "message":  f"Sharpe target reached: {sharpe:.3f}",
                    "branch":   BRANCH,
                }, indent=2))

            # Pull and check for trigger commands
            git_pull()
            trigger = read_trigger()
            if not trigger:
                git_push_data()
                continue

            ver = trigger.get("version")
            if ver == last_ver:
                git_push_data()
                continue

            cmd = trigger.get("command", "").upper()
            msg = trigger.get("message", "")
            log(f"TRIGGER  cmd={cmd}  ver={ver}  msg={msg}", Y)
            last_ver = ver

            if cmd == "STOP":
                log("STOP command — shutting down", R)
                stop_flag = True

            elif cmd == "RELOAD":
                log("RELOAD — stopping trader, pulling code, restarting", Y)
                trader.stop()
                live_data = parse_live_data(trader.logpath, live_data)
                write_live_data(live_data)
                git_pull()
                trader.start()

            elif cmd == "STATUS":
                log("STATUS acknowledged", C)

            git_push_data()

    except KeyboardInterrupt:
        log("\nCtrl+C received", Y)

    finally:
        trader.stop()
        live_data = parse_live_data(trader.logpath, live_data)
        write_live_data(live_data)
        try:
            git_push_data()
        except Exception:
            pass
        log("Runner done", B)


if __name__ == "__main__":
    main()
