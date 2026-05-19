#!/usr/bin/env python3
"""
watcher.py — Termux side of the Claude↔Termux bridge.

Start it once:
    python watcher.py

It will:
  1. Authenticate to GitHub via git (uses your existing credentials)
  2. Poll the repo every POLL_S seconds for a new trigger.json
  3. When it sees a new command ID:
       a. Send SIGINT to whatever is running (graceful stop)
       b. Wait for it to finish (up to STOP_WAIT_S)
       c. git pull (get any new code Claude pushed)
       d. Execute the bash string from trigger.json in a fresh shell
       e. Write live_data.json with status + last output lines
       f. git add / commit / push live_data.json back to the repo
  4. Loop forever — Ctrl+C exits cleanly

trigger.json  (Claude writes this):
{
  "id":      "20260518_101530",          <- unique, changes on each new command
  "command": "RELOAD",                   <- RELOAD | STOP | STATUS
  "bash":    "python live_trader.py",    <- shell command to run after pull
  "message": "why this change"
}

live_data.json  (watcher writes this, Claude reads this):
{
  "ts":              "2026-05-18T10:15:30",
  "status":          "running",           <- running | stopped | error
  "last_command_id": "20260518_101530",
  "pid":             12345,
  "tail":            ["last", "20", "lines", "of", "output"]
}
"""

import os, sys, time, json, subprocess, signal, shlex, threading
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# ── Config ────────────────────────────────────────────────────────────────────
REPO_DIR    = Path(__file__).parent.resolve()
BRANCH      = "claude/hft-mean-reversion-strategy-pJ4YU"
GITHUB_USER = "Koecyn"
TRIGGER_F   = REPO_DIR / "trigger.json"
DATA_F      = REPO_DIR / "live_data.json"
LOG_F       = REPO_DIR / "process.log"
POLL_S      = 15     # seconds between repo checks
STOP_WAIT_S = 50     # seconds to wait for graceful stop before kill

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; B='\033[1m'; Z='\033[0m'

def ts():
    return datetime.now().strftime('%H:%M:%S')

def log(msg, col=Z):
    print(f"{col}[watcher {ts()}] {msg}{Z}", flush=True)

# ── Git helpers ───────────────────────────────────────────────────────────────
def git_unlock():
    """Remove stale git lock files so operations don't hang."""
    for lock in ["index.lock", "MERGE_HEAD", "CHERRY_PICK_HEAD"]:
        p = REPO_DIR / ".git" / lock
        if p.exists():
            p.unlink()
            log(f"Removed stale {lock}", Y)
    rebase_dir = REPO_DIR / ".git" / "rebase-merge"
    if rebase_dir.exists():
        import shutil
        shutil.rmtree(rebase_dir, ignore_errors=True)
        log("Cleared stale rebase-merge dir", Y)

def git(args, check=False):
    return subprocess.run(
        ["git"] + args, cwd=REPO_DIR,
        capture_output=True, text=True)

def git_pull():
    git_unlock()
    r = git(["pull", "--rebase", "origin", BRANCH])
    if r.returncode != 0:
        log("rebase failed — hard-resetting to origin", Y)
        git(["rebase", "--abort"])
        git_unlock()
        git(["fetch", "origin", BRANCH])
        git(["reset", "--hard", f"origin/{BRANCH}"])
        log("Reset to origin — re-synced", G)
    changed = "Already up to date" not in r.stdout
    if changed:
        log("Pulled new changes", G)
    return True

def git_push_data():
    git_unlock()
    git(["add", str(DATA_F)])
    r = git(["commit", "-m",
             f"live_data {datetime.now().strftime('%Y%m%d_%H%M%S')}"])
    if "nothing to commit" in (r.stdout + r.stderr):
        return
    git(["pull", "--rebase", "origin", BRANCH])
    r2 = git(["push", "origin", BRANCH])
    if r2.returncode == 0:
        log("live_data.json pushed to repo", G)
    else:
        log(f"push failed: {r2.stderr.strip()}", R)

# ── Trigger / data helpers ────────────────────────────────────────────────────
def read_trigger():
    try:
        if TRIGGER_F.exists():
            return json.loads(TRIGGER_F.read_text())
    except Exception as e:
        log(f"trigger read error: {e}", R)
    return None

def write_data(status, last_id, pid, bash_cmd):
    tail = []
    if LOG_F.exists():
        try:
            lines = LOG_F.read_text().splitlines()
            tail  = lines[-30:]
        except Exception:
            pass
    payload = {
        "ts":              datetime.now().isoformat(),
        "status":          status,
        "last_command_id": last_id,
        "pid":             pid,
        "bash":            bash_cmd,
        "tail":            tail,
    }
    DATA_F.write_text(json.dumps(payload, indent=2))

# ── Process manager ───────────────────────────────────────────────────────────
class Proc:
    def __init__(self):
        self.p          = None
        self.logfile    = None
        self.cmd        = ""
        self._reader    = None

    def _pipe_output(self):
        """Forward subprocess stdout+stderr to terminal AND logfile."""
        try:
            for line in self.p.stdout:
                sys.stdout.write(line)
                sys.stdout.flush()
                if self.logfile:
                    self.logfile.write(line)
                    self.logfile.flush()
        except Exception:
            pass

    def start(self, bash_cmd):
        self.cmd     = bash_cmd
        self.logfile = open(LOG_F, "a")
        header = (f"\n\n{'='*60}\n"
                  f"[{datetime.now().isoformat()}] START: {bash_cmd}\n"
                  f"{'='*60}\n")
        self.logfile.write(header)
        self.logfile.flush()
        sys.stdout.write(header); sys.stdout.flush()
        self.p = subprocess.Popen(
            bash_cmd, shell=True, cwd=REPO_DIR,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            bufsize=1, universal_newlines=True,
            preexec_fn=os.setsid)
        self._reader = threading.Thread(target=self._pipe_output, daemon=True)
        self._reader.start()
        log(f"Started: {bash_cmd}  PID {self.p.pid}", G)

    def stop(self):
        if not self.running():
            return
        log(f"Stopping PID {self.p.pid} (SIGINT → wait {STOP_WAIT_S}s)…", Y)
        try:
            os.killpg(os.getpgid(self.p.pid), signal.SIGINT)
        except ProcessLookupError:
            pass
        try:
            self.p.wait(timeout=STOP_WAIT_S)
            log("Process stopped cleanly", G)
        except subprocess.TimeoutExpired:
            log("Timeout — sending SIGKILL", R)
            try:
                os.killpg(os.getpgid(self.p.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.p.wait()
        finally:
            if self._reader:
                self._reader.join(timeout=3)
                self._reader = None
            if self.logfile:
                self.logfile.close()
                self.logfile = None
            self.p = None

    def running(self):
        return self.p is not None and self.p.poll() is None

    def pid(self):
        return self.p.pid if self.p else 0

# ── Auth check ────────────────────────────────────────────────────────────────
def inject_token():
    """Embed GITHUB_TOKEN into remote URL so git never prompts."""
    token = os.getenv("GITHUB_TOKEN", "").strip()
    if not token:
        log("GITHUB_TOKEN not found in .env — will rely on system credentials", Y)
        return
    r = git(["remote", "get-url", "origin"])
    url = r.stdout.strip()
    # strip any existing credentials from URL
    import re
    url = re.sub(r"https://[^@]*@", "https://", url)
    if url.startswith("https://"):
        authed = url.replace("https://", f"https://{GITHUB_USER}:{token}@", 1)
        git(["remote", "set-url", "origin", authed])
        log("GitHub token injected into remote URL", G)

def check_auth():
    log("Checking repo access…", C)
    r = git(["remote", "-v"])
    if r.returncode != 0:
        log("Not a git repo — run from inside TRENDRIDER/", R)
        sys.exit(1)
    inject_token()
    r2 = git(["fetch", "origin", BRANCH])
    if r2.returncode != 0:
        log(f"Cannot reach remote: {r2.stderr.strip()}", R)
        log("Add GITHUB_TOKEN=ghp_xxx to your .env file", R)
        sys.exit(1)
    log("Repo access OK", G)

# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    log(f"Watcher starting  repo={REPO_DIR}", B)
    log(f"Branch: {BRANCH}", C)
    log(f"Polling every {POLL_S}s  |  Ctrl+C to stop cleanly", C)

    check_auth()
    git_pull()

    proc      = Proc()
    last_id   = None
    bash_cmd  = ""

    # Read initial trigger to know what to run first
    trigger = read_trigger()
    if trigger and trigger.get("bash"):
        bash_cmd = trigger["bash"]
        last_id  = trigger.get("id")
        log(f"Initial trigger found: {bash_cmd}", C)
    else:
        log("No trigger.json yet — waiting for Claude to post one", Y)

    if bash_cmd:
        proc.start(bash_cmd)

    try:
        while True:
            time.sleep(POLL_S)

            # Restart if process died
            if bash_cmd and not proc.running():
                log("Process exited — restarting in 5s", R)
                time.sleep(5)
                git_pull()
                proc.start(bash_cmd)

            # Push current live data
            status = "running" if proc.running() else "stopped"
            write_data(status, last_id, proc.pid(), bash_cmd)

            # Pull latest from repo
            git_pull()

            # Check for new trigger
            trigger = read_trigger()
            if not trigger:
                git_push_data()
                continue

            new_id = trigger.get("id")
            if new_id == last_id:
                git_push_data()
                continue

            cmd      = trigger.get("command", "").upper()
            new_bash = trigger.get("bash", bash_cmd)
            msg      = trigger.get("message", "")

            log(f"NEW TRIGGER  id={new_id}  cmd={cmd}  msg={msg}", Y)

            if cmd == "STOP":
                log("STOP — shutting everything down", R)
                proc.stop()
                write_data("stopped", new_id, 0, bash_cmd)
                git_push_data()
                break

            elif cmd in ("RELOAD", "STATUS"):
                log(f"{'RELOAD' if cmd=='RELOAD' else 'STATUS'}: "
                    f"stopping current process…", Y)
                proc.stop()
                write_data("reloading", new_id, 0, new_bash)
                git_push_data()

                if new_bash:
                    bash_cmd = new_bash
                    proc.start(bash_cmd)
                    write_data("running", new_id, proc.pid(), bash_cmd)

            last_id = new_id
            git_push_data()

    except KeyboardInterrupt:
        log("\nCtrl+C — stopping", Y)
        proc.stop()
        write_data("stopped", last_id, 0, bash_cmd)
        try:
            git_push_data()
        except Exception:
            pass

    log("Watcher done.", B)


if __name__ == "__main__":
    main()
