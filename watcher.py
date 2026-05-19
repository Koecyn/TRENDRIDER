#!/usr/bin/env python3
"""
watcher.py — Termux side of the Claude↔Termux bridge.

Two-branch architecture (eliminates all divergence):
  CODE_BRANCH  — Claude pushes code + trigger.json here (watcher reads only)
  DATA_BRANCH  — watcher pushes live_data.json here   (Claude reads only)

Because only one side ever writes to each branch, fast-forward push
always succeeds and divergence is impossible.
"""

import os, sys, time, json, subprocess, signal, threading
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# ── Config ────────────────────────────────────────────────────────────────────
REPO_DIR    = Path(__file__).parent.resolve()
CODE_BRANCH = "claude/hft-mean-reversion-strategy-pJ4YU"
DATA_BRANCH = "data/live"
BRANCH      = CODE_BRANCH          # kept for compat
GITHUB_USER = "Koecyn"
TRIGGER_F   = REPO_DIR / "trigger.json"
DATA_F      = REPO_DIR / "live_data.json"
LOG_F       = REPO_DIR / "process.log"
POLL_S      = 15
STOP_WAIT_S = 50

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; B='\033[1m'; Z='\033[0m'

def ts():
    return datetime.now().strftime('%H:%M:%S')

def log(msg, col=Z):
    print(f"{col}[watcher {ts()}] {msg}{Z}", flush=True)

# ── Git helpers ───────────────────────────────────────────────────────────────
def git_unlock():
    for lock in ["index.lock", "MERGE_HEAD", "CHERRY_PICK_HEAD"]:
        p = REPO_DIR / ".git" / lock
        if p.exists():
            p.unlink()
            log(f"Removed stale {lock}", Y)
    for d in ["rebase-merge", "rebase-apply"]:
        rd = REPO_DIR / ".git" / d
        if rd.exists():
            import shutil; shutil.rmtree(rd, ignore_errors=True)
            log(f"Cleared stale {d}", Y)

def git(args, stdin=None):
    return subprocess.run(
        ["git"] + args, cwd=REPO_DIR,
        capture_output=True, text=True, input=stdin)

def git_pull():
    """Pull code updates from CODE_BRANCH only."""
    git_unlock()
    r = git(["pull", "--rebase", "origin", CODE_BRANCH])
    if r.returncode != 0:
        log("rebase failed — hard-resetting to origin", Y)
        git(["rebase", "--abort"])
        git_unlock()
        git(["fetch", "origin", CODE_BRANCH])
        git(["reset", "--hard", f"origin/{CODE_BRANCH}"])
        log("Reset to origin — re-synced", G)
    elif "Already up to date" not in r.stdout:
        log("Pulled new code", G)
    return True

def git_push_data():
    """
    Push live_data.json to DATA_BRANCH using git plumbing.
    No checkout needed — works entirely via object store.
    DATA_BRANCH is only ever written by this watcher, so push
    is always fast-forward and can never diverge.
    """
    git_unlock()
    if not DATA_F.exists():
        return

    # Write blob
    r = git(["hash-object", "-w", str(DATA_F)])
    blob = r.stdout.strip()
    if not blob:
        log("hash-object failed", R)
        return

    # Build tree with one file
    r = git(["mktree"], stdin=f"100644 blob {blob}\tlive_data.json\n")
    tree = r.stdout.strip()
    if not tree:
        log("mktree failed", R)
        return

    # Find parent commit on data branch (if exists)
    r = git(["rev-parse", f"refs/remotes/origin/{DATA_BRANCH}"])
    parent_args = ["-p", r.stdout.strip()] if r.returncode == 0 else []

    # Create commit object
    msg = f"live_data {datetime.now().strftime('%Y%m%d_%H%M%S')}"
    r = git(["commit-tree", tree] + parent_args + ["-m", msg])
    commit = r.stdout.strip()
    if not commit:
        log("commit-tree failed", R)
        return

    # Push commit directly to data branch ref
    r = git(["push", "origin", f"{commit}:refs/heads/{DATA_BRANCH}"])
    if r.returncode == 0:
        # Update local remote-tracking ref so next parent lookup is correct
        git(["update-ref", f"refs/remotes/origin/{DATA_BRANCH}", commit])
    else:
        log(f"data push failed: {r.stderr.strip()}", R)

# ── Trigger reading (from CODE_BRANCH via fetch, no pull needed) ──────────────
def fetch_trigger():
    """Fetch latest trigger.json from origin CODE_BRANCH without pulling."""
    git(["fetch", "origin", CODE_BRANCH, "-q"])
    r = git(["show", f"origin/{CODE_BRANCH}:trigger.json"])
    if r.returncode == 0:
        try:
            data = json.loads(r.stdout)
            # Also write locally so git_pull picks up the same state
            TRIGGER_F.write_text(r.stdout)
            return data
        except Exception:
            pass
    return None

def read_trigger():
    try:
        if TRIGGER_F.exists():
            return json.loads(TRIGGER_F.read_text())
    except Exception as e:
        log(f"trigger read error: {e}", R)
    return None

# ── Data writer ───────────────────────────────────────────────────────────────
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
        self.p       = None
        self.logfile = None
        self.cmd     = ""
        self._reader = None

    def _pipe_output(self):
        try:
            for line in self.p.stdout:
                sys.stdout.write(line); sys.stdout.flush()
                if self.logfile:
                    self.logfile.write(line); self.logfile.flush()
        except Exception:
            pass

    def start(self, bash_cmd):
        self.cmd     = bash_cmd
        self.logfile = open(LOG_F, "a")
        header = (f"\n\n{'='*60}\n"
                  f"[{datetime.now().isoformat()}] START: {bash_cmd}\n"
                  f"{'='*60}\n")
        self.logfile.write(header); self.logfile.flush()
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
            try: os.killpg(os.getpgid(self.p.pid), signal.SIGKILL)
            except ProcessLookupError: pass
            self.p.wait()
        finally:
            if self._reader: self._reader.join(timeout=3); self._reader = None
            if self.logfile: self.logfile.close(); self.logfile = None
            self.p = None

    def running(self):
        return self.p is not None and self.p.poll() is None

    def pid(self):
        return self.p.pid if self.p else 0

# ── Auth ──────────────────────────────────────────────────────────────────────
def inject_token():
    token = os.getenv("GITHUB_TOKEN", "").strip()
    if not token:
        log("GITHUB_TOKEN not found in .env", Y); return
    r = git(["remote", "get-url", "origin"])
    url = r.stdout.strip()
    import re
    url = re.sub(r"https://[^@]*@", "https://", url)
    if url.startswith("https://"):
        git(["remote", "set-url", "origin",
             url.replace("https://", f"https://{GITHUB_USER}:{token}@", 1)])
        log("GitHub token injected", G)

def check_auth():
    log("Checking repo access…", C)
    if git(["remote", "-v"]).returncode != 0:
        log("Not a git repo", R); sys.exit(1)
    inject_token()
    if git(["fetch", "origin", CODE_BRANCH]).returncode != 0:
        log("Cannot reach remote — check GITHUB_TOKEN in .env", R); sys.exit(1)
    log("Repo access OK", G)

# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    log(f"Watcher starting  repo={REPO_DIR}", B)
    log(f"Code: {CODE_BRANCH}  Data: {DATA_BRANCH}", C)
    log(f"Polling every {POLL_S}s  |  Ctrl+C to stop", C)

    check_auth()
    git_pull()

    proc     = Proc()
    last_id  = None
    bash_cmd = ""

    trigger = fetch_trigger() or read_trigger()
    if trigger and trigger.get("bash"):
        bash_cmd = trigger["bash"]
        last_id  = trigger.get("id")
        log(f"Initial trigger: {bash_cmd}", C)
    else:
        log("No trigger yet — waiting", Y)

    if bash_cmd:
        proc.start(bash_cmd)

    try:
        while True:
            time.sleep(POLL_S)

            if bash_cmd and not proc.running():
                log("Process exited — restarting in 5s", R)
                time.sleep(5)
                git_pull()
                proc.start(bash_cmd)

            status = "running" if proc.running() else "stopped"
            write_data(status, last_id, proc.pid(), bash_cmd)

            git_pull()

            trigger = fetch_trigger()
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
                log("STOP — shutting down", R)
                proc.stop()
                write_data("stopped", new_id, 0, bash_cmd)
                git_push_data()
                break

            elif cmd in ("RELOAD", "STATUS"):
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
        try: git_push_data()
        except Exception: pass

    log("Watcher done.", B)


if __name__ == "__main__":
    main()
