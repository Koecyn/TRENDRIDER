"""
main.py — HFT paper trading engine with live hot-reload.

Signals:
  SIGUSR1  →  reload pipeline.py + simulator.py (WebSocket stays alive, position preserved)
  SIGHUP   →  reload config.json only (lighter, no module swap)
  SIGINT / SIGTERM  →  graceful shutdown

Usage:
  bash hft/start.sh               # recommended — writes PID, tees to engine.log
  python hft/main.py              # direct run
  python hft/main.py --symbol ETH/USDT

Upgrade while running:
  bash hft/upgrade.sh             # git pull → SIGUSR1 → hot-reload in <1s
"""

import argparse
import asyncio
import importlib
import json
import logging
import logging.handlers
import os
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import pipeline as _pipeline_mod
import simulator as _sim_mod
from pipeline import MarketPipeline, create_exchange
from simulator import Simulator

log = logging.getLogger("main")

PID_FILE = Path(__file__).parent / "hft.pid"


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging(cfg: dict):
    level = getattr(logging, cfg["logging"].get("log_level", "INFO"))
    log_dir = Path(cfg["logging"]["log_dir"])
    log_dir.mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03d  %(levelname)-7s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )
    root = logging.getLogger()
    root.setLevel(level)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)

    fh = logging.handlers.RotatingFileHandler(
        log_dir / "engine.log", maxBytes=10_000_000, backupCount=5,
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)


# ── PID management ────────────────────────────────────────────────────────────

def _write_pid():
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(os.getpid()))

def _remove_pid():
    try:
        PID_FILE.unlink()
    except FileNotFoundError:
        pass


# ── State snapshot / restore for hot-reload ───────────────────────────────────

def _snapshot(sim) -> dict:
    return {
        "equity":         sim.equity,
        "trade_count":    sim.trade_count,
        "winning_trades": sim.winning_trades,
        "total_pnl":      sim.total_pnl,
        "position":       sim.position,          # Position dataclass — structural compat
        "kelly_wins":     list(sim._sizer._wins),
        "kelly_losses":   list(sim._sizer._losses),
        "safety_streak":  sim._safety._loss_streak,
    }

def _restore(sim, snap: dict):
    sim.equity         = snap["equity"]
    sim.trade_count    = snap["trade_count"]
    sim.winning_trades = snap["winning_trades"]
    sim.total_pnl      = snap["total_pnl"]
    sim.position       = snap["position"]
    sim._sizer._wins.extend(snap["kelly_wins"])
    sim._sizer._losses.extend(snap["kelly_losses"])
    sim._safety._loss_streak = snap["safety_streak"]


# ── Main async engine ─────────────────────────────────────────────────────────

async def run(cfg: dict, cfg_path: str, api_key: str = "", api_secret: str = ""):
    _write_pid()

    sim      = Simulator(cfg, cfg_path)
    exchange = await create_exchange(cfg, api_key, api_secret)

    pipeline = MarketPipeline(
        cfg       = cfg,
        exchange  = exchange,
        on_signal = sim.on_signal,
        on_bar    = sim.on_bar,
        on_tick   = sim.on_tick,
        on_book   = sim.on_book,
    )

    # Mutable container — lets reload_watcher swap sim without rebinding closure vars
    sim_ref = [sim]

    reload_event = asyncio.Event()
    config_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    # ── Signal handlers ───────────────────────────────────────────────────────

    def _on_usr1():
        log.info("SIGUSR1 → hot-reload queued")
        loop.call_soon_threadsafe(reload_event.set)

    def _on_hup():
        log.info("SIGHUP → config-reload queued")
        loop.call_soon_threadsafe(config_event.set)

    def _on_shutdown():
        log.info("Shutdown signal received — stopping")
        pipeline.stop()
        _remove_pid()

    loop.add_signal_handler(signal.SIGUSR1, _on_usr1)
    loop.add_signal_handler(signal.SIGHUP,  _on_hup)
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _on_shutdown)

    # ── Hot-reload coroutine (SIGUSR1) ────────────────────────────────────────

    async def reload_watcher():
        """
        On SIGUSR1: reload pipeline.py and simulator.py via importlib,
        swap internal objects in-place, preserve position and equity state.
        The exchange WebSocket is never touched.
        """
        while True:
            await reload_event.wait()
            reload_event.clear()
            log.info("═══ HOT RELOAD START ═══")
            try:
                snap = _snapshot(sim_ref[0])

                with open(cfg_path) as f:
                    new_cfg = json.load(f)

                # Reload both modules
                importlib.reload(_pipeline_mod)
                importlib.reload(_sim_mod)

                # Swap signal-processing objects in the running pipeline
                # (producer/consumer coroutines stay alive — they access via self._*)
                pipeline._signals = _pipeline_mod.SignalEngine(new_cfg)
                pipeline._builder = _pipeline_mod.BarBuilder(
                    new_cfg["pipeline"]["bar_seconds"])
                pipeline._tps     = _pipeline_mod.TPSCounter(
                    new_cfg["pipeline"]["tps_window_seconds"])
                pipeline._cfg     = new_cfg
                pipeline._sleep_ms = new_cfg["pipeline"]["consumer_sleep_ms"] / 1000.0

                # New simulator with saved state
                new_sim = _sim_mod.Simulator(new_cfg, cfg_path)
                _restore(new_sim, snap)
                sim_ref[0] = new_sim

                # Rewire callbacks (consumer reads these attrs on each iteration)
                pipeline.on_signal = new_sim.on_signal
                pipeline.on_bar    = new_sim.on_bar
                pipeline.on_tick   = new_sim.on_tick
                pipeline.on_book   = new_sim.on_book

                s = new_sim.status()
                log.info(
                    f"═══ HOT RELOAD COMPLETE ═══  "
                    f"equity=${s['equity']:.4f}  trades={s['trades']}  "
                    f"state={s['state']}"
                )
            except Exception as exc:
                log.error(f"Hot-reload failed: {exc}", exc_info=True)

    # ── Config-only reload (SIGHUP) ───────────────────────────────────────────

    async def config_watcher():
        while True:
            await config_event.wait()
            config_event.clear()
            sim_ref[0].reload_config()

    # ── Repo watcher — self-upgrade on new commits ────────────────────────────

    async def repo_watcher():
        """
        Poll the code branch every 30s. On new commit: git pull → hot-reload.
        Fused in so the engine is self-upgrading with no external process needed.
        """
        import subprocess
        BRANCH = "claude/hft-mean-reversion-strategy-pJ4YU"
        REPO   = Path(__file__).parent.parent
        ENV    = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}

        def _git(*args):
            return subprocess.run(
                ["git"] + list(args), cwd=REPO,
                capture_output=True, text=True, env=ENV,
            )

        r = _git("rev-parse", f"origin/{BRANCH}")
        last_commit = r.stdout.strip()

        while True:
            await asyncio.sleep(30)
            try:
                _git("fetch", "origin", BRANCH, "-q")
                r = _git("rev-parse", f"origin/{BRANCH}")
                current = r.stdout.strip()
                if current and current != last_commit:
                    log.info(f"REPO  new commit {current[:8]} on {BRANCH} — pulling")
                    _git("reset", "--hard", f"origin/{BRANCH}")
                    last_commit = current
                    loop.call_soon_threadsafe(reload_event.set)
            except Exception as exc:
                log.warning(f"repo_watcher: {exc}")

    async def status_loop():
        while True:
            await asyncio.sleep(30)
            s = sim_ref[0].status()
            pos_str = ""
            if s["position"]:
                p = s["position"]
                pos_str = f"  pos={p['side']}@{p['entry']:.2f}  mae={p['mae']:.4f}"
            log.info(
                f"STATUS  equity=${s['equity']:.4f}  pnl=${s['pnl']:+.4f}  "
                f"trades={s['trades']}  win={s['win_rate']:.1%}  "
                f"state={s['state']}{pos_str}"
            )

    log.info(f"HFT Paper Engine — {cfg['symbol']} on {cfg['exchange']}")
    log.info(f"Balance: ${cfg['paper_balance']:.2f}  PID={os.getpid()}")
    log.info("Upgrade: bash hft/upgrade.sh  |  Config-only: kill -HUP $(cat hft/hft.pid)")

    try:
        await asyncio.gather(
            pipeline.run(),
            status_loop(),
            reload_watcher(),
            config_watcher(),
            repo_watcher(),
        )
    finally:
        _remove_pid()


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="HFT Paper Trading Engine")
    ap.add_argument("--config",     default="hft/config.json")
    ap.add_argument("--symbol",     default=None)
    ap.add_argument("--api-key",    default=os.getenv("BINANCE_API_KEY",    ""))
    ap.add_argument("--api-secret", default=os.getenv("BINANCE_API_SECRET", ""))
    args = ap.parse_args()

    with open(args.config) as f:
        cfg = json.load(f)

    if args.symbol:
        cfg["symbol"] = args.symbol

    setup_logging(cfg)
    asyncio.run(run(cfg, args.config, args.api_key, args.api_secret))


if __name__ == "__main__":
    main()
