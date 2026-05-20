"""
main.py — Entry point for the HFT paper trading engine.

Usage:
  cd /path/to/TRENDRIDER
  python hft/main.py                        # paper mode, no API keys needed
  python hft/main.py --symbol ETH/USDT
  python hft/main.py --config hft/config.json

Press Ctrl+C to stop cleanly.
"""

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
from pathlib import Path

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).parent))

from pipeline import MarketPipeline, Signal, Bar, Tick, create_exchange
from simulator import Simulator


def setup_logging(cfg: dict):
    level = getattr(logging, cfg["logging"].get("log_level", "INFO"))
    logging.basicConfig(
        level   = level,
        format  = "%(asctime)s.%(msecs)03d  %(levelname)-7s  %(name)s  %(message)s",
        datefmt = "%H:%M:%S",
    )


async def run(cfg: dict, api_key: str = "", api_secret: str = ""):
    sim      = Simulator(cfg)
    exchange = await create_exchange(cfg, api_key, api_secret)

    pipeline = MarketPipeline(
        cfg       = cfg,
        exchange  = exchange,
        on_signal = sim.on_signal,
        on_bar    = sim.on_bar,
        on_tick   = sim.on_tick,
        on_book   = sim.on_book,
    )

    # Status print every 30 seconds
    async def status_loop():
        import asyncio as _a
        while True:
            await _a.sleep(30)
            s = sim.status()
            print(
                f"\n── STATUS ──  equity=${s['equity']:.4f}  "
                f"pnl=${s['pnl']:+.4f}  trades={s['trades']}  "
                f"win={s['win_rate']:.1%}  state={s['state']}\n"
            )

    # Graceful shutdown
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _shutdown():
        print("\nShutting down...")
        pipeline.stop()
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    print(f"\nHFT Paper Engine — {cfg['symbol']} on {cfg['exchange']}")
    print(f"Paper balance: ${cfg['paper_balance']:.2f}  |  Press Ctrl+C to stop\n")

    await asyncio.gather(
        pipeline.run(),
        status_loop(),
    )


def main():
    ap = argparse.ArgumentParser(description="HFT Paper Trading Engine")
    ap.add_argument("--config", default="hft/config.json")
    ap.add_argument("--symbol", default=None)
    ap.add_argument("--api-key",    default=os.getenv("BINANCE_API_KEY",    ""))
    ap.add_argument("--api-secret", default=os.getenv("BINANCE_API_SECRET", ""))
    args = ap.parse_args()

    with open(args.config) as f:
        cfg = json.load(f)

    if args.symbol:
        cfg["symbol"] = args.symbol

    setup_logging(cfg)
    asyncio.run(run(cfg, args.api_key, args.api_secret))


if __name__ == "__main__":
    main()
