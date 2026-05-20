"""
pipeline.py — Asynchronous dual-queue data ingestion and signal engine.

Architecture:
  Producer 1  →  trade_queue  ─┐
  Producer 2  →  book_queue   ─┤→  Consumer (1ms loop)  →  on_signal / on_bar / on_tick callbacks
                                │
  All three coroutines run concurrently via asyncio.gather().

Signal types emitted:
  VPD_ABSORPTION   — volume spike with choking body (absorption / limit wall)
  OFI_LONG         — bid depth > 3.5× ask depth during sell climax
  OFI_SHORT        — ask depth > 3.5× bid depth during buy climax
  TAPE_FLIP_BUY    — aggressive tape flips from sell-dominant to buy
  TAPE_FLIP_SELL   — aggressive tape flips from buy-dominant to sell
  FREAKOUT         — spread or TPS anomaly (news drop / flash crash)

Usage:
  exchange = await create_exchange(config)
  pipeline = MarketPipeline(config, exchange,
                             on_signal=sim.on_signal,
                             on_bar=sim.on_bar,
                             on_tick=sim.on_tick)
  await pipeline.run()
"""

import asyncio
import collections
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Deque, List, Optional, Tuple

log = logging.getLogger("pipeline")


# ── Data models ───────────────────────────────────────────────────────────────

class SignalType(Enum):
    VPD_ABSORPTION = "vpd_absorption"
    OFI_LONG       = "ofi_long"
    OFI_SHORT      = "ofi_short"
    TAPE_FLIP_BUY  = "tape_flip_buy"
    TAPE_FLIP_SELL = "tape_flip_sell"
    FREAKOUT       = "freakout"


@dataclass
class Bar:
    ts_open:     int     # bar open timestamp ms
    open:        float
    high:        float
    low:         float
    close:       float
    volume:      float
    buy_vol:     float   # aggressive buy volume (taker buys)
    sell_vol:    float   # aggressive sell volume (taker sells)
    trade_count: int
    true_range:  float = 0.0
    prev_close:  float = 0.0

    @property
    def body_size(self) -> float:
        return abs(self.close - self.open)

    @property
    def delta(self) -> float:
        return self.buy_vol - self.sell_vol


@dataclass
class Signal:
    type:       SignalType
    ts_ms:      int
    price:      float
    meta:       dict = field(default_factory=dict)


@dataclass
class Tick:
    price:     float
    bid:       float
    ask:       float
    spread:    float
    spread_pct: float
    tps:       float     # transactions per second (10s window)
    ts_ms:     int


# ── Rolling statistics ─────────────────────────────────────────────────────────

class RollingMean:
    """Fixed-period rolling mean over the last `period` values."""

    def __init__(self, period: int):
        self._buf: Deque[float] = collections.deque(maxlen=period)
        self.period = period

    def push(self, value: float) -> Optional[float]:
        self._buf.append(value)
        if len(self._buf) == self.period:
            return sum(self._buf) / self.period
        return None

    @property
    def value(self) -> Optional[float]:
        if len(self._buf) == self.period:
            return sum(self._buf) / self.period
        return None

    @property
    def ready(self) -> bool:
        return len(self._buf) == self.period


class TPSCounter:
    """Counts trades per second over a rolling time window."""

    def __init__(self, window_seconds: float = 10.0):
        self._window = window_seconds * 1000  # ms
        self._events: Deque[int] = collections.deque()

    def tick(self, ts_ms: int) -> float:
        self._events.append(ts_ms)
        cutoff = ts_ms - self._window
        while self._events and self._events[0] < cutoff:
            self._events.popleft()
        elapsed_s = self._window / 1000
        return len(self._events) / elapsed_s


# ── Bar builder ────────────────────────────────────────────────────────────────

class BarBuilder:
    """
    Builds 1-minute OHLCV bars from a raw trade stream.
    Emits a completed Bar when the minute boundary is crossed.
    """

    def __init__(self, bar_seconds: int = 60):
        self._bar_s  = bar_seconds * 1000  # ms
        self._bar:   Optional[dict] = None
        self._prev_close: Optional[float] = None

    def update(self, price: float, volume: float, side: str,
               ts_ms: int) -> Optional[Bar]:
        """Feed one trade. Returns a closed Bar when the minute flips, else None."""
        bar_start = (ts_ms // self._bar_s) * self._bar_s

        if self._bar is None or self._bar["ts_open"] != bar_start:
            closed = self._close_bar() if self._bar else None
            self._bar = {
                "ts_open": bar_start,
                "open":    price,
                "high":    price,
                "low":     price,
                "close":   price,
                "volume":  0.0,
                "buy_vol": 0.0,
                "sell_vol":0.0,
                "trades":  0,
            }
            if closed:
                return closed

        b = self._bar
        b["high"]  = max(b["high"],  price)
        b["low"]   = min(b["low"],   price)
        b["close"] = price
        b["volume"] += volume
        b["trades"] += 1
        if side == "buy":
            b["buy_vol"] += volume
        else:
            b["sell_vol"] += volume
        return None

    def _close_bar(self) -> Bar:
        b = self._bar
        pc = self._prev_close or b["open"]
        tr = max(b["high"] - b["low"],
                 abs(b["high"] - pc),
                 abs(b["low"]  - pc))
        bar = Bar(
            ts_open     = b["ts_open"],
            open        = b["open"],
            high        = b["high"],
            low         = b["low"],
            close       = b["close"],
            volume      = b["volume"],
            buy_vol     = b["buy_vol"],
            sell_vol    = b["sell_vol"],
            trade_count = b["trades"],
            true_range  = tr,
            prev_close  = pc,
        )
        self._prev_close = b["close"]
        return bar


# ── Signal engine ──────────────────────────────────────────────────────────────

class SignalEngine:
    """
    Stateless signal detection on completed bars and live book/tape data.

    Detects:
      - VPD absorption on bar close
      - OFI imbalance on each book update
      - Tape flip on each trade
      - Freakout (spread / TPS anomaly)
    """

    def __init__(self, cfg: dict):
        s = cfg["signals"]
        p = cfg["pipeline"]

        self._vpd_mult        = s["vpd_vol_mult"]
        self._vpd_choke       = s["vpd_body_choke_pct"]
        self._ofi_long        = s["ofi_ratio_long"]
        self._ofi_short       = s["ofi_ratio_short"]
        self._ofi_levels      = s["ofi_book_levels"]
        self._flip_window     = s["tape_flip_window"]
        self._flip_threshold  = s["tape_flip_threshold"]
        self._flip_min_size   = s["tape_flip_min_size"]
        self._freak_spread    = cfg["safety"]["freakout_spread_mult"]
        self._freak_tps       = cfg["safety"]["freakout_tps_mult"]

        self._vol_ma          = RollingMean(p["vol_ma_period"])
        self._spread_ma       = RollingMean(30)   # 30-bar spread normal baseline
        self._tps_baseline    = RollingMean(30)   # 30-bar TPS normal baseline

        # Tape ring buffer: (side, size) per trade
        self._tape: Deque[Tuple[str, float]] = collections.deque(
            maxlen=self._flip_window)

        # Sell-climax tracker: count consecutive sell-dominant 1s windows
        self._sell_streak = 0
        self._buy_streak  = 0

    # ── VPD on bar close ──────────────────────────────────────────────────────

    def on_bar(self, bar: Bar, ts_ms: int) -> Optional[Signal]:
        vol_ma = self._vol_ma.push(bar.volume)
        if vol_ma is None or vol_ma == 0:
            return None

        vol_mult = bar.volume / vol_ma
        body_over_vol = bar.body_size / bar.volume if bar.volume > 0 else 0.0

        # Absorption: volume explodes but body chokes → limit wall being hit
        if vol_mult >= self._vpd_mult and body_over_vol < self._vpd_choke:
            side = "sell" if bar.delta < 0 else "buy"
            log.info(f"VPD absorption | vol_mult={vol_mult:.2f}x  body/vol={body_over_vol:.5f}  side={side}")
            return Signal(
                type=SignalType.VPD_ABSORPTION,
                ts_ms=ts_ms,
                price=bar.close,
                meta={
                    "vol_mult":     round(vol_mult, 3),
                    "body_over_vol":round(body_over_vol, 6),
                    "bar_delta":    round(bar.delta, 4),
                    "side":         side,
                },
            )
        return None

    # ── OFI on book update ─────────────────────────────────────────────────────

    def on_book(self, bids: list, asks: list, ts_ms: int) -> Optional[Signal]:
        """Measure top-N bid vs ask volume for order-flow imbalance."""
        n = self._ofi_levels
        bid_vol = sum(row[1] for row in bids[:n]) if bids else 0.0
        ask_vol = sum(row[1] for row in asks[:n]) if asks else 0.0

        if ask_vol > 0 and bid_vol / ask_vol >= self._ofi_long:
            log.info(f"OFI LONG | bid/ask={bid_vol/ask_vol:.2f}x  sell climax")
            return Signal(
                type=SignalType.OFI_LONG,
                ts_ms=ts_ms,
                price=bids[0][0] if bids else 0.0,
                meta={"bid_vol": round(bid_vol, 4),
                      "ask_vol": round(ask_vol, 4),
                      "ratio":   round(bid_vol / ask_vol, 3)},
            )

        if bid_vol > 0 and ask_vol / bid_vol >= self._ofi_short:
            log.info(f"OFI SHORT | ask/bid={ask_vol/bid_vol:.2f}x  buy climax")
            return Signal(
                type=SignalType.OFI_SHORT,
                ts_ms=ts_ms,
                price=asks[0][0] if asks else 0.0,
                meta={"bid_vol": round(bid_vol, 4),
                      "ask_vol": round(ask_vol, 4),
                      "ratio":   round(ask_vol / bid_vol, 3)},
            )
        return None

    # ── Tape flip on trade ─────────────────────────────────────────────────────

    def on_trade(self, price: float, size: float, side: str,
                 ts_ms: int) -> Optional[Signal]:
        """
        Detect aggressive tape flip: after a dominant sell (buy) streak,
        a significant buy (sell) order prints — aggressor has flipped.
        """
        if size < self._flip_min_size:
            return None

        self._tape.append((side, size))
        if len(self._tape) < self._flip_window:
            return None

        buys  = sum(s for sd, s in self._tape if sd == "buy")
        sells = sum(s for sd, s in self._tape if sd == "sell")
        total = buys + sells
        if total == 0:
            return None

        buy_dom  = buys  / total
        sell_dom = sells / total

        # Flip BUY: tape was sell-dominated, now a buy appears
        if sell_dom >= self._flip_threshold and side == "buy":
            log.info(f"TAPE FLIP BUY | sell_dom={sell_dom:.2f} → buy at {price}")
            self._tape.clear()
            return Signal(
                type=SignalType.TAPE_FLIP_BUY,
                ts_ms=ts_ms,
                price=price,
                meta={"sell_dom": round(sell_dom, 3), "size": size},
            )

        # Flip SELL: tape was buy-dominated, now a sell appears
        if buy_dom >= self._flip_threshold and side == "sell":
            log.info(f"TAPE FLIP SELL | buy_dom={buy_dom:.2f} → sell at {price}")
            self._tape.clear()
            return Signal(
                type=SignalType.TAPE_FLIP_SELL,
                ts_ms=ts_ms,
                price=price,
                meta={"buy_dom": round(buy_dom, 3), "size": size},
            )
        return None

    # ── Freakout gate ──────────────────────────────────────────────────────────

    def check_freakout(self, spread: float, tps: float,
                       ts_ms: int, price: float) -> Optional[Signal]:
        spread_base = self._spread_ma.push(spread)
        tps_base    = self._tps_baseline.push(tps)

        if spread_base is None or tps_base is None:
            return None

        spread_ratio = spread / spread_base if spread_base > 0 else 1.0
        tps_ratio    = tps    / tps_base    if tps_base    > 0 else 1.0

        if spread_ratio > self._freak_spread or tps_ratio > self._freak_tps:
            log.warning(f"FREAKOUT | spread×{spread_ratio:.1f}  TPS×{tps_ratio:.1f}")
            return Signal(
                type=SignalType.FREAKOUT,
                ts_ms=ts_ms,
                price=price,
                meta={"spread_ratio": round(spread_ratio, 2),
                      "tps_ratio":    round(tps_ratio, 2)},
            )
        return None


# ── Market pipeline ────────────────────────────────────────────────────────────

class MarketPipeline:
    """
    Orchestrates three concurrent coroutines:
      1. trade_producer  — watch_trades  → trade_queue
      2. book_producer   — watch_order_book → book_queue
      3. _consumer       — 1ms non-blocking drain → callbacks

    Callbacks injected at construction (no coupling to simulator internals):
      on_signal(Signal)
      on_bar(Bar)
      on_tick(Tick)
    """

    def __init__(self,
                 cfg:       dict,
                 exchange,
                 on_signal: Callable[[Signal], None],
                 on_bar:    Callable[[Bar], None],
                 on_tick:   Callable[[Tick], None],
                 on_book:   Optional[Callable[[list, list], None]] = None):

        self._cfg      = cfg
        self._ex       = exchange
        self._sym      = cfg["symbol"]
        self._depth    = cfg["pipeline"]["book_depth"]
        self._sleep_ms = cfg["pipeline"]["consumer_sleep_ms"] / 1000.0

        self.on_signal = on_signal
        self.on_bar    = on_bar
        self.on_tick   = on_tick
        self.on_book   = on_book  # optional: simulator.on_book(bids, asks)

        self.trade_queue: asyncio.Queue = asyncio.Queue(maxsize=50_000)
        self.book_queue:  asyncio.Queue = asyncio.Queue(maxsize=10_000)

        self._builder = BarBuilder(cfg["pipeline"]["bar_seconds"])
        self._signals = SignalEngine(cfg)
        self._tps     = TPSCounter(cfg["pipeline"]["tps_window_seconds"])

        # Latest book snapshot for tick construction
        self._best_bid = 0.0
        self._best_ask = 0.0
        self._last_price = 0.0
        self._running  = False

        # Completed bars ring buffer (for LTR and EMA — accessed by simulator)
        self.bars: Deque[Bar] = collections.deque(maxlen=200)

    # ── Producers ─────────────────────────────────────────────────────────────

    async def _trade_producer(self):
        log.info(f"Trade producer started → {self._sym}")
        while self._running:
            try:
                trades = await asyncio.wait_for(
                    self._ex.watch_trades(self._sym), timeout=5.0)
                for t in trades:
                    if self.trade_queue.full():
                        try:
                            self.trade_queue.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                    await self.trade_queue.put(t)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.error(f"trade_producer error: {exc}", exc_info=True)
                await asyncio.sleep(1)

    async def _book_producer(self):
        log.info(f"Book producer started → {self._sym} depth={self._depth}")
        while self._running:
            try:
                book = await asyncio.wait_for(
                    self._ex.watch_order_book(self._sym, self._depth), timeout=5.0)
                if self.book_queue.full():
                    try:
                        self.book_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                await self.book_queue.put({
                    "bids": list(book["bids"]),
                    "asks": list(book["asks"]),
                    "ts":   book.get("timestamp") or int(time.time() * 1000),
                })
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.error(f"book_producer error: {exc}", exc_info=True)
                await asyncio.sleep(1)

    # ── Consumer ──────────────────────────────────────────────────────────────

    async def _consumer(self):
        log.info("Consumer engine started (1ms loop)")
        while self._running:
            processed = 0

            # ── Drain trade queue ─────────────────────────────────────────
            while True:
                try:
                    t = self.trade_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                price  = float(t["price"])
                size   = float(t["amount"])
                side   = t["side"]
                ts_ms  = t.get("timestamp") or int(time.time() * 1000)
                self._last_price = price

                # Bar builder
                closed_bar = self._builder.update(price, size, side, ts_ms)
                if closed_bar:
                    self.bars.append(closed_bar)
                    sig = self._signals.on_bar(closed_bar, ts_ms)
                    if sig:
                        self.on_signal(sig)
                    self.on_bar(closed_bar)

                # Tape flip
                sig = self._signals.on_trade(price, size, side, ts_ms)
                if sig:
                    self.on_signal(sig)

                # TPS + freakout check
                tps = self._tps.tick(ts_ms)
                if self._best_bid and self._best_ask:
                    spread = self._best_ask - self._best_bid
                    sig = self._signals.check_freakout(spread, tps, ts_ms, price)
                    if sig:
                        self.on_signal(sig)

                processed += 1

            # ── Drain book queue ──────────────────────────────────────────
            while True:
                try:
                    b = self.book_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                bids = b["bids"]
                asks = b["asks"]
                ts   = b["ts"]

                if bids:
                    self._best_bid = bids[0][0]
                if asks:
                    self._best_ask = asks[0][0]

                # Emit tick
                if self._best_bid and self._best_ask:
                    spread = self._best_ask - self._best_bid
                    spread_pct = spread / self._best_ask if self._best_ask else 0.0
                    tps = self._tps.tick(ts)
                    self.on_tick(Tick(
                        price      = self._last_price or (self._best_bid + self._best_ask) / 2,
                        bid        = self._best_bid,
                        ask        = self._best_ask,
                        spread     = spread,
                        spread_pct = spread_pct,
                        tps        = tps,
                        ts_ms      = ts,
                    ))

                # OFI check
                sig = self._signals.on_book(bids, asks, ts)
                if sig:
                    self.on_signal(sig)

                # Forward book snapshot to simulator for AMD sizing
                if self.on_book:
                    self.on_book(bids, asks)

                processed += 1

            # 1ms yield when idle to prevent CPU spin
            if processed == 0:
                await asyncio.sleep(self._sleep_ms)

    # ── Entry point ───────────────────────────────────────────────────────────

    async def run(self):
        self._running = True
        try:
            await asyncio.gather(
                self._trade_producer(),
                self._book_producer(),
                self._consumer(),
            )
        finally:
            self._running = False
            try:
                await self._ex.close()
            except Exception:
                pass

    def stop(self):
        self._running = False


# ── Factory ───────────────────────────────────────────────────────────────────

async def create_exchange(cfg: dict, api_key: str = "", api_secret: str = ""):
    """Create and return an authenticated (or public) ccxt.pro exchange instance."""
    try:
        import ccxt.pro as ccxtpro
    except ImportError:
        raise ImportError("Install ccxt.pro:  pip install ccxt[pro]")

    ExClass = getattr(ccxtpro, cfg["exchange"])
    ex = ExClass({
        "apiKey":  api_key,
        "secret":  api_secret,
        "options": {"defaultType": "spot"},
    })
    return ex
