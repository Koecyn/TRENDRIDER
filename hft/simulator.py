"""
simulator.py — In-memory paper trading state machine.

State machine:
  IDLE     → signals accumulate; entry conditions checked
  PHASE1   → in trade, absolute tight stop (1 tick past climax wick)
  PHASE2   → EMA cross validated; dynamic trailing stop engaged
  COOLING  → chop or freakout pause; no new entries

Entry requires all three signals within a configurable window:
  1. VPD absorption alert
  2. OFI imbalance (directional)
  3. Tape flip confirmation (same direction as OFI)

Safety circuits:
  - Daily equity drawdown breaker (2% → kill)
  - Chop filter (4 consecutive losses → 30 min pause)
  - Freakout filter (spread or TPS spike → 3 min freeze)

Trade log:
  Every closed trade appends a JSON snapshot to hft/logs/trades_YYYYMMDD.jsonl
  Fields: timestamp_ms, entry_type, side, entry, exit, mae, volume_mult,
          slippage_est, net_pnl, state_at_exit, cumulative_equity

Hot-reload:
  call simulator.reload_config() to re-read config.json live
  without disconnecting the pipeline.
"""

import collections
import importlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Deque, List, Optional

from pipeline import Bar, Signal, SignalType, Tick

log = logging.getLogger("simulator")


# ── State machine states ───────────────────────────────────────────────────────

class State(Enum):
    IDLE     = "idle"
    PHASE1   = "phase1"   # absolute armor stop
    PHASE2   = "phase2"   # trend-rider trailing stop
    COOLING  = "cooling"  # chop / freakout pause


# ── Position ──────────────────────────────────────────────────────────────────

@dataclass
class Position:
    side:        str      # "long" | "short"
    entry_price: float
    qty:         float    # in base asset units
    entry_ts:    int      # ms
    stop:        float
    target:      float
    entry_type:  str      # "PHASE1_ENTRY"

    # MAE tracking
    mae:         float = 0.0   # maximum adverse excursion (price units)
    peak_price:  float = 0.0   # most favorable price since entry

    # Phase 2
    phase2_activated: bool  = False
    trail_stop:       float = 0.0

    # Volume metadata at entry
    vol_mult:    float = 1.0
    ofi_ratio:   float = 1.0

    def update_mae(self, price: float):
        if self.side == "long":
            self.peak_price = max(self.peak_price, price)
            self.mae        = max(self.mae, self.entry_price - price)
        else:
            self.peak_price = min(self.peak_price or price, price)
            self.mae        = max(self.mae, price - self.entry_price)


# ── EMA tracker ────────────────────────────────────────────────────────────────

class EMATracker:
    """
    Incremental EMA cross tracker on 1-minute bar closes.
    Used for Phase 1 → Phase 2 transition validation.
    """

    def __init__(self, fast: int, slow: int, atr_period: int):
        self._kf  = 2 / (fast  + 1)
        self._ks  = 2 / (slow  + 1)
        self._ka  = 1 / atr_period   # Wilder for ATR
        self._ema_fast: Optional[float] = None
        self._ema_slow: Optional[float] = None
        self._atr:      Optional[float] = None
        self._prev_cross_state: Optional[str] = None  # "bull" | "bear"
        self._last_close: Optional[float] = None

    def on_bar(self, bar: Bar) -> dict:
        """Update EMAs and ATR; return cross state."""
        c = bar.close
        tr = bar.true_range

        if self._ema_fast is None:
            self._ema_fast = c
            self._ema_slow = c
            self._atr      = tr
        else:
            self._ema_fast = c * self._kf + self._ema_fast * (1 - self._kf)
            self._ema_slow = c * self._ks + self._ema_slow * (1 - self._ks)
            self._atr      = tr * self._ka + self._atr * (1 - self._ka)

        self._last_close = c
        cross = "bull" if self._ema_fast > self._ema_slow else "bear"
        return {"ema_fast": self._ema_fast,
                "ema_slow": self._ema_slow,
                "atr":      self._atr,
                "cross":    cross,
                "close":    c}

    @property
    def ema_fast(self) -> Optional[float]: return self._ema_fast
    @property
    def ema_slow(self) -> Optional[float]: return self._ema_slow
    @property
    def atr(self)      -> Optional[float]: return self._atr


# ── Kelly position sizer ───────────────────────────────────────────────────────

class KellySizer:
    """
    Dynamic Kelly Criterion position sizer.
    Kelly % = W - ((1 - W) / R)
    Applied with safety multiplier and equity cap.
    """

    def __init__(self, cfg: dict):
        r = cfg["risk"]
        self._multiplier   = r["kelly_multiplier"]       # 0.5 = half-Kelly
        self._max_pct      = r["max_position_pct"]       # 20% equity cap
        self._amd_pct      = r["amd_impact_pct"]         # 10% book depth limit
        self._amd_levels   = r["amd_depth_levels"]       # top 3 levels
        self._min_notional = cfg["min_notional"]

        self._wins:  Deque[float] = collections.deque(maxlen=50)
        self._losses:Deque[float] = collections.deque(maxlen=50)

    def record(self, pnl: float):
        if pnl >= 0:
            self._wins.append(pnl)
        else:
            self._losses.append(abs(pnl))

    def size(self, equity: float, entry: float, stop: float,
             bids: list, asks: list, side: str) -> float:
        """
        Returns qty in base asset.
        Applies: Kelly → safety multiplier → equity cap → AMD check → min notional.
        """
        stop_dist = abs(entry - stop)
        if stop_dist == 0:
            return 0.0

        # Kelly fraction
        kelly_f = self._kelly_fraction()

        # Raw dollar risk = kelly_f × equity
        dollar_risk = kelly_f * equity

        # qty from risk: dollar_risk / stop_dist_per_unit = qty in base
        # For BTC: stop_dist is in USDT per BTC, dollar_risk in USDT
        qty = dollar_risk / stop_dist

        # Equity cap: notional ≤ max_pct × equity
        max_notional = self._max_pct * equity
        qty = min(qty, max_notional / entry)

        # AMD check: notional ≤ amd_pct × top-N depth
        depth = self._available_depth(bids if side == "long" else asks)
        if depth > 0:
            amd_limit = self._amd_pct * depth
            qty = min(qty, amd_limit / entry)

        # Minimum notional
        if qty * entry < self._min_notional:
            return 0.0

        return round(qty, 8)

    def _kelly_fraction(self) -> float:
        if len(self._wins) < 5 or len(self._losses) < 5:
            return 0.01   # conservative bootstrap fraction

        w_rate = len(self._wins) / (len(self._wins) + len(self._losses))
        avg_w  = sum(self._wins)  / len(self._wins)
        avg_l  = sum(self._losses)/ len(self._losses)
        R      = avg_w / avg_l if avg_l > 0 else 1.0
        kelly  = w_rate - ((1 - w_rate) / R)
        kelly  = max(0.0, kelly)                         # floor at 0
        kelly *= self._multiplier                        # half/quarter safety
        kelly  = min(kelly, self._max_pct)               # equity cap
        return kelly

    def _available_depth(self, levels: list) -> float:
        """Notional value of top-N book levels."""
        top = levels[:self._amd_levels]
        return sum(p * q for p, q in top)


# ── Safety circuits ────────────────────────────────────────────────────────────

class SafetyCircuits:
    """
    Three independent safety gates:
      1. Daily equity drawdown breaker  → raise SystemExit at 2% loss
      2. Chop filter                    → 30-min pause after 4 consecutive losses
      3. Freakout filter                → 3-min freeze on spread/TPS spike
    """

    def __init__(self, cfg: dict):
        s = cfg["safety"]
        self._dd_pct         = s["daily_drawdown_pct"]
        self._chop_streak    = s["chop_loss_streak"]
        self._chop_pause_ms  = s["chop_pause_minutes"] * 60 * 1000
        self._freak_freeze_ms= s["freakout_freeze_minutes"] * 60 * 1000

        self._equity_start   = cfg["paper_balance"]
        self._loss_streak    = 0
        self._chop_until_ms  = 0
        self._freak_until_ms = 0

    def reset_daily(self, equity: float):
        self._equity_start = equity
        self._loss_streak  = 0

    def record_trade(self, pnl: float, now_ms: int):
        if pnl < 0:
            self._loss_streak += 1
            if self._loss_streak >= self._chop_streak:
                self._chop_until_ms = now_ms + self._chop_pause_ms
                self._loss_streak   = 0
                log.warning(f"Chop filter activated — pausing {self._chop_pause_ms//60000}m")
        else:
            self._loss_streak = 0

    def trigger_freakout(self, now_ms: int):
        self._freak_until_ms = now_ms + self._freak_freeze_ms
        log.warning(f"Freakout filter activated — freezing {self._freak_freeze_ms//60000}m")

    def check(self, equity: float, now_ms: int) -> tuple[bool, str]:
        """Returns (can_trade, reason_if_blocked)."""
        # 1. Drawdown breaker
        if self._equity_start > 0:
            dd = (self._equity_start - equity) / self._equity_start
            if dd >= self._dd_pct:
                msg = f"DAILY DRAWDOWN {dd:.2%} ≥ {self._dd_pct:.2%} — KILL"
                log.critical(msg)
                raise SystemExit(msg)

        # 2. Chop filter
        if now_ms < self._chop_until_ms:
            remaining = (self._chop_until_ms - now_ms) // 1000
            return False, f"chop_filter ({remaining}s remaining)"

        # 3. Freakout filter
        if now_ms < self._freak_until_ms:
            remaining = (self._freak_until_ms - now_ms) // 1000
            return False, f"freakout_filter ({remaining}s remaining)"

        return True, ""


# ── Trade logger ───────────────────────────────────────────────────────────────

class TradeLogger:
    def __init__(self, log_dir: str):
        self._dir = Path(log_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

    def log_trade(self, snapshot: dict):
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        path = self._dir / f"trades_{date_str}.jsonl"
        with open(path, "a") as f:
            f.write(json.dumps(snapshot) + "\n")
        log.info(f"Trade logged → {path.name}")


# ── LTR calculator ─────────────────────────────────────────────────────────────

def local_true_range(bars: List[Bar], n: int = 3) -> float:
    """Average true range of the last n bars."""
    if not bars:
        return 0.0
    recent = bars[-n:]
    return sum(b.true_range for b in recent) / len(recent)


# ── Main simulator ─────────────────────────────────────────────────────────────

class Simulator:
    """
    Paper trading state machine.

    Feed it signals from the pipeline:
      sim.on_signal(signal)
      sim.on_bar(bar)
      sim.on_tick(tick)

    Call sim.reload_config() to hot-swap config.json without restarting.
    """

    # Signal expiry: VPD + OFI must both fire within this window for entry
    SIGNAL_WINDOW_MS = 5_000   # 5 seconds

    def __init__(self, cfg: dict, cfg_path: str = "hft/config.json"):
        self._cfg_path = cfg_path
        self._cfg      = cfg

        self.equity    = cfg["paper_balance"]
        self.state     = State.IDLE
        self.position: Optional[Position] = None

        self._sizer    = KellySizer(cfg)
        self._safety   = SafetyCircuits(cfg)
        self._ema      = EMATracker(
            fast       = cfg["phase2"]["ema_fast"],
            slow       = cfg["phase2"]["ema_slow"],
            atr_period = cfg["phase2"]["atr_period"],
        )
        self._logger   = TradeLogger(cfg["logging"]["log_dir"])

        self._vol_coeff   = cfg["risk"]["vol_coefficient"]
        self._ltr_bars    = cfg["risk"]["ltr_bars"]
        self._trail_mult  = cfg["phase2"]["trail_atr_mult"]
        self._allow_short = cfg["allow_shorts"]
        self._maker_fee   = cfg["maker_fee"]
        self._min_rr      = cfg["risk"]["min_rr"]

        # Completed bar history (fed from pipeline.bars via on_bar)
        self._bars: List[Bar] = []

        # Pending signal accumulators
        self._vpd_alert: Optional[Signal] = None
        self._ofi_alert: Optional[Signal] = None
        self._tape_alert: Optional[Signal] = None

        # Book snapshot for sizer AMD check
        self._last_bids: list = []
        self._last_asks: list = []

        # Cumulative metrics
        self.trade_count     = 0
        self.winning_trades  = 0
        self.total_pnl       = 0.0

        log.info(f"Simulator ready | equity=${self.equity:.2f}  state={self.state.value}")

    # ── Pipeline callbacks ─────────────────────────────────────────────────────

    def on_signal(self, sig: Signal):
        """Receive a signal from the pipeline and route it."""
        now = sig.ts_ms

        if sig.type == SignalType.FREAKOUT:
            self._safety.trigger_freakout(now)
            return

        if self.state in (State.PHASE1, State.PHASE2):
            # Opposing flow → accelerate exit
            self._check_opposing_flow(sig)
            return

        # Accumulate entry signals
        if sig.type == SignalType.VPD_ABSORPTION:
            self._vpd_alert = sig

        elif sig.type in (SignalType.OFI_LONG, SignalType.OFI_SHORT):
            self._ofi_alert = sig

        elif sig.type in (SignalType.TAPE_FLIP_BUY, SignalType.TAPE_FLIP_SELL):
            self._tape_alert = sig
            self._try_entry(now)

        # Expire stale signals
        self._expire_signals(now)

    def on_bar(self, bar: Bar):
        """Receive a closed 1-min bar and update EMA/ATR for Phase 2."""
        self._bars.append(bar)
        if len(self._bars) > 300:
            self._bars = self._bars[-300:]

        ema_state = self._ema.on_bar(bar)

        if self.state == State.PHASE1 and self.position:
            self._check_phase2_transition(ema_state, bar.close)

    def on_tick(self, tick: Tick):
        """Receive every tick for position management."""
        if self.state in (State.PHASE1, State.PHASE2) and self.position:
            self._manage_position(tick)

    def on_book(self, bids: list, asks: list):
        """Optional: receive book snapshot directly for AMD sizing accuracy."""
        self._last_bids = bids
        self._last_asks = asks

    # ── Entry logic ───────────────────────────────────────────────────────────

    def _try_entry(self, now_ms: int):
        if self.state != State.IDLE:
            return

        # All three signals required
        if not (self._vpd_alert and self._ofi_alert and self._tape_alert):
            return

        # OFI direction must match tape flip direction
        ofi_long   = self._ofi_alert.type == SignalType.OFI_LONG
        tape_buy   = self._tape_alert.type == SignalType.TAPE_FLIP_BUY
        if ofi_long != tape_buy:
            log.debug("OFI and tape flip disagree — skipping entry")
            return

        # Safety check
        ok, reason = self._safety.check(self.equity, now_ms)
        if not ok:
            log.info(f"Entry blocked by safety: {reason}")
            return

        side = "long" if ofi_long else "short"
        if not self._allow_short and side == "short":
            log.debug("Shorts disabled — skipping short signal")
            return

        # Determine entry price (tape flip price = fill price in paper mode)
        entry = self._tape_alert.price

        # Stop placement: entry ± (spread + LTR × vol_coeff)
        ltr   = local_true_range(self._bars, self._ltr_bars)
        if ltr == 0:
            log.debug("LTR not ready — skipping entry")
            return

        spread = self._tape_alert.meta.get("spread", 0.0)
        stop_dist = spread + ltr * self._vol_coeff

        if side == "long":
            stop   = entry - stop_dist
            target = entry + stop_dist * self._min_rr
        else:
            stop   = entry + stop_dist
            target = entry - stop_dist * self._min_rr

        # Kelly position size
        qty = self._sizer.size(
            equity = self.equity,
            entry  = entry,
            stop   = stop,
            bids   = self._last_bids,
            asks   = self._last_asks,
            side   = side,
        )
        if qty <= 0:
            log.info("Sizer returned 0 qty — skipping entry")
            return

        # Open position
        self.position = Position(
            side        = side,
            entry_price = entry,
            qty         = qty,
            entry_ts    = now_ms,
            stop        = stop,
            target      = target,
            entry_type  = "PHASE1_ENTRY",
            peak_price  = entry,
            vol_mult    = self._vpd_alert.meta.get("vol_mult", 1.0),
            ofi_ratio   = self._ofi_alert.meta.get("ratio", 1.0),
        )
        self.state = State.PHASE1

        log.info(
            f"ENTER {side.upper()} | qty={qty:.6f}  entry={entry:.2f}  "
            f"stop={stop:.2f}  target={target:.2f}  ltr={ltr:.4f}"
        )

        # Clear signal accumulators
        self._vpd_alert = self._ofi_alert = self._tape_alert = None

    # ── Position management ───────────────────────────────────────────────────

    def _manage_position(self, tick: Tick):
        pos   = self.position
        price = tick.price

        pos.update_mae(price)

        if self.state == State.PHASE1:
            hit_stop   = (price <= pos.stop   if pos.side == "long" else price >= pos.stop)
            hit_target = (price >= pos.target if pos.side == "long" else price <= pos.target)
            if hit_stop or hit_target:
                reason = "STOP" if hit_stop else "TARGET"
                self._close_position(price, tick.ts_ms, reason)

        elif self.state == State.PHASE2:
            # Dynamic trailing stop
            ema_slow = self._ema.ema_slow
            atr      = self._ema.atr
            if ema_slow and atr:
                if pos.side == "long":
                    trail = ema_slow - self._trail_mult * atr
                    pos.trail_stop = max(pos.trail_stop, trail)
                    if price <= pos.trail_stop:
                        self._close_position(price, tick.ts_ms, "TRAIL_STOP")
                else:
                    trail = ema_slow + self._trail_mult * atr
                    pos.trail_stop = min(pos.trail_stop or trail, trail)
                    if price >= pos.trail_stop:
                        self._close_position(price, tick.ts_ms, "TRAIL_STOP")

    def _check_phase2_transition(self, ema_state: dict, close: float):
        """Transition PHASE1 → PHASE2 when EMA cross confirms direction."""
        pos   = self.position
        cross = ema_state["cross"]
        atr   = ema_state.get("atr", 0.0)

        in_favor = (cross == "bull" and pos.side == "long") or \
                   (cross == "bear" and pos.side == "short")

        if in_favor:
            ema_slow = ema_state["ema_slow"]
            pos.trail_stop = (ema_slow - self._trail_mult * atr
                              if pos.side == "long"
                              else ema_slow + self._trail_mult * atr)
            self.state = State.PHASE2
            log.info(
                f"→ PHASE 2 | EMA cross confirmed  trail_stop={pos.trail_stop:.2f}"
            )

    def _check_opposing_flow(self, sig: Signal):
        """Accelerate exit on opposing OFI climax."""
        if not self.position:
            return
        pos = self.position

        if (pos.side == "long"  and sig.type == SignalType.OFI_SHORT) or \
           (pos.side == "short" and sig.type == SignalType.OFI_LONG):
            log.info(f"Opposing OFI climax — closing {pos.side}")
            self._close_position(sig.price, sig.ts_ms, "OFI_REVERSAL")

    # ── Position close ────────────────────────────────────────────────────────

    def _close_position(self, exit_price: float, ts_ms: int, reason: str):
        pos = self.position
        if not pos:
            return

        # All orders are post-only limit (maker): entries and stop/exits are limit orders.
        # Binance.US maker fee = 0%. Both legs charged for completeness.
        notional_entry = pos.entry_price * pos.qty
        notional_exit  = exit_price      * pos.qty
        fee            = (notional_entry + notional_exit) * self._maker_fee

        if pos.side == "long":
            gross_pnl = (exit_price - pos.entry_price) * pos.qty
        else:
            gross_pnl = (pos.entry_price - exit_price) * pos.qty

        net_pnl = gross_pnl - fee
        self.equity += net_pnl
        self.total_pnl += net_pnl
        self.trade_count += 1
        if net_pnl >= 0:
            self.winning_trades += 1

        win_rate = self.winning_trades / self.trade_count if self.trade_count else 0.0

        log.info(
            f"CLOSE {pos.side.upper()} | reason={reason}  exit={exit_price:.2f}  "
            f"pnl=${net_pnl:+.4f}  equity=${self.equity:.4f}  "
            f"win_rate={win_rate:.1%}  trades={self.trade_count}"
        )

        # Update Kelly history
        self._sizer.record(net_pnl)
        self._safety.record_trade(net_pnl, ts_ms)

        # Log snapshot
        self._logger.log_trade({
            "timestamp_ms":   ts_ms,
            "date_utc":       datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc).isoformat(),
            "entry_type":     pos.entry_type,
            "side":           pos.side,
            "entry_price":    pos.entry_price,
            "exit_price":     exit_price,
            "qty":            pos.qty,
            "reason":         reason,
            "state_at_exit":  self.state.value,
            "mae":            round(pos.mae, 6),
            "vol_mult":       pos.vol_mult,
            "ofi_ratio":      pos.ofi_ratio,
            "slippage_est":   0.0,       # maker orders have no slippage in paper
            "gross_pnl":      round(gross_pnl, 6),
            "fee":            round(fee, 6),
            "net_pnl":        round(net_pnl, 6),
            "cumulative_equity": round(self.equity, 4),
            "win_rate":       round(win_rate, 4),
            "phase2":         pos.phase2_activated,
        })

        self.position = None
        self.state    = State.IDLE

    # ── Signal expiry ─────────────────────────────────────────────────────────

    def _expire_signals(self, now_ms: int):
        w = self.SIGNAL_WINDOW_MS
        if self._vpd_alert  and now_ms - self._vpd_alert.ts_ms  > w:
            self._vpd_alert = None
        if self._ofi_alert  and now_ms - self._ofi_alert.ts_ms  > w:
            self._ofi_alert = None
        if self._tape_alert and now_ms - self._tape_alert.ts_ms > w:
            self._tape_alert = None

    # ── Hot-reload ────────────────────────────────────────────────────────────

    def reload_config(self):
        """Re-read config.json and update risk/signal parameters live."""
        try:
            with open(self._cfg_path) as f:
                cfg = json.load(f)
            self._cfg        = cfg
            self._vol_coeff  = cfg["risk"]["vol_coefficient"]
            self._trail_mult = cfg["phase2"]["trail_atr_mult"]
            self._min_rr     = cfg["risk"]["min_rr"]
            self._sizer      = KellySizer(cfg)
            log.info("Config hot-reloaded successfully")
        except Exception as exc:
            log.error(f"Config reload failed: {exc}")

    # ── Status ────────────────────────────────────────────────────────────────

    def status(self) -> dict:
        return {
            "state":    self.state.value,
            "equity":   round(self.equity, 4),
            "pnl":      round(self.total_pnl, 4),
            "trades":   self.trade_count,
            "win_rate": round(self.winning_trades / self.trade_count, 4)
                        if self.trade_count else 0.0,
            "position": {
                "side":  self.position.side,
                "entry": self.position.entry_price,
                "qty":   self.position.qty,
                "mae":   self.position.mae,
            } if self.position else None,
        }
