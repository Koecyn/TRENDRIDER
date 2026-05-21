"""
Bar-by-bar backtest engine — no lookahead bias.

Each bar i only sees data[:i+1].
Tracks per-trade MAE (max adverse excursion) for stop calibration.
Reports per-tier win rates separately — Tier 1 must be tracked in isolation.
"""

from __future__ import annotations
import numpy as np
from dataclasses import dataclass, field
from typing import List, Optional

from . import config as C
from . import fusion
from .signals import HydraulicAccumulator
from .position import PositionManager
from .filters import snr as compute_snr


# ─────────────────────────────────────────────────────────────────────────────
# Trade record
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Trade:
    bar_idx:    int
    timestamp:  int
    direction:  int          # +1 long / −1 short
    entry:      float
    stop:       float
    target:     float
    tier:       int
    size_usd:   float
    confidence: float
    # filled on close:
    exit_bar:    Optional[int]   = None
    exit_price:  Optional[float] = None
    exit_reason: str             = ''
    pnl_pct:     float           = 0.0
    mae:         float           = 0.0   # max adverse excursion (price units)
    mfe:         float           = 0.0   # max favorable excursion

    def tick(self, price: float):
        adverse   = (self.entry - price) * self.direction
        favorable = (price    - self.entry) * self.direction
        self.mae  = max(self.mae, float(adverse))
        self.mfe  = max(self.mfe, float(favorable))

    def close(self, bar_idx: int, price: float, reason: str):
        self.exit_bar    = bar_idx
        self.exit_price  = float(price)
        self.exit_reason = reason
        self.pnl_pct     = (price - self.entry) / self.entry * 100.0 * self.direction


# ─────────────────────────────────────────────────────────────────────────────
# Result container
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Result:
    trades:     List[Trade]
    equity:     np.ndarray
    timestamps: np.ndarray

    @property
    def wins(self):   return [t for t in self.trades if t.pnl_pct > 0]
    @property
    def losses(self): return [t for t in self.trades if t.pnl_pct <= 0]

    @property
    def win_rate(self) -> float:
        return len(self.wins) / len(self.trades) if self.trades else 0.0

    @property
    def sharpe(self) -> float:
        """Annualized Sharpe from hourly equity samples — robust for 1m HFT data."""
        if len(self.trades) < 2 or len(self.equity) < 120:
            return 0.0
        sampled = self.equity[::60]   # one sample per hour
        if len(sampled) < 3:
            return 0.0
        ret = np.diff(sampled) / (sampled[:-1] + 1e-10)
        std = float(np.std(ret))
        if std < 1e-10:
            return 0.0
        return float(np.mean(ret) / std * np.sqrt(252 * 24))

    @property
    def max_drawdown(self) -> float:
        if len(self.equity) < 2:
            return 0.0
        peak = np.maximum.accumulate(self.equity)
        dd   = (self.equity - peak) / (peak + 1e-10) * 100.0
        return float(np.min(dd))

    @property
    def profit_factor(self) -> float:
        gw = sum(t.pnl_pct for t in self.wins)
        gl = sum(abs(t.pnl_pct) for t in self.losses)
        return gw / gl if gl > 0 else float('inf')

    def tier_stats(self, tier: int) -> dict:
        tt = [t for t in self.trades if t.tier == tier]
        if not tt:
            return {'n': 0, 'win_rate': 0.0, 'sharpe': 0.0, 'avg_pnl': 0.0}
        wins = [t for t in tt if t.pnl_pct > 0]
        r    = np.array([t.pnl_pct for t in tt])
        # Per-trade sharpe scaled by tier trade frequency
        n_bars = max(len(self.equity), 1)
        tpy    = len(tt) / n_bars * (252 * 1440)
        sh     = float(np.mean(r) / (np.std(r) + 1e-10) * np.sqrt(max(tpy, 1)))
        return {
            'n':        len(tt),
            'win_rate': round(len(wins) / len(tt) * 100, 2),
            'sharpe':   round(sh, 4),
            'avg_pnl':  round(float(np.mean(r)), 4),
            'avg_mae':  round(float(np.mean([t.mae for t in wins])) if wins else 0, 4),
        }

    def summary(self) -> dict:
        n_bars    = max(len(self.equity), 1)
        hours     = n_bars / 60.0
        tph       = len(self.trades) / max(hours, 1)
        d = {
            'n_trades':      len(self.trades),
            'trades_per_hr': round(tph, 2),
            'win_rate':      round(self.win_rate * 100, 2),
            'sharpe':        round(self.sharpe, 4),
            'max_dd':        round(self.max_drawdown, 4),
            'profit_factor': round(self.profit_factor, 4),
            'avg_pnl':       round(float(np.mean([t.pnl_pct for t in self.trades])), 4) if self.trades else 0.0,
            'equity_final':  round(float(self.equity[-1]) if len(self.equity) else 0.0, 2),
        }
        for tier in [1, 2, 3]:
            ts = self.tier_stats(tier)
            for k, v in ts.items():
                d[f't{tier}_{k}'] = v
        return d


# ─────────────────────────────────────────────────────────────────────────────
# ATR helper
# ─────────────────────────────────────────────────────────────────────────────

def _calc_atr(highs, lows, closes, period=14) -> np.ndarray:
    n   = len(closes)
    out = np.zeros(n)
    if n < period + 2:
        return out
    trs = np.array([
        max(highs[i] - lows[i],
            abs(highs[i]  - closes[i-1]),
            abs(lows[i]   - closes[i-1]))
        for i in range(1, n)
    ])
    out[period] = float(np.mean(trs[:period]))
    for i in range(period, len(trs)):
        out[i+1] = (out[i] * (period - 1) + trs[i]) / period
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Main backtest loop
# ─────────────────────────────────────────────────────────────────────────────

def run(data: dict,
        initial_balance: float = None,
        long_only: bool = False,
        ob_snapshots: list = None) -> Result:
    """
    Bar-by-bar simulation.  data = dict from physics.data.parse_klines().
    long_only=True skips SHORT signals (useful for Binance spot with no margin).
    ob_snapshots: list of {'ts_ms', 'bids', 'asks'} from PHYX collector — feeds
                  real order book into Darcy, OBI, water hammer, cavitation signals.
                  Falls back to taker-ratio proxy when None (REST-only mode).
    """
    bal   = initial_balance or C.INITIAL_BAL
    prices     = data['prices']
    opens      = data['opens']
    highs      = data['highs']
    lows       = data['lows']
    closes     = data['closes']
    volumes    = data['volumes']
    taker_buy  = data['taker_buy']
    timestamps = data['timestamps']
    n          = len(prices)

    pm         = PositionManager(bal)
    accum      = HydraulicAccumulator()
    atr_arr    = _calc_atr(highs, lows, closes, C.MAE_ATR_PERIOD)
    equity     = np.full(n, bal)
    trades     = []
    open_trade: Optional[Trade] = None

    # Build OB snapshot index for O(log n) per-bar lookup
    _ob_snaps = None
    _ob_times = None
    if ob_snapshots:
        _ob_snaps = sorted(ob_snapshots, key=lambda r: r['ts_ms'])
        _ob_times = np.array([r['ts_ms'] for r in _ob_snaps], dtype=np.int64)

    for i in range(C.WARMUP_BARS, n):
        sl  = slice(max(0, i - 600), i + 1)
        p   = prices[sl];     o = opens[sl];    h = highs[sl]
        l   = lows[sl];       c = closes[sl];   v = volumes[sl]
        tb  = taker_buy[sl]
        cur = float(prices[i])
        atr = float(atr_arr[i]) if atr_arr[i] > 0 else cur * 0.002

        # ── Manage open trade ─────────────────────────────────────────────
        if open_trade is not None:
            open_trade.tick(cur)
            reason = exit_reason = None

            if open_trade.direction == +1:
                if cur <= open_trade.stop:
                    reason, exit_px = 'stop',   open_trade.stop
                elif cur >= open_trade.target:
                    reason, exit_px = 'target', open_trade.target
            else:
                if cur >= open_trade.stop:
                    reason, exit_px = 'stop',   open_trade.stop
                elif cur <= open_trade.target:
                    reason, exit_px = 'target', open_trade.target

            if reason is None and (i - open_trade.bar_idx) >= C.MAX_HOLD_BARS:
                reason, exit_px = 'timeout', cur

            if reason:
                open_trade.close(i, exit_px, reason)
                pm.on_close(open_trade.pnl_pct, open_trade.mae, open_trade.tier)
                trades.append(open_trade)
                open_trade = None

        # Equity mark-to-market
        if open_trade is not None:
            unrealised = (cur - open_trade.entry) / open_trade.entry * open_trade.size_usd * open_trade.direction
            equity[i]  = pm.balance + unrealised
        else:
            equity[i]  = pm.balance

        if open_trade is not None:
            continue   # one position at a time

        # ── Nearest OB snapshot for this bar ──────────────────────────────
        bids = asks = None
        if _ob_snaps is not None:
            idx = int(np.searchsorted(_ob_times, timestamps[i], side='right')) - 1
            if 0 <= idx < len(_ob_snaps):
                snap = _ob_snaps[idx]
                bids = snap['bids']
                asks = snap['asks']

        # ── Signal engine ─────────────────────────────────────────────────
        sig = fusion.run(p, o, c, v, tb, accum, bids=bids, asks=asks)

        direction  = sig['direction']
        tier       = sig['tier']
        confidence = sig['confidence']

        if tier >= 4 or direction == 0:
            continue
        if long_only and direction == -1:
            continue
        if sig['snr'] < C.SNR_MIN:
            continue

        # ── Build trade ───────────────────────────────────────────────────
        entry  = cur
        stop   = pm.entry_stop(entry, direction, atr, tier)
        target = pm.entry_target(entry, stop, direction, tier, atr)
        size   = pm.size(tier, confidence)

        open_trade = Trade(
            bar_idx=i, timestamp=int(timestamps[i]),
            direction=direction, entry=entry,
            stop=stop, target=target, tier=tier,
            size_usd=size, confidence=confidence,
        )

    # Force-close at end of data
    if open_trade is not None:
        open_trade.close(n - 1, float(prices[-1]), 'eod')
        pm.on_close(open_trade.pnl_pct, open_trade.mae, open_trade.tier)
        trades.append(open_trade)

    equity[-1] = pm.balance
    return Result(trades=trades, equity=equity, timestamps=timestamps)
