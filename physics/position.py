"""
Position sizing and stop placement.

  Fractional Kelly  — size based on running win/loss statistics per tier
  MAE stops         — stop distance = Nth percentile of winner MAE history

Kelly formula:  f* = (b·p − q) / b
  b = avg_win / avg_loss  (win:loss ratio)
  p = win probability
  q = 1 − p

Use half-Kelly (KELLY_FRACTION = 0.5) to account for estimation error.

MAE insight: set stops at the 90th percentile of WINNING trade MAE.
  This means stops are placed beyond where 90% of eventual winners need to breathe.
  Stop-outs are reserved for the 10% that needed more room than winners typically do.
"""

import numpy as np
from . import config as C


class KellySizer:
    """
    Fractional Kelly position sizing with per-tier tracking.
    """

    _TIER_DEFAULTS = {1: 0.40, 2: 0.30, 3: 0.20, 4: 0.10}

    def __init__(self, fraction: float = None):
        self._f      = fraction or C.KELLY_FRACTION
        self._wins   = []
        self._losses = []
        self._by_tier: dict = {t: {'w': [], 'l': []} for t in [1, 2, 3, 4]}

    def record(self, pnl_pct: float, tier: int = None):
        if pnl_pct > 0:
            self._wins.append(pnl_pct)
            if tier in self._by_tier:
                self._by_tier[tier]['w'].append(pnl_pct)
        else:
            self._losses.append(abs(pnl_pct))
            if tier in self._by_tier:
                self._by_tier[tier]['l'].append(abs(pnl_pct))

    def _kelly_raw(self, wins: list, losses: list) -> float:
        n = len(wins) + len(losses)
        if n < C.KELLY_WARMUP_TRADES:
            return None   # not enough data
        p = len(wins) / n
        q = 1.0 - p
        avg_w = float(np.mean(wins))  if wins  else 0.0
        avg_l = float(np.mean(losses)) if losses else 1e-6
        b     = avg_w / avg_l if avg_l > 0 else 1.0
        if b <= 0:
            return 0.0
        return float(np.clip((b * p - q) / b * self._f, C.KELLY_MIN, C.KELLY_MAX))

    def fraction(self, tier: int = None) -> float:
        """Return Kelly fraction for this tier (falls back to global)."""
        if tier and tier in self._by_tier:
            td  = self._by_tier[tier]
            raw = self._kelly_raw(td['w'], td['l'])
            if raw is not None:
                return raw
        raw = self._kelly_raw(self._wins, self._losses)
        if raw is not None:
            return raw
        return self._TIER_DEFAULTS.get(tier, 0.25)

    def size_usd(self, balance: float, tier: int,
                 confidence: float = 1.0) -> float:
        """Dollar amount to allocate (Kelly fraction × confidence × balance)."""
        k = self.fraction(tier) * float(np.clip(confidence, 0.0, 1.0))
        return balance * float(np.clip(k, C.KELLY_MIN, C.KELLY_MAX))

    @property
    def n_trades(self) -> int:
        return len(self._wins) + len(self._losses)

    @property
    def win_rate(self) -> float:
        n = self.n_trades
        return len(self._wins) / n if n else 0.0

    def stats(self) -> dict:
        n = self.n_trades
        if n == 0:
            return {'n': 0, 'win_rate': 0.0, 'kelly': 0.25}
        return {
            'n':        n,
            'win_rate': round(len(self._wins) / n * 100, 1),
            'kelly':    round(self.fraction() * 100, 1),
        }


class MAEStops:
    """
    Maximum Adverse Excursion stop placement.

    Records the intrabar adverse move for each trade.
    Uses the Nth percentile of WINNER MAE as the stop distance — ensuring
    we don't stop out winning trades that needed normal breathing room.

    During warmup (< MAE_WARMUP_TRADES), falls back to ATR × multiplier.
    """

    def __init__(self, percentile: float = None):
        self._pct        = percentile or C.MAE_PERCENTILE
        self._winner_mae = []
        self._loser_mae  = []
        self._by_tier    = {t: [] for t in [1, 2, 3]}

    def record(self, mae: float, pnl_pct: float, tier: int = None):
        if pnl_pct > 0:
            self._winner_mae.append(float(mae))
            if tier in self._by_tier:
                self._by_tier[tier].append(float(mae))
        else:
            self._loser_mae.append(float(mae))

    def stop_distance(self, entry: float, atr: float,
                      tier: int = None, direction: int = 1) -> float:
        """
        Return absolute stop price.
        direction: +1 = long (stop below), −1 = short (stop above).
        """
        dist = self._distance(atr, tier)
        return entry - dist * direction

    def _distance(self, atr: float, tier: int = None) -> float:
        data = self._by_tier.get(tier, [])
        if len(data) < 5:
            data = self._winner_mae
        if len(data) < C.MAE_WARMUP_TRADES:
            return (atr or 0.0) * C.MAE_ATR_FALLBACK or 1.0
        return float(np.percentile(data, self._pct))

    def distance_raw(self, tier: int = None) -> float | None:
        data = self._by_tier.get(tier, [])
        if len(data) < 5:
            data = self._winner_mae
        if len(data) < C.MAE_WARMUP_TRADES:
            return None
        return float(np.percentile(data, self._pct))

    @property
    def n_winners(self) -> int:
        return len(self._winner_mae)

    def stats(self) -> dict:
        return {
            'winner_mae_p50': round(float(np.median(self._winner_mae)), 4) if self._winner_mae else 0.0,
            'winner_mae_p90': round(float(np.percentile(self._winner_mae, 90)), 4) if self._winner_mae else 0.0,
            'n_winners':      len(self._winner_mae),
            'n_losers':       len(self._loser_mae),
        }


class PositionManager:
    """Unified entry for Kelly sizing + MAE stops."""

    def __init__(self, balance: float):
        self.balance = balance
        self.kelly   = KellySizer()
        self.mae     = MAEStops()

    def on_close(self, pnl_pct: float, mae: float, tier: int,
                 size_usd: float = None):
        self.kelly.record(pnl_pct, tier)
        self.mae.record(mae, pnl_pct, tier)
        # Portfolio return = price return × (position size / balance)
        # Without size_usd the price return would be applied to the full balance,
        # overstating equity changes by 1/kelly_fraction (~20× at kelly=0.05).
        if size_usd is not None and self.balance > 0:
            portfolio_pct = pnl_pct * (size_usd / self.balance)
        else:
            portfolio_pct = pnl_pct
        self.balance *= 1.0 + portfolio_pct / 100.0

    def entry_stop(self, entry: float, direction: int,
                   atr: float, tier: int) -> float:
        return self.mae.stop_distance(entry, atr, tier, direction)

    def entry_target(self, entry: float, stop: float,
                     direction: int, tier: int, atr: float = None) -> float:
        # Fixed ATR target: decoupled from stop so wide noise-stops don't
        # push targets to unreachable distances on 1m bars.
        if atr and atr > 0:
            mult = {1: C.TIER1_TARGET_ATR, 2: C.TIER2_TARGET_ATR,
                    3: C.TIER3_TARGET_ATR}.get(tier, C.TIER3_TARGET_ATR)
            return entry + atr * mult * direction
        rr   = {1: C.TIER1_RR, 2: C.TIER2_RR, 3: C.TIER3_RR}.get(tier, 2.0)
        dist = abs(entry - stop)
        return entry + dist * rr * direction

    def size(self, tier: int, confidence: float) -> float:
        return self.kelly.size_usd(self.balance, tier, confidence)
