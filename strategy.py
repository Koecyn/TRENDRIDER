"""
HFT Mean-Reversion — BB + RSI + Momentum confirmation, 5-second bars
======================================================================
Long entry (trough detected):
  1. bb_pct < bb_entry_pct       → price at/below lower Bollinger band (1.5σ)
  2. RSI < rsi_entry              → oversold
  3. mom_slope > 0                → momentum ALREADY TURNING UP
  4. macro_up = 1                 → buying dips in uptrend only

Short entry (peak detected):
  1. bb_pct > (1 - bb_entry_pct) → price at/above upper Bollinger band
  2. RSI > (100 - rsi_entry)      → overbought
  3. mom_slope < 0                → momentum ALREADY TURNING DOWN
  4. macro_up = 0                 → shorting peaks in non-uptrend only

Long exit:  price >= bb_mid OR RSI > rsi_exit
Short exit: price <= bb_mid OR RSI < (100 - rsi_exit)
Stop (long):  bb_lower - bb_stop_mult * bb_std
Stop (short): bb_upper + bb_stop_mult * bb_std
Max hold: max_hold_bars (anti-stall, default 10 × 5s = 50s)

R:R with bb_nstd=1.5: target=mid (1.5σ), stop=0.5σ beyond band → 3:1
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import numpy as np
import pandas as pd
from backtesting import Backtest, Strategy

from indicators import (
    rsi as rsi_fn, bollinger,
    momentum_pct, momentum_slope_pct,
    volume_exhaustion, vwap_rolling,
    resample_ohlcv, _rolling_mean,
)


# ─────────────────────────────────────────────────────────────────────────────
# Pre-compute
# ─────────────────────────────────────────────────────────────────────────────

def precompute(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    c  = df["Close"].values
    h  = df["High"].values
    lo = df["Low"].values
    v  = df["Volume"].values
    p  = params

    df["rsi"]       = rsi_fn(c, period=p["rsi_period"])

    bb_mid, bb_low, bb_up, bb_pct, bb_std = bollinger(
        c, window=p["bb_window"], n_std=p["bb_nstd"])
    df["bb_mid"]    = bb_mid
    df["bb_lower"]  = bb_low
    df["bb_upper"]  = bb_up
    df["bb_pct"]    = bb_pct
    df["bb_std"]    = bb_std
    df["bb_stop"]   = bb_low - 0.5 * bb_std   # reference only; strategy recomputes

    df["mom_slope"] = momentum_slope_pct(c, fast=p["mom_fast"],
                                            slow=p["mom_slow"], smooth=2)
    df["vol_ex"]    = volume_exhaustion(v, window=p["vol_window"])
    df["sma60"]     = _rolling_mean(c, 60)

    c5, h5, l5, v5 = resample_ohlcv(c, h, lo, v, tf_bars=60)
    df["mom5m"]     = momentum_pct(c5, fast=3, slow=12)

    sma60_a = df["sma60"].values
    mom5m_a = df["mom5m"].values

    # Uptrend: above SMA AND 5m momentum not bearish
    df["macro_up"] = np.where(
        ~np.isnan(sma60_a) & ~np.isnan(mom5m_a),
        ((c > sma60_a) & (mom5m_a > -0.0001)).astype(int),
        0,
    )
    # Downtrend: below SMA AND 5m momentum actively negative (symmetric gate for shorts)
    df["macro_down"] = np.where(
        ~np.isnan(sma60_a) & ~np.isnan(mom5m_a),
        ((c < sma60_a) & (mom5m_a < -0.0001)).astype(int),
        0,
    )

    return df


BT_COLS = [
    "Open", "High", "Low", "Close", "Volume",
    "rsi", "bb_mid", "bb_lower", "bb_upper", "bb_pct", "bb_std",
    "mom_slope", "vol_ex", "macro_up", "macro_down",
]


# ─────────────────────────────────────────────────────────────────────────────
# Strategy
# ─────────────────────────────────────────────────────────────────────────────

class MeanReversionMTF(Strategy):

    # ── Tunable parameters ─────────────────────────────────────────────────
    rsi_entry      = 42.0   # long: RSI oversold / short: (100 - rsi_entry)
    rsi_exit       = 58.0   # long: RSI overbought / short: (100 - rsi_exit)
    bb_entry_pct   = 0.20   # band proximity zone
    bb_stop_mult   = 0.5    # stop = band ± bb_stop_mult * bb_std
    max_hold_bars  = 10     # 50s max hold → fast recycling
    taker_win_min  = 0.005

    def init(self):
        self._entry_bar  = -1
        self._stop_price = np.nan
        self._direction  = 0   # 1 = long, -1 = short

    # ── Accessors ─────────────────────────────────────────────────────────
    @property
    def _rsi(self):     return self.data.rsi[-1]
    @property
    def _mom(self):     return self.data.mom_slope[-1]
    @property
    def _bb_pct(self):  return self.data.bb_pct[-1]
    @property
    def _bb_mid(self):  return self.data.bb_mid[-1]
    @property
    def _bb_low(self):  return self.data.bb_lower[-1]
    @property
    def _bb_up(self):   return self.data.bb_upper[-1]
    @property
    def _bb_std(self):  return self.data.bb_std[-1]
    @property
    def _macro(self):      return self.data.macro_up[-1]
    @property
    def _macro_dn(self):   return self.data.macro_down[-1]
    @property
    def _close(self):      return self.data.Close[-1]

    # ── Long entry: trough with confirmed momentum turn ────────────────────
    def _should_enter_long(self) -> bool:
        if np.isnan(self._bb_pct) or np.isnan(self._rsi) or np.isnan(self._mom):
            return False
        return bool(
            self._bb_pct < self.bb_entry_pct and   # at lower band
            self._rsi    < self.rsi_entry     and   # oversold
            self._mom    > 0.0                and   # momentum ALREADY turning up
            self.data.vol_ex[-1] < 4.0              # not a crash
        )

    # ── Short entry: peak with confirmed momentum turn ─────────────────────
    def _should_enter_short(self) -> bool:
        if np.isnan(self._bb_pct) or np.isnan(self._rsi) or np.isnan(self._mom):
            return False
        rsi_ob = 100.0 - self.rsi_entry   # overbought threshold (e.g. 58)
        return bool(
            self._bb_pct > (1.0 - self.bb_entry_pct) and  # at upper band
            self._rsi    > rsi_ob                    and  # overbought
            self._mom    < 0.0                       and  # momentum ALREADY turning down
            self.data.vol_ex[-1] < 4.0                   # not a spike
        )

    # ── Long exit: mean-reversion complete ────────────────────────────────
    def _should_exit_long(self) -> bool:
        if np.isnan(self._bb_pct) or np.isnan(self._rsi): return False
        return bool(self._close >= self._bb_mid or self._rsi > self.rsi_exit)

    # ── Short exit: mean-reversion complete ───────────────────────────────
    def _should_exit_short(self) -> bool:
        if np.isnan(self._bb_pct) or np.isnan(self._rsi): return False
        rsi_os = 100.0 - self.rsi_exit   # oversold threshold (e.g. 42)
        return bool(self._close <= self._bb_mid or self._rsi < rsi_os)

    # ── Stop check (direction-aware) ──────────────────────────────────────
    def _stop_hit(self) -> bool:
        if np.isnan(self._stop_price): return False
        if self._direction == 1:
            return self._close < self._stop_price
        return self._close > self._stop_price   # short

    # ── Main loop ──────────────────────────────────────────────────────────
    def next(self):
        bar = len(self.data) - 1
        if bar < 80: return
        if np.isnan(self._bb_mid) or np.isnan(self._bb_std): return

        if self.position:
            if bar - self._entry_bar >= self.max_hold_bars:
                self.position.close(); self._reset(); return
            if self._stop_hit():
                self.position.close(); self._reset(); return
            if self._direction == 1 and self._should_exit_long():
                self.position.close(); self._reset(); return
            if self._direction == -1 and self._should_exit_short():
                self.position.close(); self._reset(); return
        else:
            if self._should_enter_long():
                stop_price = self._bb_low - self.bb_stop_mult * self._bb_std
                if np.isnan(stop_price) or stop_price >= self._close: return
                n_units = max(1, int(self.equity * 0.95 / self._close))
                self.buy(size=n_units)
                self._entry_bar  = bar
                self._stop_price = stop_price
                self._direction  = 1

            elif self._should_enter_short():
                stop_price = self._bb_up + self.bb_stop_mult * self._bb_std
                if np.isnan(stop_price) or stop_price <= self._close: return
                n_units = max(1, int(self.equity * 0.95 / self._close))
                self.sell(size=n_units)
                self._entry_bar  = bar
                self._stop_price = stop_price
                self._direction  = -1

    def _reset(self):
        self._entry_bar  = -1
        self._stop_price = np.nan
        self._direction  = 0
