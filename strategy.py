"""
HFT Mean-Reversion Multi-Timeframe Strategy
============================================
Framework  : backtesting.py (bar-by-bar, zero lookahead)
Primary TF : 1-minute bars
Higher TFs : 5m and 10m resampled (in-bar, boundary-locked)

Entry  : trough during macro uptrend
         • normalized momentum flattening (negative → neutral)
         • volume exhaustion or capitulation spike
Exit   : momentum death / peak confirmed / trailing stop
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import numpy as np
import pandas as pd
from backtesting import Backtest, Strategy

from indicators import (
    momentum_pct, momentum_slope_pct, roc,
    volume_exhaustion, vwap_rolling,
    resample_ohlcv, trailing_low, _ema,
)


# ─────────────────────────────────────────────────────────────────────────────
# Pre-compute all indicators onto the DataFrame BEFORE the backtest loop
# ─────────────────────────────────────────────────────────────────────────────

def precompute(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    c  = df["Close"].values
    h  = df["High"].values
    lo = df["Low"].values
    v  = df["Volume"].values
    p  = params

    # 1m normalized momentum
    df["mom_pct"]      = momentum_pct(c, fast=p["mom_fast"], slow=p["mom_slow"])
    df["mom_slope"]    = momentum_slope_pct(c, fast=p["mom_fast"], slow=p["mom_slow"], smooth=3)
    df["roc5"]         = roc(c, 5)
    df["vol_exhaust"]  = volume_exhaustion(v, window=p["vol_window"])
    df["vwap"]         = vwap_rolling(h, lo, c, v, window=p["vwap_window"])

    # 5m resampled
    c5, h5, l5, v5 = resample_ohlcv(c, h, lo, v, tf_bars=5)
    df["low_5m"]       = l5
    df["close_5m"]     = c5
    df["mom_pct_5m"]   = momentum_pct(c5, fast=3, slow=10)

    # 10m resampled
    c10, h10, l10, v10 = resample_ohlcv(c, h, lo, v, tf_bars=10)
    df["low_10m"]      = l10

    # Trailing 5-bar low of 1m lows (stop reference)
    df["trail_sl"]     = trailing_low(lo, window=5)

    # Macro uptrend: price above VWAP AND 5m momentum positive
    vwap_arr  = df["vwap"].values
    mom5_arr  = df["mom_pct_5m"].values
    macro_up  = np.where(
        (~np.isnan(vwap_arr)) & (~np.isnan(mom5_arr)),
        ((c > vwap_arr) & (mom5_arr > 0)).astype(int),
        0,
    )
    df["macro_up"] = macro_up

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Strategy
# ─────────────────────────────────────────────────────────────────────────────

class MeanReversionMTF(Strategy):
    # ── Tunable thresholds (all normalized, dimensionless) ─────────────────
    # Momentum slope thresholds (units: Δ(ema_diff/price) per bar)
    mom_flat_thresh  = -0.0002   # slope below this (negative) but recovering
    mom_peak_thresh  =  0.0001   # peak slope (was above this, now declining)
    mom_exit_thresh  = -0.00005  # exit when slope drops below this after peak

    # Volume thresholds
    vol_exhaust_lo   =  0.65     # seller exhaustion (low vol)
    vol_exhaust_hi   =  1.70     # capitulation spike (high vol)

    # Risk management
    sl_buffer        =  0.0005   # SL = trail_sl * (1 - sl_buffer)
    risk_pct         =  0.01     # 1% equity risk per trade
    max_hold_bars    =  60       # force-exit anti-stall

    # Taker switch: only use taker if projected win covers friction
    taker_win_min    =  0.008    # 0.8% min projected gain to pay taker fee

    # ── State ──────────────────────────────────────────────────────────────
    def init(self):
        self._entry_bar       = -1
        self._peak_mom_slope  = -np.inf
        self._active_sl       = np.nan
        self._entry_price     = np.nan
        self._sl_confirm_bars = 0

    # ── Indicator accessors (direct column read, no self.I overhead) ────────
    @property
    def _mom(self): return self.data.mom_slope[-1]
    @property
    def _mom_prev(self): return self.data.mom_slope[-2] if len(self.data.mom_slope) > 1 else self._mom
    @property
    def _mom_pct(self): return self.data.mom_pct[-1]
    @property
    def _ve(self): return self.data.vol_exhaust[-1]
    @property
    def _roc5(self): return self.data.roc5[-1]
    @property
    def _vwap(self): return self.data.vwap[-1]
    @property
    def _trail_sl(self): return self.data.trail_sl[-1]
    @property
    def _low5m(self): return self.data.low_5m[-1]
    @property
    def _low10m(self): return self.data.low_10m[-1]
    @property
    def _macro_up(self): return self.data.macro_up[-1]
    @property
    def _close(self): return self.data.Close[-1]

    # ── Entry conditions ────────────────────────────────────────────────────
    def _trough_conditions(self) -> bool:
        """
        Buy trough during macro uptrend:
          1. Macro context: price above VWAP, 5m momentum positive
          2. Momentum slope was negative, is now FLATTENING (recovering toward 0)
          3. Volume shows exhaustion (sellers drying up) OR capitulation spike
        """
        if not self._macro_up:
            return False
        if np.isnan(self._mom) or np.isnan(self._mom_prev):
            return False

        # Momentum flattening: slope improving (less negative) but still negative
        flattening = (self._mom > self._mom_prev) and (self._mom < self.mom_flat_thresh)

        # Volume signal
        vol_sig = (self._ve < self.vol_exhaust_lo) or (self._ve > self.vol_exhaust_hi)

        return flattening and vol_sig

    # ── Exit conditions ─────────────────────────────────────────────────────
    def _peak_conditions(self) -> bool:
        """
        Sell peak:
          1. Momentum slope peaked (was above mom_peak_thresh during the trade)
          2. Now declining below mom_exit_thresh
          OR ROC went negative
        """
        if np.isnan(self._mom) or np.isnan(self._mom_prev):
            return False

        # Update peak
        if self._mom > self._peak_mom_slope:
            self._peak_mom_slope = self._mom

        # Peak death: was strong positive, now weakening
        peak_death = (
            self._peak_mom_slope > self.mom_peak_thresh
            and self._mom < self._mom_prev          # slope declining
            and self._mom < self.mom_exit_thresh     # below exit threshold
        )
        roc_negative = (not np.isnan(self._roc5)) and (self._roc5 < -0.001)

        return peak_death or roc_negative

    # ── Taker decision ──────────────────────────────────────────────────────
    def _use_taker(self) -> bool:
        if np.isnan(self._entry_price) or self._entry_price <= 0:
            return False
        gain = (self._close - self._entry_price) / self._entry_price
        friction = 0.00002 + 0.30 * max(gain, 0)
        return gain > friction + self.taker_win_min

    # ── Trailing stop ───────────────────────────────────────────────────────
    def _update_sl(self):
        if np.isnan(self._trail_sl):
            return
        new_sl = self._trail_sl * (1.0 - self.sl_buffer)
        if np.isnan(self._active_sl) or new_sl > self._active_sl:
            self._active_sl = new_sl

    def _stop_triggered(self) -> bool:
        if np.isnan(self._active_sl):
            return False
        price = self._close
        if price >= self._active_sl:
            self._sl_confirm_bars = 0
            return False
        # Below stop — check hardening conditions
        low10m = self._low10m
        if not np.isnan(low10m) and price <= low10m:
            return True  # Confirmed: broke macro low
        self._sl_confirm_bars += 1
        if self._sl_confirm_bars >= 5:
            self._sl_confirm_bars = 0
            return True  # Confirmed: 5 consecutive bars below stop
        return False

    # ── Main loop ───────────────────────────────────────────────────────────
    def next(self):
        bar_idx = len(self.data) - 1

        # Warm-up: need at least 30 bars for indicator stability
        if bar_idx < 30:
            return
        if np.isnan(self._vwap) or np.isnan(self._trail_sl):
            return

        # ── In position ──────────────────────────────────────────────────
        if self.position:
            self._update_sl()

            # Anti-stall
            if bar_idx - self._entry_bar >= self.max_hold_bars:
                self.position.close()
                self._reset()
                return

            # Stop check
            if self._stop_triggered():
                self.position.close()
                self._reset()
                return

            # Peak exit
            if self._peak_conditions():
                self.position.close()
                self._reset()
                return

        # ── No position: look for entry ──────────────────────────────────
        else:
            if self._trough_conditions():
                sl_price   = self._trail_sl * (1.0 - self.sl_buffer)
                entry_est  = self._close
                if sl_price >= entry_est or np.isnan(sl_price):
                    return
                risk_per_unit = entry_est - sl_price
                if risk_per_unit <= 0:
                    return

                # Integer unit sizing: risk_pct of equity, capped to 20% of equity
                # (backtesting.py cancels size < 1 → must be >= 1 integer unit)
                risk_usd    = self.equity * self.risk_pct
                n_by_risk   = risk_usd / risk_per_unit
                n_by_avail  = int(self.equity * 0.20 / entry_est)
                n_units     = max(1, min(int(round(n_by_risk)), n_by_avail))

                self.buy(size=n_units)
                self._entry_bar      = bar_idx
                self._entry_price    = entry_est
                self._peak_mom_slope = self._mom
                self._active_sl      = sl_price
                self._sl_confirm_bars = 0

    def _reset(self):
        self._entry_bar       = -1
        self._entry_price     = np.nan
        self._peak_mom_slope  = -np.inf
        self._active_sl       = np.nan
        self._sl_confirm_bars = 0
