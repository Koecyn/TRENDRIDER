#!/usr/bin/env python3
"""
ob_direction.py — Real-time order book direction analysis.

Reads raw deque lines from collect_raw.py and distils each L20 snapshot
into a single directional call:

  LONG    — thin asks, heavy bids → path of least resistance UP
  SHORT   — thin bids, heavy asks → path of least resistance DOWN
  NEUTRAL — balanced book, no clear edge

Four signals fused into one score:
  near_imbal   (0.40) — notional within 0.05% of mid (highest execution probability)
  obi_l5       (0.30) — proximity-weighted imbalance, top 5 levels
  notional     (0.20) — full-book USD imbalance
  l1_pressure  (0.10) — raw best-bid vs best-ask qty ratio

Trade validation: each aggressor trade is checked against the preceding OB
direction call to build a running accuracy score.

Wire format (collect_raw.py):
  Depth: ["D", ts_ms, [[price_cents, qty_units], ...bids], [...asks]]
  Trade: ["T", ts_ms, price_cents, qty_units, side]
         side = 1 (buy aggressor) | 0 (sell aggressor)

To add a second exchange: instantiate OBDirection(exchange='name') with its
own raw-line feed. The parser handles the same wire format; add an exchange-
specific _parse_ob/_parse_trade wrapper if needed.
"""

import collections, json
from dataclasses import dataclass, field
from typing import Optional

# Wire-format scaling
_P = 100        # prices stored as int cents  → divide by 100
_Q = 10_000     # qtys  stored as int units×10k → divide by 10_000

# Tunable constants
NEAR_PCT    = 0.0005   # 0.05% each side of mid = "near-mid zone"
CONF_THRESH = 0.18     # |score| to call LONG/SHORT (below = NEUTRAL)
SMOOTH_N    = 6        # EMA window: recent snapshots to average

# Signal weights (must sum to 1.0)
W_NEAR     = 0.40
W_OBI5     = 0.30
W_NOTIONAL = 0.20
W_L1       = 0.10


# ── Parsers ───────────────────────────────────────────────────────────────────

def _parse_ob(line: str) -> Optional[dict]:
    try:
        rec = json.loads(line)
        if rec[0] != 'D':
            return None
        bids = [(p / _P, q / _Q) for p, q in rec[2]] if rec[2] else []
        asks = [(p / _P, q / _Q) for p, q in rec[3]] if rec[3] else []
        return {'ts': int(rec[1]), 'bids': bids, 'asks': asks}
    except Exception:
        return None


def _parse_trade(line: str) -> Optional[dict]:
    try:
        rec = json.loads(line)
        if rec[0] != 'T':
            return None
        return {
            'ts':    int(rec[1]),
            'price': int(rec[2]) / _P,
            'qty':   int(rec[3]) / _Q,
            'side':  int(rec[4]),   # 1=buy aggressor, 0=sell aggressor
        }
    except Exception:
        return None


# ── Core computation ──────────────────────────────────────────────────────────

def _compute_snapshot(bids: list, asks: list) -> dict:
    """
    Compute all direction signals from one L20 snapshot.
    bids/asks: [(price_usd, qty_btc), ...] sorted best-first.
    """
    if not bids or not asks:
        return {'direction': 'NEUTRAL', 'confidence': 0.0, 'score': 0.0}

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid      = (best_bid + best_ask) / 2.0
    spread   = best_ask - best_bid

    # 1. L1 pressure — best bid qty vs best ask qty
    l1b, l1a = bids[0][1], asks[0][1]
    l1_tot      = l1b + l1a
    l1_pressure = (l1b - l1a) / l1_tot if l1_tot > 0 else 0.0

    # 2. L5 proximity-weighted OBI — closer levels carry more weight
    bid5_w = sum(q / (mid - p + 1.0) for p, q in bids[:5])
    ask5_w = sum(q / (p - mid + 1.0) for p, q in asks[:5])
    obi_l5 = (bid5_w - ask5_w) / (bid5_w + ask5_w + 1e-10)

    # 3. Full-book USD notional imbalance
    bid_notional = sum(p * q for p, q in bids)
    ask_notional = sum(p * q for p, q in asks)
    notional_tot  = bid_notional + ask_notional
    notional_imbal = (bid_notional - ask_notional) / (notional_tot + 1e-10)

    # 4. Near-mid zone — liquidity within NEAR_PCT of mid (most actionable)
    near      = mid * NEAR_PCT
    bid_near  = sum(p * q for p, q in bids if p >= mid - near)
    ask_near  = sum(p * q for p, q in asks if p <= mid + near)
    near_tot  = bid_near + ask_near
    near_imbal = (bid_near - ask_near) / (near_tot + 1e-10)

    # 5. Composite score
    score = (
        W_NEAR     * near_imbal     +
        W_OBI5     * obi_l5         +
        W_NOTIONAL * notional_imbal +
        W_L1       * l1_pressure
    )
    score     = max(-1.0, min(1.0, score))
    direction = 'LONG' if score > CONF_THRESH else ('SHORT' if score < -CONF_THRESH else 'NEUTRAL')
    confidence = min(abs(score) / 0.5, 1.0)

    return {
        'mid':           round(mid, 2),
        'spread':        round(spread, 2),
        'spread_bps':    round(spread / mid * 10_000, 2) if mid > 0 else 0.0,
        'l1_pressure':   round(l1_pressure, 4),
        'obi_l5':        round(obi_l5, 4),
        'notional_imbal':round(notional_imbal, 4),
        'near_imbal':    round(near_imbal, 4),
        'bid_notional':  round(bid_notional, 0),
        'ask_notional':  round(ask_notional, 0),
        'bid_near_usd':  round(bid_near, 0),
        'ask_near_usd':  round(ask_near, 0),
        'score':         round(score, 4),
        'direction':     direction,
        'confidence':    round(confidence, 3),
    }


# ── Stateful tracker ──────────────────────────────────────────────────────────

@dataclass
class OBDirection:
    """
    Stateful real-time OB direction tracker for one exchange.

    Usage:
        tracker = OBDirection(exchange='binance_us')
        result  = tracker.update(raw_lines)   # called on each deque snapshot
    """
    exchange: str = 'binance_us'

    _snap_scores:    collections.deque = field(default_factory=lambda: collections.deque(maxlen=SMOOTH_N))
    _last_snap:      dict              = field(default_factory=dict)
    _last_direction: str               = 'NEUTRAL'
    _smooth_score:   float             = 0.0

    # trade validation
    _pending_pred:   Optional[str]     = None
    _trade_correct:  int               = 0
    _trade_total:    int               = 0

    def update(self, raw_lines: list) -> dict:
        """
        Process a batch of mixed raw lines (trades + depth snapshots).
        Depth lines update the direction. Trade lines validate the prediction.
        Returns the current direction state dict.
        """
        for line in raw_lines:
            if not line or len(line) < 3:
                continue
            tag = line[2]   # fast path: ["D"... or ["T"...

            if tag == 'D':
                snap = _parse_ob(line)
                if snap and snap['bids'] and snap['asks']:
                    result = _compute_snapshot(snap['bids'], snap['asks'])
                    result['ts'] = snap['ts']
                    self._snap_scores.append(result['score'])
                    self._smooth_score = sum(self._snap_scores) / len(self._snap_scores)
                    if self._smooth_score > CONF_THRESH:
                        self._last_direction = 'LONG'
                    elif self._smooth_score < -CONF_THRESH:
                        self._last_direction = 'SHORT'
                    else:
                        self._last_direction = 'NEUTRAL'
                    self._last_snap = result
                    self._pending_pred = self._last_direction

            elif tag == 'T':
                trade = _parse_trade(line)
                if trade and self._pending_pred and self._pending_pred != 'NEUTRAL':
                    trade_dir = 'LONG' if trade['side'] == 1 else 'SHORT'
                    self._trade_total += 1
                    if trade_dir == self._pending_pred:
                        self._trade_correct += 1

        accuracy = (self._trade_correct / self._trade_total
                    if self._trade_total > 0 else None)
        snap = self._last_snap

        return {
            'exchange':      self.exchange,
            'direction':     self._last_direction,
            'confidence':    round(min(abs(self._smooth_score) / 0.5, 1.0), 3),
            'score':         round(self._smooth_score, 4),
            'mid':           snap.get('mid'),
            'spread':        snap.get('spread'),
            'spread_bps':    snap.get('spread_bps'),
            'obi_l5':        snap.get('obi_l5'),
            'near_imbal':    snap.get('near_imbal'),
            'notional_imbal':snap.get('notional_imbal'),
            'l1_pressure':   snap.get('l1_pressure'),
            'bid_near_usd':  snap.get('bid_near_usd'),
            'ask_near_usd':  snap.get('ask_near_usd'),
            'bid_notional':  snap.get('bid_notional'),
            'ask_notional':  snap.get('ask_notional'),
            'accuracy':      round(accuracy, 3) if accuracy is not None else None,
            'trades_scored': self._trade_total,
        }


def analyze_lines(raw_lines: list, exchange: str = 'binance_us') -> dict:
    """One-shot: create tracker, feed all lines, return final state."""
    tracker = OBDirection(exchange=exchange)
    return tracker.update(raw_lines)
