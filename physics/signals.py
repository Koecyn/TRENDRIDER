"""
8 physics signals — all operate on SGF-filtered prices.

  1. Mass Flow Rate      ṁ = ρ·A·v
  2. Darcy's Law         Q = −K·A·ΔP / μL
  3. KdV Soliton         ∂u/∂t + α·u·∂u/∂x + β·∂³u/∂x³ = 0
  4. Bernoulli / Water Hammer
  5. Hydraulic Accumulator  (stateful — one instance per session)
  6. Reynolds Number     Re = ρ·v·L / μ
  7. Shock Front         Ma = v / c_s
  8. Cavitation          P_local > P_vapor
"""

import numpy as np
from . import config as C
from .filters import sgf


# ─────────────────────────────────────────────────────────────────────────────
# 1. MASS FLOW RATE
# ─────────────────────────────────────────────────────────────────────────────

def mass_flow(prices: np.ndarray, volumes: np.ndarray) -> dict:
    """
    ṁ = ρ · A · v
    Returns: rate (magnitude), direction (+1 / -1)
    """
    w = C.MASS_WINDOW
    if len(prices) < w or len(volumes) < w:
        return {'rate': 0.0, 'direction': 0}
    ps = prices[-w:]
    vs = volumes[-w:]
    velocity  = (ps[-1] - ps[0]) / w
    density   = float(np.mean(vs))
    area      = float(np.max(ps) - np.min(ps)) + 1e-4
    rate      = density * area * abs(velocity)
    return {'rate': rate, 'direction': int(np.sign(velocity))}


# ─────────────────────────────────────────────────────────────────────────────
# 2. DARCY'S LAW
# ─────────────────────────────────────────────────────────────────────────────

def darcy(prices: np.ndarray, volumes: np.ndarray,
          bids: list = None, asks: list = None) -> dict:
    """
    Q = K · ΔP / (μ · L)
    friction = 1 / (Q + ε)

    Live: uses real order book bids/asks.
    Backtest: approximates book depth from OHLCV.
    Returns: Q (flow rate), friction, imbalance_direction
    """
    if len(prices) < 2:
        return {'Q': 0.1, 'friction': 10.0, 'direction': 0}

    velocity = abs(prices[-1] - prices[-2])

    if bids and asks:
        bid_d = sum(q for _, q in bids)
        ask_d = sum(q for _, q in asks)
    else:
        # Backtest approximation from volume and price impact
        w       = min(C.MASS_WINDOW, len(prices))
        avg_vol = float(np.mean(volumes[-w:]))
        p_range = float(np.max(prices[-w:]) - np.min(prices[-w:])) + 1e-4
        impact  = p_range / (prices[-1] * 0.001 + 1e-6)
        depth   = avg_vol / (impact + 1) / 1000
        # Approximate imbalance from price direction
        if prices[-1] > prices[-2]:
            bid_d, ask_d = depth * 1.2, depth * 0.8
        else:
            bid_d, ask_d = depth * 0.8, depth * 1.2

    L       = (bid_d + ask_d) / 2 + 1e-6
    delta_P = abs(bid_d - ask_d) / L
    mu      = C.VISC_BASE + 1.0 / (velocity + 0.01)
    K       = C.DARCY_K_BASE / (1.0 + L / C.DARCY_THRESHOLD)
    Q       = (K * delta_P) / (mu * max(L / C.DARCY_THRESHOLD, 0.01))
    fric    = 1.0 / (Q + 1e-3)
    dirn    = int(np.sign(bid_d - ask_d))  # +1 = bid-heavy = bullish flow
    return {'Q': float(Q), 'friction': float(fric), 'direction': dirn}


# ─────────────────────────────────────────────────────────────────────────────
# 3. KdV SOLITON
# ─────────────────────────────────────────────────────────────────────────────

def kdv_soliton(prices: np.ndarray) -> dict:
    """
    Detects self-reinforcing wave where nonlinearity cancels dispersion.
    Soliton UP  → confirmed breakout run — do NOT fade.
    Soliton DOWN → confirmed breakdown.
    Weight: 0.35 (highest in engine).
    """
    w = C.SOLITON_WIN
    if len(prices) < w + 5:
        return {'detected': False, 'direction': 0, 'balance': 0.0, 'amplitude': 0.0}

    clean  = sgf(prices[-w:])
    mean_p = float(np.mean(clean))
    u      = clean - mean_p
    diffs  = np.diff(clean)

    if len(diffs) < 3 or len(u) < 4:
        return {'detected': False, 'direction': 0, 'balance': 0.0, 'amplitude': 0.0}

    nonlin = C.KDV_ALPHA * u[1:] * diffs          # α·u·∂u/∂x
    d3u    = np.diff(u, n=3)                       # ∂³u/∂x³ (dispersion)

    min_len = min(len(nonlin), len(d3u))
    nlm = float(np.sqrt(np.mean(nonlin[:min_len] ** 2))) + 1e-10
    dm  = float(np.sqrt(np.mean(d3u[:min_len]  ** 2))) + 1e-10

    balance   = nlm / dm
    amplitude = float(np.max(np.abs(u)))

    detected = (
        balance   > C.SOLITON_BAL
        and amplitude > mean_p * 0.0005
        and nlm       > 0.01
    )
    direction = int(np.sign(u[-1])) if detected else 0

    return {
        'detected':  detected,
        'direction': direction,
        'balance':   float(balance),
        'amplitude': float(amplitude),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 4. BERNOULLI / WATER HAMMER + ICEBERG DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def iceberg(closes: np.ndarray, opens: np.ndarray, volumes: np.ndarray) -> dict:
    """
    High volume + minimal price move → hidden large order absorbing flow.
    Direction = break direction (iceberg will exhaust, price continues).
    """
    if len(volumes) < 6:
        return {'detected': False, 'direction': 0, 'obstruction': 0.0}

    avg_vol    = float(np.mean(volumes[-6:-1])) + 1e-6
    vol_spike  = volumes[-1] / avg_vol
    price_move = abs(closes[-1] - opens[-1])
    price_pct  = price_move / (closes[-1] * 0.001 + 1e-6)
    obstruct   = vol_spike / (price_pct + 1.0)

    detected  = obstruct > 3.0
    direction = +1 if closes[-1] >= opens[-1] else -1  # expect break in bar direction
    return {'detected': detected, 'direction': direction, 'obstruction': float(obstruct)}


def water_hammer(prices: np.ndarray, volumes: np.ndarray,
                 asks: list = None, bids: list = None) -> dict:
    """
    Price velocity hits thick wall → conservation of momentum forces reflection.
    Tier 1 signal: mechanical, 78–88% target win rate.
    Returned direction is the FADE direction (opposite of the move).
    """
    if len(prices) < 6 or len(volumes) < 6:
        return {'detected': False, 'direction': 0, 'strength': 0.0}

    if asks and bids:
        velocity    = abs(prices[-1] - prices[-3])
        hitting_ask = prices[-1] > prices[-3]
        wall_ask    = sum(q for _, q in asks[:3])
        wall_bid    = sum(q for _, q in bids[:3])

        if hitting_ask and wall_ask > 0:
            strength = velocity * wall_ask
            if strength > C.TIER1_WH_STRENGTH:
                return {'detected': True, 'direction': -1, 'strength': float(strength)}
        elif not hitting_ask and wall_bid > 0:
            strength = velocity * wall_bid
            if strength > C.TIER1_WH_STRENGTH:
                return {'detected': True, 'direction': +1, 'strength': float(strength)}
    else:
        # Backtest: detect from wick reversal pattern
        # Strong move bar[−3:−1] followed by sharp reversal bar[−1]
        prev_move = prices[-3] - prices[-5] if len(prices) >= 5 else 0.0
        cur_move  = prices[-1] - prices[-3]
        reversed_ = (prev_move > 0) != (cur_move > 0)      # direction flipped
        avg_vol   = float(np.mean(volumes[-6:-2])) + 1e-6
        vol_spike = volumes[-2] / avg_vol > 2.0             # volume surge on reversal bar

        if reversed_ and vol_spike:
            strength  = abs(prev_move) * volumes[-2] / avg_vol
            direction = int(np.sign(cur_move))
            return {'detected': True, 'direction': direction, 'strength': float(strength)}

    return {'detected': False, 'direction': 0, 'strength': 0.0}


# ─────────────────────────────────────────────────────────────────────────────
# 5. HYDRAULIC ACCUMULATOR  (stateful — instantiate once per session)
# ─────────────────────────────────────────────────────────────────────────────

class HydraulicAccumulator:
    """
    Tracks dark pool charge/discharge cycle.
    P = P_charge + V_stored / C_accum
    Must persist across bars — never recreate mid-session.
    """

    def __init__(self):
        self.stored    = 0.0
        self.last_dir  = 0

    def update(self, opens: np.ndarray, closes: np.ndarray,
               volumes: np.ndarray, darcy_friction: float) -> dict:
        if len(volumes) < 1:
            return {'firing': False, 'direction': 0, 'pressure': self.stored}

        price_move = abs(closes[-1] - opens[-1])
        vol        = volumes[-1]
        expected   = vol * 5e-6
        dark_est   = max(0.0, vol * (1.0 - price_move / (expected + 1e-10)))

        self.stored += dark_est / C.DARK_CHARGE_SCALE
        self.stored  = min(self.stored, 1.0)

        if self.stored > darcy_friction * 0.7:
            release        = self.stored
            self.stored   *= C.DARK_PARTIAL_KEEP
            direction      = +1 if closes[-1] >= opens[-1] else -1
            self.last_dir  = direction
            return {'firing': True, 'direction': direction, 'pressure': float(release)}

        return {'firing': False, 'direction': 0, 'pressure': float(self.stored)}

    def reset(self):
        self.stored   = 0.0
        self.last_dir = 0


# ─────────────────────────────────────────────────────────────────────────────
# 6. REYNOLDS NUMBER
# ─────────────────────────────────────────────────────────────────────────────

def reynolds(prices: np.ndarray, volumes: np.ndarray) -> dict:
    """
    Re = ρ·v·L / μ
    Gates all other signals — regime classifier.
    laminar < 50 < transitional < 200 < turbulent
    """
    w = C.RE_WINDOW
    if len(prices) < w or len(volumes) < w:
        return {'re': 0.0, 'regime': 'transitional', 'multiplier': 1.0}

    ps       = sgf(prices[-w:])
    vs       = volumes[-w:]
    velocity = abs(float(ps[-1] - ps[0])) / w
    rho      = float(np.mean(vs)) / 1e5
    L        = float(np.max(ps) - np.min(ps)) + 1e-3
    mu       = C.VISC_BASE * (1.0 + 1.0 / (velocity + 0.01))
    Re       = (rho * velocity * L) / mu

    if Re < C.RE_LAMINAR:
        regime, mult = 'laminar',      0.5
    elif Re > C.RE_TURBULENT:
        regime, mult = 'turbulent',    1.2
    else:
        regime, mult = 'transitional', 1.0

    return {'re': float(Re), 'regime': regime, 'multiplier': mult}


# ─────────────────────────────────────────────────────────────────────────────
# 7. SHOCK FRONT
# ─────────────────────────────────────────────────────────────────────────────

def shock_front(prices: np.ndarray, volumes: np.ndarray) -> dict:
    """
    Ma = v / c_s  (price velocity / book replenishment rate)
    Ma > MACH_SHOCK → price outruns the book → fade the move.
    Never chase a shock; it is already at terminal velocity.
    """
    w = C.SHOCK_WINDOW
    if len(prices) < w or len(volumes) < w:
        return {'detected': False, 'mach': 0.0, 'direction': 0, 'multiplier': 1.0}

    ps       = sgf(prices[-w:])
    velocity = abs(float(ps[-1] - ps[0])) / w
    avg_vol  = float(np.mean(volumes[-w:]))
    c_s      = (avg_vol / 1e5) ** 0.5 * 0.1 + 1e-3
    mach     = velocity / c_s
    detected = mach > C.MACH_SHOCK

    move_dir  = int(np.sign(float(ps[-1] - ps[0])))
    fade_dir  = -move_dir if detected else 0
    mult      = -0.5 if detected else 1.0   # reverse + halve all other signals

    return {
        'detected':   detected,
        'mach':       float(mach),
        'direction':  fade_dir,
        'multiplier': mult,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 8. CAVITATION
# ─────────────────────────────────────────────────────────────────────────────

def cavitation(prices: np.ndarray, volumes: np.ndarray,
               bids: list = None, asks: list = None) -> dict:
    """
    P_local > P_vapor → liquidity void.
    Stand aside — collapse direction is unpredictable.
    """
    if len(prices) < 9 or len(volumes) < 8:
        return {'active': False, 'risk': 0.0, 'multiplier': 1.0}

    avg_vol  = float(np.mean(volumes[-8:]))
    velocity = abs(float(prices[-1] - prices[-2]))
    P_local  = 0.5 * C.RHO * velocity ** 2

    if bids and asks:
        near_bid = sum(q for _, q in bids[:5])
        near_ask = sum(q for _, q in asks[:5])
        near_liq = (near_bid + near_ask) / 2 + 1.0
    else:
        # Backtest: estimate from volume / price-impact ratio
        impact   = velocity / (prices[-1] * 1e-4 + 1e-10)
        near_liq = avg_vol * 0.1 / (impact + 1.0)

    P_vapor   = avg_vol * 0.1 / (near_liq + 1.0)
    risk      = min(P_local / (P_vapor + 1e-10) / 10.0, 1.0)
    active    = (P_local > P_vapor * 2.0) and (near_liq < avg_vol * 0.05)
    mult      = 0.2 if active else 1.0

    return {'active': active, 'risk': float(risk), 'multiplier': mult}
