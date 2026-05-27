"""
8 physics signals — vectorized rolling implementations.

All scalar functions (mass_flow, kdv_soliton, etc.) remain for compatibility
with fusion.run() point-in-time calls.

Rolling batch functions (_rolling_*) compute the same indicator across
the entire price series in one numpy pass — orders-of-magnitude faster
for full-session scans.

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
# Rolling numpy helpers
# ─────────────────────────────────────────────────────────────────────────────

def _rolling_mean(a, w):
    out = np.empty(len(a)); out[:] = np.nan
    cs  = np.cumsum(a)
    out[w-1:] = (cs[w-1:] - np.concatenate([[0], cs[:-w]])) / w
    return out

def _rolling_max(a, w):
    from numpy.lib.stride_tricks import sliding_window_view
    out = np.empty(len(a)); out[:w-1] = np.nan
    out[w-1:] = sliding_window_view(a, w).max(axis=1)
    return out

def _rolling_min(a, w):
    from numpy.lib.stride_tricks import sliding_window_view
    out = np.empty(len(a)); out[:w-1] = np.nan
    out[w-1:] = sliding_window_view(a, w).min(axis=1)
    return out

def _rolling_rms(a, w):
    from numpy.lib.stride_tricks import sliding_window_view
    out = np.empty(len(a)); out[:w-1] = np.nan
    out[w-1:] = np.sqrt(np.mean(sliding_window_view(a**2, w), axis=1))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# ROLLING BATCH PHYSICS  (call once on full series, returns arrays)
# ─────────────────────────────────────────────────────────────────────────────

def rolling_physics(prices: np.ndarray, closes: np.ndarray,
                    opens: np.ndarray, volumes: np.ndarray,
                    taker_buy: np.ndarray) -> dict:
    """
    Compute all non-OB physics indicators as rolling arrays in one pass.
    prices  = (closes + opens) / 2
    Returns dict of 1-D arrays, same length as input.
    NaN at positions where window not yet full.
    """
    n = len(prices)
    clean = sgf(prices)   # SGF applied ONCE to full series

    # ── KdV soliton ──────────────────────────────────────────────────────────
    w_sol = C.SOLITON_WIN
    from numpy.lib.stride_tricks import sliding_window_view
    kdv_bal  = np.full(n, np.nan)
    kdv_amp  = np.full(n, np.nan)
    kdv_dir  = np.zeros(n, dtype=int)
    kdv_det  = np.zeros(n, dtype=bool)
    if n >= w_sol + 5:
        wins = sliding_window_view(clean, w_sol)        # shape (n-w+1, w)
        means = wins.mean(axis=1)
        u     = wins - means[:, None]
        diffs = np.diff(wins, axis=1)                   # (n-w+1, w-1)
        nonlin = C.KDV_ALPHA * u[:, 1:] * diffs
        d3u   = np.diff(u, n=3, axis=1)
        min_len = min(nonlin.shape[1], d3u.shape[1])
        nlm   = np.sqrt(np.mean(nonlin[:, :min_len]**2, axis=1)) + 1e-10
        dm    = np.sqrt(np.mean(d3u[:, :min_len]**2,   axis=1)) + 1e-10
        bal   = nlm / dm
        amp   = np.abs(u).max(axis=1)
        det   = (bal > C.SOLITON_BAL) & (amp > means * 0.0005) & (nlm > 0.01)
        idx   = w_sol - 1 + np.arange(len(det))
        kdv_bal[idx] = bal
        kdv_amp[idx] = amp
        kdv_det[idx] = det
        kdv_dir[idx] = np.where(det, np.sign(u[:, -1]).astype(int), 0)

    # ── Reynolds ─────────────────────────────────────────────────────────────
    w_re   = C.RE_WINDOW
    re_val = np.full(n, 0.0)
    re_reg = np.full(n, 'transitional', dtype=object)
    re_mul = np.full(n, 1.0)
    if n >= w_re:
        ps_cl  = sliding_window_view(clean, w_re)
        vs_w   = sliding_window_view(volumes, w_re)
        vel    = np.abs(ps_cl[:, -1] - ps_cl[:, 0]) / w_re
        rho    = vs_w.mean(axis=1) / 1e5
        L      = ps_cl.max(axis=1) - ps_cl.min(axis=1) + 1e-3
        mu     = C.VISC_BASE * (1.0 + 1.0 / (vel + 0.01))
        Re     = (rho * vel * L) / mu
        mul    = np.where(Re < C.RE_LAMINAR, 0.5,
                 np.where(Re > C.RE_TURBULENT, 1.2, 1.0))
        idx    = w_re - 1 + np.arange(len(Re))
        re_val[idx] = Re
        re_mul[idx] = mul
        re_reg[idx] = np.where(Re < C.RE_LAMINAR, 'laminar',
                      np.where(Re > C.RE_TURBULENT, 'turbulent', 'transitional'))

    # ── Shock front ──────────────────────────────────────────────────────────
    w_sh    = C.SHOCK_WINDOW
    sh_det  = np.zeros(n, dtype=bool)
    sh_mul  = np.full(n, 1.0)
    sh_dir  = np.zeros(n, dtype=int)
    if n >= w_sh:
        ps_cl  = sliding_window_view(clean, w_sh)
        vs_w   = sliding_window_view(volumes, w_sh)
        vel    = np.abs(ps_cl[:, -1] - ps_cl[:, 0]) / w_sh
        avg_v  = vs_w.mean(axis=1)
        c_s    = (avg_v / 1e5)**0.5 * 0.1 + 1e-3
        mach   = vel / c_s
        det    = mach > C.MACH_SHOCK
        move   = np.sign(ps_cl[:, -1] - ps_cl[:, 0]).astype(int)
        idx    = w_sh - 1 + np.arange(len(det))
        sh_det[idx] = det
        sh_mul[idx] = np.where(det, -0.5, 1.0)
        sh_dir[idx] = np.where(det, -move, 0)

    # ── Mass flow ─────────────────────────────────────────────────────────────
    w_mf    = C.MASS_WINDOW
    mf_rate = np.full(n, 0.0)
    mf_dir  = np.zeros(n, dtype=int)
    if n >= w_mf:
        ps_w   = sliding_window_view(prices, w_mf)
        vs_w   = sliding_window_view(volumes, w_mf)
        vel    = (ps_w[:, -1] - ps_w[:, 0]) / w_mf
        dens   = vs_w.mean(axis=1)
        area   = ps_w.max(axis=1) - ps_w.min(axis=1) + 1e-4
        idx    = w_mf - 1 + np.arange(len(vel))
        mf_rate[idx] = dens * area * np.abs(vel)
        mf_dir[idx]  = np.sign(vel).astype(int)

    # ── Iceberg ──────────────────────────────────────────────────────────────
    ice_det  = np.zeros(n, dtype=bool)
    ice_dir  = np.zeros(n, dtype=int)
    if n >= 6:
        avg_v6   = _rolling_mean(volumes, 6)
        with np.errstate(divide='ignore', invalid='ignore'):
            vol_spike = np.where(avg_v6 > 0, volumes / avg_v6, 0.0)
        pmove    = np.abs(closes - opens)
        with np.errstate(divide='ignore', invalid='ignore'):
            ppct     = pmove / (closes * 0.001 + 1e-6)
        obstruct = vol_spike / (ppct + 1.0)
        ice_det  = obstruct > 3.0
        ice_dir  = np.where(closes >= opens, 1, -1)

    # ── Cavitation ────────────────────────────────────────────────────────────
    cav_act = np.zeros(n, dtype=bool)
    cav_mul = np.full(n, 1.0)
    if n >= 9:
        avg_v8   = _rolling_mean(volumes, 8)
        vel1     = np.abs(np.diff(prices, prepend=prices[0]))
        P_local  = 0.5 * C.RHO * vel1**2
        near_liq = avg_v8 * 0.1  # OB not available in rolling; use vol proxy
        P_vapor  = avg_v8 * 0.1 / (near_liq + 1.0)
        act      = (P_local > P_vapor * 2.0) & (near_liq < avg_v8 * 0.05)
        cav_act  = act
        cav_mul  = np.where(act, 0.2, 1.0)

    # ── Water hammer (OB-free detection from wick pattern) ────────────────────
    wh_det = np.zeros(n, dtype=bool)
    wh_dir = np.zeros(n, dtype=int)
    if n >= 6:
        avg_v6   = _rolling_mean(volumes, 6)
        p_prev3  = np.roll(prices, 3); p_prev5 = np.roll(prices, 5)
        prev_move = prices - p_prev3
        cur_move  = p_prev3 - p_prev5
        with np.errstate(divide='ignore', invalid='ignore'):
            v_spike  = np.where(avg_v6 > 0, np.roll(volumes, 2) / avg_v6, 0.0) > 2.0
        reversed_ = (prev_move > 0) != (cur_move > 0)
        wh_det   = reversed_ & v_spike
        wh_dir   = np.sign(cur_move).astype(int)
        wh_det[:5] = False  # not enough history

    return {
        'kdv_det':  kdv_det,   'kdv_dir': kdv_dir,
        'kdv_bal':  kdv_bal,   'kdv_amp': kdv_amp,
        're_val':   re_val,    're_reg':  re_reg,   're_mul': re_mul,
        'sh_det':   sh_det,    'sh_dir':  sh_dir,   'sh_mul': sh_mul,
        'mf_rate':  mf_rate,   'mf_dir':  mf_dir,
        'ice_det':  ice_det,   'ice_dir': ice_dir,
        'cav_act':  cav_act,   'cav_mul': cav_mul,
        'wh_det':   wh_det,    'wh_dir':  wh_dir,
        'clean':    clean,
    }


def rolling_score(ph: dict, obi_arr: np.ndarray,
                  taker_buy: np.ndarray, volumes: np.ndarray) -> np.ndarray:
    """
    Fuse rolling physics arrays into a score array using the same
    weights as fusion.run().  OBI contribution uses the rolling OBI array.
    HydraulicAccumulator is excluded (stateful; handle separately).
    """
    kdv = np.where(ph['kdv_det'], ph['kdv_dir'] * 0.35, 0.0)
    ice = np.where(ph['ice_det'], ph['ice_dir'] * 0.15, 0.0)

    physics = (kdv + ice) * ph['re_mul']

    # Shock: override or multiply
    sh_override = ph['sh_det']
    physics = np.where(sh_override,
                       ph['sh_dir'] * 0.30,
                       physics * ph['sh_mul'])
    physics *= ph['cav_mul']

    # OBI micro layer (capped ±0.15)
    obi_contrib = np.clip(obi_arr * 0.6, -0.15, 0.15)

    score = physics * 0.6 + obi_contrib * 0.4
    return score


# ─────────────────────────────────────────────────────────────────────────────
# POINT-IN-TIME SCALAR FUNCTIONS (unchanged — used by fusion.run())
# ─────────────────────────────────────────────────────────────────────────────

def mass_flow(prices: np.ndarray, volumes: np.ndarray) -> dict:
    w = C.MASS_WINDOW
    if len(prices) < w or len(volumes) < w:
        return {'rate': 0.0, 'direction': 0}
    ps = prices[-w:]; vs = volumes[-w:]
    velocity = (ps[-1] - ps[0]) / w
    density  = float(np.mean(vs))
    area     = float(np.max(ps) - np.min(ps)) + 1e-4
    rate     = density * area * abs(velocity)
    return {'rate': rate, 'direction': int(np.sign(velocity))}


def darcy(prices: np.ndarray, volumes: np.ndarray,
          bids: list = None, asks: list = None) -> dict:
    if len(prices) < 2:
        return {'Q': 0.1, 'friction': 10.0, 'direction': 0}
    velocity = abs(prices[-1] - prices[-2])
    if bids and asks:
        bid_d = sum(q for _, q in bids)
        ask_d = sum(q for _, q in asks)
    else:
        w       = min(C.MASS_WINDOW, len(prices))
        avg_vol = float(np.mean(volumes[-w:]))
        p_range = float(np.max(prices[-w:]) - np.min(prices[-w:])) + 1e-4
        impact  = p_range / (prices[-1] * 0.001 + 1e-6)
        depth   = avg_vol / (impact + 1) / 1000
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
    dirn    = int(np.sign(bid_d - ask_d))
    return {'Q': float(Q), 'friction': float(fric), 'direction': dirn}


def kdv_soliton(prices: np.ndarray) -> dict:
    w = C.SOLITON_WIN
    if len(prices) < w + 5:
        return {'detected': False, 'direction': 0, 'balance': 0.0, 'amplitude': 0.0}
    clean  = sgf(prices[-w:])
    mean_p = float(np.mean(clean))
    u      = clean - mean_p
    diffs  = np.diff(clean)
    if len(diffs) < 3 or len(u) < 4:
        return {'detected': False, 'direction': 0, 'balance': 0.0, 'amplitude': 0.0}
    nonlin = C.KDV_ALPHA * u[1:] * diffs
    d3u    = np.diff(u, n=3)
    min_len = min(len(nonlin), len(d3u))
    nlm = float(np.sqrt(np.mean(nonlin[:min_len] ** 2))) + 1e-10
    dm  = float(np.sqrt(np.mean(d3u[:min_len]  ** 2))) + 1e-10
    balance   = nlm / dm
    amplitude = float(np.max(np.abs(u)))
    detected  = balance > C.SOLITON_BAL and amplitude > mean_p * 0.0005 and nlm > 0.01
    direction = int(np.sign(u[-1])) if detected else 0
    return {'detected': detected, 'direction': direction,
            'balance': float(balance), 'amplitude': float(amplitude)}


def iceberg(closes: np.ndarray, opens: np.ndarray, volumes: np.ndarray) -> dict:
    if len(volumes) < 6:
        return {'detected': False, 'direction': 0, 'obstruction': 0.0}
    avg_vol    = float(np.mean(volumes[-6:-1])) + 1e-6
    vol_spike  = volumes[-1] / avg_vol
    price_move = abs(closes[-1] - opens[-1])
    price_pct  = price_move / (closes[-1] * 0.001 + 1e-6)
    obstruct   = vol_spike / (price_pct + 1.0)
    detected   = obstruct > 3.0
    direction  = +1 if closes[-1] >= opens[-1] else -1
    return {'detected': detected, 'direction': direction, 'obstruction': float(obstruct)}


def water_hammer(prices: np.ndarray, volumes: np.ndarray,
                 asks: list = None, bids: list = None) -> dict:
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
        prev_move = prices[-3] - prices[-5] if len(prices) >= 5 else 0.0
        cur_move  = prices[-1] - prices[-3]
        reversed_ = (prev_move > 0) != (cur_move > 0)
        avg_vol   = float(np.mean(volumes[-6:-2])) + 1e-6
        vol_spike = volumes[-2] / avg_vol > 2.0
        if reversed_ and vol_spike:
            strength  = abs(prev_move) * volumes[-2] / avg_vol
            direction = int(np.sign(cur_move))
            return {'detected': True, 'direction': direction, 'strength': float(strength)}
    return {'detected': False, 'direction': 0, 'strength': 0.0}


class HydraulicAccumulator:
    def __init__(self):
        self.stored   = 0.0
        self.last_dir = 0

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
            release       = self.stored
            self.stored  *= C.DARK_PARTIAL_KEEP
            direction     = +1 if closes[-1] >= opens[-1] else -1
            self.last_dir = direction
            return {'firing': True, 'direction': direction, 'pressure': float(release)}
        return {'firing': False, 'direction': 0, 'pressure': float(self.stored)}

    def reset(self):
        self.stored   = 0.0
        self.last_dir = 0


def reynolds(prices: np.ndarray, volumes: np.ndarray) -> dict:
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
        regime, mult = 'laminar', 0.5
    elif Re > C.RE_TURBULENT:
        regime, mult = 'turbulent', 1.2
    else:
        regime, mult = 'transitional', 1.0
    return {'re': float(Re), 'regime': regime, 'multiplier': mult}


def shock_front(prices: np.ndarray, volumes: np.ndarray) -> dict:
    w = C.SHOCK_WINDOW
    if len(prices) < w or len(volumes) < w:
        return {'detected': False, 'mach': 0.0, 'direction': 0, 'multiplier': 1.0}
    ps       = sgf(prices[-w:])
    velocity = abs(float(ps[-1] - ps[0])) / w
    avg_vol  = float(np.mean(volumes[-w:]))
    c_s      = (avg_vol / 1e5) ** 0.5 * 0.1 + 1e-3
    mach     = velocity / c_s
    detected = mach > C.MACH_SHOCK
    move_dir = int(np.sign(float(ps[-1] - ps[0])))
    fade_dir = -move_dir if detected else 0
    mult     = -0.5 if detected else 1.0
    return {'detected': detected, 'mach': float(mach),
            'direction': fade_dir, 'multiplier': mult}


def cavitation(prices: np.ndarray, volumes: np.ndarray,
               bids: list = None, asks: list = None) -> dict:
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
        impact   = velocity / (prices[-1] * 1e-4 + 1e-10)
        near_liq = avg_vol * 0.1 / (impact + 1.0)
    P_vapor  = avg_vol * 0.1 / (near_liq + 1.0)
    risk     = min(P_local / (P_vapor + 1e-10) / 10.0, 1.0)
    active   = (P_local > P_vapor * 2.0) and (near_liq < avg_vol * 0.05)
    mult     = 0.2 if active else 1.0
    return {'active': active, 'risk': float(risk), 'multiplier': mult}
