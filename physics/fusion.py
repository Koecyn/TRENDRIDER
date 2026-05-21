"""
Signal fusion engine.

Physics layer  60%:  KdV(0.35) + DarkPool(0.25) + Iceberg(0.15)
                     × Reynolds multiplier × Shock multiplier × Cavitation mute

Micro layer    40%:  OBI(max 0.15) + CVD divergence(0.5)

Final score > +0.12  → LONG
Final score < −0.12  → SHORT

Conviction tier classification (1–5) gates position size.
"""

import numpy as np
from . import config as C
from . import signals as S
from . import microstructure as M
from .filters import snr as compute_snr


def run(prices: np.ndarray,
        opens:  np.ndarray,
        closes: np.ndarray,
        volumes: np.ndarray,
        taker_buy: np.ndarray,
        accum: S.HydraulicAccumulator,
        bids: list = None,
        asks: list = None) -> dict:
    """
    Compute all signals and fuse into final score + conviction tier.

    Returns full breakdown dict for logging and optimizer analysis.
    """
    # ── Physics signals ───────────────────────────────────────────────────────
    mf   = S.mass_flow(prices, volumes)
    d    = S.darcy(prices, volumes, bids, asks)
    sol  = S.kdv_soliton(prices)
    ice  = S.iceberg(closes, opens, volumes)
    wh   = S.water_hammer(prices, volumes, asks, bids)
    acc  = accum.update(opens, closes, volumes, d['friction'])
    re   = S.reynolds(prices, volumes)
    sh   = S.shock_front(prices, volumes)
    cav  = S.cavitation(prices, volumes, bids, asks)

    # ── Microstructure signals ────────────────────────────────────────────────
    cvd     = M.compute_cvd(taker_buy, volumes)
    cvd_sig = M.cvd_signal(prices, cvd)
    obi_sig = M.obi_signal(bids, asks, d['Q'], taker_buy, volumes)

    # ── Physics layer (60%) ───────────────────────────────────────────────────
    physics = 0.0

    # KdV soliton: highest weight (±0.35)
    if sol['detected']:
        physics += sol['direction'] * 0.35

    # Dark pool accumulator (±0.25)
    if acc['firing']:
        physics += acc['direction'] * 0.25

    # Iceberg pressure (±0.15) — direction = expected break direction
    if ice['detected']:
        physics += ice['direction'] * 0.15

    # Reynolds regime multiplier
    physics *= re['multiplier']

    # Shock front: override to fade signal, halved
    if sh['detected']:
        physics = sh['direction'] * 0.30   # fade overrides everything else
    else:
        physics *= sh['multiplier']        # = 1.0 when no shock

    # Cavitation: mute all signals (×0.2)
    physics *= cav['multiplier']

    # ── Microstructure layer (40%) ────────────────────────────────────────────
    micro = 0.0

    # OBI (max ±0.15, gated by Darcy Q)
    micro += obi_sig['contribution']

    # CVD divergence (±0.5 × weight when strong)
    if cvd_sig['strong']:
        micro += -cvd_sig['divergence'] * 0.5  # negative div = bullish for price

    # ── Final score ───────────────────────────────────────────────────────────
    score     = physics * 0.6 + micro * 0.4
    direction = int(np.sign(score)) if abs(score) >= C.LONG_THRESHOLD else 0
    confidence = float(min(abs(score), 1.0))
    tier       = _classify_tier(sol, wh, acc, re, ice, cvd_sig, obi_sig, score, cav)
    sig_snr    = float(compute_snr(prices))

    return {
        # Top-level decision
        'score':      float(score),
        'direction':  direction,
        'confidence': confidence,
        'tier':       tier,
        # Layers
        'physics':    float(physics),
        'micro':      float(micro),
        # Individual signals
        'soliton':    sol,
        'water_hammer': wh,
        'accum':      acc,
        'iceberg':    ice,
        'reynolds':   re,
        'shock':      sh,
        'cavitation': cav,
        'darcy':      d,
        'mass_flow':  mf,
        'cvd':        cvd_sig,
        'obi':        obi_sig,
        # Context
        'snr':        sig_snr,
    }


def _classify_tier(sol, wh, acc, re, ice, cvd_sig, obi_sig, score, cav) -> int:
    """
    Tier 1 — Mechanical    (water hammer, 78–88% target)
    Tier 2 — High conviction (soliton+turbulent OR dark pool+CVD)
    Tier 3 — Standard      (2+ signals aligned)
    Tier 4 — Low conviction (single signal)
    Tier 5 — Stand aside   (cavitation, shock alone, too noisy)
    """
    # Tier 5: dangerous conditions
    if cav['active']:
        return 5

    has_signal = abs(score) >= C.LONG_THRESHOLD

    # Tier 1: water hammer — mechanically forced reflection
    if wh['detected'] and has_signal:
        return 1

    # Tier 2: soliton + turbulent regime
    if sol['detected'] and re['regime'] == 'turbulent' and has_signal:
        return 2

    # Tier 2: dark pool + CVD confirmation + iceberg cleared
    if acc['firing'] and cvd_sig['strong'] and ice['detected'] and has_signal:
        return 2

    # Tier 3: two or more signals converge
    n_aligned = sum([
        1 if sol['detected']      else 0,
        1 if acc['firing']        else 0,
        1 if ice['detected']      else 0,
        1 if obi_sig['direction'] != 0 else 0,
        1 if cvd_sig['strong']    else 0,
    ])
    if n_aligned >= 2 and has_signal:
        return 3

    # Tier 4: single signal
    if has_signal:
        return 4

    # Tier 5: nothing meaningful
    return 5
