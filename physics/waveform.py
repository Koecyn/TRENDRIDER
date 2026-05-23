"""
physics/waveform.py — Multi-scale wave decomposition and targeting.

Decomposes the 1m price series into four frequency bands using
Savitzky-Golay smoothing at increasing scales:

  micro    (W=5)   → 1–5 bar swings    — noise/tick structure
  subharm  (W=11)  → 5–20 bar swings   — sub-harmonic wave
  carrier  (W=21)  → 15–45 bar swings  — carrier wave
  macro    (W=61)  → 40–100+ bar swings — dominant macro wave

Per band:
  amplitude  — peak-to-peak price distance over the last window (natural swing size)
  phase      — where price sits in the cycle: -1=trough, 0=mid, +1=peak
  direction  — wave slope: +1 up, -1 down, 0 flat
  velocity   — bars-per-unit rate of change (signed)

Interference:
  constructive  — all aligned, amplitudes sum → big move
  destructive   — macro opposes sub-harmonics → stay flat
  partial       — mixed

Targeting:
  Wave amplitude is the natural price target distance.
  At trough (phase=-1) with direction=+1: expect a move ≈ amplitude.
  In full resonance: expect carrier + subharm amplitudes combined.
  In macro resonance: full amplitude sum including macro.

This replaces arbitrary ATR multipliers with physically-derived distances.
"""

import numpy as np
from .filters import sgf


# ── Band definitions ──────────────────────────────────────────────────────────

BANDS = {
    'micro':   {'window': 5,  'min_bars': 15,  'weight': 0.05},
    'subharm': {'window': 11, 'min_bars': 25,  'weight': 0.20},
    'carrier': {'window': 21, 'min_bars': 35,  'weight': 0.35},
    'macro':   {'window': 61, 'min_bars': 75,  'weight': 0.40},
}

_BAND_ORDER = ['micro', 'subharm', 'carrier', 'macro']


# ── Core decomposition ────────────────────────────────────────────────────────

def _extract(prices: np.ndarray, window: int, min_bars: int) -> dict:
    """
    Extract one wave band via SGF smoothing.
    Returns amplitude, phase, direction, velocity — all from the smoothed series.
    """
    null = {'amplitude': 0.0, 'phase': 0.0, 'direction': 0, 'velocity': 0.0,
            'hi': 0.0, 'lo': 0.0, 'current': 0.0}
    if len(prices) < min_bars:
        return null

    clean   = sgf(prices, window)
    # Use last 2×window bars for amplitude measurement — captures recent swing
    lookback = min(window * 2, len(clean))
    recent   = clean[-lookback:]

    hi = float(np.max(recent))
    lo = float(np.min(recent))
    amplitude = hi - lo

    if amplitude < 1e-6:
        return {**null, 'current': float(clean[-1])}

    # Phase: -1 = at trough, +1 = at peak, 0 = midpoint
    phase = (float(clean[-1]) - lo) / amplitude * 2.0 - 1.0
    phase = float(np.clip(phase, -1.0, 1.0))

    # Direction: slope over last 3 smoothed bars
    if len(clean) >= 4:
        velocity  = float(clean[-1] - clean[-4]) / 3.0
        direction = int(np.sign(velocity)) if abs(velocity) > amplitude * 0.01 else 0
    else:
        velocity  = 0.0
        direction = 0

    return {
        'amplitude': round(float(amplitude), 4),
        'phase':     round(float(phase), 3),
        'direction': direction,
        'velocity':  round(float(velocity), 6),
        'hi':        round(hi, 4),
        'lo':        round(lo, 4),
        'current':   round(float(clean[-1]), 4),
    }


def decompose(prices: np.ndarray) -> dict:
    """
    Decompose price series into four wave bands.
    Returns dict keyed by band name, each containing amplitude/phase/direction/velocity.
    """
    return {
        band: _extract(prices, cfg['window'], cfg['min_bars'])
        for band, cfg in BANDS.items()
    }


# ── Interference ──────────────────────────────────────────────────────────────

def interference(components: dict) -> dict:
    """
    Compute wave interference across bands.

    Constructive: weighted majority of bands aligned → amplitudes reinforce.
    Destructive:  macro opposes sub-harmonics → flat or choppy.
    Partial:      some alignment, reduced amplitude.

    Returns:
      type          'constructive' | 'destructive' | 'partial'
      score         float -1..+1  (weighted direction sum)
      direction     int +1/-1/0   dominant direction
      dominant      str  band with highest amplitude×weight
      aligned       list of aligned band names
      opposing      list of opposing band names
      amplitude_sum float  sum of aligned band amplitudes (= natural target distance)
      carrier_amp   float  carrier wave amplitude alone
      macro_amp     float  macro wave amplitude alone
    """
    score      = 0.0
    total_w    = 0.0
    dominant   = 'carrier'
    dom_score  = 0.0

    for band in _BAND_ORDER:
        c = components.get(band, {})
        w = BANDS[band]['weight']
        d = c.get('direction', 0)
        a = c.get('amplitude', 0.0)
        if a < 1e-6:
            continue
        score   += d * w
        total_w += w
        if a * w > dom_score:
            dom_score = a * w
            dominant  = band

    if total_w > 0:
        score /= total_w

    direction = int(np.sign(score)) if abs(score) > 0.05 else 0

    aligned  = []
    opposing = []
    for band in _BAND_ORDER:
        c = components.get(band, {})
        if c.get('amplitude', 0) < 1e-6:
            continue
        d = c.get('direction', 0)
        if d == direction and direction != 0:
            aligned.append(band)
        elif d != 0 and d != direction:
            opposing.append(band)

    # Amplitude sum of aligned bands — this is the natural target distance
    amp_sum = sum(
        components[b]['amplitude']
        for b in aligned
        if b in components and components[b]['amplitude'] > 1e-6
    )

    # Macro opposes majority → destructive
    macro_dir  = components.get('macro',   {}).get('direction', 0)
    carrier_dir= components.get('carrier', {}).get('direction', 0)

    if direction != 0 and macro_dir != 0 and macro_dir != direction:
        itype = 'destructive'
    elif len(aligned) >= 3:
        itype = 'constructive'
    else:
        itype = 'partial'

    return {
        'type':          itype,
        'score':         round(float(score), 4),
        'direction':     direction,
        'dominant':      dominant,
        'aligned':       aligned,
        'opposing':      opposing,
        'amplitude_sum': round(float(amp_sum), 4),
        'carrier_amp':   round(float(components.get('carrier', {}).get('amplitude', 0)), 4),
        'macro_amp':     round(float(components.get('macro',   {}).get('amplitude', 0)), 4),
        'subharm_amp':   round(float(components.get('subharm', {}).get('amplitude', 0)), 4),
    }


# ── Targeting ─────────────────────────────────────────────────────────────────

def project_target(entry: float, direction: int,
                   components: dict, itf: dict) -> dict:
    """
    Project price targets from wave structure.

    Primary target:   entry ± carrier amplitude
                      (the wave you're entering on will complete its swing)

    Extended target:  entry ± (carrier + subharm) amplitudes
                      (sub-harmonic adds to the carrier move)

    Resonance target: entry ± amplitude_sum of ALL aligned bands
                      (full constructive interference — all waves add)

    Phase adjustment: if entering near the trough (phase < -0.3), there's
                      more room before the peak → use full amplitude.
                      If entering at midpoint (phase ≈ 0), use half amplitude.

    Returns: primary, extended, resonance, confidence
    """
    if entry <= 0 or direction == 0:
        return {'primary': 0.0, 'extended': 0.0, 'resonance': 0.0, 'confidence': 0.0}

    carrier = components.get('carrier', {})
    subharm = components.get('subharm', {})
    macro   = components.get('macro',   {})

    c_amp   = carrier.get('amplitude', 0.0)
    s_amp   = subharm.get('amplitude', 0.0)
    m_amp   = macro.get('amplitude',   0.0)
    c_phase = carrier.get('phase', 0.0)

    # Phase factor: how much of the amplitude remains
    # At trough (phase=-1, going long): full amplitude available
    # At midpoint (phase=0): half amplitude
    # At peak (phase=+1): zero remaining
    if direction == 1:
        phase_factor = float(np.clip((1.0 - c_phase) / 2.0, 0.1, 1.0))
    else:
        phase_factor = float(np.clip((1.0 + c_phase) / 2.0, 0.1, 1.0))

    primary   = entry + direction * c_amp * phase_factor
    extended  = entry + direction * (c_amp + s_amp * 0.5) * phase_factor
    resonance = entry + direction * itf.get('amplitude_sum', c_amp) * phase_factor

    # Confidence: phase position + alignment quality
    phase_conf = phase_factor                           # best at trough entry
    align_conf = len(itf.get('aligned', [])) / 4.0     # 4 bands max
    confidence = round(float(np.clip((phase_conf + align_conf) / 2.0, 0.0, 1.0)), 3)

    return {
        'primary':    round(float(primary),   4),
        'extended':   round(float(extended),  4),
        'resonance':  round(float(resonance), 4),
        'confidence': confidence,
        'phase_factor': round(phase_factor, 3),
        'c_amp':      round(c_amp, 4),
        's_amp':      round(s_amp, 4),
        'm_amp':      round(m_amp, 4),
    }


# ── Full pipeline ─────────────────────────────────────────────────────────────

def run(prices: np.ndarray, entry: float = None,
        direction: int = 1) -> dict:
    """
    Full waveform analysis.
    Returns components, interference, and (if entry given) targets.
    """
    comp = decompose(prices)
    itf  = interference(comp)

    result = {
        'components':    comp,
        'interference':  itf,
    }

    if entry and entry > 0:
        result['targets'] = project_target(entry, direction, comp, itf)

    return result
