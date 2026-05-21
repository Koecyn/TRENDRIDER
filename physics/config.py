"""
All tunable parameters for the physics engine.
The optimizer writes new values here between runs.
"""

# ── Savitzky-Golay noise filter ───────────────────────────────────────────────
SGF_WINDOW   = 9        # must be odd; larger = smoother
SGF_POLYORD  = 2        # polynomial order (2 = quadratic)

# ── KdV Soliton ───────────────────────────────────────────────────────────────
KDV_ALPHA    = 0.1      # nonlinearity — increase for stronger trends
KDV_BETA     = 0.01     # dispersion — increase for noisier instruments
SOLITON_BAL  = 0.92      # min nonlinearity/dispersion ratio for confirmation
SOLITON_WIN  = 20       # bars in soliton window

# ── Reynolds Number ───────────────────────────────────────────────────────────
RE_LAMINAR   = 50       # below → reversal bias (score × 0.5)
RE_TURBULENT = 200      # above → trend amplifier (score × 1.2)
RE_WINDOW    = 10
VISC_BASE    = 0.02     # base viscosity (tune to typical spread)

# ── Shock Front ───────────────────────────────────────────────────────────────
MACH_SHOCK   = 1.0      # Mach threshold — fade anything above this
SHOCK_WINDOW = 8

# ── Darcy's Law ───────────────────────────────────────────────────────────────
DARCY_K_BASE    = 0.5   # base book permeability
DARCY_THRESHOLD = 500   # depth normalization constant

# ── Mass Flow ─────────────────────────────────────────────────────────────────
MASS_WINDOW  = 10
RHO          = 1.0      # density normalization

# ── Hydraulic Accumulator (dark pool) ────────────────────────────────────────
DARK_CHARGE_SCALE = 50_000  # normalization divisor for dark vol estimate
DARK_PARTIAL_KEEP = 0.30    # fraction of pressure remaining after discharge

# ── Signal fusion thresholds ─────────────────────────────────────────────────
LONG_THRESHOLD  = +0.12
SHORT_THRESHOLD = -0.12
OBI_SIGNAL      = 0.25      # min OBI magnitude to count
CVD_DIV         = 0.45     # min CVD divergence magnitude to count (ceiling — don't exceed)
CVD_WINDOW      = 10        # bars for CVD slope comparison
SNR_MIN         = 2.0       # minimum SGF SNR to trade

# ── Conviction tiers ──────────────────────────────────────────────────────────
TIER1_WH_STRENGTH = 2.0    # water hammer strength threshold for Tier 1
TIER2_SOL_BAL     = 0.50    # soliton balance required for Tier 2
TIER2_DARK_THRESH = 0.70    # dark pool pressure for Tier 2 override

# ── R:R targets per tier ──────────────────────────────────────────────────────
TIER1_RR = 2.5
TIER2_RR = 3.0
TIER3_RR = 2.0
TIER4_RR = 0.0              # skip

# ── Fixed ATR targets (decoupled from stop distance) ───────────────────────────
TIER1_TARGET_ATR = 3.5
TIER2_TARGET_ATR = 3.0
TIER3_TARGET_ATR = 2.5     # 2.5×ATR target, 3×ATR stop → positive EV at ~57% win

# ── Fractional Kelly position sizing ─────────────────────────────────────────
KELLY_FRACTION      = 0.50  # half-Kelly
KELLY_MIN           = 0.05  # floor: 5% of balance
KELLY_MAX           = 0.50  # cap: 50% of balance
KELLY_WARMUP_TRADES = 10    # trades before Kelly activates (use defaults before)

# ── MAE-based stop placement ──────────────────────────────────────────────────
MAE_PERCENTILE      = 90    # % of winner MAE → stop distance
MAE_WARMUP_TRADES   = 20    # trades before MAE activates (use ATR fallback)
MAE_ATR_FALLBACK    = 3.0   # ATR multiplier for fallback stops
MAE_ATR_PERIOD      = 14

# ── Data / backtest ───────────────────────────────────────────────────────────
SYMBOL        = 'BTCUSDC'
INTERVAL      = '1m'
HISTORY_BARS  = 5_000       # total bars to pull for backtest
WARMUP_BARS   = 100         # bars consumed before first trade allowed
MAX_HOLD_BARS = 120         # timeout backstop (bars)
INITIAL_BAL   = 1_000.0     # starting balance for backtest ($)
