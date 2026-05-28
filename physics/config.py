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
SOLITON_BAL  = 0.70      # min nonlinearity/dispersion ratio for confirmation
SOLITON_WIN  = 20       # bars in soliton window

# ── Reynolds Number ───────────────────────────────────────────────────────────
RE_LAMINAR   = 50       # below → reversal bias (score × 0.5)
RE_TURBULENT = 200      # above → trend amplifier (score × 1.2)
RE_WINDOW    = 10
VISC_BASE    = 0.0082     # base viscosity (tune to typical spread)

# ── Shock Front ───────────────────────────────────────────────────────────────
MACH_SHOCK   = 2000.0   # Mach threshold — fade anything above this (calibrated for eff_usd inputs)
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
CVD_DIV         = 0.55     # min CVD divergence magnitude to count (ceiling — don't exceed)
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
MAE_ATR_FALLBACK    = 2.0   # ATR multiplier for fallback stops (2× = 1:1 R:R for tier3)
MAE_ATR_PERIOD      = 60   # longer period captures daily range, not just the coil

# ── Higher timeframe context ──────────────────────────────────────────────────
HTF_LEVEL_TOL          = 0.003   # ±0.3% of price = "at level"
HTF_REVERSAL_MIN_SCORE = 0.06    # score floor for reversal entries (half of normal)
HTF_1M_BARS            = 500     # 1m bars used from live window (no REST fetch needed)
HTF_5M_BARS            = 288     # 5m bars to fetch at startup (24h)
HTF_15M_BARS           = 96      # 15m bars to fetch at startup (24h)
HTF_1H_BARS            = 48      # 1h bars to fetch at startup (2 days)
HTF_4H_BARS            = 30      # 4h bars to fetch at startup (5 days)

# ── Multi-TF Wave Resonance ───────────────────────────────────────────────────
LARGE_TRADE_BTC     = 0.50   # minimum BTC size to flag as a large print

# ── Multi-TF Wave Resonance ───────────────────────────────────────────────────
RES_ALIGN_THRESH    = 0.75   # min alignment for resonance size boost + wider target
RES_FULL_TARGET_ATR = 5.0    # target ATR mult when all TFs aligned (vs tier default)
RES_SIZE_BOOST      = 1.50   # size multiplier at full resonance (alignment ≥ thresh)
RES_SIZE_REDUCE     = 0.70   # size multiplier at dissonance or low alignment (< 0.35)

# ── Signal quality gates ──────────────────────────────────────────────────────
MIN_BANDS_TO_SIGNAL = 2      # minimum active bands before signal is valid
KDV_MIN_BALANCE     = 4.0    # minimum KdV nonlinearity/dispersion ratio to count

# ── Per-TF profitability gates ────────────────────────────────────────────────
MIN_PROFIT_USD = 20.0        # minimum profitable move ($) — constant across all TFs
ATR_WINDOW     = 14          # bars for ATR + direction-ratio computation

# ── Data / backtest ───────────────────────────────────────────────────────────
SYMBOL        = 'BTCUSDT'
INTERVAL      = '1m'
HISTORY_BARS  = 5_000       # total bars to pull for backtest
WARMUP_BARS   = 100         # bars consumed before first trade allowed
MAX_HOLD_BARS = 90          # timeout backstop (bars) — 90m max hold
INITIAL_BAL   = 1_000.0     # starting balance for backtest ($)
