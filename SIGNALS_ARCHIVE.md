# TRENDRIDER — Signal & Detector Archive

Living reference for every working detector in the system.
Each entry: what it detects, why it matters, the code, and when it fires.
Updated as new parts are built and verified against live data.

---

## 1. Wave Decomposition (physics/waveform.py + pipeline.py)

**What**: Decomposes the 1m price series into four frequency bands using
Savitzky-Golay smoothing (physics_live.py) or scipy Hilbert (pipeline.py).

| Band     | Period     | Meaning                              |
|----------|------------|--------------------------------------|
| micro    | 1–5 bars   | tick noise, ignored for sizing       |
| subharm  | 5–20 bars  | sub-harmonic, LEADS the carrier      |
| carrier  | 15–45 bars | primary tradeable wave               |
| macro    | 40–100+    | dominant cycle, sets the envelope    |

**Per band outputs**: amplitude ($), phase (-1=trough, +1=peak), direction (+1/-1/0), velocity.

**Why it matters**: phase tells you WHERE in the cycle price is. At carrier trough
(phase ≤ -0.75) with sub rising = textbook long entry. At carrier peak (phase ≥ +0.75)
with macro at ceiling = exhaustion, skip.

**Sub leads carrier by 1–2 bars.** Sub turning positive BEFORE carrier bottoms is the
earliest long signal in the system.

---

## 2. Peak Exhaustion Detector (trendrider_v5.py)

**What**: Detects when the carrier is pinned at the ceiling with no macro room left
and score is bleeding negative — price is about to roll over.

**Code**:
```python
mc_head_val  = mc_amp * (1.0 - mc_ph) / 2.0
double_ceiling = at_peak and mc_ph >= 0.95 and mc_amp > 50
peak_exhaust = (
    at_peak and peak_bars >= 2 and
    mc_head_val < c_amp * 0.15 and
    (score_trend < -0.005 or double_ceiling)
)
peak_exhaust_strong = peak_exhaust and (
    score_neg_bars >= 2 or align < 0.70 or double_ceiling
)
```

**When it fires**:
- `peak_exhaust`: carrier phase ≥ 0.75 for ≥ 2 bars, macro headroom < 15% of carrier
  amplitude, AND score bleeding negative OR double-ceiling
- `peak_exhaust_strong`: above PLUS score negative 2+ bars OR alignment < 0.70 OR
  double-ceiling active — forces size = SKIP

**Double-ceiling**: carrier_phase ≥ +1.0 AND macro_phase ≥ 0.95 simultaneously.
The macro wave (amplitude $300-500) AND the carrier ($50-150) both maxed out.
Zero structural room to go up. Strongest exhaustion condition.

**Live example (bar 102-105, May 2026)**:
carrier_ph=+1.0, macro_ph=+1.0, macro_amp=$362, mc_head=$0.
Both waves pinned. Price had been locked at $76,635 for 12 bars.
When it finally resolved: price snapped $53 in 1-2 bars to $76,688.

**Output tag**: `[DOUBLE-CEILING]` on score line, size forced to SKIP.

---

## 3. Fake Market / Spoof Detection (trendrider_v5.py)

**What**: Detects when order book walls are predominantly fake (being pulled before
price reaches them) rather than real supply being consumed.

**Code**:
```python
ob_fill_ratio  = s.get('ob_fill_ratio', 0.0)   # filled / (filled + pulled)
fake_market = ob_fill_ratio < 0.20 and (ob_ask_pull + ob_bid_pull) > 0.1
```

**Wall classification per bar**:
```python
ask_fake = ask_total > 0.02 and ob_ask_pull > ob_ask_fill * 1.5
ask_real = ask_total > 0.02 and ob_ask_fill > ob_ask_pull * 1.5
```

**When it fires**: fill_ratio < 20% AND total pulled > 0.1 BTC means 80%+ of wall
activity is cancellations, not real consumption. Market makers are spoofing both sides.

**What it means for trading**:
- Fake ask wall = resistance is not real, price can pass through
- Fake bid wall = support is not real, don't rely on it
- `ob_ask_cleared`: ≥50% of an ask level yanked instantly = path opened upside
- `ob_fill_ratio` 16% (live example) = near-total spoofing, only 1 in 6 walls real

**Effect on sizing**: fake_market caps size at SMALL if R-ratio ≥ 1.0, SKIP if R < 1.0.

**Output tag**: `[FAKE-MARKET]` on OB line, `[ASK-CLEARED path open]` on entry line.

---

## 4. ATR Asymmetry Splitter (trendrider_v5.py)

**What**: HTF trend direction splits the ATR range into expected up/down fractions
rather than blocking entries. A downtrend doesn't mean zero upside — it means a
smaller fraction of the range goes up.

**The insight**: ATR=$10, trend=down → $3 up / $7 down expected. You can still trade
the $3 upside. High ATR + downtrend = big intrabar moves with tradeable bounces.
You target $3, not $10. You don't skip.

**Code**:
```python
htf_bias   = s.get('htf_bias', 0.0)          # -1 to +1
atr        = max(c_amp, 1.0)                  # carrier amplitude = natural ATR
up_frac    = max(0.10, min(0.90, 0.50 + htf_bias * 0.40))
dn_frac    = 1.0 - up_frac
atr_up     = atr * up_frac
atr_dn     = atr * dn_frac
r_ratio    = atr_up / max(atr_dn, 0.01)
```

**Bias → split**:
| htf_bias | up_frac | example atr=$75 |
|----------|---------|-----------------|
| +1.0     | 90%     | $67.5 up / $7.5 dn |
| +0.5     | 70%     | $52.5 up / $22.5 dn |
| 0.0      | 50%     | $37.5 / $37.5 |
| -0.5     | 30%     | $22.5 up / $52.5 dn |
| -1.0     | 10%     | $7.5 up / $67.5 dn |

**Targets** (scaled to atr_up, not full ATR):
- SMALL:  atr_up × 0.40  (sub-harmonic partial)
- MEDIUM: atr_up × 0.70  (carrier partial)
- LARGE:  atr_up × 1.00  (full upside allocation)

**Minimum target floor**: $40. Below $40 upside = SKIP regardless of direction.
Data is still printed — nothing is hidden.

**Minimum profit**: price × 0.00004 (0.004% round-trip fee at maker-buy/taker-sell).
All targets enforced above this floor.

**Size tier by R ratio**:
| Condition                        | Size   |
|----------------------------------|--------|
| atr_up < $40                     | SKIP   |
| r_ratio < 0.50                   | SKIP   |
| fake_market AND r_ratio < 1.0    | SKIP   |
| fake_market                      | SMALL  |
| r_ratio ≥ 2.0 AND align ≥ 0.60  | LARGE  |
| r_ratio ≥ 1.2 AND align ≥ 0.45  | MEDIUM |
| r_ratio ≥ 0.80                   | SMALL  |
| else                             | SKIP   |

**Output line**: `atr=$75 up=$42(56%) dn=$33(44%)  R=1.27  bias=+0.15`

---

## 5. Entry Signals (trendrider_v5.py)

**Primary entry — carrier trough + sub rising**:
```python
at_trough  = c_ph <= -0.75
sh_rising  = sh_dir > 0
# ENTRY when: trough + sub has turned positive
```
Sub must flip positive (sh_dir > 0). Sub leads carrier by 1-2 bars so this fires
BEFORE the carrier bottoms out. The early sub turn is the signal; carrier trough
confirms the zone.

**Fast 1s leading entry** (pipeline.py only):
```python
fast_sub_rising = f_sub_dir > 0 and f_sub_amp > 0.5
fast_at_trough  = f_c_ph <= -0.75 and f_sub_amp > 0.5
fast_lead = fast_sub_rising and (at_trough or fast_at_trough)
```
Sub-harmonic on 1s bars turns positive up to 59s before the 1m bar closes.
This is the earliest possible signal — up to 1 bar ahead of the primary entry.

**Reversal override** (at HTF support):
```python
htf_at_sup = s.get('htf_at_sup', False)
htf_reversal = s.get('htf_reversal', False)
# Entry allowed even with weak score if at_support + reversal_setup
```

---

## 6. Wall Behavior Classification (physics_live.py → stats → trendrider_v5.py)

**Data flow**:
1. `physics_live.py` tracks order book depth snapshots per bar
2. When a level disappears: if price crossed it → FILL, if not → PULL
3. Exported as: `ob_ask_fill`, `ob_ask_pull`, `ob_bid_fill`, `ob_bid_pull`
4. `ob_ask_cleared`: True if ≥50% of an ask level yanked in one snapshot

**Interpretation**:
| Signal               | Meaning                                      |
|----------------------|----------------------------------------------|
| ask_fake             | Sell wall is fake — will be pulled not hit   |
| ask_real             | Real supply — takers are consuming it        |
| bid_fake             | Support is fake — don't rely on it           |
| bid_real             | Real demand — bids absorbing sell pressure   |
| ob_ask_cleared       | Fast yank of ask wall → path open upside     |
| ob_fill_ratio < 20%  | Market dominated by spoofers                 |

**Key behavioral insight**: A sell wall that gets PULLED before price arrives is not
resistance. It was placed to fake price direction. When it disappears, the path is open.
A wall that gets FILLED means real sellers — genuine resistance level.

---

## 7. Score Architecture

**score** (last_score): composite of resonance alignment + macro direction.
Range -1 to +1. Threshold = 0.12. Above threshold = bullish signal.

**score_trend**: smoothed EMA of score delta. Negative trend at peak = early warning
of exhaustion before score crosses zero.
```python
score_trend = score_trend * 0.7 + (score - prev_score) * 0.3
```

**score_neg_bars**: consecutive bars where score < 0 while at peak. ≥ 2 bars =
strong exhaustion confirmation.

**SNR** (last_snr): signal-to-noise ratio = carrier_amp / subharm_amp * 10.
High SNR (> 1000) means carrier is much larger than sub noise → clean wave structure.
Low SNR means choppy / indeterminate.

---

## 8. Git Push Architecture

**physics_live.py**: `_write_stats()` called every ~5s intrabar (waveform throttle).
Pushes directly: `git add results.json → git commit → git push HEAD:data/live`.
No branch switching. ~2-4s per push. Intrabar updates reach the cloud every 5-10s.

**pipeline.py**: WebSocket → 1s OHLCV → scipy Hilbert → `publish()` every 5s minimum.
Same simple push pattern. Also generates fast_sub/carrier on 1s bars (59s ahead of
bar close). Bootstraps 300 bars from Binance REST on cold start.

**data/live branch**: read-only from cloud side. Contains only `physics_live_results.json`.
Commit history = time series of every push.

---

## 10. Dynamic Stop — HTF-Collared Distance

**What**: Stop distance below entry scales with HTF bias. Downtrend = tight collar
to cut losses fast if the ATR snaps back down. Macro uptrend = wider stop because
pullbacks are shallower and you can give the trade more room.

**Code**:
```python
stop_mult = 0.25 + (htf_bias + 1.0) / 2.0 * 0.65   # [-1,+1] → [0.25, 0.90]
stop_dist = atr_dn * stop_mult                        # $ distance below entry
stop_pct  = stop_dist / price * 100                   # % of price
```

**Stop multiplier by bias**:
| htf_bias | stop_mult | label   | example atr_dn=$33 |
|----------|-----------|---------|---------------------|
| -1.0     | 0.25      | TIGHT   | stop = $8           |
| -0.5     | 0.41      | TIGHT   | stop = $14          |
|  0.0     | 0.575     | NEUTRAL | stop = $19          |
| +0.5     | 0.74      | WIDE    | stop = $24          |
| +1.0     | 0.90      | WIDE    | stop = $30          |

**Logic**: In a downtrend, if the bounce fails, it fails fast and hard. A tight collar
(-$8 on $75 ATR = 0.01%) gets you out before a full reversal develops.
In a macro uptrend, the pullback that triggers the stop is shallower and recovers —
a wider stop (-$24) doesn't cost you the trade on normal variance.

**The ATR asymmetry connection**: stop_dist uses `atr_dn` (the downside fraction of ATR),
so in a downtrend where atr_dn is already larger AND stop_mult is tighter, the combined
effect is: tight dollar stop in hostile conditions. In uptrend: wider stop on smaller
atr_dn = double protection.

**Output line**: `stop: -$14 (0.02%)  [mult=0.41 TIGHT]`

---

## 9. Constraints (NEVER VIOLATE)

- **Maker only on buys** — no taker fees on entry
- **Taker sell only if profit > 0.002% of position value** (round trip ~0.004%)
- **Flat cash, no margin, no shorts**
- **Minimum trade target: $40** — below this the fee math doesn't work at any size
- **strategy_downtrend.py = THE VAULT** (Sharpe 3.863) — never touch
- **physics/signals.py and physics/fusion.py** — never modify signal logic
- **physics/config.py** — only file for parameter tuning
- **OBSERVATION ONLY** — no live order execution without explicit authorization
