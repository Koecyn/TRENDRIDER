#!/usr/bin/env python3
"""
Phase-predictive v5.1 — decelerating velocity IS the signal.
Decreasing |vel| at trough = trough forming. No direction flip required.

ENTRY FORMING  : at trough + |vel| decreasing this bar
ENTRY BUILDING : 2+ consecutive bars of decel at trough + score ok
ENTRY NOW      : 3+ bars decel OR sub already turned, score ok
"""
import json, subprocess, time, sys, datetime
from pathlib import Path

STATE = Path('/tmp/tr_v5_state.json')
REARM_AT = 1680

def get_stats():
    subprocess.run(
        ['git','fetch','origin','data/live'],
        capture_output=True, cwd='/home/user/TRENDRIDER'
    )
    r = subprocess.run(
        ['git','show','origin/data/live:physics_live_results.json'],
        capture_output=True, text=True, cwd='/home/user/TRENDRIDER'
    )
    if r.returncode != 0: return None
    return json.loads(r.stdout).get('stats', {})

def load_state():
    try: return json.loads(STATE.read_text())
    except: return {}

def save_state(st): STATE.write_text(json.dumps(st))

start     = time.time()
last_bar  = -1
last_price = 0.0
prev      = load_state()

while True:
    if time.time() - start >= REARM_AT:
        print('REARM_NOW', flush=True); sys.exit(0)

    s = get_stats()
    if not s: time.sleep(8); continue

    bar   = s.get('bars_live', 0)
    tgt_p = s.get('wf_tgt_primary', 0.0)
    c_ph  = s.get('wf_carrier_phase', 0.0)
    pf    = (1.0 - c_ph) / 2.0
    c_amp_raw = s.get('wf_carrier_amp', 0.0)
    cur_price = tgt_p - c_amp_raw * pf if pf > 1e-6 else tgt_p

    # Emit on new bar OR meaningful price/phase change (>=$5 move or phase shift >0.03)
    price_moved  = abs(cur_price - last_price) >= 5.0
    new_bar      = (bar != last_bar)
    if not new_bar and not price_moved:
        time.sleep(8); continue

    last_bar   = bar
    last_price = cur_price

    c_ph  = s.get('wf_carrier_phase', 0.0)
    c_dir = s.get('wf_carrier_direction', 0)
    c_vel = s.get('wf_carrier_velocity', 0.0)
    c_amp = s.get('wf_carrier_amp', 0.0)

    sh_ph  = s.get('wf_subharm_phase', 0.0)
    sh_dir = s.get('wf_subharm_direction', 0)
    sh_vel = s.get('wf_subharm_velocity', 0.0)
    sh_amp = s.get('wf_subharm_amp', 0.0)

    mc_ph  = s.get('wf_macro_phase', 0.0)
    mc_amp = s.get('wf_macro_amp', 0.0)
    mc_dir = s.get('wf_macro_direction', 0)

    score  = s.get('last_score', 0.0)
    thresh = s.get('threshold', 0.12)
    tier   = s.get('last_tier', 5)
    align  = s.get('res_alignment', 0.0)
    dis    = s.get('res_dissonance', False)
    at_sup = s.get('htf_at_sup', False)
    rev    = s.get('htf_reversal', False)
    fk     = s.get('htf_fk', False)
    cav    = s.get('last_cav_active', False)
    snr    = s.get('last_snr', 0.0)

    # Fast 1s wave stats — leading indicator, leads 1m bar close by up to 59s
    f_sub_ph  = s.get('fast_sub_phase',     0.0)
    f_sub_dir = s.get('fast_sub_dir',       0)
    f_sub_vel = s.get('fast_sub_vel',       0.0)
    f_sub_amp = s.get('fast_sub_amp',       0.0)
    f_c_ph    = s.get('fast_carrier_phase', 0.0)
    f_c_dir   = s.get('fast_carrier_dir',   0)
    f_mc_ph   = s.get('fast_macro_phase',   0.0)
    f_mc_dir  = s.get('fast_macro_dir',     0)
    # Fast sub turning positive at 1s resolution = entry forming before 1m bar sees it
    fast_sub_rising  = f_sub_dir > 0 and f_sub_amp > 0.5
    fast_sub_falling = f_sub_dir < 0 and f_sub_amp > 0.5
    fast_at_trough   = f_c_ph <= -0.75 and f_sub_amp > 0.5

    # Wall behavior — filled = real supply consumed, pulled = fake/cancelled
    ob_ask_fill    = s.get('ob_ask_fill', 0.0)
    ob_ask_pull    = s.get('ob_ask_pull', 0.0)
    ob_bid_fill    = s.get('ob_bid_fill', 0.0)
    ob_bid_pull    = s.get('ob_bid_pull', 0.0)
    ob_ask_cleared = s.get('ob_ask_cleared', False)
    ob_fill_ratio  = s.get('ob_fill_ratio', 0.0)

    tf  = {k: s.get(f'res_tf_{k}', 0.0) for k in ['1m','5m','15m','1h','4h']}
    htf = {k: s.get(f'htf_trend_{k}', '?') for k in ['1m','5m','15m','1h','4h']}

    # Previous state — on cold start, assume prior vel was 10% higher magnitude
    # so decel is visible immediately rather than showing 0% on first bar
    if 'c_vel' in prev:
        pc_vel = prev['c_vel']
    else:
        pc_vel = c_vel * 1.10 if abs(c_vel) > 0.1 else c_vel - 1.0
    if 'sh_vel' in prev:
        psh_vel = prev['sh_vel']
    else:
        psh_vel = sh_vel * 1.10 if abs(sh_vel) > 0.1 else sh_vel - 1.0
    psh_dir        = prev.get('sh_dir', sh_dir)
    c_decel_bars   = prev.get('c_decel_bars', 0)
    sh_decel_bars  = prev.get('sh_decel_bars', 0)
    trough_bars    = prev.get('trough_bars', 0)
    prev_score     = prev.get('score', score)
    peak_bars      = prev.get('peak_bars', 0)
    score_neg_bars = prev.get('score_neg_bars', 0)   # consecutive bars score < 0 at peak
    score_trend    = prev.get('score_trend', 0.0)    # rolling score delta (smoothed)

    # ── Core: is momentum DECREASING at trough? ──────────────────────────
    at_trough   = c_ph <= -0.75
    at_peak     = c_ph >= +0.75

    # Deceleration: |vel| smaller than previous bar = momentum exhausting
    # Decel: any velocity decrease meaningful relative to the band's own amplitude.
    # c_amp/sh_amp is the natural scale — a drop > 0.5% of amplitude per bar is signal.
    c_deceling  = abs(pc_vel) > 0.01 and abs(c_vel) < abs(pc_vel) and (abs(pc_vel) - abs(c_vel)) > c_amp * 0.005
    sh_deceling = abs(psh_vel) > 0.01 and abs(sh_vel) < abs(psh_vel) and (abs(psh_vel) - abs(sh_vel)) > sh_amp * 0.005

    # Re-acceleration: meaningfully faster than prior bar (>0.5% amplitude increase)
    c_reaccel  = abs(c_vel)  > abs(pc_vel)  + c_amp  * 0.005
    sh_reaccel = abs(sh_vel) > abs(psh_vel) + sh_amp * 0.005

    # Decel bar count: increment on decel, hold on flat, reset only on re-accel
    c_decel_bars  = (c_decel_bars  + 1) if (at_trough and c_deceling) else (0 if c_reaccel  else c_decel_bars)
    sh_decel_bars = (sh_decel_bars + 1) if sh_deceling                else (0 if sh_reaccel else sh_decel_bars)

    # Decel rate as % of prior velocity
    c_dp  = int((abs(pc_vel) - abs(c_vel)) / max(abs(pc_vel), 0.01) * 100)
    sh_dp = int((abs(psh_vel) - abs(sh_vel)) / max(abs(psh_vel), 0.01) * 100)

    sh_turned   = sh_dir > 0 and psh_dir <= 0
    sh_rising   = sh_dir > 0
    trough_bars = (trough_bars + 1) if at_trough else 0
    peak_bars   = (peak_bars + 1) if at_peak else 0

    # Score trend: smoothed delta — detects slow bleed before price moves
    score_trend    = score_trend * 0.7 + (score - prev_score) * 0.3
    # Count consecutive bars where score is negative while at peak
    score_neg_bars = (score_neg_bars + 1) if (at_peak and score < 0) else 0

    # ── Peak exhaustion: carrier ceiling + macro full + score bleeding ────
    mc_head_val  = mc_amp * (1.0 - mc_ph) / 2.0
    # Double-ceiling: both carrier AND macro pinned at top simultaneously
    double_ceiling = at_peak and mc_ph >= 0.95 and mc_amp > 50
    peak_exhaust = (
        at_peak and
        peak_bars >= 2 and               # lowered: double-ceiling needs fewer bars
        mc_head_val < c_amp * 0.15 and   # macro headroom < 15% of carrier amp
        (score_trend < -0.005 or double_ceiling)   # OR double ceiling overrides
    )
    # Stronger exhaustion: score has been negative + align degrading OR double-ceiling
    peak_exhaust_strong = peak_exhaust and (
        score_neg_bars >= 2 or align < 0.70 or double_ceiling
    )

    # ── OB fake market detection ─────────────────────────────────────────
    # fill_ratio < 20% = walls are almost entirely fake (being pulled not filled)
    # Downgrade signal confidence when market is dominated by spoofers
    fake_market = ob_fill_ratio < 0.20 and (ob_ask_pull + ob_bid_pull) > 0.1

    # ── ATR asymmetry from HTF bias ───────────────────────────────────────
    # HTF trend doesn't gate entries — it splits the ATR range.
    # bias=+0.5 → 70% up / 30% down.  bias=-0.5 → 30% up / 70% down.
    # High ATR + downtrend = big intrabar moves with upward bounces still tradeable.
    htf_bias   = s.get('htf_bias', 0.0)
    atr        = max(c_amp, 1.0)
    up_frac    = max(0.10, min(0.90, 0.50 + htf_bias * 0.40))
    dn_frac    = 1.0 - up_frac
    atr_up     = atr * up_frac    # expected upside range given HTF
    atr_dn     = atr * dn_frac    # expected downside range given HTF
    # R ratio: upside available vs downside risk for a long at current phase
    r_ratio    = atr_up / max(atr_dn, 0.01)
    # Minimum profit threshold: must exceed 0.002% taker fee (round trip ~0.004%)
    min_profit = price * 0.00004 if 'price' in dir() else 1.0   # calc after price below

    # ── Targets — asymmetric based on HTF split ───────────────────────────
    tgt_p  = s.get('wf_tgt_primary', 0.0)
    pf     = (1.0 - c_ph) / 2.0
    price  = tgt_p - c_amp * pf if pf > 1e-6 else tgt_p
    trough = price - (c_ph + 1) / 2.0 * c_amp
    peak   = trough + c_amp

    min_profit = price * 0.00004   # 0.004% round-trip fee floor
    # Targets sized to ATR upside fraction — not full ATR
    t_small = max(atr_up * 0.40, min_profit)   # sub-harmonic partial: 40% of up-range
    t_med   = max(atr_up * 0.70, min_profit)   # carrier partial:      70% of up-range
    t_large = max(atr_up,        min_profit)   # full upside allocation

    # ── Dynamic stop distance — collared by HTF bias ──────────────────────
    # Downtrend (bias < 0): tight collar — ATR can snap back hard, cut fast
    # Uptrend (bias > 0): wider stop — pullbacks are shallower, give room
    # stop_mult: 0.25 (bias=-1, tight) → 0.90 (bias=+1, wide)
    stop_mult   = 0.25 + (htf_bias + 1.0) / 2.0 * 0.65   # maps [-1,+1] → [0.25, 0.90]
    stop_dist   = atr_dn * stop_mult                        # $ distance below entry
    stop_pct    = stop_dist / max(price, 1.0) * 100        # % of price

    # ── Size tier — driven by R ratio + structural conditions ─────────────
    MIN_TARGET = 40.0   # below $40 upside = not worth the fee risk, data still shown
    if fk or cav or peak_exhaust_strong:
        size = 'SKIP'
    elif atr_up < MIN_TARGET:
        size = 'SKIP'   # insufficient upside range regardless of direction
    elif r_ratio < 0.50:
        size = 'SKIP'   # downside risk > 2× upside
    elif fake_market and r_ratio < 1.0:
        size = 'SKIP'   # spoof market + unfavorable ratio
    elif fake_market:
        size = 'SMALL'  # spoof market but ratio ok — cap at small
    elif r_ratio >= 2.0 and align >= 0.60 and not dis:
        size = 'LARGE'
    elif r_ratio >= 1.2 and align >= 0.45:
        size = 'MEDIUM'
    elif r_ratio >= 0.80:
        size = 'SMALL'
    else:
        size = 'SKIP'

    score_delta  = score - prev_score

    # ── Wall behavior classification ──────────────────────────────────────
    # PULLED > FILLED → fake wall, market maker cancelled before price got there → path open
    # FILLED > PULLED → real supply consumed by takers → genuine resistance
    # ob_ask_cleared → large ask yanked instantly (≥50% of level) → strong upside signal
    ask_total   = ob_ask_fill + ob_ask_pull
    bid_total   = ob_bid_fill + ob_bid_pull
    ask_fake    = ask_total > 0.02 and ob_ask_pull > ob_ask_fill * 1.5  # pulled > 1.5× filled
    ask_real    = ask_total > 0.02 and ob_ask_fill > ob_ask_pull * 1.5  # filled > 1.5× pulled
    bid_fake    = bid_total > 0.02 and ob_bid_pull > ob_bid_fill * 1.5
    bid_real    = bid_total > 0.02 and ob_bid_fill > ob_bid_pull * 1.5

    # At trough: fake ask wall = score drag is noise, path is actually open
    wall_fake   = ask_fake or ob_ask_cleared or (
        at_trough and c_decel_bars >= 2 and
        score < 0 and prev_score > 0 and
        score_delta < -(thresh * 1.5) and sh_rising
    )
    # Build wall annotation
    if ob_ask_cleared:
        wall_note = f' [ASK-CLEARED path open]'
    elif ask_fake:
        wall_note = f' [ask-pulled {ob_ask_pull:.3f}BTC fake]'
    elif ask_real and at_trough:
        wall_note = f' [ask-FILLED {ob_ask_fill:.3f}BTC real supply]'
    elif ask_fake is False and wall_fake:
        wall_note = f' [wall-abs score={score:+.3f}]'
    else:
        wall_note = ''

    # Peak wall note: pulled bid = support fake (bearish), filled ask = bids absorbing (bullish)
    if at_peak:
        if bid_fake:
            peak_wall_note = f' [bid-pulled {ob_bid_pull:.3f}BTC support fake → down]'
        elif bid_real:
            peak_wall_note = f' [bid-FILLED {ob_bid_fill:.3f}BTC real selling]'
        elif ob_ask_fill > 0.02 and not ask_fake:
            peak_wall_note = f' [ask-FILLED {ob_ask_fill:.3f}BTC bids absorbing]'
        else:
            peak_wall_note = ''
    else:
        peak_wall_note = ''

    # ── Signal: wave mechanics fire the signal, score/tier/align size it ──
    # score_ok gates NOTHING — it annotates conviction level only
    score_tag = f'score={score:+.3f}' if abs(score) >= thresh * 0.5 else f'score={score:+.3f}(low)'

    # Triple-wave trough: carrier + sub + macro all near trough simultaneously
    triple_trough = at_trough and mc_ph <= -0.75 and sh_ph <= -0.75

    # Fast 1s sub leading signal — fires before 1m bar sub flips
    fast_lead = fast_sub_rising and (at_trough or fast_at_trough)
    fast_lead_tag = f'  [1s-sub rising ph={f_sub_ph:+.2f}]' if fast_lead else ''

    if fk or cav:
        sig = 'AVOID — FK/CAV'
    elif fast_lead and not sh_rising and not at_trough:
        # 1s sub turning positive before 1m bar reaches trough — early warning
        sig = f'* ENTRY FORMING — 1s sub leading, 1m trough approaching  {score_tag}'
    elif triple_trough and sh_rising:
        sig = f'**** ENTRY — TRIPLE TROUGH carrier+sub+macro  sub rising  {score_tag}{wall_note}'
    elif triple_trough and (c_deceling or c_decel_bars >= 1):
        sig = f'**** ENTRY — TRIPLE TROUGH carrier+sub+macro  c_decel={c_dp}%  {score_tag}{wall_note}'
    elif triple_trough:
        sig = f'*** ENTRY — TRIPLE TROUGH all waves at bottom  {score_tag}{wall_note}'
    elif at_trough and sh_rising and (c_deceling or c_decel_bars >= 1):
        sig = f'*** ENTRY — sub rising + carrier decel {c_dp}%  {score_tag}{wall_note}'
    elif at_trough and sh_rising:
        sig = f'*** ENTRY — sub rising at trough  {score_tag}{wall_note}'
    elif at_trough and c_decel_bars >= 2:
        sig = f'** ENTRY — carrier decel {c_decel_bars} bars at trough  {score_tag}{wall_note}'
    elif at_trough and (sh_decel_bars >= 2):
        sig = f'** ENTRY — sub decel {sh_decel_bars} bars at trough  {score_tag}{wall_note}'
    elif at_trough and c_deceling:
        sig = f'* ENTRY FORMING — carrier decel {c_dp}% at trough  {score_tag}'
    elif at_trough and sh_deceling:
        sig = f'* ENTRY FORMING — sub decel {sh_dp}% at trough  {score_tag}'
    elif at_trough:
        sig = f'AT TROUGH — {score_tag}'
    elif at_peak:
        # Peak absorption analysis: score + velocity + sub + real wall behavior
        peak_abs = abs(score) < thresh * 0.5    # near-zero score = balanced
        sub_falling = sh_dir < 0                # sub confirms top
        sub_rising_at_peak = sh_dir > 0         # sub diverging = possible continuation
        # bid_real at peak = real sellers hitting the bid = confirmed supply
        # ask_fake at peak = ask walls being pulled before price reaches = bids absorbing
        if peak_exhaust_strong:
            peak_mode = f'EXHAUSTION — {peak_bars}bars ceiling  mc_full  score_trend={score_trend:+.4f}  DOWNSIDE BIAS'
        elif peak_exhaust:
            peak_mode = f'EXHAUSTION — {peak_bars}bars ceiling  mc_full  trend bleeding  {score_tag}'
        elif score < -thresh and sub_falling and bid_real:
            peak_mode = f'SMACKDOWN — supply confirmed bid-fill={ob_bid_fill:.3f}BTC  {score_tag}'
        elif score < -thresh and sub_falling:
            peak_mode = f'SMACKDOWN — supply heavy, sub confirms  {score_tag}'
        elif score < -thresh:
            peak_mode = f'REVERSAL — supply heavy  {score_tag}'
        elif score > thresh and ask_fake:
            peak_mode = f'BREAKOUT — asks being pulled, bids absorbing  {score_tag}'
        elif score > thresh and not sub_falling:
            peak_mode = f'BREAKOUT WATCH — bids absorbing at peak  {score_tag}'
        elif score > 0 and c_deceling:
            peak_mode = f'STALL+ABSORB — positive score, vel fading  {score_tag}'
        elif peak_abs and c_deceling:
            peak_mode = f'SLOW WALK — balanced, vel fading  {score_tag}'
        elif sub_rising_at_peak:
            peak_mode = f'DIVERGE — sub still rising at carrier peak  {score_tag}'
        else:
            peak_mode = f'PEAK — {score_tag}'
        sig = f'AT PEAK — {peak_mode}{peak_wall_note}'
    else:
        sig = 'NEUTRAL'

    flags = []
    if at_sup: flags.append('AT_SUP')
    if rev:    flags.append('REV')
    flag_str = ' '.join(flags)

    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'[{ts}] bar={bar}  {size}  {flag_str}', flush=True)
    print(f'{sig}', flush=True)
    print(f'carrier: ph={c_ph:+.2f}  dir={c_dir:+d}  amp=${c_amp:.0f}  vel={c_vel:+.2f}  decel={c_dp}%  ({c_decel_bars}bars)  [trough {trough_bars}bars]', flush=True)
    print(f'subharm: ph={sh_ph:+.2f}  dir={sh_dir:+d}  amp=${sh_amp:.0f}  vel={sh_vel:+.2f}  decel={sh_dp}%  ({sh_decel_bars}bars)', flush=True)
    print(f'macro:   ph={mc_ph:+.2f}  dir={mc_dir:+d}  amp=${mc_amp:.0f}  headroom=${mc_amp*(1-mc_ph)/2:.0f}  (4h only)', flush=True)
    if f_sub_amp > 0.5:
        print(f'fast1s:  sub ph={f_sub_ph:+.2f} dir={f_sub_dir:+d} amp=${f_sub_amp:.0f}  car ph={f_c_ph:+.2f} dir={f_c_dir:+d}  mac ph={f_mc_ph:+.2f} dir={f_mc_dir:+d}{fast_lead_tag}', flush=True)
    print(f'price~${price:.0f}  trough~${trough:.0f}  peak~${peak:.0f}', flush=True)
    print(f'targets: SMALL +${t_small:.0f}  MEDIUM +${t_med:.0f}  LARGE +${t_large:.0f}'
          f'  |  atr=${atr:.0f} up=${atr_up:.0f}({up_frac:.0%}) dn=${atr_dn:.0f}({dn_frac:.0%})'
          f'  R={r_ratio:.2f}  bias={htf_bias:+.2f}', flush=True)
    print(f'stop:    -${stop_dist:.0f} ({stop_pct:.2f}%)  [mult={stop_mult:.2f} '
          f'{"TIGHT" if htf_bias < -0.2 else "WIDE" if htf_bias > 0.2 else "NEUTRAL"}]', flush=True)
    tf_str = ' '.join(f'{k}={v:+.2f}' for k,v in tf.items())
    print(f'waves: {tf_str}  align={align:.2f} {"DIS" if dis else ""}', flush=True)
    # Wall behavior line — only print when there's meaningful OB activity
    if ask_total > 0.005 or bid_total > 0.005 or ob_ask_cleared:
        cleared_str = '  ASK-CLEARED' if ob_ask_cleared else ''
        ask_str = f'ask fill={ob_ask_fill:.3f} pull={ob_ask_pull:.3f}' if ask_total > 0.005 else ''
        bid_str = f'  bid fill={ob_bid_fill:.3f} pull={ob_bid_pull:.3f}' if bid_total > 0.005 else ''
        wall_class = ('FAKE' if ask_fake else 'REAL' if ask_real else 'MIX') if ask_total > 0.005 else ''
        fake_tag = '  [FAKE-MARKET]' if fake_market else ''
        print(f'OB: {ask_str}{bid_str}  ratio={ob_fill_ratio:.0%}real  [{wall_class}]{cleared_str}{fake_tag}', flush=True)
    score_delta  = score - prev_score
    score_mom    = score_delta / max(abs(thresh), 0.01)  # delta in units of threshold
    mom_str      = f'{score_mom:+.1f}x' if abs(score_mom) >= 0.1 else '~0'
    # Recovery flag: score bouncing back after absorption event
    recovering   = at_trough and score_delta > thresh * 0.5 and score < thresh
    rec_str      = '  [RECOVERING]' if recovering else ''
    mc_head      = mc_amp * (1 - mc_ph) / 2
    dbl_tag      = '  [DOUBLE-CEILING]' if double_ceiling else ''
    print(f'score={score:+.3f}  Δ={score_delta:+.3f}({mom_str}thresh)  tier={tier}  snr={snr:.0f}  mc_head=${mc_head:.0f}{rec_str}{dbl_tag}  htf:{htf["1m"]}/{htf["5m"]}/{htf["15m"]}/{htf["1h"]}/{htf["4h"]}', flush=True)
    print('---', flush=True)

    prev = {
        'c_vel': c_vel, 'c_dir': c_dir, 'c_decel_bars': c_decel_bars,
        'sh_vel': sh_vel, 'sh_dir': sh_dir, 'sh_decel_bars': sh_decel_bars,
        'score': score, 'bar': bar, 'trough_bars': trough_bars,
        'peak_bars': peak_bars, 'score_neg_bars': score_neg_bars,
        'score_trend': score_trend,
    }
    save_state(prev)
    time.sleep(8)
