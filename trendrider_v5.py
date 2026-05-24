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
    psh_dir      = prev.get('sh_dir', sh_dir)
    c_decel_bars = prev.get('c_decel_bars', 0)
    sh_decel_bars= prev.get('sh_decel_bars', 0)
    prev_score   = prev.get('score', score)  # score last bar (default=current on cold start)

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

    sh_turned = sh_dir > 0 and psh_dir <= 0
    sh_rising = sh_dir > 0

    # ── Size tier ────────────────────────────────────────────────────────
    if fk or cav:
        size = 'SKIP'
    elif align >= 0.75 and not dis:
        size = 'LARGE'
    elif align >= 0.50:
        size = 'MEDIUM'
    elif align >= 0.35:
        size = 'SMALL'
    else:
        size = 'SKIP'

    # ── Targets ──────────────────────────────────────────────────────────
    t_small = sh_amp * 0.6
    t_med   = c_amp
    t_large = c_amp + sh_amp * 0.5

    tgt_p  = s.get('wf_tgt_primary', 0.0)
    pf     = (1.0 - c_ph) / 2.0
    price  = tgt_p - c_amp * pf if pf > 1e-6 else tgt_p
    trough = price - (c_ph + 1) / 2.0 * c_amp
    peak   = trough + c_amp

    score_delta  = score - prev_score
    # Wall absorption note: sudden score drop at trough while sub rising = fake ask wall
    wall_fake    = (
        at_trough and c_decel_bars >= 2 and
        score < 0 and prev_score > 0 and
        score_delta < -(thresh * 1.5) and sh_rising
    )
    wall_note    = f' [wall-abs score={score:+.3f}]' if wall_fake else ''

    # ── Signal: wave mechanics fire the signal, score/tier/align size it ──
    # score_ok gates NOTHING — it annotates conviction level only
    score_tag = f'score={score:+.3f}' if abs(score) >= thresh * 0.5 else f'score={score:+.3f}(low)'

    if fk or cav:
        sig = 'AVOID — FK/CAV'
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
        # Peak absorption analysis: score + velocity + sub tell the story
        peak_abs = abs(score) < thresh * 0.5    # near-zero score = balanced
        sub_falling = sh_dir < 0                # sub confirms top
        sub_rising_at_peak = sh_dir > 0         # sub diverging = possible continuation
        if score < -thresh and sub_falling:
            peak_mode = f'SMACKDOWN — supply heavy, sub confirms  {score_tag}'
        elif score < -thresh:
            peak_mode = f'REVERSAL — supply heavy  {score_tag}'
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
        sig = f'AT PEAK — {peak_mode}'
    else:
        sig = 'NEUTRAL'

    flags = []
    if at_sup: flags.append('AT_SUP')
    if rev:    flags.append('REV')
    flag_str = ' '.join(flags)

    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'[{ts}] bar={bar}  {size}  {flag_str}', flush=True)
    print(f'{sig}', flush=True)
    print(f'carrier: ph={c_ph:+.2f}  dir={c_dir:+d}  amp=${c_amp:.0f}  vel={c_vel:+.2f}  decel={c_dp}%  ({c_decel_bars}bars)', flush=True)
    print(f'subharm: ph={sh_ph:+.2f}  dir={sh_dir:+d}  amp=${sh_amp:.0f}  vel={sh_vel:+.2f}  decel={sh_dp}%  ({sh_decel_bars}bars)', flush=True)
    print(f'macro:   ph={mc_ph:+.2f}  dir={mc_dir:+d}  amp=${mc_amp:.0f}  headroom=${mc_amp*(1-mc_ph)/2:.0f}  (4h only)', flush=True)
    print(f'price~${price:.0f}  trough~${trough:.0f}  peak~${peak:.0f}', flush=True)
    print(f'targets: SMALL +${t_small:.0f}  MEDIUM +${t_med:.0f}  LARGE +${t_large:.0f}', flush=True)
    tf_str = ' '.join(f'{k}={v:+.2f}' for k,v in tf.items())
    print(f'waves: {tf_str}  align={align:.2f} {"DIS" if dis else ""}', flush=True)
    score_delta  = score - prev_score
    score_mom    = score_delta / max(abs(thresh), 0.01)  # delta in units of threshold
    mom_str      = f'{score_mom:+.1f}x' if abs(score_mom) >= 0.1 else '~0'
    print(f'score={score:+.3f}  Δ={score_delta:+.3f}({mom_str}thresh)  tier={tier}  snr={snr:.0f}  htf:{htf["1m"]}/{htf["5m"]}/{htf["15m"]}/{htf["1h"]}/{htf["4h"]}', flush=True)
    print('---', flush=True)

    prev = {
        'c_vel': c_vel, 'c_dir': c_dir, 'c_decel_bars': c_decel_bars,
        'sh_vel': sh_vel, 'sh_dir': sh_dir, 'sh_decel_bars': sh_decel_bars,
        'score': score, 'bar': bar
    }
    save_state(prev)
    time.sleep(8)
