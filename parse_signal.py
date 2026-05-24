#!/usr/bin/env python3
import sys, json
try:
    d = json.load(sys.stdin)
    s = d.get('stats', {})
    c_ph   = s.get('wf_carrier_phase', 0)
    c_amp  = s.get('wf_carrier_amp', 0)
    mc_ph  = s.get('wf_macro_phase', 0)
    mc_amp = s.get('wf_macro_amp', 0)
    mc_head = mc_amp * (1.0 - mc_ph) / 2.0
    phase = 'PEAK' if c_ph > 0.75 else 'TROUGH' if c_ph < -0.75 else 'MID'
    t1  = s.get('htf_trend_1m',  '?')
    t5  = s.get('htf_trend_5m',  '?')
    t15 = s.get('htf_trend_15m', '?')
    t1h = s.get('htf_trend_1h',  '?')
    t4h = s.get('htf_trend_4h',  '?')
    bar   = s.get('bars_live', 0)
    score = s.get('last_score', 0)
    c_dir = s.get('wf_carrier_direction', 0)
    align = s.get('res_alignment', 0)
    tier  = s.get('last_tier', 5)
    tgt   = s.get('wf_tgt_primary', 0)
    f_sd  = s.get('fast_sub_dir', 0)
    f_sa  = s.get('fast_sub_amp', 0.0)
    fast  = f' fast_sub:{f_sd:+d}amp={f_sa:.1f}' if f_sa > 0.5 else ''
    print(f"{bar}|{phase}|{score:.3f}|{c_ph:+.2f}|{c_dir}|{align:.2f}|{tier}"
          f"|{t1}/{t5}/{t15}/{t1h}/{t4h}|${tgt:.0f}|mc_head=${mc_head:.0f}{fast}")
except Exception as e:
    print(f"parse_err:{e}", file=sys.stderr)
