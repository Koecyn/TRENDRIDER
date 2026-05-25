#!/usr/bin/env python3
"""
wave_scan.py — Intrabar wave engine scan.

4-state signal framework (micro phase + KdV direction):
  PEAK-REV    micro at peak,   KdV flips  -1  → SHORT
  PEAK-CONT   micro at peak,   KdV holds  +1  → LONG  (requires MTF alignment)
  TROUGH-REV  micro at trough, KdV flips  +1  → LONG
  TROUGH-CONT micro at trough, KdV holds  -1  → SHORT

Gates are DATA-DRIVEN — derived from the session's own KdV balance distribution,
not hard-coded numbers. A session with weak solitons uses a lower gate; a trending
session uses a higher one. OB pressure (book.global_pressure) can confirm
reversals when KdV flip hasn't fired yet.

Usage:
    python wave_scan.py              # last 96 minutes
    python wave_scan.py --all        # entire file
    python wave_scan.py --mins 60    # last N minutes
"""

import argparse, collections, datetime, gzip, json, subprocess, sys, os
import numpy as np
sys.path.insert(0, os.path.dirname(__file__))

from physics import fusion, waveform as WF, resonance as RES, htf
from physics.signals import HydraulicAccumulator

REPO = os.path.dirname(__file__)

THRESH     = 0.12    # fusion score floor (kept fixed — it's a physics unit)
PEAK_PH    = 0.75    # micro phase ≥ this → AT_PEAK
TROUGH_PH  = -0.75   # micro phase ≤ this → AT_TROUGH
SUSTAIN_S  = 5       # consecutive seconds at threshold before signal is valid
ALIGN_CONT = 0.75    # MTF alignment floor for continuation signals
OBI_CONF   = 0.20    # OB global_pressure threshold — alternative REV confirmation

# Dynamic gate percentiles (derived from session KdV balance distribution)
# REV signals: 35th pct of session  — mean enough momentum to flip
# CONT signals: 55th pct of session — above-average momentum required
GATE_PCT_REV  = 35
GATE_PCT_CONT = 55
GATE_FLOOR    = 1.0  # absolute minimum regardless of session

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; W='\033[97m'; Z='\033[0m'


# ── Data loading ──────────────────────────────────────────────────────────────

def _fetch_raw():
    r = subprocess.run(
        ['git','show','origin/data/raw:data/raw/BTCUSDT_LIVE.jsonl.gz'],
        capture_output=True, cwd=REPO)
    if not r.stdout:
        print("ERROR: no data on origin/data/raw"); sys.exit(1)
    return gzip.decompress(r.stdout).decode().strip().split('\n')


def _build_tfs(raw_lines):
    trade_by_sec = collections.defaultdict(list)
    ob_by_sec    = {}

    for ln in raw_lines:
        if not ln: continue
        rec = json.loads(ln)
        if rec[0] == 'T':
            _, ts, p_c, q_u, side = rec
            sec = ts // 1000
            trade_by_sec[sec].append((p_c/100, q_u/10000, side))
        elif rec[0] == 'D':
            _, ts, bids, asks = rec
            sec = ts // 1000
            ob_by_sec[sec] = ([(p/100,q/10000) for p,q in bids[:5]],
                              [(p/100,q/10000) for p,q in asks[:5]])

    all_secs = sorted(set(list(trade_by_sec.keys()) + list(ob_by_sec.keys())))
    if not all_secs:
        print("ERROR: no data parsed"); sys.exit(1)

    s1 = []; last_close = None; last_ob = None
    for sec in range(all_secs[0], all_secs[-1]+1):
        trades = trade_by_sec.get(sec, [])
        if trades:
            prices = [t[0] for t in trades]
            vols   = [t[1] for t in trades]
            tb     = sum(t[1] for t in trades if t[2]==1)
            o,h,l,c = prices[0],max(prices),min(prices),prices[-1]
            vol = sum(vols); last_close = c
        else:
            if last_close is None: continue
            o=h=l=c=last_close; vol=0.0; tb=0.0
        if sec in ob_by_sec:
            last_ob = ob_by_sec[sec]
        s1.append({'ts':sec*1000,'open':o,'high':h,'low':l,'close':c,
                   'volume':vol,'taker_buy':tb,
                   'ob':last_ob or ([],[])})

    def _agg(s1_bars, period_ms):
        by_period = collections.defaultdict(list)
        for b in s1_bars:
            by_period[(b['ts']//period_ms)*period_ms].append(b)
        result = []
        for p in sorted(by_period):
            sl = by_period[p]
            result.append({
                'ts':        p,
                'open':      sl[0]['open'],
                'high':      max(b['high'] for b in sl),
                'low':       min(b['low']  for b in sl),
                'close':     sl[-1]['close'],
                'volume':    sum(b['volume'] for b in sl),
                'taker_buy': sum(b['taker_buy'] for b in sl),
            })
        return result

    tf1m  = _agg(s1, 60_000)
    tf5m  = _agg(s1, 300_000)
    tf15m = _agg(s1, 900_000)
    tf1h  = _agg(s1, 3_600_000)
    tf4h  = _agg(s1, 14_400_000)
    return s1, tf1m, tf5m, tf15m, tf1h, tf4h, ob_by_sec


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ph(v):
    if v >= 0.75:  return 'PK'
    if v >= 0.35:  return '+½'
    if v <= -0.75: return 'TR'
    if v <= -0.35: return '-½'
    return ' 0'

def _ds(d): return {1:'▲',-1:'▼',0:'─'}.get(d,'─')
def _ts(s): return datetime.datetime.utcfromtimestamp(s).strftime('%H:%M:%S')
def _tm(s): return datetime.datetime.utcfromtimestamp(s).strftime('%H:%M')

def _bars2arr(bars):
    c = np.array([b['close']                             for b in bars], dtype=float)
    o = np.array([b['open']                              for b in bars], dtype=float)
    v = np.array([b['volume']                            for b in bars], dtype=float)
    t = np.array([b.get('taker_buy', b['volume']*0.5)   for b in bars], dtype=float)
    return c, o, v, t

def _micro_state(phase):
    if phase >= PEAK_PH:   return 'PEAK'
    if phase <= TROUGH_PH: return 'TROUGH'
    return 'MID'

def _sig_type(state, direction):
    if state == 'PEAK':
        return 'PEAK-REV' if direction < 0 else 'PEAK-CONT'
    if state == 'TROUGH':
        return 'TROUGH-REV' if direction > 0 else 'TROUGH-CONT'
    return 'MID'

def _dynamic_gates(session_kdv_bals):
    """Compute session-adaptive KdV balance gates from observed distribution."""
    if len(session_kdv_bals) < 5:
        return 1.5, 3.0  # cold start defaults
    arr = np.array(session_kdv_bals)
    gate_rev  = max(GATE_FLOOR, float(np.percentile(arr, GATE_PCT_REV)))
    gate_cont = max(GATE_FLOOR, float(np.percentile(arr, GATE_PCT_CONT)))
    return gate_rev, gate_cont

def _gates(stype, score, kdv_bal, gate_rev, gate_cont,
           obi_pressure, align, res_dir, sig_dir,
           at_sup, kdv_flipped, sustain):
    """Returns (pass: bool, reason: str)."""
    if sustain < SUSTAIN_S:
        return False, f'sustain={sustain}<{SUSTAIN_S}s'
    if abs(score) < THRESH:
        return False, f'score={score:+.3f}'

    if stype in ('PEAK-REV', 'TROUGH-REV'):
        # OB pressure can substitute for KdV flip — book sees what KdV hasn't confirmed yet
        ob_confirms = (stype == 'TROUGH-REV' and obi_pressure >=  OBI_CONF) or \
                      (stype == 'PEAK-REV'   and obi_pressure <= -OBI_CONF)
        if not kdv_flipped and not ob_confirms:
            return False, f'no-flip,obi={obi_pressure:+.2f}'
        if kdv_bal < gate_rev:
            return False, f'kdv-bal={kdv_bal:.1f}<{gate_rev:.1f}'
        if stype == 'TROUGH-REV' and not at_sup:
            return False, 'not-at-support'
        confirm = 'kdv-flip' if kdv_flipped else f'ob={obi_pressure:+.2f}'
        return True, confirm

    if stype in ('PEAK-CONT', 'TROUGH-CONT'):
        if kdv_bal < gate_cont:
            return False, f'kdv-bal={kdv_bal:.1f}<{gate_cont:.1f}'
        if align < ALIGN_CONT:
            return False, f'align={align:.2f}<{ALIGN_CONT}'
        if res_dir != 0 and res_dir != sig_dir:
            return False, f'res-dir={res_dir}≠{sig_dir}'
        return True, ''

    return False, 'mid-state'


# ── Main scan ─────────────────────────────────────────────────────────────────

def scan(mins_limit=96):
    print("Fetching raw data...", flush=True)
    subprocess.run(['git','fetch','origin','data/raw'], capture_output=True, cwd=REPO)
    raw = _fetch_raw()
    print(f"Raw lines: {len(raw)}")

    print("Building timeframes...", flush=True)
    s1, tf1m, tf5m, tf15m, tf1h, tf4h, ob_by_sec = _build_tfs(raw)

    s1_by_sec = {b['ts']//1000: b for b in s1}
    s1_secs   = sorted(s1_by_sec.keys())

    gap_idx = None
    for i in range(len(s1_secs)-1, 0, -1):
        if s1_secs[i] - s1_secs[i-1] > 300:
            gap_idx = i; break

    live_secs = s1_secs[gap_idx:] if gap_idx else s1_secs
    if mins_limit:
        cutoff = live_secs[-1] - mins_limit * 60
        live_secs = [s for s in live_secs if s >= cutoff]

    live_start_min = (live_secs[0] // 60) * 60
    history_1m     = [b for b in tf1m if b['ts']//1000 < live_start_min]

    print(f"Live session: {_ts(live_secs[0])} → {_ts(live_secs[-1])} UTC  "
          f"({len(live_secs)}s,  {len(history_1m)} history bars)")

    live_by_min = collections.defaultdict(list)
    for sec in live_secs:
        live_by_min[(sec // 60) * 60].append(sec)

    closed_1m        = list(history_1m)
    accum            = HydraulicAccumulator()
    prev_kdv_global  = 0
    session_kdv_bals = []   # growing list of non-zero KdV balance values this session
    sig_count        = 0

    print(f"\n{C}━━━ WAVE ENGINE  {_ts(live_secs[0])} → {_ts(live_secs[-1])} UTC ━━━{Z}")
    print(f"{W}[wave] TIME   PRICE       SCORE  D  μ  sh  ca  ma  "
          f"1m 5m 15 1h 4h  KdV  OBI   itype  STATE   NOTES{Z}\n")

    for min_sec in sorted(live_by_min.keys()):
        secs_in_min = sorted(live_by_min[min_sec])

        b5  = [b for b in tf5m  if b['ts']//1000 <= min_sec][-30:]
        b15 = [b for b in tf15m if b['ts']//1000 <= min_sec][-20:]
        b1h = [b for b in tf1h  if b['ts']//1000 <= min_sec][-12:]
        b4h = [b for b in tf4h  if b['ts']//1000 <= min_sec][-6:]

        p_opens=[]; p_closes=[]; p_vols=[]; p_tb=[]

        # Signal candidate state
        cand_sec   = None
        cand_score = 0.0
        cand_dir   = 0
        cand_state = 'MID'
        cand_stype = 'MID'
        cand_entry = 0.0
        cand_tgt   = 0.0
        cand_kdvbal= 0.0
        cand_obi   = 0.0

        sustain_count    = 0
        prev_sb          = 0
        prev_kdv_min     = prev_kdv_global
        prev_itype       = None

        kdv_flipped_up   = False
        kdv_flipped_down = False

        kdv_flips     = []
        wh_secs       = []
        itype_changes = []
        minute_obi    = 0.0   # last OBI seen this minute
        final         = {}

        for sec in secs_in_min:
            bar1s = s1_by_sec.get(sec)
            if bar1s is None: continue

            p_opens.append(bar1s['open'])
            p_closes.append(bar1s['close'])
            p_vols.append(bar1s['volume'])
            p_tb.append(bar1s.get('taker_buy', bar1s['volume']*0.5))

            p_close = p_closes[-1]
            partial = {'open':p_opens[0],'high':max(p_closes),'low':min(p_closes),
                       'close':p_close,'volume':sum(p_vols),'taker_buy':sum(p_tb)}
            window = closed_1m[-200:] + [partial]
            if len(window) < 15: continue

            closes, opens_a, volumes, taker_buy = _bars2arr(window)
            bids, asks = ob_by_sec.get(sec, ([], []))

            try:
                prices = (closes + opens_a) / 2.0
                fus    = fusion.run(prices, opens_a, closes, volumes, taker_buy,
                                    accum, bids, asks)
                kdv      = fus['soliton']['direction']
                kdv_bal  = abs(fus['soliton'].get('balance', 0.0))
                wh       = fus['water_hammer']['detected']
                f_sc     = fus['score']
                # OB pressure from book analyzer (in fusion output)
                obi_raw  = fus.get('obi', {})
                obi_p    = (obi_raw.get('global_pressure', 0.0)
                            if isinstance(obi_raw, dict) else float(obi_raw or 0.0))
            except Exception:
                kdv=0; kdv_bal=0.0; wh=False; f_sc=0.0; obi_p=0.0

            minute_obi = obi_p

            # Collect for dynamic gate computation
            if kdv_bal > 0 and kdv != 0:
                session_kdv_bals.append(kdv_bal)

            sb = (1 if f_sc >= THRESH else -1 if f_sc <= -THRESH else 0)

            # Waveform (use signal direction for phase-adjusted targets)
            wf_direction = sb if sb != 0 else (1 if f_sc >= 0 else -1)
            wf   = WF.run(closes, entry=p_close, direction=wf_direction)
            comp = wf['components']
            itf  = wf['interference']
            tgts = wf.get('targets', {})

            micro_phase = comp.get('micro', {}).get('phase', 0.0)
            wf_dir      = itf['direction']
            itype       = itf['type'][:4]

            # Sustain tracking
            if sb != 0 and sb == prev_sb:
                sustain_count += 1
            elif sb != 0:
                sustain_count = 1
            else:
                sustain_count = 0
            prev_sb = sb

            # KdV flip detection
            if kdv != 0 and kdv != prev_kdv_min and prev_kdv_min != 0:
                kdv_flips.append((sec, kdv))
                if kdv > 0: kdv_flipped_up   = True
                if kdv < 0: kdv_flipped_down = True
            prev_kdv_min = kdv if kdv != 0 else prev_kdv_min

            if wh and not wh_secs:
                wh_secs.append(sec)

            if itype != prev_itype and prev_itype is not None:
                itype_changes.append((sec, prev_itype, itype))
            prev_itype = itype

            # First candidate once sustain gate met
            if sb != 0 and sustain_count >= SUSTAIN_S and cand_sec is None:
                state = _micro_state(micro_phase)
                stype = _sig_type(state, sb)
                cand_sec    = sec
                cand_score  = f_sc
                cand_dir    = sb
                cand_state  = state
                cand_stype  = stype
                cand_entry  = p_close
                cand_tgt    = tgts.get('primary',
                                p_close + comp.get('carrier',{}).get('amplitude',0)*sb)
                cand_kdvbal = kdv_bal
                cand_obi    = obi_p

            final = {'sec':sec,'price':p_close,'score':f_sc,'wf_dir':wf_dir,
                     'kdv':kdv,'kdv_bal':kdv_bal,'wh':wh,'itype':itype,
                     'comp':comp,'itf':itf,'sb':sb,'micro_phase':micro_phase,
                     'sustain':sustain_count,'obi':obi_p}

        if not final:
            for b in tf1m:
                if b['ts']//1000 == min_sec:
                    closed_1m.append(b); break
            continue

        # HTF context
        htf_ctx = htf.regime(final['price'], b1h, b4h, closed_1m[-30:], b5, b15)
        at_sup  = htf_ctx.get('at_support',    False)
        at_res  = htf_ctx.get('at_resistance', False)

        # MTF resonance
        try:
            res_out    = RES.resonance(closed_1m[-30:], b5, b15, b1h, b4h)
            res_dir    = res_out.get('direction',  0)
            align      = res_out.get('alignment',  0.0)
            dissonance = res_out.get('dissonance', False)
        except Exception:
            res_dir=0; align=0.0; dissonance=False

        # Dynamic gates from session KdV distribution
        gate_rev, gate_cont = _dynamic_gates(session_kdv_bals)

        # Gate check
        passed = False; fail_reason = ''; confirm_str = ''
        if cand_sec is not None:
            kdv_flipped = (cand_dir > 0 and kdv_flipped_up) or \
                          (cand_dir < 0 and kdv_flipped_down)
            passed, fail_reason = _gates(
                cand_stype, cand_score, cand_kdvbal, gate_rev, gate_cont,
                cand_obi, align, res_dir, cand_dir,
                at_sup, kdv_flipped, SUSTAIN_S)
            if passed:
                confirm_str = fail_reason  # _gates returns confirm string on pass
                fail_reason = ''

        # ── Output ───────────────────────────────────────────────────────────
        comp_f = final['comp']
        mi  = comp_f.get('micro',{});  sh_ = comp_f.get('subharm',{})
        ca  = comp_f.get('carrier',{}); ma  = comp_f.get('macro',{})
        price  = final['price']; score = final['score']
        wf_dir = final['wf_dir']; kdv  = final['kdv']
        itype  = final['itype']; sb    = final['sb']
        obi_f  = final['obi']

        t1m = htf_ctx.get('trend_1m', 'ne')[:2]
        t5m = htf_ctx.get('trend_5m', 'ne')[:2]
        t15 = htf_ctx.get('trend_15m','ne')[:2]
        t1h = htf_ctx.get('trend_1h', 'ne')[:2]
        t4h = htf_ctx.get('trend_4h', 'ne')[:2]

        state_now = _micro_state(final['micro_phase'])

        notes = []
        if cand_sec is not None:
            lbl   = 'LONG' if cand_dir > 0 else 'SHORT'
            t_off = cand_sec - min_sec
            if passed:
                sig_count += 1
                conf = f' [{confirm_str}]' if confirm_str else ''
                notes.append(f"[{sig_count}] *** {cand_stype} {lbl} @ {_ts(cand_sec)} "
                             f"(t+{t_off}s){conf}  ${cand_entry:,.2f} → ${cand_tgt:,.2f}")
            else:
                notes.append(f"sig:{cand_stype}/{lbl} BLOCKED:{fail_reason}")

        for fs, fd in kdv_flips:
            notes.append(f"KdV→{fd:+d}@{_ts(fs)}")
            prev_kdv_global = fd

        for ws in wh_secs:
            notes.append(f"WH@{_ts(ws)}")

        for cs, ot, nt in itype_changes:
            if nt == 'dest': notes.append(f"→DEST@{_ts(cs)}")
            elif nt == 'cons': notes.append(f"→CONS@{_ts(cs)}")

        if at_sup: notes.append('SUP')
        if at_res: notes.append('RES')
        if htf_ctx.get('reversal_setup'): notes.append('REVERSAL')
        if htf_ctx.get('falling_knife'):  notes.append('FALL-KNIFE')
        if dissonance: notes.append('DISS')

        note_str = '  '.join(notes)
        kdv_str  = f"{kdv:+d}" if kdv != 0 else " 0"
        obi_str  = f"{obi_f:+.2f}"

        if passed:
            col = G if cand_dir > 0 else R
        elif cand_sec is not None:
            col = Y
        elif notes:
            col = Y
        else:
            col = Z

        state_col = (C if state_now == 'PEAK' else
                     G if state_now == 'TROUGH' else W)

        print(f"{col}[wave] {_tm(min_sec)}  ${price:>9,.2f}  {score:>+7.4f} {_ds(wf_dir)}  "
              f"{_ph(mi.get('phase',0)):>2} {_ph(sh_.get('phase',0)):>2} "
              f"{_ph(ca.get('phase',0)):>2} {_ph(ma.get('phase',0)):>2}  "
              f"{t1m} {t5m} {t15} {t1h} {t4h}  "
              f"{kdv_str}  {obi_str}  {itype:4}  "
              f"{state_col}{state_now:6}{col}  {note_str}{Z}")

        if kdv != 0: prev_kdv_global = kdv

        for b in tf1m:
            if b['ts']//1000 == min_sec:
                closed_1m.append(b); break

    gate_rev_f, gate_cont_f = _dynamic_gates(session_kdv_bals)
    print(f"\n{C}━━━ END  ({sig_count} signals) ━━━{Z}")
    print(f"Session KdV gate: REV≥{gate_rev_f:.2f}  CONT≥{gate_cont_f:.2f}  "
          f"(from {len(session_kdv_bals)} samples, p{GATE_PCT_REV}/p{GATE_PCT_CONT})")
    print(f"\nKey: STATE = micro position (PEAK/TROUGH/MID)  OBI = OB global_pressure")
    print(f"     PEAK-REV=short peak  TROUGH-REV=long trough  TROUGH-CONT=short break")
    print(f"     REV confirms via KdV-flip OR OB-pressure≥{OBI_CONF}")
    print(f"     Gates adapt to session — tighter in trending, looser in quiet")


if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Intrabar wave engine scan')
    p.add_argument('--all',  action='store_true', help='scan entire file')
    p.add_argument('--mins', type=int, default=96, help='last N minutes (default 96)')
    args = p.parse_args()
    scan(mins_limit=None if args.all else args.mins)
