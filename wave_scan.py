#!/usr/bin/env python3
"""
wave_scan.py — Intrabar wave engine scan.

4-state signal framework (micro phase + KdV direction):
  PEAK-REV    micro at peak,   KdV flips  -1  → SHORT
  PEAK-CONT   micro at peak,   KdV holds  +1  → LONG  (requires MTF alignment)
  TROUGH-REV  micro at trough, KdV flips  +1  → LONG
  TROUGH-CONT micro at trough, KdV holds  -1  → SHORT

ALL thresholds derive from the last 30 completed candles' value distributions.
Nothing is fixed except SUSTAIN_S (a timing floor, not a market threshold).
At each minute: peak/trough thresholds come from the session's own phase
distribution, score gate from the session's own score distribution, OBI gate
from the session's OBI distribution, KdV gates from the session's KdV
balance distribution, alignment gate from the session's alignment distribution.

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

REPO         = os.path.dirname(__file__)
SUSTAIN_S    = 5    # timing floor — minimum seconds score must persist
SUSTAIN_FLIP = 1    # when KdV flips, 1 confirmed second is enough (the flip IS confirmation)
PEAK_PH      =  0.75  # structural: top outer-quarter of -1..+1 wave cycle
TROUGH_PH    = -0.75  # structural: bottom outer-quarter of -1..+1 wave cycle

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


# ── Adaptive thresholds ───────────────────────────────────────────────────────

def _thresholds(hist_scores, hist_phases, hist_obi, hist_kdv_bals, hist_aligns,
                window=30):
    """
    Derive all signal thresholds from the last `window` completed candles.
    Every value comes from the actual session distribution — nothing fixed.

    Returns: (thresh, peak_ph, trough_ph, obi_conf, gate_rev, gate_cont, align_cont)
    """
    def _pct(arr, p, lo=None, hi=None):
        if len(arr) < 3:
            return None
        v = float(np.percentile(arr[-window:], p))
        if lo is not None: v = max(lo, v)
        if hi is not None: v = min(hi, v)
        return v

    sc = hist_scores;   ph = hist_phases
    ob = hist_obi;      kd = hist_kdv_bals;  al = hist_aligns

    # Score gate: 55th percentile of absolute scores (above-median signal strength)
    thresh     = _pct(sc, 55, lo=0.05)      or 0.12

    # OBI confirmation: 55th percentile of absolute OBI seen in session
    obi_conf   = _pct([abs(x) for x in ob], 55, lo=0.08) or 0.20

    # KdV balance gates: session percentiles
    gate_rev   = _pct(kd, 30, lo=0.5)       or 1.5   # lighter for reversals
    gate_cont  = _pct(kd, 60, lo=1.0)       or 4.0   # stricter for continuations

    # MTF alignment gate: 40th percentile of session alignment
    align_cont = _pct(al, 40, lo=0.40, hi=0.95) or 0.65

    # Phase thresholds are structural constants (position on -1..+1 cycle), not data-derived
    peak_ph   = PEAK_PH
    trough_ph = TROUGH_PH

    return thresh, peak_ph, trough_ph, obi_conf, gate_rev, gate_cont, align_cont


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

def _micro_state(phase, peak_ph, trough_ph):
    if phase >= peak_ph:   return 'PEAK'
    if phase <= trough_ph: return 'TROUGH'
    return 'MID'

def _sig_type(state, direction):
    if state == 'PEAK':
        return 'PEAK-REV' if direction < 0 else 'PEAK-CONT'
    if state == 'TROUGH':
        return 'TROUGH-REV' if direction > 0 else 'TROUGH-CONT'
    return 'MID'

def _gates(stype, score, kdv_bal, kdv_dir,
           thresh, gate_rev, gate_cont, obi_conf, align_cont,
           obi_pressure, align, res_dir, sig_dir,
           at_sup, kdv_flipped, sc_gate=None):
    """Returns (pass: bool, reason_or_confirm: str)."""
    # Skip score magnitude gate when KdV flip is the confirmation:
    # the flip (direction reversal) IS the signal; gate_rev on balance provides strength filter
    if not kdv_flipped:
        effective_thresh = sc_gate if sc_gate is not None else thresh
        if abs(score) < effective_thresh:
            return False, f'score={score:+.3f}<{effective_thresh:.3f}'

    if stype in ('PEAK-REV', 'TROUGH-REV'):
        kdv_matches = (sig_dir < 0 and kdv_dir <= -1) or (sig_dir > 0 and kdv_dir >= 1)
        ob_confirms = (stype == 'TROUGH-REV' and obi_pressure >=  obi_conf) or \
                      (stype == 'PEAK-REV'   and obi_pressure <= -obi_conf)
        if not (kdv_matches or kdv_flipped or ob_confirms):
            return False, f'kdv-dir={kdv_dir},obi={obi_pressure:+.2f}'
        if kdv_bal < gate_rev:
            return False, f'kdv-bal={kdv_bal:.1f}<{gate_rev:.1f}'
        if stype == 'TROUGH-REV' and not at_sup:
            return False, 'not-at-support'
        if kdv_flipped:   confirm = 'kdv-flip'
        elif kdv_matches: confirm = f'kdv={kdv_dir:+d}'
        else:             confirm = f'ob={obi_pressure:+.2f}'
        return True, confirm

    if stype in ('PEAK-CONT', 'TROUGH-CONT'):
        if kdv_bal < gate_cont:
            return False, f'kdv-bal={kdv_bal:.1f}<{gate_cont:.1f}'
        if align < align_cont:
            return False, f'align={align:.2f}<{align_cont:.2f}'
        if res_dir != 0 and res_dir != sig_dir:
            return False, f'res-dir={res_dir}≠{sig_dir}'
        return True, ''

    return False, 'mid-state'


# ── Main scan ─────────────────────────────────────────────────────────────────

def _preseed(session_tf1m, ob_by_sec):
    """
    Pre-populate distribution histories from the session's own 1m bars.
    Used when there are no prior-session history bars (e.g. first session of the day).
    Runs a silent physics pass — no signals, just stats collection.
    """
    hist_s=[]; hist_ph=[]; hist_ob=[]; hist_kd=[]; hist_al=[]
    seed_bars = []
    seed_accum = HydraulicAccumulator()
    for b in session_tf1m:
        seed_bars.append(b)
        if len(seed_bars) < 15: continue
        closes, opens_a, volumes, taker_buy = _bars2arr(seed_bars[-200:])
        try:
            prices = (closes + opens_a) / 2.0
            sec    = b['ts'] // 1000
            bids, asks = ob_by_sec.get(sec, ([], []))
            fus    = fusion.run(prices, opens_a, closes, volumes, taker_buy,
                                seed_accum, bids, asks)
            f_sc   = fus['score']
            kd_bal = abs(fus['soliton'].get('balance', 0.0))
            obi_r  = fus.get('obi', {})
            obi_p  = (obi_r.get('obi', 0.0) if isinstance(obi_r, dict) else float(obi_r or 0.0))
            wf     = WF.run(closes, entry=closes[-1], direction=1 if f_sc >= 0 else -1)
            mph    = wf['components'].get('micro', {}).get('phase', 0.0)
            hist_s.append(abs(f_sc)); hist_ph.append(mph)
            hist_ob.append(obi_p);   hist_kd.append(kd_bal)
            hist_al.append(0.5)
        except Exception:
            pass
    return hist_s, hist_ph, hist_ob, hist_kd, hist_al


def scan(mins_limit=96, session_idx=0):
    print("Fetching raw data...", flush=True)
    subprocess.run(['git','fetch','origin','data/raw'], capture_output=True, cwd=REPO)
    raw = _fetch_raw()
    print(f"Raw lines: {len(raw)}")

    print("Building timeframes...", flush=True)
    s1, tf1m, tf5m, tf15m, tf1h, tf4h, ob_by_sec = _build_tfs(raw)

    s1_by_sec = {b['ts']//1000: b for b in s1}
    s1_secs   = sorted(s1_by_sec.keys())

    # Session detection uses TRADE timestamps (forward-filled s1 has no gaps).
    # Session boundary = gap > 30 min between actual trades (not forward-fill).
    trade_secs = sorted(sec for sec in s1_secs if s1_by_sec[sec]['volume'] > 0)
    trade_gaps = sorted(
        [i for i in range(1, len(trade_secs))
         if trade_secs[i] - trade_secs[i-1] > 1800],   # >30 min = new session
        reverse=True   # newest gap first
    )

    if session_idx < len(trade_gaps):
        t_start = trade_secs[trade_gaps[session_idx]]
        t_end   = trade_secs[trade_gaps[session_idx - 1] - 1] if session_idx > 0 \
                  else trade_secs[-1]
    elif session_idx == len(trade_gaps):
        # Earliest session: start of data to just before the oldest gap
        t_start = trade_secs[0]
        t_end   = trade_secs[trade_gaps[-1] - 1]
    else:
        t_start = trade_secs[0]
        t_end   = trade_secs[-1]

    # Include all s1 seconds within the session's trade-active window
    live_secs = [s for s in s1_secs if t_start <= s <= t_end]

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

    closed_1m       = list(history_1m)
    accum           = HydraulicAccumulator()
    prev_kdv_global = 0
    sig_count       = 0

    # Cooldown: prevent stacking same-direction signals in the same price zone
    last_sig_time   = None   # min_sec of last fired signal
    last_sig_dir    = 0
    last_sig_price  = 0.0

    # Per-closed-candle history for adaptive thresholds
    hist_scores  = []   # abs(score) at minute end
    hist_phases  = []   # micro phase at minute end
    hist_obi     = []   # raw OBI at minute end
    hist_kdv_bals= []   # KdV balance at minute end
    hist_aligns  = []   # MTF alignment at minute end

    # Pre-seed distributions when no prior history (first session of the day)
    if len(history_1m) < 15:
        session_tf1m = [b for b in tf1m
                        if live_secs[0]//60*60 <= b['ts']//1000 <= live_secs[-1]//60*60]
        ps, pp, po, pk, pa = _preseed(session_tf1m, ob_by_sec)
        hist_scores   = ps; hist_phases = pp
        hist_obi      = po; hist_kdv_bals = pk; hist_aligns = pa
        print(f"Pre-seeded from {len(ps)} session bars (no prior history)")

    # Micro layer: rolling 1s window across minute boundaries
    micro_window  = []   # list of 1s bar dicts
    micro_accum   = HydraulicAccumulator()

    print(f"\n{C}━━━ WAVE ENGINE  {_ts(live_secs[0])} → {_ts(live_secs[-1])} UTC ━━━{Z}")
    print(f"{W}[wave] TIME   PRICE       SCORE  D  μ  sh  ca  ma  "
          f"1m 5m 15 1h 4h  KdV  OBI   mSC    mPH  itype  STATE   NOTES{Z}\n")

    for min_sec in sorted(live_by_min.keys()):
        secs_in_min = sorted(live_by_min[min_sec])

        # Live-partial HTF bars built from the closed_1m pool.
        # Completed HTF periods: aggregate 1m bars whose period has fully elapsed.
        # Current HTF period: live partial from completed 1m bars so far in the period.
        # No lookahead — the pool only contains bars completed before this minute.
        def _live_htf(full_tf, period, n):
            ps   = (min_sec // period) * period          # start of current HTF period
            done = [b for b in full_tf if b['ts']//1000 < ps]
            cur  = [b for b in closed_1m
                    if ps <= b['ts']//1000 < ps + period]
            if cur:
                live = {
                    'ts':        ps * 1000,
                    'open':      cur[0]['open'],
                    'high':      max(b['high'] for b in cur),
                    'low':       min(b['low']  for b in cur),
                    'close':     cur[-1]['close'],
                    'volume':    sum(b['volume'] for b in cur),
                    'taker_buy': sum(b.get('taker_buy', 0) for b in cur),
                }
                done = done + [live]
            return done[-n:]

        b5  = _live_htf(tf5m,  300,   30)
        b15 = _live_htf(tf15m, 900,   20)
        b1h = _live_htf(tf1h,  3600,  12)
        b4h = _live_htf(tf4h,  14400,  6)

        # Thresholds from completed candle distributions
        thresh, peak_ph, trough_ph, obi_conf, gate_rev, gate_cont, align_cont = \
            _thresholds(hist_scores, hist_phases, hist_obi, hist_kdv_bals, hist_aligns)

        p_opens=[]; p_closes=[]; p_vols=[]; p_tb=[]

        cand_sec    = None
        cand_score  = 0.0
        cand_dir    = 0
        cand_stype  = 'MID'
        cand_entry  = 0.0
        cand_tgt    = 0.0
        cand_kdvbal = 0.0
        cand_kdvdir = 0
        cand_obi    = 0.0
        cand_sc_gate = thresh

        sustain_count    = 0
        prev_sb          = 0
        prev_kdv_min     = prev_kdv_global
        prev_itype       = None

        kdv_flipped_up   = False
        kdv_flipped_down = False

        kdv_flips     = []
        wh_secs       = []
        itype_changes = []
        final         = {}
        best_obi_long  = 0.0   # most positive OBI seen this minute (TROUGH-REV use)
        best_obi_short = 0.0   # most negative OBI seen this minute (PEAK-REV use)

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

            # ── Micro layer: 1s rolling window physics ───────────────────────
            micro_window.append(bar1s)
            if len(micro_window) > 120: micro_window = micro_window[-120:]
            micro_sc = 0.0; micro_ph = 0.0; micro_kdv = 0
            if len(micro_window) >= 15:
                mc = np.array([b['close']  for b in micro_window], dtype=float)
                mo = np.array([b['open']   for b in micro_window], dtype=float)
                mv = np.array([b['volume'] for b in micro_window], dtype=float)
                mt = np.array([b.get('taker_buy', b['volume']*0.5)
                               for b in micro_window], dtype=float)
                try:
                    mp   = (mc + mo) / 2.0
                    mfus = fusion.run(mp, mo, mc, mv, mt, micro_accum, [], [])
                    micro_sc  = mfus['score']
                    micro_kdv = mfus['soliton']['direction']
                    mwf       = WF.run(mc, entry=mc[-1],
                                       direction=1 if micro_sc >= 0 else -1)
                    micro_ph  = mwf['components'].get('micro',{}).get('phase', 0.0)
                except Exception:
                    pass

            try:
                prices = (closes + opens_a) / 2.0
                fus    = fusion.run(prices, opens_a, closes, volumes, taker_buy,
                                    accum, bids, asks)
                kdv      = fus['soliton']['direction']
                kdv_bal  = abs(fus['soliton'].get('balance', 0.0))
                wh       = fus['water_hammer']['detected']
                f_sc     = fus['score']
                obi_raw  = fus.get('obi', {})
                obi_p    = (obi_raw.get('obi', 0.0)
                            if isinstance(obi_raw, dict) else float(obi_raw or 0.0))
            except Exception:
                kdv=0; kdv_bal=0.0; wh=False; f_sc=0.0; obi_p=0.0

            # Detect KdV flip NOW (before score gate) so the reduced gate
            # applies on the same second the flip occurs, not one second late
            kdv_just_flipped = (kdv != 0 and kdv != prev_kdv_min and prev_kdv_min != 0)
            if kdv_just_flipped:
                kdv_flips.append((sec, kdv))
                if kdv > 0: kdv_flipped_up   = True
                if kdv < 0: kdv_flipped_down = True
            prev_kdv_min = kdv if kdv != 0 else prev_kdv_min

            # When KdV has flipped this minute, the flip is structural confirmation.
            # Only require score direction (sign), not magnitude — gate_rev on balance
            # provides the strength filter. Non-flip bars still need score ≥ thresh.
            flip_active = kdv_flipped_up or kdv_flipped_down
            if flip_active:
                sb = int(np.sign(f_sc)) if abs(f_sc) > 0.01 else 0
                sc_gate = 0.01  # direction-only; gate at minute-end uses kdv_bal
            else:
                sc_gate = thresh
                sb = (1 if f_sc >= thresh else -1 if f_sc <= -thresh else 0)

            # Track extremes for gate check
            if obi_p > best_obi_long:  best_obi_long  = obi_p
            if obi_p < best_obi_short: best_obi_short = obi_p

            wf_direction = sb if sb != 0 else (1 if f_sc >= 0 else -1)
            wf   = WF.run(closes, entry=p_close, direction=wf_direction)
            comp = wf['components']
            itf  = wf['interference']
            tgts = wf.get('targets', {})

            micro_phase = comp.get('micro', {}).get('phase', 0.0)
            wf_dir      = itf['direction']
            itype       = itf['type'][:4]

            if sb != 0 and sb == prev_sb:
                sustain_count += 1
            elif sb != 0:
                sustain_count = 1
            else:
                sustain_count = 0
            prev_sb = sb

            if wh and not wh_secs:
                wh_secs.append(sec)

            if itype != prev_itype and prev_itype is not None:
                itype_changes.append((sec, prev_itype, itype))
            prev_itype = itype

            sustain_needed = SUSTAIN_FLIP if (kdv_flipped_up or kdv_flipped_down) else SUSTAIN_S
            if sb != 0 and sustain_count >= sustain_needed and cand_sec is None:
                state = _micro_state(micro_phase, peak_ph, trough_ph)
                stype = _sig_type(state, sb)
                cand_sec    = sec
                cand_score  = f_sc
                cand_dir    = sb
                cand_stype  = stype
                cand_entry  = p_close
                cand_tgt    = tgts.get('primary',
                                p_close + comp.get('carrier',{}).get('amplitude',0)*sb)
                cand_kdvbal = kdv_bal
                cand_kdvdir = kdv
                cand_obi    = obi_p
                cand_sc_gate = sc_gate   # effective score gate at candidate time

            final = {'sec':sec,'price':p_close,'score':f_sc,'wf_dir':wf_dir,
                     'kdv':kdv,'kdv_bal':kdv_bal,'wh':wh,'itype':itype,
                     'comp':comp,'itf':itf,'sb':sb,'micro_phase':micro_phase,
                     'sustain':sustain_count,'obi':obi_p,
                     'micro_sc':micro_sc,'micro_ph':micro_ph,'micro_kdv':micro_kdv}

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

        # Gate check
        passed = False; fail_reason = ''; confirm_str = ''
        if cand_sec is not None:
            # Cooldown: skip repeat signals in same direction & price zone
            # Zone defined as within 50% of carrier amplitude from last signal price
            carrier_amp = closed_1m[-1]['high'] - closed_1m[-1]['low'] if closed_1m else 0.0
            zone_thresh = max(carrier_amp * 0.5, cand_entry * 0.0015)  # at least 0.15%
            in_cooldown = (
                last_sig_time is not None
                and last_sig_dir == cand_dir
                and (min_sec - last_sig_time) < 180           # within 3 minutes
                and abs(cand_entry - last_sig_price) < zone_thresh
            )
            if in_cooldown:
                fail_reason = f'cooldown({(min_sec - last_sig_time)//60}m,Δ${abs(cand_entry-last_sig_price):.0f})'
            else:
                kdv_flipped = (cand_dir > 0 and kdv_flipped_up) or \
                              (cand_dir < 0 and kdv_flipped_down)
                # Use best OBI seen during the minute (most extreme in signal direction)
                gate_obi = (best_obi_long  if cand_dir > 0 else
                            best_obi_short if cand_dir < 0 else cand_obi)
                passed, fail_reason = _gates(
                    cand_stype, cand_score, cand_kdvbal, cand_kdvdir,
                    thresh, gate_rev, gate_cont, obi_conf, align_cont,
                    gate_obi, align, res_dir, cand_dir,
                    at_sup, kdv_flipped, sc_gate=cand_sc_gate)
            if passed:
                confirm_str = fail_reason
                fail_reason = ''

        # Record completed-minute stats for next minute's thresholds
        hist_scores.append(abs(final['score']))
        hist_phases.append(final['micro_phase'])
        hist_obi.append(final['obi'])
        hist_kdv_bals.append(final['kdv_bal'])
        hist_aligns.append(align)

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

        state_now = _micro_state(final['micro_phase'], peak_ph, trough_ph)

        notes = []
        if cand_sec is not None:
            lbl   = 'LONG' if cand_dir > 0 else 'SHORT'
            t_off = cand_sec - min_sec
            if passed:
                sig_count += 1
                last_sig_time  = min_sec
                last_sig_dir   = cand_dir
                last_sig_price = cand_entry
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

        msc_f  = final.get('micro_sc',  0.0)
        mph_f  = final.get('micro_ph',  0.0)
        mkdv_f = final.get('micro_kdv', 0)
        msc_str = f"{msc_f:>+6.3f}" if msc_f != 0.0 else "  ---  "
        mph_str = _ph(mph_f)
        print(f"{col}[wave] {_tm(min_sec)}  ${price:>9,.2f}  {score:>+7.4f} {_ds(wf_dir)}  "
              f"{_ph(mi.get('phase',0)):>2} {_ph(sh_.get('phase',0)):>2} "
              f"{_ph(ca.get('phase',0)):>2} {_ph(ma.get('phase',0)):>2}  "
              f"{t1m} {t5m} {t15} {t1h} {t4h}  "
              f"{kdv_str}  {obi_str}  {msc_str}  {mph_str}  {itype:4}  "
              f"{state_col}{state_now:6}{col}  {note_str}{Z}")

        if kdv != 0: prev_kdv_global = kdv

        for b in tf1m:
            if b['ts']//1000 == min_sec:
                closed_1m.append(b); break

    # Final threshold state
    thresh_f, pk_f, tr_f, obi_f2, gr_f, gc_f, al_f = \
        _thresholds(hist_scores, hist_phases, hist_obi, hist_kdv_bals, hist_aligns)
    print(f"\n{C}━━━ END  ({sig_count} signals) ━━━{Z}")
    print(f"Final thresholds (from {len(hist_scores)} closed candles):")
    print(f"  score≥{thresh_f:.3f}  peak_ph≥{pk_f:.2f}  trough_ph≤{tr_f:.2f}")
    print(f"  obi_conf≥{obi_f2:.2f}  kdv_rev≥{gr_f:.1f}  kdv_cont≥{gc_f:.1f}  align≥{al_f:.2f}")
    print(f"\nKey: STATE = micro position (PEAK/TROUGH/MID)  OBI = order book imbalance")
    print(f"     All thresholds derived from completed candle value distributions")
    print(f"     SUSTAIN_S={SUSTAIN_S}s is the only fixed constant")


if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Intrabar wave engine scan')
    p.add_argument('--all',     action='store_true', help='scan entire file')
    p.add_argument('--mins',    type=int, default=96, help='last N minutes (default 96)')
    p.add_argument('--session', type=int, default=0,  help='session to scan (0=latest, 1=previous, …)')
    args = p.parse_args()
    scan(mins_limit=None if args.all else args.mins, session_idx=args.session)
