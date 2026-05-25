#!/usr/bin/env python3
"""
wave_scan.py — Offline intrabar wave engine scan.

Fetches BTCUSDT_LIVE.jsonl.gz from origin/data/raw, builds all timeframes
from the raw data, then runs the full physics signal engine at 1-second
resolution (partial bar accumulation) and prints one [wave] line per minute.

Usage:
    python wave_scan.py              # last 96 minutes (live session)
    python wave_scan.py --all        # entire file
    python wave_scan.py --mins 60    # last N minutes

Output per line:
    [wave] HH:MM  $PRICE  SCORE  D  μ sh ca ma  1m 5m 15 1h 4h  KdV  itype  NOTES

NOTES includes:
    *** LONG/SHORT @ HH:MM:SS (t+Xs)  $ENTRY → $TARGET
    KdV→±1@HH:MM:SS   (soliton flip intrabar)
    WH@HH:MM:SS        (water hammer first fire)
    →CONS / →DEST      (interference type change)
    SUP / RES           (price at HTF support/resistance)
    REVERSAL            (HTF downtrend at support setup)
"""

import argparse, collections, datetime, gzip, json, pickle, subprocess, sys, os
import numpy as np
sys.path.insert(0, os.path.dirname(__file__))

from physics import signals as S, fusion, waveform as WF, resonance as RES, htf
from physics.signals import HydraulicAccumulator

REPO = os.path.dirname(__file__)
THRESH = 0.12

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
    """Build 1s, 1m, 5m, 15m, 1h, 4h candles from raw JSONL."""
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

    # 1s candles with forward fill
    s1 = []; last_close = None; last_ob = None
    for sec in range(all_secs[0], all_secs[-1]+1):
        trades = trade_by_sec.get(sec, [])
        if trades:
            prices = [t[0] for t in trades]
            vols   = [t[1] for t in trades]
            tb     = sum(t[1] for t in trades if t[2]==1)
            o,h,l,c = prices[0],max(prices),min(prices),prices[-1]
            vol = sum(vols); last_close=c
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
                'ts':     p,
                'open':   sl[0]['open'],
                'high':   max(b['high'] for b in sl),
                'low':    min(b['low']  for b in sl),
                'close':  sl[-1]['close'],
                'volume': sum(b['volume'] for b in sl),
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
    if v>= 0.75: return '+1'
    if v>= 0.35: return '+½'
    if v<=-0.75: return '-1'
    if v<=-0.35: return '-½'
    return ' 0'

def _ds(d): return {1:'▲',-1:'▼',0:'─'}.get(d,'─')
def _ts(s): return datetime.datetime.utcfromtimestamp(s).strftime('%H:%M:%S')
def _tm(s): return datetime.datetime.utcfromtimestamp(s).strftime('%H:%M')

def _bars2arr(bars):
    c = np.array([b['close']     for b in bars], dtype=float)
    o = np.array([b['open']      for b in bars], dtype=float)
    v = np.array([b['volume']    for b in bars], dtype=float)
    t = np.array([b.get('taker_buy', b['volume']*0.5) for b in bars], dtype=float)
    return c, o, v, t


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

    # Find live session (last block after big gap)
    gap_idx = None
    for i in range(len(s1_secs)-1, 0, -1):
        if s1_secs[i] - s1_secs[i-1] > 300:
            gap_idx = i; break

    live_secs = s1_secs[gap_idx:] if gap_idx else s1_secs
    if mins_limit:
        cutoff = live_secs[-1] - mins_limit * 60
        live_secs = [s for s in live_secs if s >= cutoff]

    live_start_min = (live_secs[0] // 60) * 60
    history_1m = [b for b in tf1m if b['ts']//1000 < live_start_min]

    print(f"Live session: {_ts(live_secs[0])} → {_ts(live_secs[-1])} UTC  "
          f"({len(live_secs)}s,  {len(history_1m)} history bars)")

    live_by_min = collections.defaultdict(list)
    for sec in live_secs:
        live_by_min[(sec // 60) * 60].append(sec)

    closed_1m = list(history_1m)
    accum     = HydraulicAccumulator()
    prev_kdv_global = 0

    print(f"\n{C}━━━ WAVE ENGINE  {_ts(live_secs[0])} → {_ts(live_secs[-1])} UTC ━━━{Z}")
    print(f"{W}[wave] TIME    PRICE       SCORE  D   μ  sh  ca  ma  "
          f"1m 5m 15 1h 4h  KdV  itype  NOTES{Z}\n")

    for min_sec in sorted(live_by_min.keys()):
        secs_in_min = sorted(live_by_min[min_sec])

        b5  = [b for b in tf5m  if b['ts']//1000 <= min_sec][-30:]
        b15 = [b for b in tf15m if b['ts']//1000 <= min_sec][-20:]
        b1h = [b for b in tf1h  if b['ts']//1000 <= min_sec][-12:]
        b4h = [b for b in tf4h  if b['ts']//1000 <= min_sec][-6:]

        p_opens=[]; p_closes=[]; p_vols=[]; p_tb=[]
        first_sig_sec=None; first_sig_score=0.0; first_sig_dir=0
        sig_entry=0.0; sig_tgt=0.0
        kdv_flips=[]; wh_secs=[]; itype_changes=[]
        prev_kdv_min=prev_kdv_global; prev_itype_min=None
        final={}

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

            wf   = WF.run(closes, entry=p_close, direction=1)
            comp = wf['components']; itf = wf['interference']
            wf_dir = itf['direction']
            itype  = itf['type'][:4]
            carr_amp = comp.get('carrier', {}).get('amplitude', 0.0)

            try:
                prices = (closes + opens_a) / 2.0
                fus    = fusion.run(prices, opens_a, closes, volumes, taker_buy,
                                    accum, bids, asks)
                kdv = fus['soliton']['direction']
                wh  = fus['water_hammer']['detected']
                f_sc = fus['score']
            except Exception:
                kdv=0; wh=False; f_sc=0.0

            sb = (1 if f_sc >= THRESH else -1 if f_sc <= -THRESH else 0)

            if sb != 0 and first_sig_sec is None:
                first_sig_sec=sec; first_sig_score=f_sc; first_sig_dir=sb
                sig_entry=p_close; sig_tgt=p_close+carr_amp*sb

            if kdv!=0 and kdv!=prev_kdv_min and prev_kdv_min!=0:
                kdv_flips.append((sec, kdv))
            prev_kdv_min = kdv if kdv!=0 else prev_kdv_min

            if wh and not wh_secs:
                wh_secs.append(sec)

            if itype != prev_itype_min and prev_itype_min is not None:
                itype_changes.append((sec, prev_itype_min, itype))
            prev_itype_min = itype

            final = {'sec':sec,'price':p_close,'score':f_sc,'wf_dir':wf_dir,
                     'kdv':kdv,'wh':wh,'itype':itype,'comp':comp,'sb':sb,
                     'carr_amp':carr_amp}

        if not final:
            for b in tf1m:
                if b['ts']//1000 == min_sec:
                    closed_1m.append(b); break
            continue

        # HTF context
        htf_ctx = htf.regime(final['price'], b1h, b4h, closed_1m[-30:], b5, b15)
        t1m = htf_ctx.get('trend_1m','ne')[:2]
        t5m = htf_ctx.get('trend_5m','ne')[:2]
        t15 = htf_ctx.get('trend_15m','ne')[:2]
        t1h = htf_ctx.get('trend_1h','ne')[:2]
        t4h = htf_ctx.get('trend_4h','ne')[:2]

        comp   = final['comp']
        mi     = comp.get('micro',{});  sh_ = comp.get('subharm',{})
        ca     = comp.get('carrier',{}); ma  = comp.get('macro',{})
        price  = final['price']; score = final['score']
        wf_dir = final['wf_dir']; kdv  = final['kdv']
        itype  = final['itype']; sb    = final['sb']

        # Build notes
        notes = []
        if first_sig_sec is not None:
            lbl = 'LONG' if first_sig_dir>0 else 'SHORT'
            t_offset = first_sig_sec - min_sec
            notes.append(f"*** {lbl} @ {_ts(first_sig_sec)} (t+{t_offset}s) "
                         f"${sig_entry:,.2f} → ${sig_tgt:,.2f}")

        for fs, fd in kdv_flips:
            notes.append(f"KdV→{fd:+d}@{_ts(fs)}")
            prev_kdv_global = fd

        for ws in wh_secs:
            notes.append(f"WH@{_ts(ws)}")

        for cs, ot, nt in itype_changes:
            if nt == 'dest': notes.append(f"→DEST@{_ts(cs)}")
            elif nt == 'cons': notes.append(f"→CONS@{_ts(cs)}")

        if htf_ctx.get('at_support'):     notes.append('SUP')
        if htf_ctx.get('at_resistance'):  notes.append('RES')
        if htf_ctx.get('reversal_setup'): notes.append('REVERSAL')
        if htf_ctx.get('falling_knife'):  notes.append('FALLING-KNIFE')

        note_str = '  '.join(notes)
        col = G if sb>0 else R if sb<0 else Y if notes else Z
        kdv_str = f"{kdv:+d}" if kdv!=0 else " 0"

        print(f"{col}[wave] {_tm(min_sec)}  ${price:>9,.2f}  {score:>+7.4f} {_ds(wf_dir)}  "
              f"{_ph(mi.get('phase',0)):>2} {_ph(sh_.get('phase',0)):>2} "
              f"{_ph(ca.get('phase',0)):>2} {_ph(ma.get('phase',0)):>2}  "
              f"{t1m} {t5m} {t15} {t1h} {t4h}  "
              f"{kdv_str}  {itype:4}  {note_str}{Z}")

        if kdv != 0: prev_kdv_global = kdv

        for b in tf1m:
            if b['ts']//1000 == min_sec:
                closed_1m.append(b); break

    print(f"\n{C}━━━ END ━━━{Z}")
    print(f"\nKey: D=wave-dir  μshcama=band-phases  1m…4h=HTF-trends  KdV=soliton-dir")
    print(f"     itype: cons=constructive  dest=destructive  part=partial")
    print(f"     *** LONG/SHORT = score crossed ±{THRESH} (first intrabar crossing)")
    print(f"     Colors: green=long  red=short  yellow=event-only  white=quiet")


if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Intrabar wave engine scan')
    p.add_argument('--all',  action='store_true', help='scan entire file')
    p.add_argument('--mins', type=int, default=96, help='last N minutes (default 96)')
    args = p.parse_args()
    limit = None if args.all else args.mins
    scan(mins_limit=limit)
