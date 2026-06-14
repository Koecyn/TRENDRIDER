#!/usr/bin/env python3
"""
A1.py — BTCUSDT live tick collector + real-time candle builder.

Every 1s bar that closes immediately updates the leading edge of ALL higher
timeframes in memory. TF bars print as each boundary crosses. The background
thread writes to disk (gzip-9) and pushes to git independently.

Termux setup (run once):
    echo "alias A1='python3 ~/TRENDRIDER/A1.py'" >> ~/.bashrc && source ~/.bashrc
    # zsh:
    echo "alias A1='python3 ~/TRENDRIDER/A1.py'" >> ~/.zshrc  && source ~/.zshrc
Run:
    A1
"""

import asyncio, gzip, io, json, os, signal, subprocess, sys, threading, time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

try:
    import aiohttp
except ImportError:
    print("ERROR: pip install aiohttp"); sys.exit(1)

REPO     = Path(__file__).resolve().parent
DATA_DIR = Path.home() / '.trendrider'
GZ_FILE  = DATA_DIR / 'BTCUSDT_LIVE.jsonl.gz'
TMP_IDX  = REPO / '.git' / 'a1_push.idx'

WS_URL = ('wss://stream.binance.us:9443/stream'
          '?streams=btcusdt@aggTrade/btcusdt@depth20@100ms')

WRITE_S = 5    # flush raw ticks to disk every N seconds
BUILD_S = 30   # persist candle files to disk / push every N seconds
PUSH_S  = 60   # git push to data/raw every N seconds

# Timeframe name → seconds per bar
TIMEFRAMES = {
    '1m': 60, '5m': 300, '10m': 600, '15m': 900,
    '30m': 1800, '45m': 2700, '1h': 3600, '4h': 14400,
}

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; W='\033[97m'; B='\033[94m'; Z='\033[0m'
def _utc(): return datetime.now(timezone.utc).strftime('%H:%M:%S')
SEP  = C + '─' * 56 + Z
SEPE = W + '═' * 56 + Z   # heavy border for status blocks
SEPS = G + '█' * 56 + Z   # solid bar for LONG signals
SEPR = R + '█' * 56 + Z   # solid bar for SHORT signals


# ── Shared state ───────────────────────────────────────────────────────────────

_raw        = deque(maxlen=15000)   # raw JSON lines — bounded ring buffer
_lock       = threading.Lock()
_last_write = 0.0
_last_build = 0.0
_last_push  = 0.0
_n_ticks    = 0
_n_depth    = 0

# Live 1s accumulator (in-progress bar for current second)
_bar_sec = 0
_bar     = None   # {o, h, l, c, v, bv, sv, n}

# Real-time TF state — updated on every 1s close
# _partial[tf] = current open bar for that TF (in-memory, not yet closed)
_partial    = {}   # {tf: {'_aligned':int, ts, open, high, low, close, volume, buy_v, sell_v, n}}
_counts     = {}   # {tf: total closed bar count} — base from file + live increments
_snap_count = 0    # 1s-bar counter; snapshot + physics print every 5 counts

# Physics engine state — set by _init_physics() at startup
_scan_state = None   # wave_scan.ScanState


# ── Terminal output ────────────────────────────────────────────────────────────

def _hdr():
    print(f'\n{SEPE}')
    print(f'{W}  A1 · BTCUSDT  real-time ticks + candles + physics{Z}')
    print(f'  {_utc()} UTC  ·  {DATA_DIR}')
    print(f'  WRITE={WRITE_S}s  BUILD={BUILD_S}s  PUSH={PUSH_S}s  gzip-9')
    print(f'{SEPE}\n', flush=True)


def _g(ok):
    return f'{G}✓{Z}' if ok else f'{R}✗{Z}'


def _print_status(new_sigs=None):
    """
    Full physics status block — no abbreviations, every sub-component exposed.
    Sections: price/session · knife decay · dark pool · order book flow ·
              waveform/score · resonance/HTF · per-TF (3 lines each).
    """
    now_sec  = int(time.time())
    st       = _scan_state
    live     = getattr(st, 'live',    {}) if st else {}
    tf_live  = getattr(st, 'tf_live', {}) if st else {}

    def _sec(label):
        print(f'  {C}── {label} {"─"*(48-len(label))}{Z}', flush=True)

    print(f'\n{SEPE}', flush=True)

    if not live:
        status = f'{Y}initializing…{Z}' if st is not None else f'{R}no seed — physics offline{Z}'
        print(f'  {W}LIVE{Z}  {_utc()}  {status}', flush=True)
    else:
        # ── Price / session ──────────────────────────────────────────────
        price  = live.get('price', 0)
        spr    = live.get('spr',   0)
        conc   = live.get('conc',  0)
        print(f'  {W}LIVE{Z}  {_utc()}  ${price:>12,.2f}  '
              f'spread:${spr:.2f}  book_conc:{conc:>+.3f}', flush=True)

        closed = getattr(st, 'closed_1m', [])
        if closed:
            now_ms = int(time.time() * 1000)
            today  = [b for b in closed if b['ts'] >= now_ms - (now_ms % 86_400_000)]
            if today:
                s_o = today[0]['open']; s_h = max(b['high'] for b in today)
                s_l = min(b['low'] for b in today); s_r = s_h - s_l
                s_chg = price - s_o; s_pct = s_chg / s_o * 100 if s_o else 0
                cc = G if s_chg >= 0 else R
                print(f'  session  open:{s_o:>10,.2f}  high:{s_h:>10,.2f}  '
                      f'low:{s_l:>10,.2f}  range:${s_r:.0f}  '
                      f'{cc}{s_chg:>+.2f}({s_pct:>+.2f}%){Z}', flush=True)

        # ── Knife decay buffer ───────────────────────────────────────────
        _sec('knife decay buffer')
        dk       = live.get('dk',       '?')
        decay_sc = live.get('decay_sc', 0)
        floor_sc = live.get('floor_sc', 0)
        vel      = live.get('vel',      0)
        buf      = getattr(st, 'knife_buf', None)
        buf_ph   = getattr(buf, 'phase', '?') if buf else '?'
        dk_c     = G if dk == 'FLOOR' else (Y if 'FLR' in dk or 'DEC' in dk else R)
        vel_c    = G if vel > 0 else R
        dec_c    = G if decay_sc >= 0.6 else (Y if decay_sc >= 0.3 else '')
        flr_c    = G if floor_sc >= 0.7 else (Y if floor_sc >= 0.4 else '')
        print(f'  state:{dk_c}{dk}{Z}  momentum_phase:{buf_ph}  '
              f'velocity:{vel_c}{vel:>+.4f}{Z}', flush=True)
        print(f'  decay_score:{dec_c}{decay_sc:.3f}{Z}  '
              f'floor_score:{flr_c}{floor_sc:.3f}{Z}', flush=True)

        # ── Dark pool / hydraulic accumulator ────────────────────────────
        _sec('dark pool accumulator')
        accum       = getattr(st, 'accum',       None)
        micro_accum = getattr(st, 'micro_accum', None)
        acc_p  = getattr(accum,       'stored',   0)
        acc_d  = getattr(accum,       'last_dir', 0)
        macc_p = getattr(micro_accum, 'stored',   0)
        macc_d = getattr(micro_accum, 'last_dir', 0)
        acc_arrow  = {1: G+'↑'+Z, -1: R+'↓'+Z}.get(acc_d,  '→')
        macc_arrow = {1: G+'↑'+Z, -1: R+'↓'+Z}.get(macc_d, '→')
        acc_c  = G if acc_p  >= 0.6 else (Y if acc_p  >= 0.3 else '')
        macc_c = G if macc_p >= 0.6 else (Y if macc_p >= 0.3 else '')
        print(f'  dark_pool_pressure:{acc_c}{acc_p:.3f}{Z}  dir:{acc_arrow}', flush=True)
        print(f'  micro_pool_pressure:{macc_c}{macc_p:.3f}{Z}  dir:{macc_arrow}', flush=True)

        # ── Order book flow ──────────────────────────────────────────────
        _sec('order book · flow')
        obi   = live.get('obi',      0); obi_n = live.get('obi_need', 1)
        bid_r = ask_r = 0.0
        if buf is not None:
            try:
                bid_r, ask_r = buf._flow_rates()
            except Exception:
                pass
        obi_ok  = obi >= obi_n
        flow_c  = G if bid_r > ask_r else R
        print(f'  obi:{obi:>+.3f}(need≥{obi_n:.3f}){_g(obi_ok)}  '
              f'bid_flow:{flow_c}{bid_r:>+.4f}{Z}  ask_flow:{ask_r:>+.4f}', flush=True)

        # ── Waveform · score (fusion of all physics signals) ─────────────
        _sec('waveform · fused score')
        micro    = live.get('micro_ph',  0)
        score    = live.get('score',     0); score_n = live.get('score_need',   1)
        kdv      = live.get('kdv_bal',   0); kdv_r   = live.get('kdv_need_rev', 999)
        kdv_cn   = live.get('kdv_need_cont', 999)
        align_n  = live.get('align_need', 1)
        mic_c    = G if micro < -0.5 else (R if micro > 0.5 else Y)
        sc_ok    = score >= score_n; kdv_ok = kdv >= kdv_r
        # score = fusion(soliton + water_hammer + accum + iceberg +
        #                reynolds + shock + cavitation + darcy + mass_flow + cvd + obi)
        print(f'  micro_phase:{mic_c}{micro:>+.3f}{Z}', flush=True)
        print(f'  fused_score:{score:.3f}(need≥{score_n:.3f}){_g(sc_ok)}'
              f'  [soliton+hammer+pool+iceberg+reynolds+shock+cavitation+darcy+flow+cvd+obi]',
              flush=True)
        print(f'  kdv_balance:{kdv:.3f}  '
              f'reversal_gate≥{kdv_r:.3f}{_g(kdv_ok)}  '
              f'continuation_gate≥{kdv_cn:.3f}{_g(kdv >= kdv_cn)}', flush=True)

        # ── Resonance · multi-TF alignment · HTF ────────────────────────
        _sec('resonance · HTF')
        res_align  = getattr(st, 'last_align',     0)
        res_dir    = getattr(st, 'last_res_dir',   0)
        htf_sup    = getattr(st, 'last_htf_sup',   False)
        tf_states  = getattr(st, 'last_tf_states', {})
        last_1m    = getattr(st, 'last_1m_state',  '?')
        ra_ok      = res_align >= align_n
        rd_arrow   = {1: G+'↑'+Z, -1: R+'↓'+Z}.get(res_dir, '→')
        htf_c      = G if htf_sup else R
        print(f'  res_alignment:{res_align:.3f}(need≥{align_n:.3f}){_g(ra_ok)}  '
              f'res_dir:{rd_arrow}  htf_support:{htf_c}{"✓" if htf_sup else "✗"}{Z}',
              flush=True)
        ts_str = '  '.join(f'{W}{k}{Z}:{v}' for k, v in sorted(tf_states.items()))
        print(f'  1m_state:{last_1m}  {ts_str}', flush=True)

    # ── Fusion sub-signals (every component that feeds fused_score) ──────
    if st is not None:
        fus = getattr(st, 'last_fusion', {})
        if fus:
            _sec('fusion sub-signals  (each feeds fused_score)')
            sol  = fus.get('soliton',      {})
            wh   = fus.get('water_hammer', {})
            acc  = fus.get('accum',        {})
            ib   = fus.get('iceberg',      {})
            rey  = fus.get('reynolds',     {})
            shk  = fus.get('shock',        {})
            cav  = fus.get('cavitation',   {})
            dar  = fus.get('darcy',        {})
            mfl  = fus.get('mass_flow',    {})
            cvd  = fus.get('cvd',          {})
            obi_f = fus.get('obi',         {})
            _da = lambda d, *ks, default=0: d.get(ks[0], default) if len(ks)==1 else _da(d.get(ks[0],{}), *ks[1:], default=default)
            def _dir(v): return {1:G+'↑'+Z, -1:R+'↓'+Z}.get(v,'→')
            def _yn(v):  return (G+'YES'+Z) if v else (R+'NO '+Z)
            print(f'  soliton:      dir:{_dir(sol.get("direction",0))}  '
                  f'balance:{sol.get("balance",0):.4f}  '
                  f'amplitude:{sol.get("amplitude",0):.4f}  '
                  f'detected:{_yn(sol.get("detected",False))}', flush=True)
            print(f'  water_hammer: detected:{_yn(wh.get("detected",False))}  '
                  f'dir:{_dir(wh.get("direction",0))}  '
                  f'strength:{wh.get("strength",wh.get("amplitude",0)):.4f}', flush=True)
            print(f'  dark_pool:    firing:{_yn(acc.get("firing",False))}  '
                  f'dir:{_dir(acc.get("direction",0))}  '
                  f'pressure:{acc.get("pressure",acc.get("accumulation",0)):.4f}', flush=True)
            print(f'  iceberg:      detected:{_yn(ib.get("detected",False))}  '
                  f'dir:{_dir(ib.get("direction",0))}  '
                  f'obstruction:{ib.get("obstruction",ib.get("amplitude",0)):.4f}', flush=True)
            print(f'  reynolds:     Re:{rey.get("re",0):.2f}  '
                  f'regime:{rey.get("regime","?")}  '
                  f'multiplier:{rey.get("multiplier",1):.2f}', flush=True)
            print(f'  shock_front:  detected:{_yn(shk.get("detected",False))}  '
                  f'mach:{shk.get("mach",0):.3f}  '
                  f'dir:{_dir(shk.get("direction",0))}  '
                  f'mult:{shk.get("multiplier",1):.2f}', flush=True)
            print(f'  cavitation:   active:{_yn(cav.get("active",False))}  '
                  f'risk:{cav.get("risk",0):.4f}  '
                  f'multiplier:{cav.get("multiplier",1):.2f}', flush=True)
            print(f'  darcy:        Q:{dar.get("Q",0):.4f}  '
                  f'friction:{dar.get("friction",0):.4f}  '
                  f'dir:{_dir(dar.get("direction",0))}', flush=True)
            print(f'  mass_flow:    rate:{mfl.get("rate",mfl.get("amplitude",0)):.4f}  '
                  f'dir:{_dir(mfl.get("direction",0))}', flush=True)
            print(f'  cvd:          divergence:{cvd.get("divergence",0):.4f}  '
                  f'dir:{_dir(cvd.get("direction",0))}  '
                  f'strong:{_yn(cvd.get("strong",False))}  '
                  f'contribution:{cvd.get("contribution",0):.4f}', flush=True)
            obi_v = obi_f.get('obi',0) if isinstance(obi_f,dict) else float(obi_f or 0)
            print(f'  obi_signal:   obi:{obi_v:>+.4f}  '
                  f'contribution:{obi_f.get("contribution",0) if isinstance(obi_f,dict) else 0:.4f}  '
                  f'dir:{_dir(obi_f.get("direction",0) if isinstance(obi_f,dict) else 0)}',
                  flush=True)
            print(f'  fusion_tier:{fus.get("tier","?")}  '
                  f'confidence:{fus.get("confidence",0):.4f}  '
                  f'physics_layer:{fus.get("physics",0):.4f}  '
                  f'micro_layer:{fus.get("micro",0):.4f}', flush=True)

        # ── Waveform bands + interference + targets ───────────────────────
        wf = getattr(st, 'last_wf', {})
        if wf:
            _sec('waveform bands  (4-band decomposition)')
            comp = wf.get('components', {})
            for band in ('micro', 'subharm', 'carrier', 'macro'):
                b = comp.get(band, {})
                if not b:
                    continue
                bph_c = G if b.get('phase',0) < -0.4 else (R if b.get('phase',0) > 0.4 else Y)
                print(f'  {band:<8}  '
                      f'phase:{bph_c}{b.get("phase",0):>+.3f}{Z}  '
                      f'dir:{_dir(b.get("direction",0))}  '
                      f'velocity:{b.get("velocity",0):>+.4f}  '
                      f'amplitude:{b.get("amplitude",0):.4f}  '
                      f'hi:{b.get("hi",0):>10,.2f}  '
                      f'lo:{b.get("lo",0):>10,.2f}', flush=True)
            itf = wf.get('interference', {})
            if itf:
                _sec('waveform interference')
                ic = G if itf.get('type','')=='constructive' else (R if itf.get('type','')=='destructive' else Y)
                print(f'  type:{ic}{itf.get("type","?")}{Z}  '
                      f'score:{itf.get("score",0):>+.4f}  '
                      f'dir:{_dir(itf.get("direction",0))}  '
                      f'dominant:{itf.get("dominant","?")}', flush=True)
                print(f'  aligned:{itf.get("aligned",[])}  '
                      f'opposing:{itf.get("opposing",[])}', flush=True)
                print(f'  amp_sum:{itf.get("amplitude_sum",0):.4f}  '
                      f'carrier_amp:{itf.get("carrier_amp",0):.4f}  '
                      f'macro_amp:{itf.get("macro_amp",0):.4f}  '
                      f'subharm_amp:{itf.get("subharm_amp",0):.4f}', flush=True)
            tgts = wf.get('targets', {})
            if tgts:
                _sec('waveform price targets')
                print(f'  primary:${tgts.get("primary",0):>10,.2f}  '
                      f'extended:${tgts.get("extended",0):>10,.2f}  '
                      f'resonance:${tgts.get("resonance",0):>10,.2f}', flush=True)
                print(f'  confidence:{tgts.get("confidence",0):.4f}  '
                      f'phase_factor:{tgts.get("phase_factor",0):.4f}  '
                      f'carrier_amp:{tgts.get("c_amp",0):.4f}  '
                      f'subharm_amp:{tgts.get("s_amp",0):.4f}  '
                      f'macro_amp:{tgts.get("m_amp",0):.4f}', flush=True)

        # ── Resonance detail — per-TF scores and directions ───────────────
        res = getattr(st, 'last_res', {})
        if res:
            _sec('resonance  (multi-TF soliton alignment)')
            print(f'  score:{res.get("score",0):>+.4f}  '
                  f'alignment:{res.get("alignment",0):.4f}  '
                  f'dir:{_dir(res.get("direction",0))}  '
                  f'dominant:{res.get("dominant_tf","?")}  '
                  f'dissonance:{"YES" if res.get("dissonance") else "NO"}', flush=True)
            tf_sc  = res.get('tf_scores', {})
            tf_dir = res.get('tf_dirs',   {})
            if tf_sc:
                row = '  '.join(f'{W}{tf}{Z}:{tf_sc.get(tf,0):>+.3f}{_dir(tf_dir.get(tf,0))}'
                                for tf in ('1m','5m','15m','1h','4h'))
                print(f'  per-TF scores:  {row}', flush=True)

    # ── Bar counts ────────────────────────────────────────────────────────
    _sec('closed bar counts')
    tfs_all = ('1s', '1m', '5m', '10m', '15m', '30m', '45m', '1h', '4h')
    print(f'  ' + '  '.join(f'{W}{tf}{Z}:{_counts.get(tf,0):>4}' for tf in tfs_all),
          flush=True)

    # ── Per-TF: 3 lines each ──────────────────────────────────────────────
    _sec('per-TF physics  (each TF runs its own adaptive thresholds)')
    for tf, tf_secs in TIMEFRAMES.items():
        p  = _partial.get(tf)
        lv = tf_live.get(tf, {})

        # Line 1: candle progress + velocity + wave direction + ATR
        if p is not None:
            elapsed  = max(0, now_sec - p['_aligned'])
            pct      = min(100, int(elapsed / tf_secs * 100))
            chg      = p['close'] - p['open']
            chg_c    = G if chg >= 0 else R
            buy_pct  = int(p['buy_v'] / p['volume'] * 100) if p['volume'] else 0
            bar_f    = ('█' * (pct // 10)).ljust(10)
            l1_bar   = (f'[{bar_f}]{pct:>3}%  {elapsed:>5}s  '
                        f'{chg_c}{chg:>+8.2f}{Z}  buy:{buy_pct}%')
        else:
            l1_bar = f'{Y}(no bar yet){Z}'

        if lv:
            wdir   = lv.get('wave_dir', 0)
            wamp   = lv.get('wave_amp', 0)
            vel_t  = lv.get('vel',      0)
            atr    = lv.get('atr',      0)
            atr_u  = lv.get('atr_up',   0)
            atr_d  = lv.get('atr_dn',   0)
            pj_pk  = lv.get('proj_peak',   0)
            pj_tr  = lv.get('proj_trough', 0)
            phase  = lv.get('phase',       0)
            obi    = lv.get('obi',         0);  obi_n = lv.get('obi_need',     1)
            score  = lv.get('score',       0);  sc_n  = lv.get('thresh',       1)
            kdv    = lv.get('kdv_bal',     0);  kdv_r = lv.get('kdv_need_rev', 999)
            kdv_c  = lv.get('kdv_need_cont', 999)

            arrow   = {1: G+'↑'+Z, -1: R+'↓'+Z}.get(wdir, '→')
            vel_c_t = G if vel_t > 0 else R
            ph_c    = G if phase < -0.5 else (R if phase > 0.5 else Y)
            obi_ok  = obi >= obi_n; sc_ok = score >= sc_n; kdv_ok = kdv >= kdv_r
            n_ok    = sum([obi_ok, sc_ok, kdv_ok])
            row_c   = G if n_ok == 3 else (Y if n_ok == 2 else '')

            l1_phys = (f'velocity:{vel_c_t}{vel_t:>+.3f}{Z}  '
                       f'swing_amp:${wamp:.1f}  dir:{arrow}  '
                       f'atr:${atr:.1f}(↑{atr_u:.1f}/↓{atr_d:.1f})')
            l2_phys = (f'phase:{ph_c}{phase:>+.3f}{Z}  '
                       f'obi:{obi:>+.3f}(≥{obi_n:.3f}){_g(obi_ok)}  '
                       f'score:{score:.3f}(≥{sc_n:.3f}){_g(sc_ok)}  '
                       f'kdv:{kdv:.3f}(rev≥{kdv_r:.3f}){_g(kdv_ok)}  '
                       f'kdv_cont≥{kdv_c:.3f}{_g(kdv>=kdv_c)}')
            l3_proj = (f'projected  peak:${pj_pk:>10,.2f}  trough:${pj_tr:>10,.2f}')

            print(f'  {row_c}{W}{tf:>4}{Z}  {l1_bar}  {l1_phys}', flush=True)
            print(f'        {l2_phys}', flush=True)
            print(f'        {l3_proj}', flush=True)
        else:
            print(f'  {W}{tf:>4}{Z}  {l1_bar}  {Y}physics pending{Z}', flush=True)

    print(f'{SEPE}', flush=True)

    # ── Signal cards ─────────────────────────────────────────────────────
    if new_sigs:
        for sig in new_sigs:
            _print_signal_card(sig)


def _print_signal_card(sig: dict):
    """Large unmissable signal card — LONG=green solid bar, SHORT=red solid bar."""
    d     = sig.get('dir', 0)
    label = sig.get('label', sig.get('type', '?'))
    price = sig.get('price', sig.get('entry', 0))
    obi   = sig.get('obi',   0)
    cph   = sig.get('cph',   0)
    taker = sig.get('taker', 0)
    nn    = sig.get('n',     '?')
    BAR   = SEPS if d > 0 else SEPR
    arrow = '▲▲  LONG  ▲▲' if d > 0 else '▼▼  SHORT ▼▼'
    c     = G if d > 0 else R
    print(f'\n{BAR}', flush=True)
    print(f'{c}  {arrow}  ·  {label}  [#{nn}]{Z}', flush=True)
    print(f'{c}  {_utc()}  ·  ${price:>12,.2f}{Z}', flush=True)
    print(f'{c}  taker:{taker*100:.0f}%  obi:{obi:>+.3f}  cph:{cph:>+.3f}{Z}',
          flush=True)
    print(f'{BAR}', flush=True)
    print('\a', end='', flush=True)   # terminal bell


# ── Real-time TF aggregation ───────────────────────────────────────────────────

def _on_1s_close(bar1s: dict) -> list:
    """
    Feed one closed 1s bar into every TF partial bar.
    Returns list of (tf, closed_bar) for each TF boundary crossed.
    Increments _counts[tf] for each TF bar that closes.
    """
    ts_sec = bar1s['ts'] // 1000
    _counts['1s'] = _counts.get('1s', 0) + 1

    just_closed = []

    for tf, tf_secs in TIMEFRAMES.items():
        aligned = (ts_sec // tf_secs) * tf_secs
        prev = _partial.get(tf)

        if prev is None or prev['_aligned'] != aligned:
            # Boundary crossed — close the previous partial bar if it exists
            if prev is not None:
                just_closed.append((tf, dict(prev)))
                _counts[tf] = _counts.get(tf, 0) + 1
            # Open a new partial bar
            _partial[tf] = {
                '_aligned': aligned,
                'ts':       aligned * 1000,
                'open':     bar1s['open'],
                'high':     bar1s['high'],
                'low':      bar1s['low'],
                'close':    bar1s['close'],
                'volume':   bar1s['volume'],
                'buy_v':    bar1s['buy_v'],
                'sell_v':   bar1s['sell_v'],
                'n':        bar1s['n'],
            }
        else:
            # Same TF bar — extend the open partial bar
            p = _partial[tf]
            if bar1s['high'] > p['high']:  p['high']   = bar1s['high']
            if bar1s['low']  < p['low']:   p['low']    = bar1s['low']
            p['close']  = bar1s['close']
            p['volume'] += bar1s['volume']
            p['buy_v']  += bar1s['buy_v']
            p['sell_v'] += bar1s['sell_v']
            p['n']      += bar1s['n']

    return just_closed


def _print_tf_close(tf: str, bar: dict):
    """Compact closed-bar line — full status block prints immediately after."""
    buy_pct = int(bar['buy_v'] / bar['volume'] * 100) if bar['volume'] else 0
    bc   = G if bar['close'] >= bar['open'] else R
    chg  = bar['close'] - bar['open']
    cc   = G if chg >= 0 else R
    print(f'{SEP}', flush=True)
    print(f'  {W}{tf} CLOSED  {_utc()}{Z}  '
          f'{cc}{chg:>+.2f}{Z}  '
          f'O:{bar["open"]:>12,.2f}  H:{bar["high"]:>12,.2f}  '
          f'L:{bar["low"]:>12,.2f}  {bc}C:{bar["close"]:>12,.2f}{Z}  '
          f'vol:{bar["volume"]:.4f}  buy:{buy_pct}%  n={bar["n"]}s',
          flush=True)


# ── Disk flush (gzip level 9) ─────────────────────────────────────────────────

def _flush():
    global _last_write
    with _lock:
        lines = list(_raw)
    if not lines:
        return 0
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw_bytes  = ('\n'.join(lines) + '\n').encode()
    compressed = gzip.compress(raw_bytes, compresslevel=9)
    GZ_FILE.write_bytes(compressed)
    _last_write = time.time()
    kb = len(compressed) / 1024
    print(f'  {G}disk{Z}  {len(lines):,} lines → {GZ_FILE.name}'
          f'  ({kb:.1f} KB, gzip-9)', flush=True)
    return len(lines)


# ── Candle file build (disk persistence) ──────────────────────────────────────

def _build():
    """
    Rebuild all TF candle files from disk (historical klines + live ticks).
    Also resets _counts base to file-accurate values and re-seeds _partial
    from the file data so the in-memory state stays consistent.
    """
    global _last_build
    try:
        sys.path.insert(0, str(REPO))
        import build_candles as _bc

        data = _bc.build(verbose=False)
        _bc.save(data, verbose=False)

        # Reset counts from authoritative file data
        for tf, d in data.items():
            _counts[tf] = d['count']

        # Re-seed partial bars from the last bar in each TF
        # (file bars are closed; partial starts fresh at the current boundary)
        # We don't overwrite in-memory partial bars here — they continue from
        # wherever they are in the current second.  The count reset above is enough.

        _last_build = time.time()

        _print_status()   # shows updated counts + edges after build

    except Exception as e:
        print(f'  {Y}build error: {e}{Z}', flush=True)


# ── Git push ──────────────────────────────────────────────────────────────────

def _run_git(*args, env=None):
    return subprocess.run(list(args), capture_output=True, text=True,
                          cwd=str(REPO), env=env, timeout=60)


def _del_obj(sha):
    if sha and len(sha) >= 4:
        (REPO / '.git' / 'objects' / sha[:2] / sha[2:]).unlink(missing_ok=True)


def _push():
    global _last_push
    blob = tree = commit = ''
    try:
        # Clear stale lock files
        for lock in [TMP_IDX,
                     REPO / '.git' / 'refs' / 'heads' / 'data' / 'raw.lock',
                     REPO / '.git' / 'index.lock']:
            lock.unlink(missing_ok=True)

        env_idx = {**os.environ, 'GIT_NO_AUTO_GC': '1',
                   'GIT_INDEX_FILE': str(TMP_IDX)}
        env_obj = {**os.environ, 'GIT_NO_AUTO_GC': '1'}

        # Build the file map: live ticks + all TF files
        push_map = {'data/raw/BTCUSDT_LIVE.jsonl.gz': GZ_FILE}
        for f in sorted(DATA_DIR.glob('tf_*.json.gz')):
            push_map[f'data/raw/{f.name}'] = f
        stats = DATA_DIR / 'tf_candles.json'
        if stats.exists():
            push_map['data/raw/tf_candles.json'] = stats
        push_map = {k: v for k, v in push_map.items()
                    if v.exists() and v.stat().st_size > 0}
        if not push_map:
            return

        # Seed the index from the existing data/raw tree
        r = _run_git('git', 'rev-parse', '--verify', 'origin/data/raw')
        parent = r.stdout.strip() if r.returncode == 0 else ''
        if parent:
            _run_git('git', 'read-tree', 'origin/data/raw', env=env_idx)

        blobs = {}
        for git_path, local_path in push_map.items():
            r = _run_git('git', 'hash-object', '-w', str(local_path), env=env_obj)
            sha = r.stdout.strip()
            if sha:
                blobs[git_path] = sha
                _run_git('git', 'update-index', '--add', '--cacheinfo',
                         f'100644,{sha},{git_path}', env=env_idx)

        blob = next(iter(blobs.values()), '')
        r = _run_git('git', 'write-tree', env=env_idx)
        tree = r.stdout.strip()
        TMP_IDX.unlink(missing_ok=True)
        if not tree:
            return

        with _lock:
            n = len(_raw)
        cmd = ['git', 'commit-tree', tree, '-m', f'A1 ticks={n} ts={int(time.time())}']
        if parent:
            cmd += ['-p', parent]
        r = _run_git(*cmd, env=env_obj)
        commit = r.stdout.strip()
        if not commit:
            return

        r = _run_git('git', 'push', '--force', 'origin',
                     f'{commit}:refs/heads/data/raw')
        if r.returncode != 0:
            print(f'  {Y}push failed: {r.stderr.strip()[:120]}{Z}', flush=True)
            return

        _last_push = time.time()
        for sha in set(list(blobs.values()) + [tree, commit]):
            _del_obj(sha)

        print(f'\n  {G}push{Z}  {n:,} ticks + {len(push_map)} files → data/raw', flush=True)
        for git_path, local_path in push_map.items():
            kb = local_path.stat().st_size / 1024
            print(f'        {git_path}  ({kb:.1f} KB)', flush=True)
        print(flush=True)

    except Exception as e:
        print(f'  {Y}push error: {e}{Z}', flush=True)
        TMP_IDX.unlink(missing_ok=True)
        for sha in (blob, tree, commit):
            _del_obj(sha)


# ── Physics engine (wave_scan) ────────────────────────────────────────────────

def _init_physics(seed_bars_1m: list):
    """Seed ScanState from 1m bars. Called once at startup."""
    global _scan_state
    try:
        import wave_scan as _ws
        _scan_state = _ws.ScanState()
        _ws.scan_incremental(_scan_state, raw_lines=[], signals_only=True,
                             seed_bars=seed_bars_1m)
        print(f'  {G}physics{Z}  seeded  '
              f'({len(_scan_state.closed_1m)} 1m bars)', flush=True)
    except Exception as e:
        print(f'  {Y}physics init (non-fatal): {e}{Z}', flush=True)


def _run_physics():
    """
    Incremental physics tick — captures wave_scan stdout (we handle display),
    collects new_sigs, then calls _print_status() to render everything.
    Called every 5 1s-bar closes and on any TF boundary crossing.
    """
    try:
        import wave_scan as _ws
        with _lock:
            raw_snap = list(_raw)

        # Suppress wave_scan's own prints; we render signals our way
        _buf = io.StringIO()
        _old = sys.stdout
        sys.stdout = _buf
        try:
            new_sigs = (_ws.scan_incremental(_scan_state, raw_lines=raw_snap,
                                             signals_only=True)
                        if _scan_state is not None else [])
        finally:
            sys.stdout = _old

        _print_status(new_sigs if new_sigs else None)

    except Exception as e:
        sys.stdout = sys.__stdout__   # safety restore
        print(f'  {Y}physics: {e}{Z}', flush=True)
        _print_status()   # still show candle edges even if physics errors


# ── Background worker — write / build / push on timers ────────────────────────

def _worker():
    _flush()
    _build()   # initial build: set _counts base from any existing files
    while True:
        time.sleep(1)
        now = time.time()
        if now - _last_write >= WRITE_S:
            _flush()
        if now - _last_build >= BUILD_S:
            _flush()     # write ticks first so build sees them
            _build()
        if now - _last_push >= PUSH_S:
            _flush()
            _push()


# ── WebSocket handler ─────────────────────────────────────────────────────────

async def _stream():
    global _n_ticks, _n_depth, _bar_sec, _bar

    backoff = 2
    while True:
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.ws_connect(WS_URL, heartbeat=20) as ws:
                    print(f'  {G}connected{Z} → Binance.US  '
                          f'({_utc()} UTC)\n', flush=True)
                    backoff = 2

                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue

                        obj  = json.loads(msg.data)
                        d    = obj.get('data', obj)
                        name = obj.get('stream', '')

                        # ── Trade tick ───────────────────────────────────────
                        if name.endswith('aggTrade'):
                            ts_ms   = int(d['T'])
                            price   = float(d['p'])
                            qty     = float(d['q'])
                            is_sell = bool(d['m'])   # m=True → sell taker
                            p_int   = round(price * 100)
                            q_int   = round(qty   * 10000)

                            # Exact record written to BTCUSDT_LIVE.jsonl.gz
                            line = json.dumps(['T', ts_ms, p_int, q_int, int(is_sell)])
                            with _lock:
                                _raw.append(line)
                            _n_ticks += 1

                            side_c = f'{R}SELL{Z}' if is_sell else f'{G}BUY {Z}'
                            print(f'{_utc()} T  ${price:>12,.2f}  {qty:>10.6f}  '
                                  f'{side_c}  {line}', flush=True)

                            # ── 1s bar accumulation ──────────────────────────
                            sec = ts_ms // 1000

                            if sec != _bar_sec and _bar is not None:
                                # ── 1s bar just closed ───────────────────────
                                b = _bar
                                buy_pct = int(b['bv'] / b['v'] * 100) if b['v'] else 0
                                bc = G if b['c'] >= b['o'] else R
                                print(
                                    f'{_utc()} {W}1s{Z}  '
                                    f'O:{b["o"]:>12,.2f}  '
                                    f'H:{b["h"]:>12,.2f}  '
                                    f'L:{b["l"]:>12,.2f}  '
                                    f'{bc}C:{b["c"]:>12,.2f}{Z}  '
                                    f'V:{b["v"]:.4f}btc  buy:{buy_pct}%  '
                                    f'n={b["n"]}',
                                    flush=True)

                                # Feed closed 1s bar into every TF
                                bar1s = {
                                    'ts':     _bar_sec * 1000,
                                    'open':   b['o'], 'high': b['h'],
                                    'low':    b['l'], 'close': b['c'],
                                    'volume': b['v'], 'buy_v': b['bv'],
                                    'sell_v': b['sv'], 'n': b['n'],
                                }
                                just_closed = _on_1s_close(bar1s)

                                # Print any TF bars that just closed
                                # Sort by TF size so 1m prints before 5m etc.
                                just_closed.sort(key=lambda x: TIMEFRAMES[x[0]])
                                for tf, closed_bar in just_closed:
                                    _print_tf_close(tf, closed_bar)

                                # Every 5s (or on any TF close): unified status block
                                global _snap_count
                                _snap_count += 1
                                if _snap_count % 5 == 0 or just_closed:
                                    _run_physics()

                                _bar = None

                            # Accumulate into the in-progress 1s bar
                            if _bar is None:
                                _bar = {'o': price, 'h': price, 'l': price,
                                        'c': price, 'v': 0.0, 'bv': 0.0,
                                        'sv': 0.0, 'n': 0}
                                _bar_sec = sec

                            b = _bar
                            if price > b['h']:  b['h'] = price
                            if price < b['l']:  b['l'] = price
                            b['c']  = price
                            b['v'] += qty
                            b['n'] += 1
                            if is_sell:
                                b['sv'] += qty
                            else:
                                b['bv'] += qty

                        # ── Depth snapshot (depth20@100ms) ───────────────────
                        elif name.endswith('depth20@100ms'):
                            ts_ms = int(time.time() * 1000)
                            bids  = [[round(float(p) * 100), round(float(q) * 10000)]
                                     for p, q in d.get('bids', [])[:20]]
                            asks  = [[round(float(p) * 100), round(float(q) * 10000)]
                                     for p, q in d.get('asks', [])[:20]]
                            line  = json.dumps(['D', ts_ms, bids, asks])
                            with _lock:
                                _raw.append(line)
                            _n_depth += 1

                            # Print every 10th — 10/s arrive, 1/s shows on screen
                            if _n_depth % 10 == 0 and bids and asks:
                                bid0 = bids[0][0] / 100
                                ask0 = asks[0][0] / 100
                                spr  = ask0 - bid0
                                bv20 = sum(q for _, q in bids) / 10000
                                av20 = sum(q for _, q in asks) / 10000
                                print(f'{_utc()} D  bid ${bid0:>12,.2f}'
                                      f'  ask ${ask0:>12,.2f}'
                                      f'  spr ${spr:.2f}'
                                      f'  bv={bv20:.2f}  av={av20:.2f}'
                                      f'  D#{_n_depth}',
                                      flush=True)

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f'  {Y}ws: {e} — retry {backoff}s{Z}', flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    _hdr()

    print(f'  {Y}fetching historical candles (ingest)…{Z}', flush=True)
    try:
        sys.path.insert(0, str(REPO))
        import ingest as _ig
        _ig.run(verbose=True)
    except Exception as e:
        print(f'  {Y}ingest error (continuing): {e}{Z}', flush=True)

    # Seed physics engine from pre-built 1m candles
    print(f'  {Y}seeding physics engine…{Z}', flush=True)
    try:
        _seed_path = DATA_DIR / 'tf_1m.json.gz'
        if _seed_path.exists():
            with gzip.open(_seed_path, 'rt') as _sf:
                _raw_1m = json.loads(_sf.read())
            # wave_scan expects taker_buy field
            _seed_1m = [dict(b, taker_buy=b.get('buy_v', b.get('volume', 0) * 0.5))
                        for b in _raw_1m]
            _init_physics(_seed_1m)
        else:
            print(f'  {Y}tf_1m.json.gz not found — physics starts cold{Z}', flush=True)
    except Exception as e:
        print(f'  {Y}physics seed (non-fatal): {e}{Z}', flush=True)

    threading.Thread(target=_worker, daemon=True).start()

    loop = asyncio.new_event_loop()
    task = loop.create_task(_stream())
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, task.cancel)
    try:
        loop.run_until_complete(task)
    finally:
        print(f'\n  {Y}shutting down — final flush + push…{Z}', flush=True)
        _flush()
        _push()
        loop.close()
        print('  done.\n', flush=True)
