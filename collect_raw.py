#!/usr/bin/env python3
"""
collect_raw.py — WebSocket → deque → pipeline to wave_scan + GitHub backup.

Pipeline:
  WebSocket → deque(maxlen=15000) → ~/.trendrider/BTCUSDT_LIVE.jsonl.gz
                                  → git push queue → GitHub data/raw

Data dir is ~/.trendrider/ — always writable, outside the repo, not git-tracked.
Git push is queued (non-blocking): write never waits on network.

Trade line : ["T", ts_ms, price_cents, qty_units, side]
Depth line : ["D", ts_ms, [[p_c,q_u],...bids], [[p_c,q_u],...asks]]
"""

import asyncio, collections, gc, gzip, io, json, os, queue, resource
import signal, subprocess, sys, threading, time
from pathlib import Path
from datetime import datetime, timezone

try:
    import aiohttp
except ImportError:
    print("ERROR: pip install aiohttp"); sys.exit(1)

REPO      = Path(__file__).resolve().parent
DATA_DIR  = Path.home() / '.trendrider'          # pipeline data dir — always writable
GZ_FILE   = DATA_DIR / 'BTCUSDT_LIVE.jsonl.gz'  # wave_scan reads this
TMP_IDX   = REPO / '.git' / 'data_push.idx'

DATA_BRANCH   = 'data/raw'
GIT_TREE_PATH = 'data/raw/BTCUSDT_LIVE.jsonl.gz'
SESSIONS_BRANCH_PREFIX = 'data/sessions/'   # archive — one branch per session, never overwritten

WS_URL = ('wss://stream.binance.us:9443/stream'
          '?streams=btcusdt@depth20@100ms/btcusdt@aggTrade')

# Deep book — REST poll for 100 levels, refreshed every second
DEEP_BOOK_URL  = 'https://api.binance.us/api/v3/depth?symbol=BTCUSDT&limit=100'
DEEP_POLL_S    = 1.0   # poll interval seconds

WINDOW_LINES   = 15_000   # bounded deque — ~125 min at 2 records/sec
PUSH_S         = 2        # write snapshot every N seconds
GC_EVERY       = 60
LOG_MEM_EVERY  = 300
SIG_PUSH_S     = 60       # push signals to git at most once per minute

SIGNALS_DIR  = DATA_DIR / 'signals'
SIGNALS_TXT  = SIGNALS_DIR / 'BTCUSDT_SIGNALS.txt'
SIGNALS_JSON = SIGNALS_DIR / 'BTCUSDT_SIGNALS.json'
SIG_BRANCH   = 'data/signals'
SIG_IDX      = REPO / '.git' / 'scan_push.idx'
TF_VIEW_FILE = DATA_DIR / 'tf_view'   # live TF selector — write anytime, no restart

_raw_deque    = collections.deque(maxlen=WINDOW_LINES)
_deque_lock   = threading.Lock()
_push_queue   = queue.Queue(maxsize=1)   # non-blocking git push pipeline
_scan_trigger = threading.Event()        # set by on_trade/on_depth; scanner also wakes on timeout
_git_lock     = threading.Lock()         # serialize all git operations — one at a time
_last_trade_sec = 0
_last_depth_sec = 0

# ── Deep order book (separate slow stream, diff-based) ────────────────────────
# Fast 20-level stream feeds the physics engine unchanged.
# Deep book is maintained here for directional/target confirmation only.
_deep_bids   = {}   # price → qty  (full book, bid side)
_deep_asks   = {}   # price → qty  (full book, ask side)
_deep_lock   = threading.Lock()
_deep_ready  = False
_deep_probe  = {}
_deep_status = 'init'   # last error or 'ok' — visible in signals JSON

# Per-level history for friction classification
# price → collections.deque of (ts_ms, qty) — last 40 snapshots
_level_hist       = {}
_level_first_seen = {}   # (side, price) → ts_ms when first seen in diff stream
_level_last_seen  = {}   # (side, price) → ts_ms of most recent diff update
# Reload tracker: price → {'ts': ms, 'qty': qty_before_pull, 'side': str}
_reload_pend = {}
# Reload events: price → {'ratio': float, 'side': str, 'ts': ms}
# ratio = new_qty / (prev_qty - filled_qty)
# >1 = reloaded more than consumed (defending), <1 = withdrawing, ~1 = MM
_reload_evts = {}   # kept for last 60s

GIT_TIMEOUT = 60   # seconds before a git subprocess is killed

G='\033[92m'; R='\033[91m'; Y='\033[93m'; Z='\033[0m'
C='\033[96m'; B='\033[1m'
def log(m, c=Z): print(f'{c}[raw] {m}{Z}', flush=True)

VALID_TFS   = ('1m', '5m', '10m', '15m', '30m', '45m', '1h', '4h')
_TF_SECS    = {'1m':60,'5m':300,'10m':600,'15m':900,'30m':1800,'45m':2700,'1h':3600,'4h':14400}
_last_dash_t  = 0.0
_DASH_MIN_S   = 1.0
_last_n_lines = 0   # lines in previous dashboard — drives in-place cursor rewrite


def _live_partial(state, tf):
    """Build in-progress bar for `tf` from the per-second tick store."""
    s1 = getattr(state, 's1_by_sec', {})
    if not s1:
        return None
    tf_secs   = _TF_SECS[tf]
    last_sec  = max(s1)
    win_start = (last_sec // tf_secs) * tf_secs
    secs      = sorted(s for s in s1 if win_start <= s <= last_sec)
    if not secs:
        return None
    bars = [s1[s] for s in secs]
    vol  = sum(b['volume'] for b in bars)
    buy  = sum(b.get('taker_buy', 0) for b in bars)
    chg  = bars[-1]['close'] - bars[0]['open']
    el   = max(1, len(secs))
    return {
        'elapsed':  el,
        'pct':      min(100, el * 100 // tf_secs),
        'open':     bars[0]['open'],
        'high':     max(b['high'] for b in bars),
        'low':      min(b['low']  for b in bars),
        'close':    bars[-1]['close'],
        'volume':   vol,
        'buy_v':    buy,
        'chg':      chg,
        'vel':      chg / el,          # $/s
        'buy_pct':  int(buy / vol * 100) if vol else 0,
    }


def _dashboard(state, tfs, recent_signals):
    """Full-screen live dashboard — ALL physics sub-signals exposed.

    TF selection (live, no restart needed):
      echo '1m 5m 1h'  > ~/.trendrider/tf_view
      echo 'all'       > ~/.trendrider/tf_view
      rm ~/.trendrider/tf_view   # back to --tf default
    """
    global _last_dash_t, _last_n_lines
    now_t = time.monotonic()
    if now_t - _last_dash_t < _DASH_MIN_S:
        return
    _last_dash_t = now_t

    active_tfs = tfs
    try:
        txt = TF_VIEW_FILE.read_text().strip()
        if txt.lower() in ('all', 'default', ''):
            active_tfs = list(VALID_TFS)
        else:
            chosen = [t for t in txt.split() if t in VALID_TFS]
            if chosen:
                active_tfs = chosen
    except Exception:
        pass

    live    = getattr(state, 'live', {})
    tf_live = getattr(state, 'tf_live', {})
    now_s   = datetime.now(timezone.utc).strftime('%H:%M:%S')

    price    = live.get('price',    0.0)
    dk       = live.get('dk',       '?')
    vel      = live.get('vel',      0.0)
    conc     = live.get('conc',     0.0)
    spr      = live.get('spr',      0.0)
    decay_sc = live.get('decay_sc', 0.0)
    floor_sc = live.get('floor_sc', 0.0)
    bf       = live.get('bid_flow', 0.0)
    af       = live.get('ask_flow', 0.0)
    obi_g    = live.get('obi',      0.0)
    obi_n    = live.get('obi_need', 0.0)
    score_g  = live.get('score',    0.0)
    score_n  = live.get('score_need', 0.0)
    kdv_g    = live.get('kdv_bal',  0.0)
    kdv_rev  = live.get('kdv_need_rev',  0.0)
    kdv_con  = live.get('kdv_need_cont', 0.0)
    res_al   = live.get('res_align',     0.0)
    res_dir  = live.get('res_dir',       0)
    htf_sup  = live.get('htf_sup',       False)
    micro_ph = live.get('micro_ph',      0.0)
    mph_pk   = live.get('micro_ph_peak',   0.75)
    mph_tr   = live.get('micro_ph_trough', -0.75)
    align_n  = live.get('align_need',    0.0)
    htf_bars = live.get('htf_bars',      '')
    s_open   = live.get('sess_open',     0.0)
    s_high   = live.get('sess_high',     0.0)
    s_low    = live.get('sess_low',      0.0)
    s_range  = live.get('sess_range',    0.0)
    s_chg    = live.get('sess_chg',      0.0)
    s_pct    = live.get('sess_chg_pct',  0.0)
    tr_4h    = live.get('trend_4h',      0)
    tr_1h    = live.get('trend_1h',      0)

    def _trend(t): return (f'{G}BULL▲{Z}' if t > 0 else
                           (f'{R}BEAR▼{Z}' if t < 0 else f'{Y}COIL─{Z}'))
    def _yn(v):  return (G+'YES'+Z) if v else (R+'NO '+Z)
    def _dir(v): return {1:G+'↑'+Z, -1:R+'↓'+Z}.get(int(v) if v else 0, '─')
    def _gx(ok): return f'{G}✓{Z}' if ok else f'{R}✗{Z}'

    W = 68
    rows = []

    # ── price / session ──────────────────────────────────────────────────
    rows.append(f'{B}{C}{"─"*W}{Z}')
    rows.append(f'{B}{C}  BTC ${price:>11,.2f}   {now_s}   dk={dk}{Z}')
    rows.append(f'{C}{"─"*W}{Z}')
    if s_open > 0:
        cc = G if s_chg >= 0 else R
        rows.append(f'  SESSION  o=${s_open:,.2f}  h=${s_high:,.2f}'
                    f'  l=${s_low:,.2f}  rng=${s_range:,.0f}'
                    f'  {cc}{s_chg:+,.2f}({s_pct:+.2f}%){Z}')
    rows.append(f'  TREND  4h={_trend(tr_4h)}  1h={_trend(tr_1h)}'
                f'  htf_sup={G+"▲SUP"+Z if htf_sup else Y+"no-sup"+Z}')

    # ── deep book ────────────────────────────────────────────────────────
    dp = _deep_probe
    if dp:
        bs = (f'{G}▲PULL{Z}' if dp.get('bias',0) > 0
              else (f'{R}▼PULL{Z}' if dp.get('bias',0) < 0 else f'{Y}FLAT{Z}'))
        aw = dp.get('nearest_ask_wall'); bw = dp.get('nearest_bid_wall')
        aw_s = f'ask_wall=${aw:,.0f}(+${aw-price:,.0f})' if aw else 'ask_wall=─'
        bw_s = f'bid_wall=${bw:,.0f}(-${price-bw:,.0f})' if bw else 'bid_wall=─'
        rows.append(f'  DEEP({dp.get("levels_bid",0)}b/{dp.get("levels_ask",0)}a)'
                    f'  deep_obi={dp.get("deep_obi",0):+.3f}  {bs}'
                    f'  {aw_s}  {bw_s}')
        rows.append(f'  deep_gravity  bid=${dp.get("gravity_bid",0):,.2f}'
                    f'  ask=${dp.get("gravity_ask",0):,.2f}'
                    f'  reload_bid_defending:{dp.get("reloads_bid_defending",0)}'
                    f'  ask_defending:{dp.get("reloads_ask_defending",0)}')

    # ── order book / live physics input ──────────────────────────────────
    rows.append(f'{C}{"─"*W}{Z}')
    n_buf = len(_raw_deque)
    rows.append(f'  input: {W}{n_buf:,}{Z} lines in buffer'
                f'  (trades T + depth snapshots D — both feed physics)')
    sc_ok = abs(score_g) >= score_n > 0
    mok   = micro_ph <= mph_tr or micro_ph >= mph_pk
    ra_ok = res_al >= align_n
    rows.append(f'  vel={vel:+.4f}  obi={obi_g:+.3f}(need≥{obi_n:.3f})'
                f'  conc={conc:+.3f}  spr={spr:.2f}')
    rows.append(f'  fused_score={G if sc_ok else Z}{score_g:+.4f}{Z}(need≥{score_n:.3f}){_gx(sc_ok)}'
                f'  kdv={kdv_g:.4f}  rev≥{kdv_rev:.3f}  cont≥{kdv_con:.3f}')
    rows.append(f'  micro_phase={G if mok else Z}{micro_ph:+.4f}{Z}'
                f'[trough:{mph_tr:.3f} peak:{mph_pk:.3f}]'
                f'  res_alignment={res_al:.3f}(need≥{align_n:.3f}){_gx(ra_ok)}')
    dir_str = f'{G}▲{Z}' if res_dir > 0 else (f'{R}▼{Z}' if res_dir < 0 else '─')
    rows.append(f'  decay_score={decay_sc:.3f}  floor_score={floor_sc:.3f}'
                f'  bid_flow={bf:+.4f}  ask_flow={af:+.4f}  {dir_str}')
    if htf_bars:
        rows.append(f'  htf_bars: {htf_bars}')

    # ── per-TF blocks ─────────────────────────────────────────────────────
    # [LIVE] = score/phase/kdv_bal/obi update every scan tick from partial-bar physics
    # [bar]  = vel/atr/wave_dir/thresholds freeze until a full bar closes
    rows.append(f'{C}{"─"*W}{Z}')
    rows.append(f'{C}  per-TF  [LIVE]=updates every tick  [bar]=refreshes on close{Z}')

    for tf in active_tfs:
        lv          = tf_live.get(tf, {})
        score       = lv.get('score',        0.0)
        thresh      = lv.get('thresh',        0.0)
        phase       = lv.get('phase',         0.0)
        proj_peak   = lv.get('proj_peak',     0.0)
        proj_trough = lv.get('proj_trough',   0.0)
        wave_amp    = lv.get('wave_amp',      0.0)
        wave_dir    = lv.get('wave_dir',      0)
        atr_tf      = lv.get('atr',          0.0)
        atr_up_tf   = lv.get('atr_up',       0.0)
        atr_dn_tf   = lv.get('atr_dn',       0.0)
        kdv_bal     = lv.get('kdv_bal',       0.0)
        kdv_rev     = lv.get('kdv_need_rev',  0.0)
        kdv_con     = lv.get('kdv_need_cont', 0.0)
        obi         = lv.get('obi',           0.0)
        obi_need    = lv.get('obi_need',      0.0)
        al          = lv.get('align_need',    0.0)

        # live partial bar from s1_by_sec
        pb = _live_partial(state, tf)
        if pb:
            pct   = pb['pct']
            bar_f = ('█' * (pct // 10)).ljust(10)
            cc    = G if pb['chg'] >= 0 else R
            vc    = G if pb['vel'] >= 0 else R
            l0    = (f'  {B}{tf:<4}{Z} [{bar_f}]{pct:>3}%  {pb["elapsed"]:>5}s  '
                     f'{cc}{pb["chg"]:>+8.2f}{Z}  buy:{pb["buy_pct"]}%  '
                     f'live_vel:{vc}{pb["vel"]:>+.3f}{Z}$/s')
            l1    = (f'       O:{pb["open"]:>10,.2f}  H:{pb["high"]:>10,.2f}'
                     f'  L:{pb["low"]:>10,.2f}  C:{pb["close"]:>10,.2f}'
                     f'  vol:{pb["volume"]:.4f}  bvol:{pb["buy_v"]:.4f}')
        else:
            l0 = f'  {B}{tf:<4}{Z} {Y}(partial bar pending){Z}'
            l1 = ''

        sc_hit  = abs(score) >= thresh > 0
        obi_hit = abs(obi)   >= obi_need > 0
        kdv_hit = kdv_bal >= kdv_rev > 0
        ph_c    = G if phase < -0.5 else (R if phase > 0.5 else Y)
        n_ok    = sum([sc_hit, obi_hit, kdv_hit])
        near_pk = proj_peak   > 0 and price >= proj_peak   * 0.998
        near_tr = proj_trough > 0 and price <= proj_trough * 1.002
        if wave_dir == 1 and near_pk:   state_s = f'{R}PEAK▼{Z}'
        elif wave_dir == -1 and near_tr: state_s = f'{G}TRGR▲{Z}'
        elif wave_dir == 1:              state_s = f'{G}UP  ▲{Z}'
        elif wave_dir == -1:             state_s = f'{R}DN  ▼{Z}'
        else:                            state_s = f'{Y}MID  {Z}'

        atr_s = (f'  atr:${atr_tf:,.2f}(↑{atr_up_tf:.2f}/↓{atr_dn_tf:.2f})'
                 if atr_tf > 0 else '')
        l2 = (f'  {C}[LIVE]{Z} {state_s}'
              f'  phase:{ph_c}{phase:>+.4f}{Z}'
              f'  score:{G if sc_hit else Z}{score:>+.4f}{Z}(≥{thresh:.3f}){_gx(sc_hit)}'
              f'  obi:{G if obi_hit else Z}{obi:>+.4f}{Z}(≥{obi_need:.3f}){_gx(obi_hit)}'
              f'  kdv:{G if kdv_hit else Z}{kdv_bal:.4f}{Z}(rev≥{kdv_rev:.3f}){_gx(kdv_hit)}')
        l3 = (f'  {Y}[bar]{Z}  dir:{_dir(wave_dir)}'
              f'  swing_amp:${wave_amp:,.2f}'
              f'  proj_peak:${proj_peak:>10,.2f}  proj_trough:${proj_trough:>10,.2f}'
              f'{atr_s}'
              f'  kdv_cont≥{kdv_con:.3f}{_gx(kdv_bal>=kdv_con)}'
              f'  align_need:{al:.3f}')

        rows.append(l0)
        if l1: rows.append(l1)
        rows.append(l2)
        rows.append(l3)

    # ── fusion sub-signals — every component that contributes to fused_score ──
    rows.append(f'{C}{"─"*W}{Z}')
    fus = getattr(state, 'last_fusion', {})
    if fus:
        rows.append(f'{C}  fusion sub-signals  (all feed fused_score={score_g:+.4f}){Z}')
        sol = fus.get('soliton',      {})
        wh  = fus.get('water_hammer', {})
        acc = fus.get('accum',        {})
        ib  = fus.get('iceberg',      {})
        rey = fus.get('reynolds',     {})
        shk = fus.get('shock',        {})
        cav = fus.get('cavitation',   {})
        dar = fus.get('darcy',        {})
        mfl = fus.get('mass_flow',    {})
        cvd = fus.get('cvd',          {})
        obi_f = fus.get('obi',        {})
        rows.append(f'  soliton:      detected:{_yn(sol.get("detected",False))}'
                    f'  dir:{_dir(sol.get("direction",0))}'
                    f'  balance:{sol.get("balance",0):.4f}'
                    f'  amplitude:{sol.get("amplitude",0):.4f}')
        rows.append(f'  water_hammer: detected:{_yn(wh.get("detected",False))}'
                    f'  dir:{_dir(wh.get("direction",0))}'
                    f'  strength:{wh.get("strength",wh.get("amplitude",0)):.4f}')
        rows.append(f'  dark_pool:    firing:{_yn(acc.get("firing",False))}'
                    f'  dir:{_dir(acc.get("direction",0))}'
                    f'  pressure:{acc.get("pressure",acc.get("accumulation",0)):.4f}')
        rows.append(f'  iceberg:      detected:{_yn(ib.get("detected",False))}'
                    f'  dir:{_dir(ib.get("direction",0))}'
                    f'  obstruction:{ib.get("obstruction",ib.get("amplitude",0)):.4f}')
        rows.append(f'  reynolds:     Re:{rey.get("re",0):.2f}'
                    f'  regime:{rey.get("regime","?")}  multiplier:{rey.get("multiplier",1):.3f}')
        rows.append(f'  shock_front:  detected:{_yn(shk.get("detected",False))}'
                    f'  mach:{shk.get("mach",0):.4f}'
                    f'  dir:{_dir(shk.get("direction",0))}'
                    f'  mult:{shk.get("multiplier",1):.3f}')
        rows.append(f'  cavitation:   active:{_yn(cav.get("active",False))}'
                    f'  risk:{cav.get("risk",0):.4f}'
                    f'  multiplier:{cav.get("multiplier",1):.3f}')
        rows.append(f'  darcy:        Q:{dar.get("Q",0):.4f}'
                    f'  friction:{dar.get("friction",0):.4f}'
                    f'  dir:{_dir(dar.get("direction",0))}')
        rows.append(f'  mass_flow:    rate:{mfl.get("rate",mfl.get("amplitude",0)):.4f}'
                    f'  dir:{_dir(mfl.get("direction",0))}')
        rows.append(f'  cvd:          divergence:{cvd.get("divergence",0):.4f}'
                    f'  dir:{_dir(cvd.get("direction",0))}'
                    f'  strong:{_yn(cvd.get("strong",False))}'
                    f'  contribution:{cvd.get("contribution",0):.4f}')
        obi_v = obi_f.get('obi',0) if isinstance(obi_f,dict) else float(obi_f or 0)
        rows.append(f'  obi_signal:   obi:{obi_v:>+.4f}'
                    f'  contribution:{obi_f.get("contribution",0) if isinstance(obi_f,dict) else 0:.4f}'
                    f'  dir:{_dir(obi_f.get("direction",0) if isinstance(obi_f,dict) else 0)}')
        rows.append(f'  fusion_tier:{fus.get("tier","?")}  confidence:{fus.get("confidence",0):.4f}'
                    f'  physics_layer:{fus.get("physics",0):.4f}'
                    f'  micro_layer:{fus.get("micro",0):.4f}')
    else:
        rows.append(f'  {Y}fusion sub-signals: warming up (first 1m cycle not complete yet){Z}')

    # ── waveform bands ───────────────────────────────────────────────────
    wf = getattr(state, 'last_wf', {})
    if wf:
        rows.append(f'{C}  waveform bands  (4-band decomposition){Z}')
        comp = wf.get('components', {})
        for band in ('micro', 'subharm', 'carrier', 'macro'):
            b = comp.get(band, {})
            if not b: continue
            bph_c = G if b.get('phase',0) < -0.4 else (R if b.get('phase',0) > 0.4 else Y)
            rows.append(f'  {band:<8}  phase:{bph_c}{b.get("phase",0):>+.3f}{Z}'
                        f'  dir:{_dir(b.get("direction",0))}'
                        f'  velocity:{b.get("velocity",0):>+.4f}'
                        f'  amplitude:{b.get("amplitude",0):.4f}'
                        f'  hi:${b.get("hi",0):>10,.2f}  lo:${b.get("lo",0):>10,.2f}')
        itf = wf.get('interference', {})
        if itf:
            ic = G if itf.get('type','')=='constructive' else (R if itf.get('type','')=='destructive' else Y)
            rows.append(f'  interference: type:{ic}{itf.get("type","?")}{Z}'
                        f'  score:{itf.get("score",0):>+.4f}'
                        f'  dir:{_dir(itf.get("direction",0))}'
                        f'  dominant:{itf.get("dominant","?")}')
            rows.append(f'               aligned:{itf.get("aligned",[])}  opposing:{itf.get("opposing",[])}')
            rows.append(f'               amp_sum:{itf.get("amplitude_sum",0):.4f}'
                        f'  carrier:{itf.get("carrier_amp",0):.4f}'
                        f'  macro:{itf.get("macro_amp",0):.4f}'
                        f'  subharm:{itf.get("subharm_amp",0):.4f}')
        tgts = wf.get('targets', {})
        if tgts:
            rows.append(f'  targets: primary:${tgts.get("primary",0):>10,.2f}'
                        f'  extended:${tgts.get("extended",0):>10,.2f}'
                        f'  resonance:${tgts.get("resonance",0):>10,.2f}')
            rows.append(f'           confidence:{tgts.get("confidence",0):.4f}'
                        f'  phase_factor:{tgts.get("phase_factor",0):.4f}')

    # ── resonance — per-TF soliton scores ────────────────────────────────
    res = getattr(state, 'last_res', {})
    if res:
        rows.append(f'{C}  resonance  (multi-TF soliton alignment){Z}')
        rows.append(f'  score:{res.get("score",0):>+.4f}'
                    f'  alignment:{res.get("alignment",0):.4f}'
                    f'  dir:{_dir(res.get("direction",0))}'
                    f'  dominant:{res.get("dominant_tf","?")}'
                    f'  dissonance:{"YES" if res.get("dissonance") else "NO"}')
        tf_sc  = res.get('tf_scores', {})
        tf_dir = res.get('tf_dirs',   {})
        if tf_sc:
            row_r = '  '.join(f'{B}{tf}{Z}:{tf_sc.get(tf,0):>+.3f}{_dir(tf_dir.get(tf,0))}'
                              for tf in ('1m','5m','15m','1h','4h'))
            rows.append(f'  per-TF:  {row_r}')

    # ── recent signals ────────────────────────────────────────────────────
    rows.append(f'{C}{"─"*W}{Z}')
    if recent_signals:
        for sig in recent_signals[-5:]:
            d   = sig.get('dir', 0)
            lbl = sig.get('label', sig.get('stype', '?'))
            px  = sig.get('price', 0.0)
            t   = sig.get('time', '')
            cl  = G if d > 0 else R
            rows.append(f'  {cl}{"▲" if d>0 else "▼"} {lbl:<18} {t}  ${px:,.2f}{Z}')

    cur = ' '.join(active_tfs)
    rows.append(f'\033[2m  view:{cur}'
                f'  |  echo "1m 5m 1h" > {TF_VIEW_FILE.name}'
                f'  |  echo all > {TF_VIEW_FILE.name}\033[0m')

    content  = '\n'.join(rows)
    n_lines  = content.count('\n') + 1
    import sys as _sys
    if _last_n_lines > 0:
        # Move cursor up to start of previous dashboard and overwrite in place.
        # \033[{N}A = cursor up N lines,  \r = start of line,  \033[J = erase to end.
        _sys.stdout.write(f'\033[{_last_n_lines}A\r{content}\033[J\n')
    else:
        _sys.stdout.write(content + '\n')
    _sys.stdout.flush()
    _last_n_lines = n_lines


def _agg_tf(bars_1m, n, keep=20):
    """Aggregate 1m bars into n-minute bars from the end."""
    if not bars_1m or n <= 1:
        return bars_1m[-keep:]
    result = []
    i = len(bars_1m)
    while i >= n and len(result) < keep:
        chunk = bars_1m[i - n:i]
        result.insert(0, {
            'ts':        chunk[0]['ts'],
            'open':      chunk[0]['open'],
            'high':      max(b['high'] for b in chunk),
            'low':       min(b['low'] for b in chunk),
            'close':     chunk[-1]['close'],
            'volume':    round(sum(b.get('volume', 0) for b in chunk), 6),
            'taker_buy': round(sum(b.get('taker_buy', b.get('taker', 0)) for b in chunk), 6),
        })
        i -= n
    return result


def _live_5m_bar(state):
    """Build the current incomplete 5m bar from accumulated second bars."""
    s1 = getattr(state, 's1_by_sec', {})
    if not s1:
        return None
    last_sec  = max(s1)
    win_start = (last_sec // 300) * 300
    secs      = sorted(s for s in s1 if win_start <= s <= last_sec)
    if not secs:
        return None
    bars     = [s1[s] for s in secs]
    last_ob  = bars[-1].get('ob', ([], []))
    bids, asks = last_ob if len(last_ob) == 2 else ([], [])
    return {
        'ts':        win_start * 1000,
        'open':      bars[0]['open'],
        'high':      max(b['high'] for b in bars),
        'low':       min(b['low']  for b in bars),
        'close':     bars[-1]['close'],
        'volume':    round(sum(b['volume'] for b in bars), 6),
        'taker_buy': round(sum(b.get('taker_buy', b['volume'] * 0.5) for b in bars), 6),
        'ob_mid':    round((asks[0][0] + bids[0][0]) / 2, 2) if bids and asks else None,
        'ob_spr':    round(asks[0][0] - bids[0][0], 2)       if bids and asks else None,
        'live':      True,
    }

def _run(*a, env=None):
    return subprocess.run(list(a), capture_output=True, text=True,
                          cwd=str(REPO), env=env, timeout=GIT_TIMEOUT)


# ── Git push thread — reads from queue, never blocks the write loop ───────────

def _del_obj(sha):
    """Delete a loose git object by SHA — no-op if already packed or missing."""
    if sha and len(sha) >= 4:
        (REPO / '.git' / 'objects' / sha[:2] / sha[2:]).unlink(missing_ok=True)

def _clear_git_locks():
    """Remove stale ref lock files left by killed git push subprocesses."""
    git_dir = REPO / '.git'
    for branch in (DATA_BRANCH, SIG_BRANCH):
        lock = git_dir / 'refs' / 'heads' / Path(branch.replace('/', os.sep) + '.lock')
        lock.unlink(missing_ok=True)
    (git_dir / 'index.lock').unlink(missing_ok=True)


def _archive_session():
    """Push final snapshot to a dated session branch — never force-pushed, never deleted."""
    if not GZ_FILE.exists() or GZ_FILE.stat().st_size == 0:
        return
    try:
        label = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        arc_branch = f'{SESSIONS_BRANCH_PREFIX}{label}'
        arc_path   = f'data/raw/BTCUSDT_LIVE.jsonl.gz'
        env_gc  = {**os.environ, 'GIT_NO_AUTO_GC': '1', 'GIT_INDEX_FILE': str(TMP_IDX)}
        env_obj = {**os.environ, 'GIT_NO_AUTO_GC': '1'}
        TMP_IDX.unlink(missing_ok=True)
        r = _run('git', 'hash-object', '-w', str(GZ_FILE), env=env_obj)
        blob = r.stdout.strip()
        if not blob:
            return
        _run('git', 'update-index', '--add', '--cacheinfo', f'100644,{blob},{arc_path}', env=env_gc)
        r = _run('git', 'write-tree', env=env_gc)
        tree = r.stdout.strip()
        TMP_IDX.unlink(missing_ok=True)
        if not tree:
            return
        env_no_gc = {k: v for k, v in env_gc.items() if k != 'GIT_INDEX_FILE'}
        r = _run('git', 'commit-tree', tree, '-m', f'session {label}', env=env_no_gc)
        commit = r.stdout.strip()
        if not commit:
            return
        r = _run('git', 'push', 'origin', f'{commit}:refs/heads/{arc_branch}')
        if r.returncode == 0:
            log(f'archive → {arc_branch}', G)
        else:
            log(f'archive push failed: {r.stderr.strip()[:120]}', R)
        for sha in (blob, tree, commit):
            _del_obj(sha)
    except Exception as e:
        log(f'archive error: {e}', R)


def _git_push_worker():
    """Dedicated thread: drains _push_queue and pushes to GitHub.

    Each push cycle creates exactly 3 loose objects (blob, tree, commit).
    We delete them immediately after a successful push so the phone's
    .git/objects/ never accumulates data — zero net growth per cycle.
    GIT_NO_AUTO_GC=1 prevents git from packing the objects before we can
    delete them.
    """
    while True:
        gz_path = _push_queue.get()   # blocks until a snapshot is queued
        if gz_path is None:
            # Session ending — archive the final snapshot to a dated branch.
            # data/raw is a rolling force-push; data/sessions/* accumulates forever.
            _archive_session()
            break
        blob = tree = commit = ''
        try:
            with _git_lock:
                _clear_git_locks()
                if not gz_path.exists():
                    log(f'raw: gz missing {gz_path}', R)
                    continue
                sz = gz_path.stat().st_size
                if sz == 0:
                    log(f'raw: gz empty', R)
                    continue
                env_gc  = {**os.environ, 'GIT_NO_AUTO_GC': '1',
                           'GIT_INDEX_FILE': str(TMP_IDX)}
                env_obj = {**os.environ, 'GIT_NO_AUTO_GC': '1'}  # no index for hash-object
                TMP_IDX.unlink(missing_ok=True)

                # All files to commit to data/raw in one tree
                raw_files = {GIT_TREE_PATH: gz_path}
                for fname in ('BTCUSDT_1m.json.gz', 'BTCUSDT_1h.json.gz'):
                    p = DATA_DIR / fname
                    if p.exists() and p.stat().st_size > 0:
                        raw_files[f'data/raw/{fname}'] = p

                all_blobs = {}
                for git_path, local_path in raw_files.items():
                    r = _run('git', 'hash-object', '-w', str(local_path), env=env_obj)
                    b = r.stdout.strip()
                    if not b and local_path == gz_path:
                        _clear_git_locks()
                        r = _run('git', 'hash-object', '-w', str(local_path), env=env_obj)
                        b = r.stdout.strip()
                    if b:
                        all_blobs[git_path] = b

                blob = all_blobs.get(GIT_TREE_PATH, '')
                if not blob:
                    log(f'raw: hash-object failed (sz={sz}): {r.stderr.strip()[:200]}', R)
                    continue
                for git_path, b in all_blobs.items():
                    _run('git', 'update-index', '--add',
                         '--cacheinfo', f'100644,{b},{git_path}', env=env_gc)
                r = _run('git', 'write-tree', env=env_gc)
                tree = r.stdout.strip()
                TMP_IDX.unlink(missing_ok=True)
                if not tree:
                    log(f'raw: write-tree failed: {r.stderr.strip()[:80]}', R)
                    continue
                env_no_gc = {k: v for k, v in env_gc.items()
                             if k != 'GIT_INDEX_FILE'}
                r = _run('git', 'commit-tree', tree, '-m', f'raw {int(time.time())}',
                         env=env_no_gc)
                commit = r.stdout.strip()
                if not commit:
                    log(f'raw: commit-tree failed: {r.stderr.strip()[:80]}', R)
                    continue
                r = _run('git', 'push', '--force', 'origin',
                         f'{commit}:refs/heads/{DATA_BRANCH}')
                if r.returncode != 0:
                    log(f'raw: push failed: {r.stderr.strip()[:120]}', R)
                for sha in (blob, tree, commit):
                    _del_obj(sha)
        except Exception as e:
            log(f'git push error: {e}', R)
            for sha in (blob, tree, commit):
                _del_obj(sha)


# ── Signal push ───────────────────────────────────────────────────────────────

def _push_signals(txt: str, summary: dict) -> bool:
    SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
    SIGNALS_TXT.write_text(txt or '(no signals yet)\n', encoding='utf-8')
    SIGNALS_JSON.write_text(json.dumps(summary, indent=2), encoding='utf-8')
    with _git_lock:
        try:
            _clear_git_locks()
            env = {**os.environ, 'GIT_INDEX_FILE': str(SIG_IDX), 'GIT_NO_AUTO_GC': '1'}
            SIG_IDX.unlink(missing_ok=True)
            tree_paths = {
                SIGNALS_TXT:  'data/signals/BTCUSDT_SIGNALS.txt',
                SIGNALS_JSON: 'data/signals/BTCUSDT_SIGNALS.json',
            }
            # Include deep probe snapshot if available
            dp_file = DATA_DIR / 'deep_probe.json'
            if dp_file.exists() and dp_file.stat().st_size > 0:
                tree_paths[dp_file] = 'data/signals/deep_probe.json'
            for p, git_path in tree_paths.items():
                r = _run('git', 'hash-object', '-w', str(p))
                blob = r.stdout.strip()
                if not blob:
                    log(f'sig: hash-object failed: {r.stderr.strip()[:120]}', R)
                    return False
                _run('git', 'update-index', '--add',
                     '--cacheinfo', f'100644,{blob},{git_path}', env=env)
            r = _run('git', 'write-tree', env=env)
            tree = r.stdout.strip()
            SIG_IDX.unlink(missing_ok=True)
            if not tree:
                log(f'sig: write-tree failed: {r.stderr.strip()[:120]}', R)
                return False
            r = _run('git', 'commit-tree', tree, '-m', f'signals {int(time.time())}')
            commit = r.stdout.strip()
            if not commit:
                log(f'sig: commit-tree failed: {r.stderr.strip()[:120]}', R)
                return False
            r = _run('git', 'push', '--force', 'origin',
                     f'{commit}:refs/heads/{SIG_BRANCH}')
            if r.returncode != 0:
                log(f'sig: push failed: {r.stderr.strip()[:200]}', R)
            return r.returncode == 0
        except Exception:
            import traceback
            log(f'sig: exception: {traceback.format_exc()[:200]}', R)
            SIG_IDX.unlink(missing_ok=True)
            return False


# ── Scan loop — triggered by on_trade, reads deque directly ───────────────────

def _scan_loop(seed_bars=None, display_tfs=None):
    try:
        import traceback
        sys.path.insert(0, str(REPO))
        log('scanner: importing wave_scan…', Y)
        import wave_scan as _ws
        log('scanner: wave_scan loaded', Y)

        state          = _ws.ScanState()
        last_push_time = 0.0
        _tfs           = [t for t in (display_tfs or []) if t in VALID_TFS] or list(VALID_TFS)

        # Seed directly from exchange bars — no deque needed for startup
        log('scanner: seeding from exchange bars…', Y)
        _ws.scan_incremental(state, raw_lines=[], signals_only=True,
                             seed_bars=seed_bars or [])
        log(f'scanner: ready | seeded {len(state.closed_1m)} bars', G)

        # Wait for first live data
        log('scanner: waiting for first data…', Y)
        while True:
            _scan_trigger.wait(timeout=2.0)
            with _deque_lock:
                has_data = len(_raw_deque) > 0
            if has_data:
                break
        log('scanner: live data flowing — starting main loop', G)

        while True:
            try:
                _scan_trigger.wait(timeout=10.0)
                _scan_trigger.clear()

                with _deque_lock:
                    snapshot = list(_raw_deque)

                # suppress wave_scan's own print output (signal cards etc.)
                _buf = io.StringIO()
                _old = sys.stdout
                sys.stdout = _buf
                try:
                    new_sigs = _ws.scan_incremental(state, raw_lines=snapshot,
                                                     signals_only=True)
                finally:
                    sys.stdout = _old

                # ── full live dict every tick — dashboard always has fresh data ──
                live = {}
                try:
                    if state.s1_by_sec:
                        last_sec = max(state.s1_by_sec)
                        bar  = state.s1_by_sec[last_sec]
                        bids, asks = bar['ob']
                        mid  = bar['close']
                        bv   = sum(q for _,q in bids[:5]); av = sum(q for _,q in asks[:5])
                        obi  = round((bv-av)/(bv+av), 3) if bv+av else 0.0
                        bv2  = sum(q for p,q in bids if abs(p-mid)<=25)
                        av2  = sum(q for p,q in asks if abs(p-mid)<=25)
                        conc = round((bv2-av2)/(bv2+av2), 3) if bv2+av2 else 0.0
                        spr  = round(asks[0][0]-bids[0][0], 2) if bids and asks else 99.0
                        buf  = state.knife_buf
                        br, ar = buf._flow_rates()
                        # micro_window gives bar-close velocity — never zeros on
                        # quiet ticks the way buf._vel() does (which needs ≥2 trades
                        # within VEL_WIN_MS).
                        _mw = getattr(state, 'micro_window', [])
                        _nv = min(10, len(_mw))
                        vel  = round((_mw[-1]['close'] - _mw[-_nv]['close'])
                                     / max(_nv - 1, 1), 4) if _nv > 1 else round(buf._vel(), 4)
                        ds   = round(buf._decay_score(), 3)
                        fos  = round(buf._floor_score(conc, spr, mid=mid, bids=bids, asks=asks), 3)
                        thr  = _ws._thresholds(
                            state.hist_scores, state.hist_phases,
                            state.hist_obi, state.hist_kdv_bals,
                            state.hist_aligns)
                        thresh, peak_ph, trough_ph, obi_conf, gate_rev, gate_cont, align_cont = thr
                        # ── session stats from seeded candles ──────────────
                        import time as _time
                        _now_ms  = int(_time.time() * 1000)
                        _today0  = _now_ms - (_now_ms % 86_400_000)
                        _1m_bars = getattr(state, 'closed_1m', [])
                        _today1m = [b for b in _1m_bars if b['ts'] >= _today0]
                        if _today1m:
                            _s_open  = _today1m[0]['open']
                            _s_high  = max(b['high']  for b in _today1m)
                            _s_low   = min(b['low']   for b in _today1m)
                            _s_range = _s_high - _s_low
                            _s_chg   = mid - _s_open
                            _s_chg_pct = _s_chg / _s_open * 100 if _s_open else 0.0
                        else:
                            _s_open = _s_high = _s_low = _s_range = _s_chg = _s_chg_pct = 0.0
                        # HTF trend: 4h higher-highs + higher-lows = BULL
                        _4h = getattr(state, 'tf4h', [])
                        _1h = getattr(state, 'tf1h', [])
                        def _htf_trend(bars, n=3):
                            if len(bars) < n + 1: return 0
                            hh = all(bars[-i]['high'] > bars[-i-1]['high'] for i in range(1, n+1))
                            hl = all(bars[-i]['low']  > bars[-i-1]['low']  for i in range(1, n+1))
                            lh = all(bars[-i]['high'] < bars[-i-1]['high'] for i in range(1, n+1))
                            ll = all(bars[-i]['low']  < bars[-i-1]['low']  for i in range(1, n+1))
                            if hh and hl: return  1
                            if lh and ll: return -1
                            return 0
                        _trend_4h = _htf_trend(_4h)
                        _trend_1h = _htf_trend(_1h)
                        live = {
                            'at':              datetime.fromtimestamp(last_sec, tz=timezone.utc).strftime('%H:%M:%SZ'),
                            'price':           round(mid, 2),
                            'obi':             obi,
                            'obi_need':        round(obi_conf, 3),
                            'conc':            conc,
                            'spr':             spr,
                            'vel':             vel,
                            'bid_flow':        round(br, 4),
                            'ask_flow':        round(ar, 4),
                            'phase':           buf.phase,
                            'dk':              buf.LABELS.get(buf.state, str(buf.state)),
                            'decay_sc':        ds,
                            'floor_sc':        fos,
                            'score':           round(state.hist_scores[-1], 3) if state.hist_scores else 0.0,
                            'score_need':      round(thresh, 3),
                            'kdv_bal':         round(state.hist_kdv_bals[-1], 3) if state.hist_kdv_bals else 0.0,
                            'kdv_need_rev':    round(gate_rev, 3),
                            'kdv_need_cont':   round(gate_cont, 3),
                            'micro_ph':        round(state.hist_phases[-1], 3) if state.hist_phases else 0.0,
                            'micro_ph_peak':   round(peak_ph, 3),
                            'micro_ph_trough': round(trough_ph, 3),
                            'align_need':      round(align_cont, 3),
                            'res_align':       round(state.last_align, 3),
                            'res_dir':         state.last_res_dir,
                            'htf_sup':         state.last_htf_sup,
                            'htf_bars':        (f'{len(state.tf5m)}x5m {len(state.tf15m)}x15m'
                                                f' {len(state.tf1h)}x1h {len(state.tf4h)}x4h'),
                            'sess_open':       round(_s_open,   2),
                            'sess_high':       round(_s_high,   2),
                            'sess_low':        round(_s_low,    2),
                            'sess_range':      round(_s_range,  2),
                            'sess_chg':        round(_s_chg,    2),
                            'sess_chg_pct':    round(_s_chg_pct, 3),
                            'trend_4h':        _trend_4h,
                            'trend_1h':        _trend_1h,
                        }
                        state.live = live   # dashboard reads state.live every redraw
                except Exception:
                    live = getattr(state, 'live', {})

                # ── dashboard on every tick ──────────────────────────────
                _dashboard(state, _tfs, state.signals)

                now = time.time()
                if new_sigs or (now - last_push_time >= SIG_PUSH_S):

                    # ── order book depth snapshot ────────────────────────
                    ob_depth = {}
                    try:
                        if state.s1_by_sec:
                            _ls  = max(state.s1_by_sec)
                            _bar = state.s1_by_sec[_ls]
                            _ob  = _bar.get('ob', ([], []))
                            _bids, _asks = (_ob if len(_ob) == 2 else ([], []))
                            if _bids and _asks:
                                ob_depth['bids'] = [[round(p,2), round(q,4)] for p,q in _bids[:20]]
                                ob_depth['asks'] = [[round(p,2), round(q,4)] for p,q in _asks[:20]]
                                ob_depth['mid']  = round((_asks[0][0] + _bids[0][0]) / 2, 2)
                                ob_depth['spr']  = round(_asks[0][0] - _bids[0][0], 2)
                                for depth in (5, 10, 20):
                                    _b = _bids[:depth]; _a = _asks[:depth]
                                    _bv = sum(x[1] for x in _b); _av = sum(x[1] for x in _a)
                                    _tot = _bv + _av
                                    ob_depth[f'obi{depth}'] = round((_bv-_av)/_tot, 4) if _tot else 0
                                def _wall(lvls):
                                    if not lvls: return None
                                    avg = sum(x[1] for x in lvls) / len(lvls)
                                    for i, (p, q) in enumerate(lvls):
                                        if q >= avg:
                                            return {'lvl': i, 'price': round(p,2), 'qty': round(q,4)}
                                    return None
                                ob_depth['bid_wall'] = _wall(_bids[:20])
                                ob_depth['ask_wall'] = _wall(_asks[:20])
                                ob_depth['bid_liq']  = round(sum(x[1] for x in _bids[:20]), 4)
                                ob_depth['ask_liq']  = round(sum(x[1] for x in _asks[:20]), 4)
                    except Exception:
                        pass

                    # ── knife buffer state ───────────────────────────────
                    kb = getattr(state, 'knife_buf', None)
                    knife_data = {}
                    if kb:
                        try:
                            knife_data = {
                                'state':    getattr(kb, 'state', 0),
                                'label':    kb.LABELS.get(kb.state, '?'),
                                'decay_sc': round(kb._decay_score(), 4),
                                'floor_sc': round(kb._floor_score(
                                    live.get('conc', 0), live.get('spr', 99),
                                    mid=live.get('price', 0),
                                    bids=[], asks=[]), 4),
                                'lows':     getattr(kb, 'lows', []),
                            }
                        except Exception:
                            pass

                    # ── live 5m bar from second data ─────────────────────
                    _live5    = _live_5m_bar(state)
                    _tf5c     = getattr(state, 'tf5m', [])
                    _live5_ts = (_live5 or {}).get('ts')
                    tf5m_live = [b for b in _tf5c[-30:] if b['ts'] != _live5_ts]
                    if _live5:
                        tf5m_live.append(_live5)

                    summary = {
                        'signal_count': state.sig_count,
                        'signals':      state.signals,
                        'scanned_at':   datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'new_this_run': len(new_sigs),
                        'live':         live,
                        'tf_physics':   getattr(state, 'tf_live', {}),
                        'tf_states':    getattr(state, 'last_tf_states', {}),
                        'shelves':      getattr(state, 'session_shelves', []),
                        'knife':        knife_data,
                        'ob_depth':     ob_depth,
                        'ob_deep':      {**_deep_probe, 'deep_status': _deep_status},
                        'timeframes': {
                            'tf1m':  (state.closed_1m[-60:]  if getattr(state, 'closed_1m', None) else []),
                            'tf5m':  tf5m_live,
                            'tf10m': _agg_tf(getattr(state, 'closed_1m', []), 10, keep=20),
                            'tf15m': (state.tf15m[-20:]      if getattr(state, 'tf15m', None) else []),
                            'tf30m': (state.tf30m[-12:]      if getattr(state, 'tf30m', None) else []),
                            'tf45m': (state.tf45m[-10:]      if getattr(state, 'tf45m', None) else []),
                            'tf1h':  (state.tf1h[-12:]       if getattr(state, 'tf1h',  None) else []),
                            'tf4h':  (state.tf4h[-6:]        if getattr(state, 'tf4h',  None) else []),
                        },
                    }
                    if not _push_signals('', summary):
                        log('sig push failed', R)
                    else:
                        px   = live.get('price', '?')
                        dk_l = live.get('dk', '?')
                        sc   = live.get('score', '?')
                        bars = len(state.closed_1m)
                        sup  = ' SUP' if live.get('htf_sup') else ''
                        log(f'push ok | ${px:,.2f}  dk={dk_l}  score={sc}'
                            f'  {live.get("htf_bars","")}{sup}  bars={bars}', Y)
                    last_push_time = time.time()

            except Exception:
                log(f'scanner error:\n{traceback.format_exc()}', R)

    except Exception:
        import traceback as _tb
        log(f'scanner FATAL:\n{_tb.format_exc()}', R)


# ── Write loop — snapshot deque to disk, queue git push ──────────────────────

def _push_loop():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log(f'pipeline → {DATA_DIR}', G)
    cycle = 0
    while True:
        time.sleep(PUSH_S)
        cycle += 1
        try:
            with _deque_lock:
                if not _raw_deque: continue
                lines = list(_raw_deque)

            tmp = GZ_FILE.with_suffix('.tmp')
            with gzip.open(tmp, 'wt', compresslevel=1) as f:
                f.write('\n'.join(lines))
            os.replace(tmp, GZ_FILE)   # atomic: push thread never sees 0-byte file

            # Queue for git push — drop if queue is full (push still in progress)
            try:
                _push_queue.put_nowait(GZ_FILE)
            except queue.Full:
                pass

            if cycle % GC_EVERY == 0:
                gc.collect()

            if cycle % LOG_MEM_EVERY == 0:
                rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                sz  = GZ_FILE.stat().st_size if GZ_FILE.exists() else 0
                log(f'mem={rss}KB  lines={len(lines)}  gz={sz//1024}kB  '
                    f'cycle={cycle}', Y)
        except Exception as e:
            log(f'write error: {e}', R)



# ── WebSocket → deque (hot path — zero I/O) ───────────────────────────────────


def _deep_book_loop():
    """
    Poll /api/v3/depth?limit=100 every second — full snapshot of 100 levels,
    same concept as depth20@100ms but via REST at 1s cadence.
    No diff handling, no state sync. Each response replaces the book.
    """
    global _deep_ready, _deep_probe, _deep_status
    import urllib.request as _ur
    backoff = 2
    _dp_path = DATA_DIR / 'deep_probe.json'

    while True:
        try:
            ts_ms = int(time.time() * 1000)
            req = _ur.Request(DEEP_BOOK_URL,
                              headers={'User-Agent': 'python-trendrider/1.0'})
            with _ur.urlopen(req, timeout=5) as resp:
                raw = json.loads(resp.read())

            bids_raw = raw.get('bids', [])
            asks_raw = raw.get('asks', [])
            if not bids_raw or not asks_raw:
                _deep_status = f'empty response: {list(raw.keys())}'
                time.sleep(DEEP_POLL_S)
                continue

            with _deep_lock:
                        # Expire old reload events (>60s)
                        stale = [k for k,v in _reload_evts.items() if ts_ms - v['ts'] > 60000]
                        for k in stale: _reload_evts.pop(k, None)

                        # Replace book — full snapshot each poll
                        prev_bids = dict(_deep_bids)
                        prev_asks = dict(_deep_asks)
                        _deep_bids.clear()
                        _deep_asks.clear()

                        for p, q in bids_raw:
                            pf, qf = float(p), float(q)
                            if qf <= 0: continue
                            key = ('bid', pf)
                            prev_q = prev_bids.get(pf, 0.0)
                            _deep_bids[pf] = qf

                            if key not in _level_hist:
                                _level_hist[key] = collections.deque(maxlen=40)
                            _level_hist[key].append((ts_ms, qf))

                            if key not in _level_first_seen:
                                _level_first_seen[key] = ts_ms
                            _level_last_seen[key] = ts_ms

                            # Reload detection: level was gone last poll, back now
                            if prev_q == 0.0 and key in _reload_pend:
                                pend = _reload_pend.pop(key)
                                ratio = round(qf / pend['qty'], 3) if pend['qty'] else 0
                                _reload_evts[key] = {'ts': ts_ms, 'ratio': ratio,
                                                     'prev': round(pend['qty'], 4),
                                                     'curr': round(qf, 4), 'side': 'bid'}

                        for p, q in asks_raw:
                            pf, qf = float(p), float(q)
                            if qf <= 0: continue
                            key = ('ask', pf)
                            prev_q = prev_asks.get(pf, 0.0)
                            _deep_asks[pf] = qf

                            if key not in _level_hist:
                                _level_hist[key] = collections.deque(maxlen=40)
                            _level_hist[key].append((ts_ms, qf))

                            if key not in _level_first_seen:
                                _level_first_seen[key] = ts_ms
                            _level_last_seen[key] = ts_ms

                            if prev_q == 0.0 and key in _reload_pend:
                                pend = _reload_pend.pop(key)
                                ratio = round(qf / pend['qty'], 3) if pend['qty'] else 0
                                _reload_evts[key] = {'ts': ts_ms, 'ratio': ratio,
                                                     'prev': round(pend['qty'], 4),
                                                     'curr': round(qf, 4), 'side': 'ask'}

                        # Detect disappeared levels (reload pending)
                        for pf, prev_q in prev_bids.items():
                            if pf not in _deep_bids and prev_q > 0.001:
                                _reload_pend[('bid', pf)] = {'ts': ts_ms, 'qty': prev_q, 'side': 'bid'}
                        for pf, prev_q in prev_asks.items():
                            if pf not in _deep_asks and prev_q > 0.001:
                                _reload_pend[('ask', pf)] = {'ts': ts_ms, 'qty': prev_q, 'side': 'ask'}

                        if not _deep_ready:
                            _deep_ready = True
                            log(f'deep-book: ready  {len(_deep_bids)}b/{len(_deep_asks)}a levels', G)

            _deep_probe = _probe_deep_book()
            try:
                _dp_path.write_text(json.dumps(
                    {**_deep_probe, '_status': _deep_status}, indent=2), encoding='utf-8')
            except Exception:
                pass

            _deep_status = f'ok {len(_deep_bids)}b/{len(_deep_asks)}a'
            backoff = 2
            time.sleep(DEEP_POLL_S)

        except Exception as e:
            _deep_status = f'{type(e).__name__}: {e}'
            log(f'deep-book: {_deep_status} — retry {backoff}s', Y)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


def _probe_deep_book(orphan_threshold=3.0):
    """
    Full-depth order book analysis — no level cap (full satoshi depth).

    Friction classification per orphan wall:
      STABLE     — qty unchanged across recent history (real resting order)
      ABSORBING  — qty monotonically decreasing (being filled by takers)
      PULLING    — disappeared without a fill (spoof/cancel)
      RELOAD_+   — reloaded with MORE than before (defending, directional)
      RELOAD_=   — reloaded ~equal (market maker maintaining level)
      RELOAD_-   — reloaded with LESS (withdrawing, weakening wall)
      NEW        — just appeared (fewer than 3 history snapshots)

    Reload ratio = new_qty / prev_qty_before_pull.
    >1.05 = RELOAD_+, 0.95-1.05 = RELOAD_=, <0.95 = RELOAD_-
    """
    with _deep_lock:
        if not _deep_bids or not _deep_asks:
            return {}
        # Full depth — no cap
        bids = sorted(_deep_bids.items(), reverse=True)
        asks = sorted(_deep_asks.items())
        reload_snap = dict(_reload_evts)
        hist_snap   = {k: list(v) for k, v in _level_hist.items()}

    if not bids or not asks:
        return {}

    mid = (bids[0][0] + asks[0][0]) / 2.0

    def _classify(side, price):
        key = (side, price)
        now_ms = time.time() * 1000
        # Recent reload event takes priority
        if key in reload_snap:
            r = reload_snap[key]['ratio']
            if   r > 1.05: return 'RELOAD_+'
            elif r < 0.95: return 'RELOAD_-'
            else:          return 'RELOAD_='
        # Static wall: seen in diff stream but no update in >30s = resting order
        # (diff stream only sends messages when qty changes; silence = unchanged)
        first = _level_first_seen.get(key)
        last  = _level_last_seen.get(key)
        if first and last and (now_ms - last) > 30_000 and (now_ms - first) > 30_000:
            return 'STABLE'
        hist = hist_snap.get(key, [])
        if len(hist) < 3:
            return 'NEW'
        qtys = [q for _, q in hist]
        if all(qtys[i] >= qtys[i+1] for i in range(len(qtys)-1)) and qtys[-1] < qtys[0]*0.95:
            return 'ABSORBING'
        if max(qtys) - min(qtys) < qtys[0] * 0.01:
            return 'STABLE'
        return 'DYNAMIC'

    def _orphan_walls(levels, side):
        if len(levels) < 5:
            return []
        vols    = [q for _, q in levels]
        avg     = sum(vols) / len(vols)
        walls   = []
        for i, (p, q) in enumerate(levels):
            nbrs    = vols[max(0,i-3):i] + vols[i+1:i+4]
            nbr_avg = sum(nbrs)/len(nbrs) if nbrs else avg
            if nbr_avg > 0 and q >= orphan_threshold * nbr_avg and q >= avg:
                key = (side, p)
                reload_info = reload_snap.get(key)
                walls.append({
                    'price':    round(p, 2),
                    'qty':      round(q, 4),
                    'dist':     round(abs(p - mid), 2),
                    'ratio':    round(q / nbr_avg, 1),
                    'side':     side,
                    'friction': _classify(side, p),
                    'reload_r': round(reload_info['ratio'], 3) if reload_info else None,
                    'reload_prev': reload_info['prev'] if reload_info else None,
                    'reload_curr': reload_info['curr'] if reload_info else None,
                })
        return sorted(walls, key=lambda w: w['dist'])

    bid_walls = _orphan_walls(bids, 'bid')
    ask_walls = _orphan_walls(asks, 'ask')

    def _cog(levels):
        tot = sum(q for _, q in levels)
        return sum(p*q for p,q in levels)/tot if tot else mid

    bv = sum(q for _, q in bids)
    av = sum(q for _, q in asks)
    deep_obi = round((bv - av) / (bv + av), 4) if bv + av else 0.0

    nearest_ask_wall = ask_walls[0]['price'] if ask_walls else None
    nearest_bid_wall = bid_walls[0]['price'] if bid_walls else None

    bias = 0
    if nearest_ask_wall and nearest_bid_wall:
        if (nearest_ask_wall - mid) < (mid - nearest_bid_wall) * 0.7: bias =  1
        elif (mid - nearest_bid_wall) < (nearest_ask_wall - mid) * 0.7: bias = -1
    elif nearest_ask_wall: bias =  1
    elif nearest_bid_wall: bias = -1

    # Reload directional summary: net reload pressure
    reloads_up   = [v for v in reload_snap.values() if v['side']=='ask' and v['ratio']>1.05]
    reloads_dn   = [v for v in reload_snap.values() if v['side']=='bid' and v['ratio']>1.05]
    reload_bias  = (1 if len(reloads_up) > len(reloads_dn) else
                   -1 if len(reloads_dn) > len(reloads_up) else 0)

    return {
        'mid':              round(mid, 2),
        'levels_bid':       len(bids),
        'levels_ask':       len(asks),
        'deep_obi':         deep_obi,
        'gravity_bid':      round(_cog(bids), 2),
        'gravity_ask':      round(_cog(asks), 2),
        'bid_walls':        bid_walls[:8],
        'ask_walls':        ask_walls[:8],
        'nearest_bid_wall': nearest_bid_wall,
        'nearest_ask_wall': nearest_ask_wall,
        'bias':             bias,
        'reload_bias':      reload_bias,
        'reloads_ask_defending': len(reloads_up),
        'reloads_bid_defending': len(reloads_dn),
    }


async def stream():
    def on_trade(d):
        global _last_trade_sec
        ts_ms = int(d['T'])
        ts_s  = ts_ms // 1000
        with _deque_lock:
            _raw_deque.append(json.dumps(
                ['T', ts_ms, int(float(d['p'])*100),
                 int(float(d['q'])*10000), 0 if d.get('m') else 1],
                separators=(',',':')))
        if ts_s > _last_trade_sec:
            _last_trade_sec = ts_s
            _scan_trigger.set()

    def on_depth(d):
        global _last_depth_sec
        ts   = int(time.time()*1000)
        ts_s = ts // 1000
        bid  = [[int(float(p)*100), int(float(q)*10000)] for p,q in d.get('bids',[])]
        ask  = [[int(float(p)*100), int(float(q)*10000)] for p,q in d.get('asks',[])]
        with _deque_lock:
            _raw_deque.append(json.dumps(['D', ts, bid, ask], separators=(',',':')))
        if ts_s > _last_depth_sec:
            _last_depth_sec = ts_s
            _scan_trigger.set()

    log('connecting...', G)
    backoff = 1
    try:
        while True:
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.ws_connect(WS_URL, heartbeat=20,
                                               receive_timeout=30) as ws:
                        backoff = 1
                        log(f'connected  deque={WINDOW_LINES}', G)
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    d  = json.loads(msg.data)
                                    st = d.get('stream', '')
                                    da = d.get('data', {})
                                    if 'aggTrade' in st: on_trade(da)
                                    elif '@depth'  in st: on_depth(da)
                                except Exception as e:
                                    log(f'parse: {e}', R)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                log(f'error ({type(e).__name__}: {e}) retry {backoff}s', R)
                await asyncio.sleep(backoff)
                backoff = min(backoff*2, 60)
    finally:
        _push_queue.put(None)


if __name__ == '__main__':
    import argparse as _ap
    _parser = _ap.ArgumentParser(description='collect_raw — BTC live collector + scanner')
    _parser.add_argument('--tf', nargs='+', default=list(VALID_TFS),
                         metavar='TF',
                         help=f'Timeframes to display ({", ".join(VALID_TFS)})')
    _args       = _parser.parse_args()
    _disp_tfs   = [t for t in _args.tf if t in VALID_TFS] or list(VALID_TFS)

    _seed_bars = []
    try:
        import ingest as _ingest
        import build_candles as _bc
        import gzip as _gz_seed, json as _json_seed
        _ingest.run(verbose=True)         # fetch 1m/1h/4h klines → ~/.trendrider/
        _bc.save(verbose=True)            # aggregate all TFs + rolling stats → ~/.trendrider/tf_*.json.gz
        _tf1m_p = DATA_DIR / 'tf_1m.json.gz'
        if _tf1m_p.exists():
            with _gz_seed.open(_tf1m_p, 'rt') as _f:
                _raw_bars = _json_seed.loads(_f.read())
            # Map buy_v → taker_buy for wave_scan compatibility
            _seed_bars = [dict(b, taker_buy=b.get('buy_v', b.get('volume', 0) * 0.5))
                          for b in _raw_bars]
            log(f'candle seed: {len(_seed_bars)} x 1m bars from tf_1m.json.gz', G)
    except Exception as e:
        log(f'candle seed (non-fatal): {e}', R)

    threading.Thread(target=_push_loop,                                        daemon=True).start()
    threading.Thread(target=_git_push_worker,                                  daemon=True).start()
    threading.Thread(target=_scan_loop, args=(_seed_bars, _disp_tfs),          daemon=True).start()
    threading.Thread(target=_deep_book_loop,                                   daemon=True).start()

    loop = asyncio.new_event_loop()
    task = loop.create_task(stream())
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, task.cancel)
    try:
        loop.run_until_complete(task)
    finally:
        loop.close()
