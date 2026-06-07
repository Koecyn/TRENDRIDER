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

GIT_TIMEOUT = 60   # seconds before a git subprocess is killed

G='\033[92m'; R='\033[91m'; Y='\033[93m'; Z='\033[0m'
C='\033[96m'; B='\033[1m'
def log(m, c=Z): print(f'{c}[raw] {m}{Z}', flush=True)

VALID_TFS   = ('1m', '5m', '10m', '15m', '30m', '45m', '1h', '4h')
_last_dash_t = 0.0
_DASH_MIN_S  = 1.0      # redraw at most once per second


def _dashboard(state, tfs, recent_signals):
    """Clear screen and redraw full TF dashboard.

    Uses full-screen clear so concurrent log output from other threads
    never corrupts cursor positioning.

    TF selection (live, no restart needed):
      echo '1m 5m 1h'  > ~/.trendrider/tf_view   # narrow view
      echo 'all'       > ~/.trendrider/tf_view   # all 8 TFs
      rm               ~/.trendrider/tf_view      # back to --tf default
    """
    global _last_dash_t
    now_t = time.monotonic()
    if now_t - _last_dash_t < _DASH_MIN_S:
        return
    _last_dash_t = now_t

    # ── live TF selection — read on every draw, no restart needed ────────
    active_tfs = tfs  # fallback: whatever --tf gave us
    try:
        txt = TF_VIEW_FILE.read_text().strip()
        if txt.lower() in ('all', 'default', ''):
            active_tfs = list(VALID_TFS)
        else:
            chosen = [t for t in txt.split() if t in VALID_TFS]
            if chosen:
                active_tfs = chosen
    except FileNotFoundError:
        pass   # no file → use --tf default
    except Exception:
        pass

    live    = getattr(state, 'live', {})
    tf_live = getattr(state, 'tf_live', {})
    now_s   = datetime.now(timezone.utc).strftime('%H:%M:%S')

    # ── global live indicators ────────────────────────────────────────────
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

    def _trend_str(t):
        return f'{G}BULL▲{Z}' if t > 0 else (f'{R}BEAR▼{Z}' if t < 0 else f'{Y}COIL─{Z}')

    W = 64
    rows = ['\033[2J\033[H']   # clear screen, cursor to home
    rows.append(f'{B}{C}{"─"*W}{Z}')
    rows.append(f'{B}{C}  BTC ${price:>11,.2f}   {now_s}   dk={dk}{Z}')
    rows.append(f'{C}{"─"*W}{Z}')
    # session range line
    if s_open > 0:
        chg_c = G if s_chg >= 0 else R
        rows.append(f'  SESSION  o=${s_open:,.0f}  h=${s_high:,.0f}  l=${s_low:,.0f}'
                    f'  rng=${s_range:,.0f}  {chg_c}{s_chg:+,.0f}({s_pct:+.2f}%){Z}')
    # trend line
    rows.append(f'  TREND  4h={_trend_str(tr_4h)}  1h={_trend_str(tr_1h)}'
                f'  htf_sup={G+"▲SUP"+Z if htf_sup else Y+"no-sup"+Z}')

    # line 1: velocity / OB imbalance / concentration / spread
    rows.append(f'  vel={vel:+.4f}  obi={obi_g:+.3f}(n={obi_n:.3f})'
                f'  conc={conc:+.3f}  spr={spr:.2f}')
    # line 2: score / KdV
    sc_ok = abs(score_g) >= score_n > 0
    rows.append(f'  score={G if sc_ok else Z}{score_g:+.4f}{Z}(n={score_n:.3f})'
                f'  kdv={kdv_g:.4f}  rev_n={kdv_rev:.3f}  con_n={kdv_con:.3f}')
    # line 3: micro phase with its own thresholds / alignment
    mok = micro_ph <= mph_tr or micro_ph >= mph_pk
    rows.append(f'  μph={G if mok else Z}{micro_ph:+.4f}{Z}'
                f'[trg:{mph_tr:.3f} pk:{mph_pk:.3f}]'
                f'  align={res_al:.3f}(n={align_n:.3f})')
    # line 4: decay / floor / flow rates
    dir_str = f'{G}▲{Z}' if res_dir > 0 else (f'{R}▼{Z}' if res_dir < 0 else '─')
    rows.append(f'  ds={decay_sc:.3f}  fos={floor_sc:.3f}'
                f'  bf={bf:+.4f}  af={af:+.4f}  {dir_str}')
    # line 5: HTF bar counts
    if htf_bars:
        rows.append(f'  htf: {htf_bars}')

    # ── per-TF blocks (2 lines each) ─────────────────────────────────────
    rows.append(f'{C}{"─"*W}{Z}')

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
        vel_tf      = lv.get('vel',           0.0)

        # State from wave direction + how close price is to projection
        cur_px = price  # global price from live dict
        near_pk = proj_peak   > 0 and cur_px >= proj_peak   * 0.998
        near_tr = proj_trough > 0 and cur_px <= proj_trough * 1.002
        if wave_dir == 1 and near_pk:
            st = f'{R}PEAK▼{Z}'
        elif wave_dir == -1 and near_tr:
            st = f'{G}TRGR▲{Z}'
        elif wave_dir == 1:
            st = f'{G}UP  ▲{Z}'
        elif wave_dir == -1:
            st = f'{R}DN  ▼{Z}'
        else:
            st = f'{Y}MID  {Z}'

        sc_hit  = abs(score) >= thresh > 0
        obi_hit = abs(obi)   >= obi_need > 0
        sc_s  = f'{G if sc_hit  else Z}{score:+.3f}{Z}'
        obi_s = f'{G if obi_hit else Z}{obi:+.3f}{Z}'
        dir_s = '▲' if wave_dir == 1 else ('▼' if wave_dir == -1 else '─')

        rows.append(
            f'  {B}{tf:<4}{Z} {st}'
            f'  sc={sc_s}(n={thresh:.3f})'
            f'  ph={phase:+.3f}'
            f'  vel={vel_tf:+.1f}'
        )
        atr_s = (f'  atr=${atr_tf:,.0f}(↑${atr_up_tf:,.0f} ↓${atr_dn_tf:,.0f})'
                 if atr_tf > 0 else '')
        rows.append(
            f'       {dir_s} pk=${proj_peak:,.0f}  tr=${proj_trough:,.0f}'
            f'  amp=${wave_amp:,.0f}{atr_s}'
        )
        rows.append(
            f'       kdv={kdv_bal:.3f}(rv={kdv_rev:.3f} cn={kdv_con:.3f})'
            f'  obi={obi_s}(n={obi_need:.3f})'
            f'  al={al:.3f}'
        )

    rows.append(f'{C}{"─"*W}{Z}')

    if recent_signals:
        for sig in recent_signals[-3:]:
            d   = sig.get('dir', 0)
            lbl = sig.get('label', sig.get('stype', '?'))
            px  = sig.get('price', 0.0)
            t   = sig.get('time', '')
            cl  = G if d > 0 else R
            rows.append(f'  {cl}{"▲" if d>0 else "▼"} {lbl:<16} {t}  ${px:,.2f}{Z}')
        rows.append('')

    # live TF control hint
    cur = ' '.join(active_tfs)
    rows.append(f'\033[2m  view: {cur}'
                f'  |  echo "1m 5m 1h" > {TF_VIEW_FILE.name}'
                f'  |  echo all > {TF_VIEW_FILE.name}\033[0m')

    print('\n'.join(rows), flush=True)


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
                r = _run('git', 'hash-object', '-w', str(gz_path), env=env_obj)
                blob = r.stdout.strip()
                if not blob:
                    # One retry after clearing any stale locks
                    _clear_git_locks()
                    r = _run('git', 'hash-object', '-w', str(gz_path), env=env_obj)
                    blob = r.stdout.strip()
                if not blob:
                    log(f'raw: hash-object failed (sz={sz}): {r.stderr.strip()[:200]}', R)
                    continue
                _run('git', 'update-index', '--add',
                     '--cacheinfo', f'100644,{blob},{GIT_TREE_PATH}', env=env_gc)
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
            for p in (SIGNALS_TXT, SIGNALS_JSON):
                r = _run('git', 'hash-object', '-w', str(p))
                blob = r.stdout.strip()
                if not blob:
                    log(f'sig: hash-object failed: {r.stderr.strip()[:120]}', R)
                    return False
                _run('git', 'update-index', '--add',
                     '--cacheinfo', f'100644,{blob},{tree_paths[p]}', env=env)
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
            _scan_trigger.set()   # new second → wake scanner

    def on_depth(d):
        global _last_depth_sec
        ts  = int(time.time()*1000)
        ts_s = ts // 1000
        bid = [[int(float(p)*100), int(float(q)*10000)] for p,q in d.get('bids',[])]
        ask = [[int(float(p)*100), int(float(q)*10000)] for p,q in d.get('asks',[])]
        with _deque_lock:
            _raw_deque.append(json.dumps(['D', ts, bid, ask], separators=(',',':')))
        if ts_s > _last_depth_sec:
            _last_depth_sec = ts_s
            _scan_trigger.set()   # new second of OB data → wake scanner

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
        _push_queue.put(None)   # signal git worker to exit


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
        import pull_candles
        _seed_bars = pull_candles.fetch_seed_bars(1000)  # 1000 → enough for 45m (22 bars) + 4h seeding
    except Exception as e:
        log(f'candle seed (non-fatal): {e}', R)

    threading.Thread(target=_push_loop,                                        daemon=True).start()
    threading.Thread(target=_git_push_worker,                                  daemon=True).start()
    threading.Thread(target=_scan_loop, args=(_seed_bars, _disp_tfs),          daemon=True).start()

    loop = asyncio.new_event_loop()
    task = loop.create_task(stream())
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, task.cancel)
    try:
        loop.run_until_complete(task)
    finally:
        loop.close()
