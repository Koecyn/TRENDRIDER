#!/usr/bin/env python3
"""
physics_live.py — Live wave equation signal engine.

Connects directly to Binance.US WebSocket:
  btcusdc@kline_1m       — 1m bars; processes on bar CLOSE (x=True)
  btcusdc@depth20@100ms  — L20 order book every 100ms

On each closed bar:
  • Runs all physics + microstructure signals with real order book depth
  • Logs the waveform state (KdV, water hammer, Reynolds, dark pool, OBI, CVD)
  • Tracks paper trades (long-only, stop/target/timeout)
  • Writes live stats to physics_results.json every minute
  • Pulls parameter changes from git every 30s (hot-reload)

Run on Termux:
  cd ~/TRENDRIDER
  pip install aiohttp  (if not installed)
  python physics_live.py
"""

import asyncio
import json
import logging
import os
import re
import signal
import subprocess
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

try:
    import aiohttp
except ImportError:
    print("ERROR: pip install aiohttp")
    sys.exit(1)

REPO        = Path(__file__).resolve().parent
CODE_BRANCH = "claude/hft-mean-reversion-strategy-pJ4YU"
DATA_BRANCH = "data/live"
RESULTS_F   = REPO / "physics_live_results.json"   # separate from optimizer
TRIGGER_F   = REPO / "physics_live_trigger.json"
LOG_F       = REPO / "physics_live.log"

WS_URL = ("wss://stream.binance.us:9443/stream"
          "?streams=btcusdc@kline_1m/btcusdc@depth20@100ms")

FETCH_EVERY_S = 30   # pull param changes from git

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; B='\033[1m'; Z='\033[0m'

ENV = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}


def ts_str():
    return datetime.now().strftime('%H:%M:%S')


def log(msg, col=Z):
    line = f"{col}[live {ts_str()}] {msg}{Z}"
    print(line, flush=True)
    try:
        with open(LOG_F, 'a') as f:
            f.write(re.sub(r'\033\[[0-9;]*m', '', line) + '\n')
    except Exception:
        pass


def git(*args):
    return subprocess.run(["git"] + list(args), cwd=REPO,
                          capture_output=True, text=True, env=ENV)


# ── Physics hot-reload ────────────────────────────────────────────────────────

def flush_physics():
    for key in list(sys.modules.keys()):
        if key.startswith('physics'):
            del sys.modules[key]


def load_physics():
    flush_physics()
    from physics.signals  import HydraulicAccumulator
    from physics.position import PositionManager
    from physics          import config as CF
    return CF, HydraulicAccumulator, PositionManager


# ── ATR ───────────────────────────────────────────────────────────────────────

def _atr(highs, lows, closes, period=14):
    n = len(closes)
    out = np.zeros(n)
    if n < period + 2:
        return out
    trs = np.array([
        max(highs[i] - lows[i],
            abs(highs[i]  - closes[i-1]),
            abs(lows[i]   - closes[i-1]))
        for i in range(1, n)
    ])
    out[period] = float(np.mean(trs[:period]))
    for i in range(period, len(trs)):
        out[i+1] = (out[i] * (period-1) + trs[i]) / period
    return out


# ── Bootstrap: seed window with REST history ──────────────────────────────────

def bootstrap_rest(symbol: str, n: int = 300) -> list:
    """Return list of bar dicts from REST (for ATR/signal warmup)."""
    log(f"Fetching {n} bars from Binance REST for warmup...", C)
    flush_physics()
    try:
        from physics import data as D, config as CF
        raw  = D.fetch_klines_bulk(symbol, CF.INTERVAL, n)
        data = D.parse_klines(raw)
        bars = []
        for i in range(len(data['closes'])):
            bars.append({
                'ts':        int(data['timestamps'][i]),
                'open':      float(data['opens'][i]),
                'high':      float(data['highs'][i]),
                'low':       float(data['lows'][i]),
                'close':     float(data['closes'][i]),
                'volume':    float(data['volumes'][i]),
                'taker_buy': float(data['taker_buy'][i]),
                'live':      False,
            })
        log(f"  {len(bars)} REST bars loaded", G)
        return bars
    except Exception as e:
        log(f"REST bootstrap error: {e}", R)
        return []


# ── Paper trade tracker ───────────────────────────────────────────────────────

class PaperTrader:
    """
    Tracks live paper trades using real-time price data.
    One position at a time, long-only (flat cash, no margin).
    """

    def __init__(self, CF, PositionManager):
        self._CF          = CF
        self._pm          = PositionManager(CF.INITIAL_BAL)
        self._open        = None
        self._trades      = []
        self._bars        = 0
        self._start       = time.time()
        self._last_bids_n = 0    # depth diagnostic
        self._last_cav    = {}   # cavitation breakdown

    def reload_params(self, CF):
        self._CF = CF

    def scan_intrabar(self, partial_bar: dict, bids, asks, accum):
        """
        Intrabar update — do NOT call fusion.run() here.
        The HydraulicAccumulator is stateful; calling fusion dozens of times
        per minute saturates it and freezes the score at a fixed value.
        Intrabar monitoring is handled by _handle_depth() (peak OB walls,
        dark pool events) which is stateless and safe to call on every push.
        """
        pass

    def tick_price(self, price: float) -> bool:
        """
        Called on EVERY intrabar kline update (x=False).
        Checks stop/target on the live price — don't wait 60s for bar close.
        Returns True if a trade was closed so the caller can write stats.
        """
        if self._open is None:
            return False
        t   = self._open
        cur = price
        t['mae'] = max(t['mae'], float(t['entry'] - cur))
        t['mfe'] = max(t['mfe'], float(cur - t['entry']))

        reason = exit_px = None
        if cur <= t['stop']:     reason, exit_px = 'stop',   t['stop']
        elif cur >= t['target']: reason, exit_px = 'target', t['target']

        if reason:
            pnl = (exit_px - t['entry']) / t['entry'] * 100.0
            t.update(pnl_pct=pnl, exit_price=exit_px,
                     exit_reason=reason, exit_bar=self._bars)
            self._pm.on_close(pnl, t['mae'], t['tier'], t['size_usd'])
            self._trades.append(t)
            self._open = None
            col = G if pnl > 0 else R
            log(f"  [TICK {reason}] pnl={pnl:+.3f}%  tier={t['tier']}", col)
            return True
        return False

    def on_bar(self, bar: dict, bids, asks, accum):
        """
        Process one closed 1m bar.
        Stop/target already handled by tick_price() intrabar.
        This handles: timeout, new signal entry, MAE/MFE tracking, bar counter.
        Returns signal dict if a trade was entered.
        """
        from physics import fusion, config as CF

        self._bars += 1
        cur = bar['close']

        # ── Manage open trade (timeout only — stop/target caught by tick_price) ──
        if self._open is not None:
            t = self._open
            t['mae'] = max(t['mae'], float(t['entry'] - cur))
            t['mfe'] = max(t['mfe'], float(cur - t['entry']))

            if self._bars - t['bar_idx'] >= self._CF.MAX_HOLD_BARS:
                pnl = (cur - t['entry']) / t['entry'] * 100.0
                t.update(pnl_pct=pnl, exit_price=cur,
                         exit_reason='timeout', exit_bar=self._bars)
                self._pm.on_close(pnl, t['mae'], t['tier'], t['size_usd'])
                self._trades.append(t)
                self._open = None
                col = G if pnl > 0 else R
                log(f"  [timeout] pnl={pnl:+.3f}%  "
                    f"held={self._CF.MAX_HOLD_BARS}bars  tier={t['tier']}", col)

        if self._open is not None:
            return None   # one position at a time

        # ── Signal engine (ALL physics + microstructure) ──────────────────
        w = self._window_arrays()
        n_bars = len(w['closes'])
        if n_bars < self._CF.WARMUP_BARS:
            log(f"  warmup {n_bars}/{self._CF.WARMUP_BARS} bars", C)
            return None

        sl = slice(max(0, n_bars - 600), n_bars)
        p  = w['prices'][sl];   o = w['opens'][sl]
        c  = w['closes'][sl];   v = w['volumes'][sl]
        tb = w['taker_buy'][sl]

        atr_arr = _atr(w['highs'], w['lows'], w['closes'], self._CF.MAE_ATR_PERIOD)
        atr = float(atr_arr[-1]) if atr_arr[-1] > 0 else cur * 0.002

        sig = fusion.run(p, o, c, v, tb, accum, bids=bids, asks=asks)

        self._last_bids_n = len(bids) if bids else 0
        self._last_cav    = sig.get('cavitation', {})
        self._last_score  = float(sig.get('score', 0))
        self._last_snr    = float(sig.get('snr', 0))
        self._last_dir    = int(sig.get('direction', 0))
        self._last_tier   = int(sig.get('tier', 4))

        log(f"  score={self._last_score:+.4f}  dir={self._last_dir:+d}  "
            f"tier={self._last_tier}  snr={self._last_snr:.2f}  "
            f"threshold={self._CF.LONG_THRESHOLD}", C)

        if sig['direction'] != 1 or sig['tier'] >= 4:
            return sig
        if sig['snr'] < self._CF.SNR_MIN:
            return sig

        entry  = cur
        stop   = self._pm.entry_stop(entry, +1, atr, sig['tier'])
        target = self._pm.entry_target(entry, stop, +1, sig['tier'], atr)
        size   = self._pm.size(sig['tier'], sig['confidence'])

        self._open = {
            'bar_idx':    self._bars,
            'ts':         bar['ts'],
            'direction':  +1,
            'entry':      entry,
            'stop':       stop,
            'target':     target,
            'tier':       sig['tier'],
            'size_usd':   size,
            'confidence': sig['confidence'],
            'mae':        0.0,
            'mfe':        0.0,
        }
        return sig

    # window stored by LiveEngine and passed in — keep a ref
    _w = None

    def set_window(self, window):
        self._w = window

    def _window_arrays(self):
        bars = list(self._w)
        return {
            'timestamps': np.array([b['ts']        for b in bars], dtype=np.int64),
            'opens':      np.array([b['open']       for b in bars]),
            'highs':      np.array([b['high']       for b in bars]),
            'lows':       np.array([b['low']        for b in bars]),
            'closes':     np.array([b['close']      for b in bars]),
            'prices':     np.array([b['close']      for b in bars]),
            'volumes':    np.array([b['volume']     for b in bars]),
            'taker_buy':  np.array([b['taker_buy']  for b in bars]),
        }

    # Last fusion scores — updated every bar for remote diagnostics
    _last_score: float = 0.0
    _last_snr:   float = 0.0
    _last_dir:   int   = 0
    _last_tier:  int   = 4

    def stats(self):
        CF = self._CF
        n      = len(self._trades)
        wins   = [t for t in self._trades if t['pnl_pct'] > 0]
        hr     = max((time.time() - self._start) / 3600, 1/60)
        tph    = n / hr
        win_rt = len(wins) / n * 100 if n else 0.0
        pnls   = [t['pnl_pct'] for t in self._trades]

        # Properly-sized equity curve
        bal    = CF.INITIAL_BAL
        equity = [bal]
        for t in self._trades:
            sized = t['pnl_pct'] / 100.0 * (t['size_usd'] / max(bal, 1.0))
            bal  *= (1.0 + sized)
            equity.append(bal)
        eq = np.array(equity)

        # Per-trade Sharpe scaled to annual
        if len(pnls) >= 3:
            r    = np.array(pnls)
            std  = float(np.std(r))
            tpy  = n / max(self._bars, 1) * (252 * 1440)
            sh   = float(np.mean(r) / (std + 1e-10) * np.sqrt(max(tpy, 1)))
        else:
            sh = 0.0

        max_dd = 0.0
        if len(eq) >= 2:
            peak   = np.maximum.accumulate(eq)
            max_dd = float(np.min((eq - peak) / (peak + 1e-10) * 100))

        d = {
            'n_trades':      n,
            'trades_per_hr': round(tph, 2),
            'win_rate':      round(win_rt, 2),
            'sharpe':        round(sh, 4),
            'max_dd':        round(max_dd, 4),
            'avg_pnl':       round(float(np.mean(pnls)), 4) if pnls else 0.0,
            'equity_final':  round(float(eq[-1]), 2),
            'bars_live':     self._bars,
            # Diagnostics — last fusion score so remote monitor can see why
            # signals aren't firing without needing Termux access
            'last_score':    round(self._last_score, 4),
            'last_snr':      round(self._last_snr, 2),
            'last_dir':      self._last_dir,
            'last_tier':     self._last_tier,
            'threshold':     float(CF.LONG_THRESHOLD),
            'snr_min':       float(CF.SNR_MIN),
            # OB diagnostic — tells us whether real depth data reached fusion
            'last_bids_n':   self._last_bids_n,
            'last_cav_active': bool(self._last_cav.get('active', False)),
            'last_cav_risk':   round(float(self._last_cav.get('risk', 0)), 4),
        }
        for tier in [1, 2, 3]:
            tt = [t for t in self._trades if t['tier'] == tier]
            if tt:
                tw = [x for x in tt if x['pnl_pct'] > 0]
                d[f't{tier}_n']        = len(tt)
                d[f't{tier}_win_rate'] = round(len(tw)/len(tt)*100, 1)
        return d


# ── Main async engine ─────────────────────────────────────────────────────────

class LiveEngine:
    """
    Connects directly to Binance.US WebSocket.
    Processes each 1m bar close with real-time L20 order book.
    """

    def __init__(self):
        CF, HydAccum, PM = load_physics()
        self._CF     = CF
        self._accum  = HydAccum()
        self._trader = PaperTrader(CF, PM)
        self._window = deque(maxlen=700)
        self._trader.set_window(self._window)

        # Current order book — updated on every WebSocket depth push (variable rate)
        self._bids: list = []
        self._asks: list = []

        # Peak wall tracking across all pushes since last bar close.
        # Captures dark pool walls that post and absorb mid-bar before close.
        self._peak_bid_wall: float = 0.0
        self._peak_ask_wall: float = 0.0
        self._peak_bids: list = []
        self._peak_asks: list = []

        self._last_fetch  = 0.0
        self._running     = True

    def _reload(self):
        """Hot-reload params from updated config.py."""
        flush_physics()
        from physics import config as CF
        self._CF = CF
        self._trader.reload_params(CF)
        log(f"Params reloaded: threshold={CF.LONG_THRESHOLD}  "
            f"SNR={CF.SNR_MIN}  tier3_target={CF.TIER3_TARGET_ATR}×ATR", C)

    def _handle_kline(self, k: dict):
        is_closed = k.get('x', False)
        cur_price = float(k['c'])

        # Every update: check stops/targets against live price immediately
        trade_closed = self._trader.tick_price(cur_price)
        if trade_closed:
            self._write_stats()

        # Build the current bar state (partial or complete)
        bar = {
            'ts':        int(k['t']),
            'open':      float(k['o']),
            'high':      float(k['h']),
            'low':       float(k['l']),
            'close':     cur_price,
            'volume':    float(k['v']),
            'taker_buy': float(k['V']),
            'live':      True,
        }

        bids = list(self._peak_bids or self._bids)
        asks = list(self._peak_asks or self._asks)

        if not is_closed:
            # Intrabar: run wave equations on historical window + current partial
            # bar so the hydraulic accumulator and soliton charge in real time,
            # not just once per minute at bar close.
            self._trader.scan_intrabar(bar, bids or None, asks or None, self._accum)
            return

        # Bar closed — commit to permanent window and run full signal engine
        self._window.append(bar)
        sig = self._trader.on_bar(bar, bids or None, asks or None, self._accum)
        if sig:
            self._log_waveforms(bar, sig, bids)

        # Reset peak tracking for the next bar
        self._peak_bid_wall = 0.0
        self._peak_ask_wall = 0.0
        self._peak_bids     = []
        self._peak_asks     = []

        self._write_stats()

    def _handle_depth(self, data: dict):
        bids = [(float(p), float(q)) for p, q in data.get('bids', [])]
        asks = [(float(p), float(q)) for p, q in data.get('asks', [])]
        self._bids = bids
        self._asks = asks

        if not bids or not asks:
            return

        # Track peak wall seen across every push since last bar close.
        # Pushes are event-driven (variable rate, not fixed 100ms intervals) —
        # we process each one as it arrives. Dark pool walls post and absorb
        # in seconds; the bar-close snapshot often misses them entirely.
        top_bid = bids[0][1]
        top_ask = asks[0][1]

        if top_bid > self._peak_bid_wall:
            self._peak_bid_wall = top_bid
            self._peak_bids     = bids   # snapshot when wall was largest

        if top_ask > self._peak_ask_wall:
            self._peak_ask_wall = top_ask
            self._peak_asks     = asks

        # Log unusually large walls (>3× normal) as dark pool events
        bid_vol5 = sum(q for _, q in bids[:5])
        ask_vol5 = sum(q for _, q in asks[:5])
        total5   = bid_vol5 + ask_vol5
        if total5 > 0:
            obi = (bid_vol5 - ask_vol5) / total5
            if abs(obi) > 0.40:   # extreme imbalance — dark pool absorbing
                side = 'BID' if obi > 0 else 'ASK'
                log(f"  [DARK] {side} wall OBI={obi:+.3f}  "
                    f"bid5={bid_vol5:.2f}  ask5={ask_vol5:.2f}", Y)

    def _log_waveforms(self, bar: dict, sig: dict, bids: list):
        """Print the wave equation breakdown for every signal bar."""
        sol = sig['soliton'];      wh = sig['water_hammer']
        acc = sig['accum'];        re = sig['reynolds']
        sh  = sig['shock'];        d  = sig['darcy']
        cav = sig['cavitation'];   mf = sig['mass_flow']
        cvd = sig['cvd'];          obi= sig['obi']

        has_ob = bool(bids)
        ob_str = f"OBI={obi['obi']:+.3f}" if has_ob else "OBI=proxy(no book)"

        log(f"BAR {datetime.fromtimestamp(bar['ts']/1000).strftime('%H:%M')} "
            f"close={bar['close']:.2f}  score={sig['score']:.3f}  "
            f"snr={sig['snr']:.2f}  tier={sig['tier']}", B)
        log(f"  KdV-soliton  detected={sol['detected']}  "
            f"balance={sol.get('balance',0):.3f}  dir={sol.get('direction',0):+d}", C)
        log(f"  WaterHammer  detected={wh['detected']}  "
            f"strength={wh.get('strength',0):.3f}", C)
        log(f"  DarkPool     firing={acc['firing']}  "
            f"pressure={acc.get('pressure',0):.3f}  dir={acc.get('direction',0):+d}", C)
        log(f"  Reynolds     regime={re['regime']}  "
            f"Re={re.get('re',0):.1f}  mult={re.get('multiplier',1):.2f}", C)
        log(f"  Shock        detected={sh['detected']}  "
            f"Mach={sh.get('mach',0):.3f}", C)
        log(f"  Darcy        Q={d['Q']:.3f}  friction={d.get('friction',0):.3f}", C)
        log(f"  Cavitation   active={cav['active']}  "
            f"mult={cav.get('multiplier',1):.2f}", C)
        log(f"  MassFlow     flow={mf.get('flow',0):.3f}", C)
        log(f"  CVD          strong={cvd['strong']}  "
            f"div={cvd.get('divergence',0):.3f}", C)
        log(f"  {ob_str}  physics={sig['physics']:.3f}  "
            f"micro={sig['micro']:.3f}", C)

        if sig['direction'] == 1 and sig['tier'] < 4:
            t = self._trader._open
            if t:
                log(f"  → ENTERED  stop={t['stop']:.2f}  "
                    f"target={t['target']:.2f}", Y)

    def _write_stats(self):
        try:
            trigger_id = json.loads(TRIGGER_F.read_text()).get('id', 'p0')
        except Exception:
            trigger_id = 'p0'

        s = self._trader.stats()
        col = G if s['win_rate'] >= 60 else (Y if s['win_rate'] >= 45 else R)
        log(f"[{trigger_id}] sharpe={s['sharpe']:+.3f} | "
            f"win={s['win_rate']:.1f}% | tph={s['trades_per_hr']:.2f} | "
            f"n={s['n_trades']}", col)

        result = {
            'ts':         datetime.utcnow().isoformat() + 'Z',
            'trigger_id': trigger_id,
            'status':     'live',
            'stats':      s,
        }
        content = json.dumps(result, indent=2)
        RESULTS_F.write_text(content)
        # Push in background thread — don't block the async event loop
        asyncio.get_event_loop().run_in_executor(None, self._push_results, content)

    def _push_results(self, content: str):
        """Runs in a thread executor — never blocks the WebSocket event loop."""
        rel = str(RESULTS_F.relative_to(REPO))
        try:
            git("stash")
            git("fetch", "origin", DATA_BRANCH)
            r = git("checkout", "-B", DATA_BRANCH, f"origin/{DATA_BRANCH}")
            if r.returncode != 0:
                log(f"  push: checkout data/live failed: {r.stderr[:80]}", R)
                git("checkout", CODE_BRANCH)
                git("stash", "pop")
                RESULTS_F.write_text(content)
                return
            RESULTS_F.write_text(content)
            git("add", rel)
            git("commit", "--allow-empty", "-m", f"live {ts_str()}")
            pushed = False
            for wait in [0, 2, 4, 8]:
                time.sleep(wait)
                r = git("push", "-u", "origin", DATA_BRANCH, "--force")
                if r.returncode == 0:
                    pushed = True
                    break
                log(f"  push attempt failed: {r.stderr[:60]}", Y)
            if not pushed:
                log("  push: all attempts failed — optimizer reads from local file", R)
        except Exception as e:
            log(f"  push error: {e}", R)
        finally:
            git("checkout", CODE_BRANCH)
            git("stash", "pop")
            RESULTS_F.write_text(content)   # restore local copy

    async def _fetch_params(self):
        """Pull latest params from remote (every FETCH_EVERY_S seconds)."""
        now = time.time()
        if now - self._last_fetch < FETCH_EVERY_S:
            return
        self._last_fetch = now
        await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: (git("fetch", "origin", CODE_BRANCH, "--quiet"),
                     git("reset", "--hard", f"origin/{CODE_BRANCH}"))
        )
        self._reload()

    async def run(self):
        backoff = 1
        while self._running:
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.ws_connect(WS_URL, heartbeat=20,
                                               receive_timeout=90) as ws:
                        backoff = 1
                        log(f"Connected to {WS_URL}", G)
                        async for msg in ws:
                            if not self._running:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    d      = json.loads(msg.data)
                                    stream = d.get('stream', '')
                                    data   = d.get('data', {})
                                    if '@kline' in stream:
                                        self._handle_kline(data.get('k', {}))
                                    elif '@depth' in stream:
                                        self._handle_depth(data)
                                    await self._fetch_params()
                                except Exception as e:
                                    log(f"Parse error: {e}", R)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                log(f"WS closed/error — reconnecting", Y)
                                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self._running:
                    break
                log(f"Connection error ({e}) — retry in {backoff}s", R)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

        log("Engine stopped", Y)
        self._write_stats()

    def stop(self):
        self._running = False


# ── Entry point ───────────────────────────────────────────────────────────────

async def _main():
    flush_physics()
    from physics import config as CF

    log(f"Physics Live Engine — {CF.SYMBOL}", B)
    log(f"  Direct WebSocket → Binance.US", G)
    log(f"  Streams: kline_1m (bar close) + depth20@100ms (real OB)", G)
    log(f"  Params: threshold={CF.LONG_THRESHOLD}  SNR={CF.SNR_MIN}  "
        f"stop={CF.MAE_ATR_FALLBACK}×ATR  target={CF.TIER3_TARGET_ATR}×ATR", C)

    engine = LiveEngine()

    # Seed window with REST history for warmup context
    boot = await asyncio.get_event_loop().run_in_executor(
        None, lambda: bootstrap_rest(CF.SYMBOL, 300)
    )
    for bar in boot:
        engine._window.append(bar)
    if boot:
        log(f"Window seeded with {len(boot)} REST bars — now on live stream", G)

    # Seed OB so cavitation has real depth data from bar 1, not just after the
    # first depth WebSocket push (which could be seconds into the first bar).
    try:
        from physics import data as D
        ob = await asyncio.get_event_loop().run_in_executor(
            None, lambda: D.fetch_orderbook(CF.SYMBOL, 20)
        )
        engine._bids = ob['bids']
        engine._asks = ob['asks']
        log(f"OB seeded: {len(ob['bids'])} bid levels  {len(ob['asks'])} ask levels", G)
    except Exception as e:
        log(f"OB seed error (depth stream will populate): {e}", Y)

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, engine.stop)

    await engine.run()


if __name__ == '__main__':
    asyncio.run(_main())
