#!/usr/bin/env python3
"""
build_candles.py — Candle pipeline: raw ticks → 1s → all timeframes.

Pipeline:
  1. Load raw tick data (aggTrades from BTCUSDT_LIVE.jsonl.gz)
  2. Build 1-second OHLCV candles from trades
  3. Merge with historical 1m candles from ingest.py
  4. Aggregate to 1m, 5m, 10m, 15m, 30m, 45m, 1h, 4h
  5. Compute rolling window stats per TF (ATR, avg_vol, avg_range, etc.)
  6. Write ~/.trendrider/tf_{tf}.json.gz per timeframe
     Write ~/.trendrider/tf_candles.json  (summary stats, no bar arrays)

Standalone:  python3 build_candles.py
             python3 build_candles.py --dump   # print TF summary
Import:      import build_candles; data = build_candles.build()
"""

import gzip, json, sys, time
from collections import defaultdict
from pathlib import Path

DATA_DIR = Path.home() / '.trendrider'

# Timeframe name → seconds per bar
TIMEFRAMES = {
    '1s':  1,
    '1m':  60,
    '5m':  300,
    '10m': 600,
    '15m': 900,
    '30m': 1800,
    '45m': 2700,
    '1h':  3600,
    '4h':  14400,
}

# Rolling window — number of closed bars to keep per TF
WINDOW = {
    '1s':  120,
    '1m':  1440,
    '5m':  288,
    '10m': 144,
    '15m': 96,
    '30m': 48,
    '45m': 32,
    '1h':  168,
    '4h':  90,
}

ATR_PERIOD = 14


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_gz(fname: str):
    p = DATA_DIR / fname
    if p.exists():
        try:
            with gzip.open(p, 'rt') as f:
                return json.loads(f.read())
        except Exception:
            pass
    return None


def _load_live_lines() -> list:
    p = DATA_DIR / 'BTCUSDT_LIVE.jsonl.gz'
    if not p.exists():
        return []
    try:
        with gzip.open(p, 'rt') as f:
            return [l for l in f.read().splitlines() if l]
    except Exception:
        return []


# ── Block 1: raw ticks → 1-second OHLCV ──────────────────────────────────────

def ticks_to_seconds(lines: list) -> dict:
    """
    Parse raw aggTrade lines into per-second OHLCV candles.
    Record format: ['T', ts_ms, price_int, qty_int, is_buyer_maker]
      price = price_int / 100.0
      qty   = qty_int / 10000.0
      is_buyer_maker=1 → sell taker; 0 → buy taker
    Returns: {ts_sec_int: bar_dict}
    """
    by_sec = {}
    for ln in lines:
        try:
            rec = json.loads(ln)
        except Exception:
            continue
        if rec[0] != 'T':
            continue
        ts_ms   = int(rec[1])
        price   = int(rec[2]) / 100.0
        qty     = int(rec[3]) / 10000.0
        is_sell = bool(rec[4])
        ts_sec  = ts_ms // 1000

        if ts_sec not in by_sec:
            by_sec[ts_sec] = {
                'ts':     ts_sec * 1000,
                'open':   price, 'high': price,
                'low':    price, 'close': price,
                'volume': 0.0, 'buy_v': 0.0, 'sell_v': 0.0, 'n': 0,
            }
        bar = by_sec[ts_sec]
        if price > bar['high']:  bar['high']  = price
        if price < bar['low']:   bar['low']   = price
        bar['close']   = price
        bar['volume'] += qty
        bar['n']      += 1
        if is_sell:  bar['sell_v'] += qty
        else:        bar['buy_v']  += qty

    return by_sec


# ── Block 2: historical candles → normalised bar dicts ────────────────────────

def normalise_klines(klines: list) -> list:
    """
    Convert Binance kline arrays [open_ms, o, h, l, c, vol, taker_buy, ...]
    into uniform bar dicts compatible with the rest of the pipeline.
    """
    bars = []
    for k in klines:
        vol      = float(k[5])
        buy_v    = float(k[6])
        bars.append({
            'ts':     int(k[0]),
            'open':   float(k[1]),
            'high':   float(k[2]),
            'low':    float(k[3]),
            'close':  float(k[4]),
            'volume': vol,
            'buy_v':  buy_v,
            'sell_v': round(vol - buy_v, 6),
            'n':      0,
        })
    return bars


# ── Block 3: aggregate bars into a higher timeframe ───────────────────────────

def aggregate(source_bars: list, tf_secs: int) -> list:
    """
    Aggregate a list of bar dicts (any source TF) into tf_secs-aligned bars.
    Each output bar spans [ts_aligned, ts_aligned + tf_secs).
    """
    buckets = defaultdict(list)
    for b in source_bars:
        ts_sec  = b['ts'] // 1000
        aligned = (ts_sec // tf_secs) * tf_secs
        buckets[aligned].append(b)

    result = []
    for aligned, bars in sorted(buckets.items()):
        bars.sort(key=lambda b: b['ts'])
        result.append({
            'ts':     aligned * 1000,
            'open':   bars[0]['open'],
            'high':   max(b['high']   for b in bars),
            'low':    min(b['low']    for b in bars),
            'close':  bars[-1]['close'],
            'volume': round(sum(b['volume'] for b in bars), 6),
            'buy_v':  round(sum(b.get('buy_v',  0) for b in bars), 6),
            'sell_v': round(sum(b.get('sell_v', 0) for b in bars), 6),
            'n':      sum(b.get('n', 1) for b in bars),
        })
    return result


# ── Block 4: merge live + historical, trim to rolling window ──────────────────

def merge_and_window(live: list, historical: list, window: int) -> list:
    """
    Merge live bars (built from ticks) with historical bars.
    Live bars take priority on any overlapping timestamp.
    Trims result to the last `window` bars.
    """
    merged = {}
    for b in historical:
        merged[b['ts']] = b
    for b in live:
        merged[b['ts']] = b   # live overrides
    bars = sorted(merged.values(), key=lambda b: b['ts'])
    return bars[-window:]


# ── Block 5: rolling stats per TF ─────────────────────────────────────────────

def rolling_stats(bars: list) -> dict:
    """
    Compute rolling statistics over a TF window for use in equations.
    Returns: atr, atr_up, atr_dn, avg_vol, avg_range, session stats.
    """
    if not bars:
        return {}

    closes  = [b['close']  for b in bars]
    volumes = [b['volume'] for b in bars]
    highs   = [b['high']   for b in bars]
    lows    = [b['low']    for b in bars]
    ranges  = [h - l for h, l in zip(highs, lows)]

    # ATR + directional split
    trs, ups, dns = [], [], []
    for i in range(1, len(bars)):
        h, l, pc = bars[i]['high'], bars[i]['low'], bars[i-1]['close']
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
        if bars[i]['close'] >= bars[i]['open']:
            ups.append(h - l)
        else:
            dns.append(h - l)

    n      = min(ATR_PERIOD, len(trs))
    atr    = round(sum(trs[-n:]) / n, 4)    if trs else 0.0
    atr_up = round(sum(ups[-n:]) / min(n, len(ups)), 4) if ups else atr
    atr_dn = round(sum(dns[-n:]) / min(n, len(dns)), 4) if dns else atr

    avg_vol   = round(sum(volumes) / len(volumes), 6)
    avg_range = round(sum(ranges)  / len(ranges),  4)

    # Session stats (today UTC)
    now_ms = int(time.time() * 1000)
    today0 = now_ms - (now_ms % 86_400_000)
    today  = [b for b in bars if b['ts'] >= today0]
    s_open  = today[0]['open']               if today else closes[-1]
    s_high  = max(b['high']  for b in today) if today else highs[-1]
    s_low   = min(b['low']   for b in today) if today else lows[-1]
    s_range = round(s_high - s_low, 4)
    s_chg   = round(closes[-1] - s_open, 4)

    # HTF trend: n=3 consecutive HH+HL = bull, LH+LL = bear
    def _htf_trend(n=3):
        if len(bars) < n + 1: return 0
        hh = all(bars[-i]['high'] > bars[-i-1]['high'] for i in range(1, n+1))
        hl = all(bars[-i]['low']  > bars[-i-1]['low']  for i in range(1, n+1))
        lh = all(bars[-i]['high'] < bars[-i-1]['high'] for i in range(1, n+1))
        ll = all(bars[-i]['low']  < bars[-i-1]['low']  for i in range(1, n+1))
        if hh and hl: return  1
        if lh and ll: return -1
        return 0

    return {
        'atr':       atr,
        'atr_up':    atr_up,
        'atr_dn':    atr_dn,
        'avg_vol':   avg_vol,
        'avg_range': avg_range,
        's_open':    round(s_open,  2),
        's_high':    round(s_high,  2),
        's_low':     round(s_low,   2),
        's_range':   s_range,
        's_chg':     s_chg,
        's_chg_pct': round(s_chg / s_open * 100 if s_open else 0.0, 4),
        'trend':     _htf_trend(),
    }


# ── Main pipeline ─────────────────────────────────────────────────────────────

def build(verbose: bool = False) -> dict:
    """
    Run the full candle pipeline. Returns dict keyed by TF:
      {bars: [...], stats: {...}, last_ts: int, count: int}
    """
    # --- Block 1: raw ticks → 1s candles ---
    lines = _load_live_lines()
    secs  = ticks_to_seconds(lines)
    if verbose:
        print(f'[build] ticks: {len(lines)} lines → {len(secs)} second bars', flush=True)

    # --- Block 2: load + normalise historical candles ---
    hist_1m_raw = _load_gz('BTCUSDT_1m.json.gz') or []
    hist_1h_raw = _load_gz('BTCUSDT_1h.json.gz') or []
    hist_4h_raw = _load_gz('BTCUSDT_4h.json.gz') or []
    hist_1m = normalise_klines(hist_1m_raw)
    hist_1h = normalise_klines(hist_1h_raw)
    hist_4h = normalise_klines(hist_4h_raw)

    # --- Block 3: aggregate 1s → 1m live bars ---
    live_1m = aggregate(list(secs.values()), TIMEFRAMES['1m'])

    # --- Block 4: merge live 1m + historical 1m ---
    merged_1m = merge_and_window(live_1m, hist_1m, WINDOW['1m'])

    output = {}

    # 1s — live ticks only
    bars_1s = sorted(secs.values(), key=lambda b: b['ts'])[-WINDOW['1s']:]
    output['1s'] = {
        'bars':    bars_1s,
        'stats':   rolling_stats(bars_1s),
        'last_ts': bars_1s[-1]['ts'] if bars_1s else 0,
        'count':   len(bars_1s),
    }

    # 1m — merged historical + live
    output['1m'] = {
        'bars':    merged_1m,
        'stats':   rolling_stats(merged_1m),
        'last_ts': merged_1m[-1]['ts'] if merged_1m else 0,
        'count':   len(merged_1m),
    }

    # Higher TFs: aggregate from merged 1m, then merge with historical where available
    tf_hist = {'1h': hist_1h, '4h': hist_4h}

    for tf in ('5m', '10m', '15m', '30m', '45m', '1h', '4h'):
        tf_secs = TIMEFRAMES[tf]
        live_tf = aggregate(merged_1m, tf_secs)
        hist_tf = tf_hist.get(tf, [])
        bars    = merge_and_window(live_tf, hist_tf, WINDOW[tf])
        stats   = rolling_stats(bars)
        output[tf] = {
            'bars':    bars,
            'stats':   stats,
            'last_ts': bars[-1]['ts'] if bars else 0,
            'count':   len(bars),
        }
        if verbose:
            print(f'[build] {tf:>4s}: {len(bars):4d} bars  '
                  f'atr={stats.get("atr", 0):.2f}  '
                  f'trend={stats.get("trend", 0):+d}', flush=True)

    return output


def save(data: dict = None, verbose: bool = True) -> Path:
    """Build (if needed) and write output files to DATA_DIR."""
    if data is None:
        data = build(verbose=verbose)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Per-TF bar files (for wave_scan to read)
    for tf, d in data.items():
        with gzip.open(DATA_DIR / f'tf_{tf}.json.gz', 'wt') as f:
            json.dump(d['bars'], f)

    # Summary stats file (no bar arrays — small, fast to read)
    summary = {
        tf: {'stats': d['stats'], 'last_ts': d['last_ts'], 'count': d['count']}
        for tf, d in data.items()
    }
    out = DATA_DIR / 'tf_candles.json'
    out.write_text(json.dumps(summary, indent=2))

    if verbose:
        print(f'[build] saved → {DATA_DIR}/', flush=True)
    return out


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--dump', action='store_true', help='Print TF summary to stdout')
    a = p.parse_args()
    data = build(verbose=True)
    save(data, verbose=True)
    if a.dump:
        print()
        for tf, d in data.items():
            s = d['stats']
            print(f'{tf:>4s}  bars={d["count"]:4d}  '
                  f'atr={s.get("atr",0):.4f}  '
                  f'avg_vol={s.get("avg_vol",0):.4f}  '
                  f's_range={s.get("s_range",0):.2f}  '
                  f'trend={s.get("trend",0):+d}')
