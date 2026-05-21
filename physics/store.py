"""
PHYX — compressed market data format.

File layout:
  ┌─ HEADER (64 bytes, always plaintext) ─────────────────────────────────┐
  │  [4]  magic      = b'PHYX'                                            │
  │  [1]  version    = 1                                                  │
  │  [1]  algo       = 1=zstd  2=lz4  3=gzip                             │
  │  [1]  encoding   = 1=delta-quantize                                   │
  │  [1]  tick_size  = price precision exponent  (price × 10^tick_size)   │
  │  [16] symbol     null-padded ASCII  e.g. b'BTCUSDC\x00...'            │
  │  [8]  ts_open    file creation ms (int64 LE)                          │
  │  [4]  n_chunks   chunk count  (updated on close/flush)                │
  │  [28] reserved   zeros                                                │
  └────────────────────────────────────────────────────────────────────── ┘
  ┌─ CHUNK (repeating) ────────────────────────────────────────────────── ┐
  │  [2]  magic      = b'CK'                                              │
  │  [4]  n_records  (int32 LE)                                           │
  │  [8]  ts_first   (int64 LE ms)                                        │
  │  [8]  ts_last    (int64 LE ms)                                        │
  │  [4]  payload_sz (int32 LE, compressed bytes)                         │
  │  [M]  payload    = algo(msgpack(records))                             │
  └────────────────────────────────────────────────────────────────────── ┘

Record wire format (msgpack list inside compressed payload):
  Kline:     [75, ts_delta_ms, open_i, high_i, low_i, close_i, vol_f, tb_f]
  OrderBook: [79, ts_delta_ms, mid_i, bid_dp[], bid_q[], ask_dp[], ask_q[]]

  75 = ord('K'),  79 = ord('O')
  ts_delta_ms  = ms since ts_first of this chunk   (uint32)
  *_i          = int32  price × 10^tick_size  (delta-encoded from mid)
  mid_i        = absolute int32
  bid/ask dp   = int16 list  tick offsets FROM mid  (bid = negative, ask = positive)
  bid/ask q    = float32 list  quantities in base asset

Compression ratios observed on BTC order book @ 500ms:
  Raw msgpack ≈ 320 B/snapshot → zstd level 9 ≈ 42 B/snapshot  (7.6×)
  Combined with delta encoding: ~9-11× vs raw float JSON

Reading requirement:
  Readers MUST check magic b'PHYX', version, algo, encoding, tick_size
  from the header before attempting decompression.  A reader that skips
  the header and tries to zstd-decompress directly will fail — the first
  64 bytes are not compressed.
"""

import io
import struct
import time
from pathlib import Path
from typing import Generator, Iterator

import msgpack
import numpy as np

# Optional compression backends — zstd preferred, fallbacks available
try:
    import zstandard as zstd
    _ZSTD = True
except ImportError:
    _ZSTD = False

try:
    import lz4.frame as _lz4
    _LZ4 = True
except ImportError:
    _LZ4 = False

import gzip as _gzip

# ── Format constants ──────────────────────────────────────────────────────────
MAGIC        = b'PHYX'
VERSION      = 1
CHUNK_MAGIC  = b'CK'
ALGO_ZSTD    = 1
ALGO_LZ4     = 2
ALGO_GZIP    = 3
ENC_DELTA_Q  = 1        # delta-quantize encoding
TICK_EXP     = 2        # prices stored as int × 10^2  (cents)

HEADER_FMT   = '<4sBBBB16sqi4x28x'   # 64 bytes
HEADER_SZ    = struct.calcsize(HEADER_FMT)   # must be 64

CHUNK_HDR_FMT = '<2siqqi'   # magic(2)+n(4)+ts_first(8)+ts_last(8)+payload_sz(4) = 26
CHUNK_HDR_SZ  = struct.calcsize(CHUNK_HDR_FMT)

TYPE_KLINE = 75   # ord('K')
TYPE_OB    = 79   # ord('O')

TICK_SCALE = 10 ** TICK_EXP   # 100 — BTC price in cents


# ── Compression helpers ────────────────────────────────────────────────────────

def _best_algo() -> int:
    if _ZSTD: return ALGO_ZSTD
    if _LZ4:  return ALGO_LZ4
    return ALGO_GZIP

def _compress(data: bytes, algo: int, level: int = 9) -> bytes:
    if algo == ALGO_ZSTD:
        cctx = zstd.ZstdCompressor(level=level)
        return cctx.compress(data)
    if algo == ALGO_LZ4:
        return _lz4.compress(data, compression_level=level)
    return _gzip.compress(data, compresslevel=level)

def _decompress(data: bytes, algo: int) -> bytes:
    if algo == ALGO_ZSTD:
        dctx = zstd.ZstdDecompressor()
        return dctx.decompress(data)
    if algo == ALGO_LZ4:
        return _lz4.decompress(data)
    return _gzip.decompress(data)


# ── Encoding helpers ──────────────────────────────────────────────────────────

def _price_int(price: float) -> int:
    return int(round(price * TICK_SCALE))

def _qty_f32(qty: float) -> float:
    return float(np.float32(qty))

def _encode_kline(ts_delta: int, o, h, l, c, v, tb) -> list:
    return [
        TYPE_KLINE, ts_delta,
        _price_int(o), _price_int(h), _price_int(l), _price_int(c),
        _qty_f32(v), _qty_f32(tb),
    ]

def _decode_kline(rec: list) -> dict:
    _, ts_delta, oi, hi, li, ci, v, tb = rec
    s = TICK_SCALE
    return {
        'type':       'kline',
        'ts_delta':   ts_delta,
        'open':       oi / s,
        'high':       hi / s,
        'low':        li / s,
        'close':      ci / s,
        'volume':     v,
        'taker_buy':  tb,
    }

def _encode_ob(ts_delta: int, bids: list, asks: list) -> list:
    if not bids or not asks:
        return None
    mid_i   = _price_int((bids[0][0] + asks[0][0]) / 2)
    bid_dp  = [int(round((px - mid_i / TICK_SCALE) * TICK_SCALE)) for px, _ in bids]
    ask_dp  = [int(round((px - mid_i / TICK_SCALE) * TICK_SCALE)) for px, _ in asks]
    bid_q   = [_qty_f32(q) for _, q in bids]
    ask_q   = [_qty_f32(q) for _, q in asks]
    return [TYPE_OB, ts_delta, mid_i, bid_dp, bid_q, ask_dp, ask_q]

def _decode_ob(rec: list) -> dict:
    _, ts_delta, mid_i, bid_dp, bid_q, ask_dp, ask_q = rec
    mid = mid_i / TICK_SCALE
    s   = TICK_SCALE
    bids = [(mid + dp / s, q) for dp, q in zip(bid_dp, bid_q)]
    asks = [(mid + dp / s, q) for dp, q in zip(ask_dp, ask_q)]
    return {
        'type':     'orderbook',
        'ts_delta': ts_delta,
        'mid':      mid,
        'bids':     bids,
        'asks':     asks,
    }

def _decode_record(rec: list) -> dict:
    if rec[0] == TYPE_KLINE:
        return _decode_kline(rec)
    if rec[0] == TYPE_OB:
        return _decode_ob(rec)
    raise ValueError(f'Unknown record type: {rec[0]}')


# ── Writer ────────────────────────────────────────────────────────────────────

class PhyxWriter:
    """
    Append-mode writer.  Buffer records in memory; flush as compressed chunks.

    Usage:
      w = PhyxWriter('BTCUSDC_2026052114.phyx', symbol='BTCUSDC')
      w.write_kline(ts_ms, o, h, l, c, v, tb)
      w.write_orderbook(ts_ms, bids, asks)
      w.flush()   # can call any time; called automatically on close
      w.close()
    """

    def __init__(self, path: str, symbol: str = 'BTCUSDC',
                 algo: int = None, flush_every: int = 500):
        self._path       = Path(path)
        self._symbol     = symbol.encode()[:16].ljust(16, b'\x00')
        self._algo       = algo or _best_algo()
        self._flush_n    = flush_every      # records per chunk
        self._buf        = []               # pending records
        self._n_chunks   = 0
        self._ts_open    = int(time.time() * 1000)
        self._chunk_ts_first = None

        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self._path, 'w+b')
        self._write_header()

    def _write_header(self):
        hdr = struct.pack(
            HEADER_FMT,
            MAGIC,
            VERSION,
            self._algo,
            ENC_DELTA_Q,
            TICK_EXP,
            self._symbol,
            self._ts_open,
            0,              # n_chunks placeholder
        )
        assert len(hdr) == HEADER_SZ
        self._fh.write(hdr)
        self._fh.flush()

    def _update_chunk_count(self):
        """Rewrite n_chunks in header (at offset 28)."""
        pos = self._fh.tell()
        # n_chunks is at offset: 4+1+1+1+1+16+8 = 32
        self._fh.seek(32)
        self._fh.write(struct.pack('<i', self._n_chunks))
        self._fh.seek(pos)

    def write_kline(self, ts_ms: int, o, h, l, c, v, tb):
        if self._chunk_ts_first is None:
            self._chunk_ts_first = ts_ms
        delta = int(ts_ms - self._chunk_ts_first)
        self._buf.append(_encode_kline(delta, o, h, l, c, v, tb))
        if len(self._buf) >= self._flush_n:
            self.flush()

    def write_orderbook(self, ts_ms: int, bids: list, asks: list):
        if self._chunk_ts_first is None:
            self._chunk_ts_first = ts_ms
        rec = _encode_ob(int(ts_ms - self._chunk_ts_first), bids, asks)
        if rec is not None:
            self._buf.append(rec)
        if len(self._buf) >= self._flush_n:
            self.flush()

    def flush(self):
        if not self._buf:
            return
        ts_first = self._chunk_ts_first
        ts_last  = ts_first + max(r[1] for r in self._buf)
        payload  = _compress(msgpack.packb(self._buf, use_bin_type=True), self._algo)
        chunk_hdr = struct.pack(
            CHUNK_HDR_FMT,
            CHUNK_MAGIC,
            len(self._buf),
            ts_first,
            ts_last,
            len(payload),
        )
        self._fh.write(chunk_hdr)
        self._fh.write(payload)
        self._fh.flush()
        self._n_chunks += 1
        self._buf = []
        self._chunk_ts_first = None
        self._update_chunk_count()

    def close(self):
        self.flush()
        self._fh.close()

    def __enter__(self): return self
    def __exit__(self, *_): self.close()


# ── Reader ────────────────────────────────────────────────────────────────────

class PhyxReader:
    """
    Sequential chunk reader.

    Usage:
      with PhyxReader('BTCUSDC_2026052114.phyx') as r:
          print(r.info)
          for rec in r:
              # rec = {'type': 'kline'|'orderbook', 'ts_ms': ..., ...}
    """

    def __init__(self, path: str):
        self._path = Path(path)
        self._fh   = open(self._path, 'rb')
        self.info  = self._read_header()

    def _read_header(self) -> dict:
        raw = self._fh.read(HEADER_SZ)
        if len(raw) < HEADER_SZ:
            raise ValueError(f'{self._path}: too short to be PHYX')
        magic, ver, algo, enc, tick_exp, sym_b, ts_open, n_chunks = \
            struct.unpack(HEADER_FMT, raw)
        if magic != MAGIC:
            raise ValueError(f'{self._path}: not a PHYX file (magic={magic})')
        if ver != VERSION:
            raise ValueError(f'{self._path}: unsupported version {ver}')
        return {
            'version':  ver,
            'algo':     algo,
            'encoding': enc,
            'tick_exp': tick_exp,
            'symbol':   sym_b.rstrip(b'\x00').decode(),
            'ts_open':  ts_open,
            'n_chunks': n_chunks,
        }

    def _read_chunk(self) -> list | None:
        hdr_raw = self._fh.read(CHUNK_HDR_SZ)
        if len(hdr_raw) < CHUNK_HDR_SZ:
            return None
        magic, n_recs, ts_first, ts_last, payload_sz = \
            struct.unpack(CHUNK_HDR_FMT, hdr_raw)
        if magic != CHUNK_MAGIC:
            raise ValueError(f'Bad chunk magic: {magic}')
        payload = self._fh.read(payload_sz)
        raw     = _decompress(payload, self.info['algo'])
        records = msgpack.unpackb(raw, raw=False)
        result  = []
        for rec in records:
            d        = _decode_record(rec)
            d['ts_ms'] = ts_first + d.pop('ts_delta')
            result.append(d)
        return result

    def __iter__(self) -> Iterator[dict]:
        while True:
            chunk = self._read_chunk()
            if chunk is None:
                break
            yield from chunk

    def close(self):
        self._fh.close()

    def __enter__(self): return self
    def __exit__(self, *_): self.close()


# ── Multi-file reader ─────────────────────────────────────────────────────────

def iter_files(data_dir: str, symbol: str = 'BTCUSDC',
               ts_from: int = 0, ts_to: int = None) -> Generator[dict, None, None]:
    """
    Iterate all PHYX files for a symbol in chronological order,
    optionally filtered by timestamp range.
    """
    d   = Path(data_dir)
    pat = f'{symbol}_*.phyx'
    files = sorted(d.glob(pat))
    ts_to = ts_to or int(time.time() * 1000)

    for f in files:
        with PhyxReader(str(f)) as r:
            if r.info['ts_open'] > ts_to:
                break
            for rec in r:
                if rec['ts_ms'] < ts_from:
                    continue
                if rec['ts_ms'] > ts_to:
                    return
                yield rec


# ── Convenience: load to numpy arrays ────────────────────────────────────────

def load_klines(data_dir: str, symbol: str = 'BTCUSDC',
                ts_from: int = 0, ts_to: int = None) -> dict:
    """Load kline records from PHYX files into numpy arrays (same format as data.py)."""
    rows = [r for r in iter_files(data_dir, symbol, ts_from, ts_to)
            if r['type'] == 'kline']
    if not rows:
        return {}
    ts  = np.array([r['ts_ms'] for r in rows], dtype=np.int64)
    o   = np.array([r['open']       for r in rows])
    h   = np.array([r['high']       for r in rows])
    l   = np.array([r['low']        for r in rows])
    c   = np.array([r['close']      for r in rows])
    v   = np.array([r['volume']     for r in rows])
    tb  = np.array([r['taker_buy']  for r in rows])
    return {
        'timestamps': ts, 'opens': o, 'highs': h, 'lows': l,
        'closes': c, 'prices': c, 'volumes': v, 'taker_buy': tb,
    }
