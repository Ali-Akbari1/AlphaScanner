from __future__ import annotations

import asyncio
import csv
import io
import itertools
import logging
import lzma
import math
import os
import time
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Dict, List, Literal, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel
try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency
    load_dotenv = None
try:
    from twilio.rest import Client
except Exception:  # pragma: no cover - optional dependency
    Client = None

_BASE_DIR = os.path.dirname(os.path.dirname(__file__))
if load_dotenv is not None:
    load_dotenv(os.path.join(_BASE_DIR, ".env"))

app = FastAPI(title="AlphaScanner API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "https://alpha-scanner-ecru.vercel.app",
    ],
    allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1|.+\.vercel\.app)(:\d+)?$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

INTERVAL_MAP = {
    "1m": "1m",
    "5m": "5m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
    "1w": "1wk",
    "1mo": "1mo",
}

PERIOD_MAP = {
    "1m": "7d",
    "5m": "60d",
    "1h": "2y",
    "4h": "2y",
    "1d": "max",
    "1w": "max",
    "1mo": "max",
}

INTERVAL_SECONDS = {
    "1m": 60,
    "5m": 5 * 60,
    "1h": 60 * 60,
    "4h": 4 * 60 * 60,
    "1d": 24 * 60 * 60,
    "1w": 7 * 24 * 60 * 60,
    "1mo": 30 * 24 * 60 * 60,
}

INTRADAY_PERIODS = ["2y", "1y", "6mo", "3mo", "60d", "30d", "14d", "7d"]
INTRADAY_PERIODS_1M = ["7d", "5d", "1d"]

DEFAULT_TICKERS = [
    "BTC-USD",
    "ETH-USD",
    "AAPL",
    "NVDA",
    "MSFT",
    "TSLA",
    "AMZN",
    "META",
    "GOOGL",
    "SPY",
    "QQQ",
    "NFLX",
]

MAX_TICKERS = 20
CACHE_TTL_SECONDS = 60
_ohlcv_cache: dict[tuple[str, str], tuple[float, pd.DataFrame]] = {}

_YF_SESSION = requests.Session()
_YF_SESSION.headers.update(
    {
        "User-Agent": os.getenv(
            "YF_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        )
    }
)

_LOGGER = logging.getLogger("uvicorn.error")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw.strip())
    except ValueError:
        return default


def _env_time(name: str, default: dt_time) -> dt_time:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        parts = [int(part) for part in raw.strip().split(":")]
        if len(parts) != 2:
            return default
        return dt_time(hour=parts[0], minute=parts[1])
    except Exception:
        return default


def _parse_ticker_list(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    tickers: List[str] = []
    for item in raw.split(","):
        normalized = _normalize_fx_ticker(item)
        if normalized:
            tickers.append(normalized)
    return tickers


def _normalize_fx_ticker(raw: str) -> str:
    cleaned = raw.strip().upper()
    if not cleaned:
        return ""
    if "/" in cleaned:
        cleaned = cleaned.replace("/", "")
    if cleaned.endswith("=X") or "-" in cleaned:
        return cleaned
    if len(cleaned) == 6 and cleaned.isalpha():
        return f"{cleaned}=X"
    return cleaned


def _format_price(value: float) -> str:
    if value >= 100:
        return f"{value:.3f}"
    if value >= 10:
        return f"{value:.4f}"
    return f"{value:.5f}"


def _dukascopy_instrument(ticker: str) -> str:
    cleaned = ticker.strip().upper()
    if not cleaned:
        return ""
    cleaned = cleaned.replace("=X", "")
    cleaned = cleaned.replace("/", "")
    cleaned = cleaned.replace("-", "")
    return cleaned


def _dukascopy_price_scale(instrument: str) -> int:
    override = os.getenv(f"DUKASCOPY_PRICE_SCALE_{instrument}")
    if override:
        try:
            return int(override)
        except ValueError:
            pass
    if instrument.startswith(("XAU", "XAG", "XPT", "XPD")):
        return 1000
    if instrument.endswith(("JPY", "RUB")):
        return 1000
    return 100000


def _dukascopy_hour_url(instrument: str, dt: datetime) -> str:
    month = dt.month - 1
    return f"{DUKASCOPY_BASE_URL}/{instrument}/{dt.year}/{month:02d}/{dt.day:02d}/{dt.hour:02d}h_ticks.bi5"


def _dukascopy_cache_path(instrument: str, dt: datetime) -> str:
    month = dt.month - 1
    return os.path.join(
        DUKASCOPY_CACHE_DIR,
        instrument,
        f"{dt.year}",
        f"{month:02d}",
        f"{dt.day:02d}",
        f"{dt.hour:02d}h_ticks.bi5",
    )


def _dukascopy_skip_hour(dt: datetime) -> bool:
    if not DUKASCOPY_SKIP_WEEKENDS:
        return False
    return dt.weekday() >= 5


def _dukascopy_wait_if_paused() -> bool:
    while _DUKASCOPY_CONTROL["pause"] and not _DUKASCOPY_CONTROL["cancel"]:
        time.sleep(0.5)
    return _DUKASCOPY_CONTROL["cancel"]


def _dukascopy_load_hour(instrument: str, dt: datetime) -> Tuple[Optional[bytes], bool]:
    cache_path = _dukascopy_cache_path(instrument, dt)
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "rb") as handle:
                return handle.read(), True
        except OSError:
            return None, False

    url = _dukascopy_hour_url(instrument, dt)
    for _ in range(max(DUKASCOPY_RETRY_MAX, 1)):
        try:
            response = requests.get(
                url,
                headers=_DUKASCOPY_SESSION.headers,
                timeout=DUKASCOPY_TIMEOUT_SECONDS,
            )
            if response.status_code != 200:
                continue
            payload = response.content
            break
        except Exception:
            continue
    else:
        return None, False

    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    try:
        with open(cache_path, "wb") as handle:
            handle.write(payload)
    except OSError:
        pass
    return payload, False


def _dukascopy_ticks_to_bars(
    payload: bytes,
    base_dt: datetime,
    interval: str,
    price_scale: int,
) -> pd.DataFrame:
    if not payload:
        return pd.DataFrame()
    try:
        raw = lzma.decompress(payload)
    except Exception:
        return pd.DataFrame()
    if not raw:
        return pd.DataFrame()

    dtype = np.dtype(
        [
            ("ms", ">u4"),
            ("ask", ">u4"),
            ("bid", ">u4"),
            ("askvol", ">f4"),
            ("bidvol", ">f4"),
        ]
    )
    if len(raw) < dtype.itemsize:
        return pd.DataFrame()

    ticks = np.frombuffer(raw, dtype=dtype)
    if ticks.size == 0:
        return pd.DataFrame()

    base_ts = base_dt.replace(tzinfo=timezone.utc).timestamp()
    times = base_ts + (ticks["ms"].astype("float64") / 1000.0)
    index = pd.to_datetime(times, unit="s", utc=True)
    price = (ticks["ask"].astype("float64") + ticks["bid"].astype("float64")) / (2 * price_scale)
    volume = ticks["askvol"].astype("float64") + ticks["bidvol"].astype("float64")
    df = pd.DataFrame({"price": price, "volume": volume}, index=index)

    rule = "1min" if interval == "1m" else "5min"
    ohlc = df["price"].resample(rule).ohlc()
    vol = df["volume"].resample(rule).sum()
    bars = pd.concat([ohlc, vol], axis=1)
    bars = bars.rename(columns={"volume": "volume"})
    return bars.dropna(subset=["open", "high", "low", "close"])


def fetch_ohlcv_dukascopy(
    ticker: str,
    interval: str,
    start: Optional[str],
    end: Optional[str],
) -> pd.DataFrame:
    if interval not in {"1m", "5m"}:
        raise HTTPException(status_code=400, detail="Dukascopy supports 1m and 5m intervals only.")

    instrument = _dukascopy_instrument(ticker)
    if not instrument:
        raise HTTPException(status_code=400, detail=f"Unsupported ticker: {ticker}")

    start_date = _parse_date_only(start)
    end_date = _parse_date_only(end)
    if not start_date or not end_date:
        end_date = datetime.now(timezone.utc).date()
        window_days = 7 if interval == "1m" else 60
        start_date = end_date - timedelta(days=window_days)

    price_scale = _dukascopy_price_scale(instrument)
    bars: List[pd.DataFrame] = []

    current = datetime.combine(start_date, dt_time(0, 0))
    final = datetime.combine(end_date, dt_time(23, 0))
    hours: List[datetime] = []
    skipped = 0

    while current <= final:
        if _dukascopy_skip_hour(current):
            skipped += 1
        else:
            hours.append(current)
        current += timedelta(hours=1)

    total_hours = len(hours)
    processed = 0
    cached_hits = 0
    downloaded = 0
    missing = 0
    retry_attempts = 0
    start_ts = time.time()

    progress_key = f"{_dukascopy_instrument(ticker)}|{interval}"
    _DUKASCOPY_PROGRESS[progress_key] = {
        "processed": 0.0,
        "total": float(total_hours),
        "cached": 0.0,
        "downloaded": 0.0,
        "missing": 0.0,
        "retry_attempts": 0.0,
        "skipped": float(skipped),
        "speed": 0.0,
        "eta_seconds": 0.0,
        "updated_at": time.time(),
    }

    if not hours:
        return pd.DataFrame()

    batch_size = max(DUKASCOPY_BATCH_HOURS, 1)
    workers = max(DUKASCOPY_MAX_WORKERS, 1)

    def update_progress() -> None:
        elapsed = max(time.time() - start_ts, 0.001)
        speed = processed / elapsed if processed else 0.0
        remaining = max(total_hours - processed, 0)
        if remaining == 0 and missing > 0:
            remaining = missing
        eta_seconds = (remaining / speed) if speed else 0.0
        _DUKASCOPY_PROGRESS[progress_key] = {
            "processed": float(processed),
            "total": float(total_hours),
            "cached": float(cached_hits),
            "downloaded": float(downloaded),
            "missing": float(missing),
            "retry_attempts": float(retry_attempts),
            "skipped": float(skipped),
            "speed": float(speed),
            "eta_seconds": float(eta_seconds),
            "updated_at": time.time(),
        }

    def process_hours(hours_to_fetch: List[datetime], count_unique: bool) -> List[datetime]:
        nonlocal processed, cached_hits, downloaded, missing, retry_attempts
        remaining_missing: List[datetime] = []

        total = len(hours_to_fetch)
        for batch_start in range(0, total, batch_size):
            if _dukascopy_wait_if_paused():
                _LOGGER.info("Dukascopy download canceled for %s %s", instrument, interval)
                return remaining_missing
            batch = hours_to_fetch[batch_start : batch_start + batch_size]
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(_dukascopy_load_hour, instrument, hour): hour
                    for hour in batch
                }
                for future in as_completed(futures):
                    if _DUKASCOPY_CONTROL["cancel"]:
                        return remaining_missing
                    hour = futures[future]
                    try:
                        payload, was_cached = future.result()
                    except Exception:
                        payload, was_cached = None, False

                    if count_unique:
                        processed += 1
                    else:
                        retry_attempts += 1

                    if payload:
                        if was_cached:
                            cached_hits += 1
                        else:
                            downloaded += 1
                        if not count_unique and missing > 0:
                            missing -= 1
                        bars.append(_dukascopy_ticks_to_bars(payload, hour, interval, price_scale))
                    else:
                        remaining_missing.append(hour)
                        if count_unique:
                            missing += 1

                    update_progress()

            if DUKASCOPY_LOG_PROGRESS:
                log_every = max(DUKASCOPY_LOG_EVERY_HOURS, 1)
                if processed == total_hours or processed % log_every == 0 or processed <= batch_size:
                    _LOGGER.info(
                        "Dukascopy %s %s: %s/%s hours (cached %s, downloaded %s, missing %s, skipped %s)",
                        instrument,
                        interval,
                        processed,
                        total_hours,
                        cached_hits,
                        downloaded,
                        missing,
                        skipped,
                    )

        return remaining_missing

    missing_hours = process_hours(hours, True)

    if DUKASCOPY_AUTO_RESUME and missing_hours:
        for _ in range(max(DUKASCOPY_RETRY_PASSES, 1)):
            if not missing_hours or _DUKASCOPY_CONTROL["cancel"]:
                break
            missing_hours = process_hours(missing_hours, False)
            missing = len(missing_hours)
            update_progress()

    if not bars:
        return pd.DataFrame()

    df = pd.concat(bars).sort_index()
    df = df[~df.index.duplicated(keep="last")]
    df = df[["open", "high", "low", "close", "volume"]]
    df.index = pd.to_datetime(df.index, utc=True)
    return df


def fetch_ohlcv_backtest(ticker: str, request: BacktestRequest) -> pd.DataFrame:
    if BACKTEST_DATA_SOURCE == "dukascopy":
        return fetch_ohlcv_dukascopy(ticker, request.interval, request.start, request.end)
    return fetch_ohlcv(ticker, request.interval)


DEFAULT_SWEEP_TICKERS = [
    "EURUSD=X",
    "GBPUSD=X",
    "USDJPY=X",
    "AUDUSD=X",
    "USDCAD=X",
]

SWEEP_TIMEZONE = os.getenv("SWEEP_TIMEZONE", "America/New_York")
SWEEP_INTERVAL = os.getenv("SWEEP_INTERVAL", "1h")
if SWEEP_INTERVAL not in INTERVAL_MAP:
    SWEEP_INTERVAL = "1h"

SWEEP_ALERTS_ENABLED = _env_bool("SWEEP_ALERTS_ENABLED", False)
SWEEP_SMS_ENABLED = _env_bool("SWEEP_SMS_ENABLED", False)
SWEEP_POLL_SECONDS = _env_int("SWEEP_POLL_SECONDS", 60)
_DEFAULT_SWEEP_LOOKBACK = 288 if SWEEP_INTERVAL in {"5m"} else 72
SWEEP_LOOKBACK_BARS = _env_int("SWEEP_LOOKBACK_BARS", _DEFAULT_SWEEP_LOOKBACK)
SWEEP_BREACH_ATR_MULT = _env_float("SWEEP_BREACH_ATR_MULT", 0.2)
SWEEP_WICK_ATR_MULT = _env_float("SWEEP_WICK_ATR_MULT", 0.3)
SWEEP_RECLAIM_BARS = _env_int("SWEEP_RECLAIM_BARS", 1)
SWEEP_ALERT_TTL_SECONDS = _env_int("SWEEP_ALERT_TTL_SECONDS", 6 * 60 * 60)

SWEEP_ASIA_START = _env_time("SWEEP_ASIA_START", dt_time(19, 0))
SWEEP_ASIA_END = _env_time("SWEEP_ASIA_END", dt_time(2, 0))
SWEEP_LONDON_START = _env_time("SWEEP_LONDON_START", dt_time(2, 0))
SWEEP_LONDON_END = _env_time("SWEEP_LONDON_END", dt_time(8, 0))
SWEEP_NY_OPEN_START = _env_time("SWEEP_NY_OPEN_START", dt_time(8, 0))
SWEEP_NY_OPEN_END = _env_time("SWEEP_NY_OPEN_END", dt_time(10, 0))

SWEEP_TICKERS = _parse_ticker_list(os.getenv("SWEEP_TICKERS")) or DEFAULT_SWEEP_TICKERS
_SWEEP_ALERT_CACHE: dict[tuple[str, str, str, int], float] = {}

BACKTEST_DATA_SOURCE = os.getenv("BACKTEST_DATA_SOURCE", "yahoo").strip().lower()
DUKASCOPY_BASE_URL = os.getenv("DUKASCOPY_BASE_URL", "https://datafeed.dukascopy.com/datafeed")
DUKASCOPY_CACHE_DIR = os.getenv(
    "DUKASCOPY_CACHE_DIR",
    os.path.join(_BASE_DIR, "data", "dukascopy"),
)
DUKASCOPY_TIMEOUT_SECONDS = _env_int("DUKASCOPY_TIMEOUT_SECONDS", 12)
DUKASCOPY_LOG_PROGRESS = _env_bool("DUKASCOPY_LOG_PROGRESS", False)
DUKASCOPY_LOG_EVERY_HOURS = _env_int("DUKASCOPY_LOG_EVERY_HOURS", 24)
DUKASCOPY_SKIP_WEEKENDS = _env_bool("DUKASCOPY_SKIP_WEEKENDS", True)
DUKASCOPY_MAX_WORKERS = _env_int("DUKASCOPY_MAX_WORKERS", 6)
DUKASCOPY_BATCH_HOURS = _env_int("DUKASCOPY_BATCH_HOURS", 24)
DUKASCOPY_RETRY_MAX = _env_int("DUKASCOPY_RETRY_MAX", 2)
DUKASCOPY_AUTO_RESUME = _env_bool("DUKASCOPY_AUTO_RESUME", True)
DUKASCOPY_RETRY_PASSES = _env_int("DUKASCOPY_RETRY_PASSES", 1)

_DUKASCOPY_SESSION = requests.Session()
_DUKASCOPY_SESSION.headers.update(
    {
        "User-Agent": os.getenv(
            "DUKASCOPY_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        )
    }
)

_DUKASCOPY_PROGRESS: dict[str, dict[str, float]] = {}
_DUKASCOPY_CONTROL = {"pause": False, "cancel": False}
_BATCH_PROGRESS = {
    "status": "idle",
    "processed": 0,
    "total": 0,
    "current_year": None,
    "updated_at": 0.0,
}
_BATCH_CONTROL = {"cancel": False}


def _yf_download(*args, **kwargs) -> pd.DataFrame:
    try:
        return yf.download(*args, session=_YF_SESSION, **kwargs)
    except TypeError:
        return yf.download(*args, **kwargs)


def _yf_ticker(ticker: str) -> yf.Ticker:
    try:
        return yf.Ticker(ticker, session=_YF_SESSION)
    except TypeError:
        return yf.Ticker(ticker)


class Meta(BaseModel):
    ticker: str
    interval: str
    rows: int


class Candle(BaseModel):
    time: int
    open: float
    high: float
    low: float
    close: float
    volume: float


class Signal(BaseModel):
    time: int
    type: Literal["buy", "sell"]
    strength: Literal["strong", "weak"]
    conditions: List[str]


class Indicator(BaseModel):
    time: int
    ema9: Optional[float]
    ema21: Optional[float]
    ema50: Optional[float]
    ema200: Optional[float]
    supertrend: Optional[float]
    rsi: Optional[float]
    vwap: Optional[float]
    bb_upper: Optional[float]
    bb_middle: Optional[float]
    bb_lower: Optional[float]
    macd: Optional[float]
    macd_signal: Optional[float]


class AnalyzeResponse(BaseModel):
    meta: Meta
    candles: List[Candle]
    signals: List[Signal]
    indicators: List[Indicator]


class ScanMeta(BaseModel):
    interval: str
    tickers: List[str]
    total: int


class ScanResult(BaseModel):
    ticker: str
    latest_close: Optional[float]
    last_signal_time: Optional[int]
    last_signal_type: Optional[Literal["buy", "sell"]]
    last_signal_strength: Optional[Literal["strong"]]
    bias_type: Optional[Literal["buy", "sell", "neutral"]]
    bias_time: Optional[int]
    signal_count: int
    status: Literal["ok", "error"]
    error: Optional[str] = None


class ScanResponse(BaseModel):
    meta: ScanMeta
    results: List[ScanResult]


class SweepEvent(BaseModel):
    ticker: str
    time: int
    ny_time: str
    direction: Literal["bull", "bear"]
    level_name: str
    level_price: float
    close: float
    high: float
    low: float
    sent: bool = False


class SweepScanMeta(BaseModel):
    interval: str
    tickers: List[str]
    timezone: str
    ny_open_start: str
    ny_open_end: str


class SweepScanResponse(BaseModel):
    meta: SweepScanMeta
    events: List[SweepEvent]


class SweepStatusResponse(BaseModel):
    alerts_enabled: bool
    sms_enabled: bool
    sms_ready: bool
    twilio_configured: bool
    poll_seconds: int
    interval: str
    timezone: str
    ny_open_start: str
    ny_open_end: str
    tickers: List[str]


class BacktestRequest(BaseModel):
    tickers: List[str]
    interval: Literal["1m", "5m"] = "5m"
    session: Literal["london", "newyork", "both"] = "newyork"
    start: Optional[str] = None  # YYYY-MM-DD or ISO date
    end: Optional[str] = None
    starting_balance: float = 10000.0
    risk_per_trade: float = 0.005
    max_trades_per_day: int = 2
    sweep_atr_mult: float = 0.8
    return_within_bars: int = 4
    fvg_min_atr_mult: float = 0.1
    fvg_retrace_window: int = 8
    stop_atr_mult: float = 1.2
    target_rr: float = 2.0


class BacktestTrade(BaseModel):
    ticker: str
    session: Literal["london", "newyork"]
    direction: Literal["long", "short"]
    level_name: str
    sweep_time: int
    fvg_time: Optional[int]
    entry_time: int
    entry_price: float
    stop_price: float
    target_price: float
    exit_time: int
    exit_price: float
    result: Literal["win", "loss", "breakeven"]
    r_multiple: float
    pnl: float


class BacktestSummary(BaseModel):
    starting_balance: float
    ending_balance: float
    return_pct: float
    total_trades: int
    wins: int
    losses: int
    breakeven: int
    win_rate: float
    profit_factor: Optional[float]
    max_drawdown: float


class BacktestResponse(BaseModel):
    meta: dict
    summary: BacktestSummary
    session_breakdown: List[SessionBreakdown]
    equity_curve: List[EquityPoint]
    trades: List[BacktestTrade]


class EquityPoint(BaseModel):
    time: int
    equity: float


class SessionBreakdown(BaseModel):
    session: Literal["london", "newyork"]
    total_trades: int
    wins: int
    losses: int
    breakeven: int
    win_rate: float
    profit_factor: Optional[float]
    return_pct: float


class GridSearchRequest(BaseModel):
    base: BacktestRequest
    sweep_atr_mults: List[float] = [0.4, 0.5, 0.6]
    return_within_bars: List[int] = [10, 20, 30]
    fvg_min_atr_mults: List[float] = [0.0, 0.1]
    fvg_retrace_windows: List[int] = [8, 12, 16]
    stop_atr_mults: List[float] = [1.0, 1.2]
    target_rrs: List[float] = [1.5, 2.0]
    max_combinations: int = 200
    top_n: int = 10
    sort_by: Literal["return_pct", "profit_factor", "score"] = "return_pct"


class GridSearchResult(BaseModel):
    params: dict
    summary: BacktestSummary
    score: float


class GridSearchResponse(BaseModel):
    meta: dict
    results: List[GridSearchResult]


class BacktestBatchRequest(BaseModel):
    base: BacktestRequest
    start_year: Optional[int] = None
    end_year: Optional[int] = None


class BacktestYearResult(BaseModel):
    year: int
    summary: BacktestSummary
    total_trades: int
    data_ranges: dict


class BacktestBatchResponse(BaseModel):
    meta: dict
    results: List[BacktestYearResult]


class DukascopyProgressResponse(BaseModel):
    paused: bool
    canceled: bool
    sources: List[dict]


class DukascopyControlResponse(BaseModel):
    paused: bool
    canceled: bool


class BatchProgressResponse(BaseModel):
    status: Literal["idle", "running", "done"]
    processed: int
    total: int
    current_year: Optional[int]
    updated_at: float


def fetch_ohlcv(ticker: str, interval: str) -> pd.DataFrame:
    cache_key = (ticker.upper(), interval)
    cached = _ohlcv_cache.get(cache_key)
    if cached:
        cached_at, cached_df = cached
        if time.time() - cached_at < CACHE_TTL_SECONDS:
            return cached_df.copy()
        _ohlcv_cache.pop(cache_key, None)

    yf_interval = INTERVAL_MAP.get(interval)
    if not yf_interval:
        raise HTTPException(status_code=400, detail=f"Unsupported interval: {interval}")

    if interval in {"1m", "5m", "1h", "4h"}:
        periods = INTRADAY_PERIODS_1M if interval == "1m" else INTRADAY_PERIODS
        raw_df = fetch_intraday_ohlcv(ticker, yf_interval, INTERVAL_SECONDS[interval], periods=periods)
    else:
        period = PERIOD_MAP[interval]
        try:
            raw_df = _yf_download(
                ticker,
                period=period,
                interval=yf_interval,
                auto_adjust=False,
                progress=False,
                threads=False,
            )
        except Exception:
            raw_df = pd.DataFrame()

    df = normalize_ohlcv(raw_df)

    if df.empty:
        period = PERIOD_MAP[interval]
        df = normalize_ohlcv(fetch_ohlcv_chart(ticker, period, yf_interval))

    if df.empty:
        period = PERIOD_MAP[interval]
        df = normalize_ohlcv(fetch_ohlcv_history(ticker, period, yf_interval))

    if df.empty and interval in {"1m", "5m", "1h", "4h"}:
        fallbacks = ("7d", "5d", "1d") if interval == "1m" else ("2y", "1y", "6mo", "3mo", "60d")
        for fallback in fallbacks:
            df = normalize_ohlcv(fetch_ohlcv_history(ticker, fallback, yf_interval))
            if not df.empty:
                break

    if df.empty and interval == "1d" and period != "1y":
        df = normalize_ohlcv(fetch_ohlcv_history(ticker, "1y", yf_interval))

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No data for ticker: {ticker}. Yahoo returned empty data.",
        )

    df = normalize_ohlcv(df)
    df = resample_ohlcv(df, interval)
    _ohlcv_cache[cache_key] = (time.time(), df.copy())
    return df


def fetch_ohlcv_history(ticker: str, period: str, interval: str) -> pd.DataFrame:
    try:
        history = _yf_ticker(ticker).history(
            period=period,
            interval=interval,
            auto_adjust=False,
        )
    except Exception:
        return pd.DataFrame()
    return history


def fetch_ohlcv_chart(ticker: str, period: str, interval: str) -> pd.DataFrame:
    try:
        response = _YF_SESSION.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
            params={
                "interval": interval,
                "range": period,
                "includePrePost": "false",
                "events": "div,splits",
            },
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return pd.DataFrame()

    result = payload.get("chart", {}).get("result")
    if not result:
        return pd.DataFrame()

    data = result[0] or {}
    timestamps = data.get("timestamp")
    quote = (data.get("indicators", {}).get("quote") or [None])[0] or {}

    if not timestamps or not quote:
        return pd.DataFrame()

    df = pd.DataFrame(quote, index=pd.to_datetime(timestamps, unit="s", utc=True))
    return df


def fetch_intraday_ohlcv(
    ticker: str,
    yf_interval: str,
    expected_seconds: int,
    periods: Optional[List[str]] = None,
) -> pd.DataFrame:
    best_df = pd.DataFrame()
    best_step = None

    for period in (periods or INTRADAY_PERIODS):
        df = pd.DataFrame()
        try:
            df = _yf_download(
                ticker,
                period=period,
                interval=yf_interval,
                auto_adjust=False,
                progress=False,
                threads=False,
            )
        except Exception:
            df = pd.DataFrame()

        df = normalize_ohlcv(df)
        if df.empty:
            df = normalize_ohlcv(fetch_ohlcv_chart(ticker, period, yf_interval))

        step = _median_step_seconds(df.index)
        if df.empty or step is None:
            continue

        if step <= expected_seconds * 1.6:
            return df

        if best_step is None or step < best_step:
            best_step = step
            best_df = df

    return best_df


def normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    if isinstance(df.columns, pd.MultiIndex):
        level_to_use = _select_ohlcv_level(df.columns)
        if level_to_use is not None:
            df.columns = df.columns.get_level_values(level_to_use)
        else:
            df.columns = [
                " ".join(str(part).strip() for part in tup if part is not None).strip()
                for tup in df.columns
            ]

    df = df.rename(columns=lambda name: str(name).strip().lower().replace("_", " "))

    if "close" not in df.columns:
        if "adj close" in df.columns:
            df = df.rename(columns={"adj close": "close"})
        elif "adjclose" in df.columns:
            df = df.rename(columns={"adjclose": "close"})

    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()]

    required = ["open", "high", "low", "close", "volume"]
    if any(col not in df.columns for col in required):
        inferred = _infer_ohlcv_columns(df.columns)
        if inferred is None:
            return pd.DataFrame()
        df = df.rename(columns=inferred)
        if any(col not in df.columns for col in required):
            return pd.DataFrame()

    df = df[required].copy()
    df.index = pd.to_datetime(df.index, utc=True)
    if df.index.has_duplicates:
        df = df[~df.index.duplicated(keep="last")]
    if not df.index.is_monotonic_increasing:
        df = df.sort_index()
    return df


def _median_step_seconds(index: pd.Index) -> Optional[float]:
    if not isinstance(index, pd.DatetimeIndex):
        return None
    if len(index) < 2:
        return None
    times = (index.view("int64") // 10**9).astype("int64")
    diffs = np.diff(times)
    diffs = diffs[diffs > 0]
    if diffs.size == 0:
        return None
    return float(np.median(diffs))


def resample_ohlcv(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    if df.empty:
        return df
    if interval not in {"1w", "1mo"}:
        return df

    rule = "W" if interval == "1w" else "ME"
    ohlcv = df[["open", "high", "low", "close", "volume"]]
    resampled = ohlcv.resample(rule, label="right", closed="right").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    return resampled.dropna(subset=["open", "high", "low", "close"])


def _select_ohlcv_level(columns: pd.MultiIndex) -> Optional[int]:
    required = {"open", "high", "low", "close", "volume"}
    for level_idx in range(columns.nlevels):
        values = {str(value).strip().lower() for value in columns.get_level_values(level_idx)}
        if required.issubset(values) or {"open", "high", "low", "adj close", "volume"}.issubset(values):
            return level_idx
    return None


def _infer_ohlcv_columns(columns: pd.Index) -> Optional[dict[str, str]]:
    required = ["open", "high", "low", "close", "volume"]
    mapping: dict[str, str] = {}
    normalized = [str(col).strip().lower().replace("_", " ") for col in columns]

    def candidates_for(keyword: str) -> List[str]:
        matches = []
        for original, normed in zip(columns, normalized):
            if keyword in normed.split() or normed.startswith(f"{keyword} ") or normed.endswith(f" {keyword}"):
                matches.append(str(original))
        return matches

    for req in required:
        matches = candidates_for(req)
        if not matches and req == "close":
            for alt in ("adj close", "adjclose"):
                matches = [str(original) for original, normed in zip(columns, normalized) if alt in normed]
                if matches:
                    break

        if len(matches) != 1:
            return None
        mapping[matches[0]] = req

    return mapping


def fetch_ohlcv_batch(tickers: List[str], interval: str) -> dict[str, pd.DataFrame]:
    if not tickers:
        return {}

    yf_interval = INTERVAL_MAP.get(interval)
    if not yf_interval:
        raise HTTPException(status_code=400, detail=f"Unsupported interval: {interval}")

    period = PERIOD_MAP[interval]
    now = time.time()
    results: dict[str, pd.DataFrame] = {}
    missing: List[str] = []

    for ticker in tickers:
        cache_key = (ticker.upper(), interval)
        cached = _ohlcv_cache.get(cache_key)
        if cached:
            cached_at, cached_df = cached
            if now - cached_at < CACHE_TTL_SECONDS:
                results[ticker] = cached_df.copy()
                continue
        _ohlcv_cache.pop(cache_key, None)
        missing.append(ticker)

    if not missing:
        return results

    try:
        downloaded = _yf_download(
            " ".join(missing),
            period=period,
            interval=yf_interval,
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    except Exception:
        downloaded = pd.DataFrame()

    if downloaded.empty:
        for ticker in missing:
            results[ticker] = pd.DataFrame()
        return results

    if isinstance(downloaded.columns, pd.MultiIndex):
        available = set(downloaded.columns.get_level_values(0))
        for ticker in missing:
            if ticker in available:
                df = downloaded[ticker].copy()
            else:
                df = pd.DataFrame()
            df = normalize_ohlcv(df)
            df = resample_ohlcv(df, interval)
            results[ticker] = df
            if not df.empty:
                _ohlcv_cache[(ticker.upper(), interval)] = (time.time(), df.copy())
    else:
        ticker = missing[0]
        df = normalize_ohlcv(downloaded.copy())
        df = resample_ohlcv(df, interval)
        results[ticker] = df
        if not df.empty:
            _ohlcv_cache[(ticker.upper(), interval)] = (time.time(), df.copy())
        for extra in missing[1:]:
            results[extra] = pd.DataFrame()

    return results


def compute_ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False, min_periods=length).mean()


def compute_rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_macd(
    series: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[pd.Series, pd.Series]:
    ema_fast = series.ewm(span=fast, adjust=False, min_periods=fast).mean()
    ema_slow = series.ewm(span=slow, adjust=False, min_periods=slow).mean()
    macd = ema_fast - ema_slow
    macd_signal = macd.ewm(span=signal, adjust=False, min_periods=signal).mean()
    return macd, macd_signal


def compute_atr(high: pd.Series, low: pd.Series, close: pd.Series, length: int = 14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()


def compute_supertrend(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    length: int = 14,
    multiplier: float = 3.0,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    atr = compute_atr(high, low, close, length)
    hl2 = (high + low) / 2
    basic_upper = hl2 + (multiplier * atr)
    basic_lower = hl2 - (multiplier * atr)

    final_upper = basic_upper.copy()
    final_lower = basic_lower.copy()
    direction = pd.Series(index=close.index, dtype="int64")
    supertrend = pd.Series(index=close.index, dtype="float64")

    if len(close) == 0:
        return supertrend, final_upper, final_lower

    direction.iloc[0] = 1
    supertrend.iloc[0] = np.nan

    for i in range(1, len(close)):
        if close.iloc[i] > final_upper.iloc[i - 1]:
            direction.iloc[i] = 1
        elif close.iloc[i] < final_lower.iloc[i - 1]:
            direction.iloc[i] = -1
        else:
            direction.iloc[i] = direction.iloc[i - 1]
            if direction.iloc[i] == 1 and final_lower.iloc[i] < final_lower.iloc[i - 1]:
                final_lower.iloc[i] = final_lower.iloc[i - 1]
            if direction.iloc[i] == -1 and final_upper.iloc[i] > final_upper.iloc[i - 1]:
                final_upper.iloc[i] = final_upper.iloc[i - 1]

        supertrend.iloc[i] = final_lower.iloc[i] if direction.iloc[i] == 1 else final_upper.iloc[i]

    return supertrend, final_upper, final_lower


def add_indicators(df: pd.DataFrame, interval: str = "1d") -> pd.DataFrame:
    df = df.copy()
    df["ema9"] = compute_ema(df["close"], length=9)
    df["ema21"] = compute_ema(df["close"], length=21)
    df["ema50"] = compute_ema(df["close"], length=50)
    df["ema200"] = compute_ema(df["close"], length=200)
    df["rsi"] = compute_rsi(df["close"], length=14)

    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    cumulative_volume = df["volume"].cumsum()
    df["vwap"] = (typical_price * df["volume"]).cumsum() / cumulative_volume

    bb_middle = df["close"].rolling(window=20, min_periods=20).mean()
    bb_std = df["close"].rolling(window=20, min_periods=20).std()
    df["bb_middle"] = bb_middle
    df["bb_upper"] = bb_middle + 2 * bb_std
    df["bb_lower"] = bb_middle - 2 * bb_std

    macd, macd_signal = compute_macd(df["close"])
    df["macd"] = macd
    df["macd_signal"] = macd_signal

    supertrend, supertrend_upper, supertrend_lower = compute_supertrend(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        length=14,
        multiplier=3.0,
    )

    df["supertrend"] = supertrend
    df["supertrend_upper"] = supertrend_upper
    df["supertrend_lower"] = supertrend_lower

    df = df.replace([np.inf, -np.inf], np.nan)
    df["time"] = _build_time_index(df.index, len(df), interval)
    return df


def _build_time_index(index: pd.Index, length: int, interval: str) -> pd.Series:
    times: Optional[np.ndarray] = None

    if isinstance(index, pd.PeriodIndex):
        index = index.to_timestamp()

    if isinstance(index, pd.DatetimeIndex):
        times = (index.view("int64") // 10**9).astype("int64")
    else:
        parsed = pd.to_datetime(index, utc=True, errors="coerce")
        if isinstance(parsed, pd.DatetimeIndex) and not parsed.isna().any():
            times = (parsed.view("int64") // 10**9).astype("int64")

    if times is not None and length > 1:
        diffs = np.diff(times)
        diffs = diffs[diffs > 0]
        if diffs.size > 0:
            median = float(np.median(diffs))
            expected = INTERVAL_SECONDS.get(interval, 60 * 60 * 24)
            if median < expected * 0.5:
                times = None

    if times is None:
        # Fallback to a synthetic cadence ending now to keep charts usable.
        step = INTERVAL_SECONDS.get(interval, 60 * 60 * 24)
        end = int(time.time())
        start = end - max(length - 1, 0) * step
        times = np.arange(start, start + length * step, step, dtype="int64")

    if length > 1 and (np.diff(times) <= 0).any():
        # Ensure strictly increasing timestamps without discarding data.
        fixed = times.copy()
        for idx in range(1, len(fixed)):
            if fixed[idx] <= fixed[idx - 1]:
                fixed[idx] = fixed[idx - 1] + 1
        times = fixed

    return pd.Series(times, index=index)


def build_signals(df: pd.DataFrame, include_weak: bool = False) -> List[Signal]:
    df = df.dropna(subset=["close", "ema200", "rsi", "supertrend", "time", "ema9", "ema21"])
    if df.empty:
        return []
    df = df.sort_values("time")

    st_upper = df["supertrend_upper"].fillna(df["supertrend"])
    st_lower = df["supertrend_lower"].fillna(df["supertrend"])

    trend_up = df["close"] > df["ema200"]
    trend_down = df["close"] < df["ema200"]
    volatility_up = df["close"] > st_upper
    volatility_down = df["close"] < st_lower

    momentum_up = (df["rsi"].shift(1) < 30) & (df["rsi"] >= 30)
    momentum_down = (df["rsi"].shift(1) > 70) & (df["rsi"] <= 70)

    strong_buy = trend_up & volatility_up & momentum_up
    strong_sell = trend_down & volatility_down & momentum_down

    signals_by_key: dict[tuple[int, str], Signal] = {}
    for _, row in df.loc[strong_buy | strong_sell].iterrows():
        signal_type = "buy" if strong_buy.loc[row.name] else "sell"
        signal = Signal(
            time=int(row["time"]),
            type=signal_type,
            strength="strong",
            conditions=["trend", "volatility", "momentum"],
        )
        signals_by_key[(signal.time, signal.type)] = signal

    if include_weak:
        ema_cross_up = (df["ema9"].shift(1) <= df["ema21"].shift(1)) & (df["ema9"] > df["ema21"])
        ema_cross_down = (df["ema9"].shift(1) >= df["ema21"].shift(1)) & (df["ema9"] < df["ema21"])

        st_flip_up = (df["close"].shift(1) <= df["supertrend"].shift(1)) & (df["close"] > df["supertrend"])
        st_flip_down = (df["close"].shift(1) >= df["supertrend"].shift(1)) & (df["close"] < df["supertrend"])

        rsi_mid_up = (df["rsi"].shift(1) < 50) & (df["rsi"] >= 50)
        rsi_mid_down = (df["rsi"].shift(1) > 50) & (df["rsi"] <= 50)

        weak_buy = ema_cross_up | st_flip_up | rsi_mid_up
        weak_sell = ema_cross_down | st_flip_down | rsi_mid_down

        for _, row in df.loc[weak_buy | weak_sell].iterrows():
            signal_type = "buy" if weak_buy.loc[row.name] else "sell"
            signal = Signal(
                time=int(row["time"]),
                type=signal_type,
                strength="weak",
                conditions=["ema_cross", "supertrend_flip", "rsi_mid"],
            )
            key = (signal.time, signal.type)
            if key not in signals_by_key:
                signals_by_key[key] = signal

    signals = sorted(signals_by_key.values(), key=lambda signal: signal.time)
    if not include_weak:
        return signals

    filtered: List[Signal] = []
    last_type: Optional[Literal["buy", "sell"]] = None
    for signal in signals:
        if signal.strength == "weak" and signal.type == last_type:
            continue
        filtered.append(signal)
        last_type = signal.type

    return filtered


def build_candles(df: pd.DataFrame) -> List[Candle]:
    candles: List[Candle] = []
    valid = df.dropna(subset=["open", "high", "low", "close", "volume", "time"])
    valid = valid.sort_values("time")
    for _, row in valid.iterrows():
        candles.append(
            Candle(
                time=int(row["time"]),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"]),
            )
        )
    return candles


def build_indicators(df: pd.DataFrame) -> List[Indicator]:
    indicators: List[Indicator] = []
    valid = df.dropna(subset=["time"]).sort_values("time")

    def safe_float(value: object) -> Optional[float]:
        try:
            result = float(value)
        except (TypeError, ValueError):
            return None
        return result if math.isfinite(result) else None

    for _, row in valid.iterrows():
        indicators.append(
            Indicator(
                time=int(row["time"]),
                ema9=safe_float(row.get("ema9")),
                ema21=safe_float(row.get("ema21")),
                ema50=safe_float(row.get("ema50")),
                ema200=safe_float(row.get("ema200")),
                supertrend=safe_float(row.get("supertrend")),
                rsi=safe_float(row.get("rsi")),
                vwap=safe_float(row.get("vwap")),
                bb_upper=safe_float(row.get("bb_upper")),
                bb_middle=safe_float(row.get("bb_middle")),
                bb_lower=safe_float(row.get("bb_lower")),
                macd=safe_float(row.get("macd")),
                macd_signal=safe_float(row.get("macd_signal")),
            )
        )
    return indicators


def _get_sweep_zone() -> ZoneInfo:
    try:
        return ZoneInfo(SWEEP_TIMEZONE)
    except Exception:
        return ZoneInfo("UTC")


def _time_in_window(value: dt_time, start: dt_time, end: dt_time) -> bool:
    if start <= end:
        return start <= value < end
    return value >= start or value < end


def _build_session_mask(
    local_index: pd.DatetimeIndex,
    target_date: date,
    start: dt_time,
    end: dt_time,
    include_prev_for_wrap: bool = False,
) -> np.ndarray:
    local_dates = local_index.date
    local_times = local_index.time
    if start <= end:
        return (local_dates == target_date) & (local_times >= start) & (local_times < end)

    if include_prev_for_wrap:
        prev_date = target_date - timedelta(days=1)
        return ((local_dates == prev_date) & (local_times >= start)) | (
            (local_dates == target_date) & (local_times < end)
        )

    return (local_dates == target_date) & ((local_times >= start) | (local_times < end))


def _compute_sweep_levels(df: pd.DataFrame, zone: ZoneInfo) -> List[tuple[str, float]]:
    if df.empty:
        return []

    df = df.dropna(subset=["high", "low"]).sort_index()
    if df.empty:
        return []

    local_index = df.index
    if local_index.tz is None:
        local_index = local_index.tz_localize("UTC")
    local_index = local_index.tz_convert(zone)

    current_date = local_index[-1].date()
    prev_date = current_date - timedelta(days=1)
    local_dates = local_index.date

    levels: List[tuple[str, float]] = []

    prev_mask = local_dates == prev_date
    if prev_mask.any():
        prev_high = float(df.loc[prev_mask, "high"].max())
        prev_low = float(df.loc[prev_mask, "low"].min())
        levels.append(("prev_day_high", prev_high))
        levels.append(("prev_day_low", prev_low))

    asia_mask = _build_session_mask(
        local_index,
        current_date,
        SWEEP_ASIA_START,
        SWEEP_ASIA_END,
        include_prev_for_wrap=True,
    )
    if asia_mask.any():
        asia_high = float(df.loc[asia_mask, "high"].max())
        asia_low = float(df.loc[asia_mask, "low"].min())
        levels.append(("asia_high", asia_high))
        levels.append(("asia_low", asia_low))

    london_mask = _build_session_mask(
        local_index,
        current_date,
        SWEEP_LONDON_START,
        SWEEP_LONDON_END,
    )
    if london_mask.any():
        london_high = float(df.loc[london_mask, "high"].max())
        london_low = float(df.loc[london_mask, "low"].min())
        levels.append(("london_high", london_high))
        levels.append(("london_low", london_low))

    return levels


def _reclaim_after_breach(
    df: pd.DataFrame,
    start_idx: int,
    level: float,
    direction: Literal["bull", "bear"],
    bars: int,
) -> bool:
    if bars <= 0:
        return True
    end_idx = min(start_idx + bars, len(df) - 1)
    window = df.iloc[start_idx : end_idx + 1]
    if direction == "bull":
        return bool((window["close"] > level).any())
    return bool((window["close"] < level).any())


def _detect_liquidity_sweeps(
    ticker: str,
    df: pd.DataFrame,
    levels: List[tuple[str, float]],
    zone: ZoneInfo,
) -> List[SweepEvent]:
    if df.empty or not levels:
        return []

    df = df.dropna(subset=["open", "high", "low", "close"]).sort_index()
    if df.empty:
        return []

    df = df.tail(SWEEP_LOOKBACK_BARS)
    if df.empty:
        return []

    atr = compute_atr(df["high"], df["low"], df["close"], length=14)
    df = df.assign(atr=atr)

    local_index = df.index
    if local_index.tz is None:
        local_index = local_index.tz_localize("UTC")
    local_index = local_index.tz_convert(zone)
    current_date = local_index[-1].date()

    events: List[SweepEvent] = []

    for idx, (ts, row) in enumerate(df.iterrows()):
        local_dt = local_index[idx]
        if local_dt.date() != current_date:
            continue
        if not _time_in_window(local_dt.time(), SWEEP_NY_OPEN_START, SWEEP_NY_OPEN_END):
            continue

        atr_value = row.get("atr")
        if atr_value is None or not math.isfinite(atr_value) or atr_value <= 0:
            continue

        breach = max(SWEEP_BREACH_ATR_MULT * atr_value, 0.0)
        wick_min = SWEEP_WICK_ATR_MULT * atr_value

        for level_name, level_price in levels:
            if row["low"] < level_price - breach:
                wick = level_price - row["low"]
                if wick >= wick_min and _reclaim_after_breach(df, idx, level_price, "bull", SWEEP_RECLAIM_BARS):
                    events.append(
                        SweepEvent(
                            ticker=ticker,
                            time=int(ts.timestamp()),
                            ny_time=local_dt.strftime("%Y-%m-%d %H:%M %Z"),
                            direction="bull",
                            level_name=level_name,
                            level_price=float(level_price),
                            close=float(row["close"]),
                            high=float(row["high"]),
                            low=float(row["low"]),
                        )
                    )

            if row["high"] > level_price + breach:
                wick = row["high"] - level_price
                if wick >= wick_min and _reclaim_after_breach(df, idx, level_price, "bear", SWEEP_RECLAIM_BARS):
                    events.append(
                        SweepEvent(
                            ticker=ticker,
                            time=int(ts.timestamp()),
                            ny_time=local_dt.strftime("%Y-%m-%d %H:%M %Z"),
                            direction="bear",
                            level_name=level_name,
                            level_price=float(level_price),
                            close=float(row["close"]),
                            high=float(row["high"]),
                            low=float(row["low"]),
                        )
                    )

    return events


def _prune_sweep_cache(now: float) -> None:
    if not _SWEEP_ALERT_CACHE:
        return
    for key, ts in list(_SWEEP_ALERT_CACHE.items()):
        if now - ts > SWEEP_ALERT_TTL_SECONDS:
            _SWEEP_ALERT_CACHE.pop(key, None)


def _format_sweep_sms(event: SweepEvent) -> str:
    direction = "Bullish" if event.direction == "bull" else "Bearish"
    level_label = event.level_name.replace("_", " ").title()
    return (
        f"{direction} liquidity sweep on {event.ticker} at {event.ny_time}. "
        f"Swept {level_label} ({_format_price(event.level_price)}). "
        f"Close {_format_price(event.close)}."
    )


def _send_sweep_sms(events: List[SweepEvent]) -> List[SweepEvent]:
    if not events:
        return events
    if not SWEEP_SMS_ENABLED:
        return events
    if Client is None:
        _LOGGER.warning("Twilio client not available; install the twilio package to enable SMS.")
        return events

    account_sid = os.getenv("TWILIO_ACCOUNT_SID")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN")
    from_number = os.getenv("TWILIO_FROM_NUMBER")
    to_numbers = [num.strip() for num in (os.getenv("TWILIO_TO_NUMBER") or "").split(",") if num.strip()]

    if not account_sid or not auth_token or not from_number or not to_numbers:
        _LOGGER.warning("Twilio credentials or numbers missing; SMS alerts are disabled.")
        return events

    client = Client(account_sid, auth_token)
    now = time.time()
    _prune_sweep_cache(now)

    for event in events:
        key = (event.ticker, event.level_name, event.direction, event.time)
        if key in _SWEEP_ALERT_CACHE and now - _SWEEP_ALERT_CACHE[key] < SWEEP_ALERT_TTL_SECONDS:
            continue

        body = _format_sweep_sms(event)
        sent_any = False
        for to_number in to_numbers:
            try:
                client.messages.create(body=body, from_=from_number, to=to_number)
                sent_any = True
            except Exception as exc:  # pragma: no cover - provider errors
                _LOGGER.warning("Failed to send sweep SMS to %s: %s", to_number, exc)

        if sent_any:
            event.sent = True
            _SWEEP_ALERT_CACHE[key] = now

    return events


def run_sweep_scan(
    tickers: List[str],
    interval: str,
    send_sms: bool = False,
) -> List[SweepEvent]:
    zone = _get_sweep_zone()
    events: List[SweepEvent] = []

    for ticker in tickers:
        try:
            df = fetch_ohlcv(ticker, interval)
        except HTTPException as exc:
            _LOGGER.warning("Sweep scan skipped %s: %s", ticker, exc.detail)
            continue
        except Exception as exc:  # pragma: no cover - safety net
            _LOGGER.warning("Sweep scan failed %s: %s", ticker, exc)
            continue

        levels = _compute_sweep_levels(df, zone)
        events.extend(_detect_liquidity_sweeps(ticker, df, levels, zone))

    if send_sms:
        events = _send_sweep_sms(events)
    return events


def _parse_date_only(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        parsed = pd.to_datetime(value)
    except Exception:
        return None
    if isinstance(parsed, pd.Timestamp):
        return parsed.date()
    return None


def _filter_df_by_dates(df: pd.DataFrame, zone: ZoneInfo, start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    if df.empty:
        return df
    start_date = _parse_date_only(start)
    end_date = _parse_date_only(end)
    if not start_date and not end_date:
        return df

    index = df.index
    if index.tz is None:
        index = index.tz_localize("UTC")
    local_index = index.tz_convert(zone)
    local_dates = local_index.date

    mask = np.ones(len(df), dtype=bool)
    if start_date:
        mask &= local_dates >= start_date
    if end_date:
        mask &= local_dates <= end_date

    return df.loc[mask]


def _build_daily_levels(df: pd.DataFrame, zone: ZoneInfo) -> Dict[date, Dict[str, float]]:
    if df.empty:
        return {}

    index = df.index
    if index.tz is None:
        index = index.tz_localize("UTC")
    local_index = index.tz_convert(zone)
    local_dates = local_index.date

    levels_by_date: Dict[date, Dict[str, float]] = {}
    for current_date in sorted(set(local_dates)):
        day_levels: Dict[str, float] = {}
        prev_date = current_date - timedelta(days=1)

        prev_mask = local_dates == prev_date
        if prev_mask.any():
            day_levels["prev_day_high"] = float(df.loc[prev_mask, "high"].max())
            day_levels["prev_day_low"] = float(df.loc[prev_mask, "low"].min())

        asia_mask = _build_session_mask(
            local_index,
            current_date,
            SWEEP_ASIA_START,
            SWEEP_ASIA_END,
            include_prev_for_wrap=True,
        )
        if asia_mask.any():
            day_levels["asia_high"] = float(df.loc[asia_mask, "high"].max())
            day_levels["asia_low"] = float(df.loc[asia_mask, "low"].min())

        london_mask = _build_session_mask(
            local_index,
            current_date,
            SWEEP_LONDON_START,
            SWEEP_LONDON_END,
        )
        if london_mask.any():
            day_levels["london_high"] = float(df.loc[london_mask, "high"].max())
            day_levels["london_low"] = float(df.loc[london_mask, "low"].min())

        levels_by_date[current_date] = day_levels

    return levels_by_date


def _levels_for_session(levels_by_date: Dict[date, Dict[str, float]], trade_date: date, session: str) -> List[Tuple[str, float]]:
    levels = levels_by_date.get(trade_date, {})
    selected: List[Tuple[str, float]] = []

    if "prev_day_high" in levels:
        selected.append(("prev_day_high", levels["prev_day_high"]))
    if "prev_day_low" in levels:
        selected.append(("prev_day_low", levels["prev_day_low"]))

    if "asia_high" in levels:
        selected.append(("asia_high", levels["asia_high"]))
    if "asia_low" in levels:
        selected.append(("asia_low", levels["asia_low"]))

    if session == "newyork":
        if "london_high" in levels:
            selected.append(("london_high", levels["london_high"]))
        if "london_low" in levels:
            selected.append(("london_low", levels["london_low"]))

    return selected


def _detect_fvg(
    df: pd.DataFrame,
    idx: int,
    direction: Literal["long", "short"],
    min_gap: float,
) -> Optional[Tuple[float, float]]:
    if idx < 2:
        return None
    high_prev2 = float(df.iloc[idx - 2]["high"])
    low_prev2 = float(df.iloc[idx - 2]["low"])
    high_curr = float(df.iloc[idx]["high"])
    low_curr = float(df.iloc[idx]["low"])

    if direction == "long":
        gap = low_curr - high_prev2
        if gap >= min_gap:
            return (high_prev2, low_curr)
    else:
        gap = low_prev2 - high_curr
        if gap >= min_gap:
            return (high_curr, low_prev2)
    return None


def _simulate_exit(
    df: pd.DataFrame,
    entry_idx: int,
    session_end_idx: int,
    direction: Literal["long", "short"],
    stop_price: float,
    target_price: float,
) -> Tuple[int, float]:
    for idx in range(entry_idx + 1, session_end_idx + 1):
        high = float(df.iloc[idx]["high"])
        low = float(df.iloc[idx]["low"])
        if direction == "long":
            hit_stop = low <= stop_price
            hit_target = high >= target_price
        else:
            hit_stop = high >= stop_price
            hit_target = low <= target_price

        if hit_stop and hit_target:
            return idx, stop_price
        if hit_stop:
            return idx, stop_price
        if hit_target:
            return idx, target_price

    final_close = float(df.iloc[session_end_idx]["close"])
    return session_end_idx, final_close


def _run_backtest_for_ticker(
    ticker: str,
    df: pd.DataFrame,
    request: BacktestRequest,
    zone: ZoneInfo,
) -> List[BacktestTrade]:
    if df.empty:
        return []

    df = df.dropna(subset=["open", "high", "low", "close"]).sort_index()
    if df.empty:
        return []

    df = _filter_df_by_dates(df, zone, request.start, request.end)
    if df.empty:
        return []

    df = df.copy()
    df["atr"] = compute_atr(df["high"], df["low"], df["close"], length=14)

    index = df.index
    if index.tz is None:
        index = index.tz_localize("UTC")
    local_index = index.tz_convert(zone)
    local_dates = local_index.date

    levels_by_date = _build_daily_levels(df, zone)
    unique_dates = sorted(set(local_dates))
    trades: List[BacktestTrade] = []

    for current_date in unique_dates:
        trades_today = 0

        for session_name in ("london", "newyork"):
            if request.session not in {session_name, "both"}:
                continue
            if trades_today >= request.max_trades_per_day:
                break

            if session_name == "london":
                start_time = SWEEP_LONDON_START
                end_time = SWEEP_LONDON_END
            else:
                start_time = SWEEP_NY_OPEN_START
                end_time = SWEEP_NY_OPEN_END

            session_mask = _build_session_mask(local_index, current_date, start_time, end_time)
            session_positions = np.where(session_mask)[0]
            if session_positions.size == 0:
                continue

            levels = _levels_for_session(levels_by_date, current_date, session_name)
            if not levels:
                continue

            session_end_idx = int(session_positions[-1])
            pos_ptr = 0

            while pos_ptr < len(session_positions) and trades_today < request.max_trades_per_day:
                idx = int(session_positions[pos_ptr])
                row = df.iloc[idx]
                atr_value = row.get("atr")
                if atr_value is None or not math.isfinite(atr_value) or atr_value <= 0:
                    pos_ptr += 1
                    continue

                sweep_threshold = request.sweep_atr_mult * float(atr_value)
                sweep_found = None

                for level_name, level_price in levels:
                    if row["low"] < level_price - sweep_threshold:
                        sweep_found = ("long", level_name, level_price)
                        break
                    if row["high"] > level_price + sweep_threshold:
                        sweep_found = ("short", level_name, level_price)
                        break

                if not sweep_found:
                    pos_ptr += 1
                    continue

                direction, level_name, level_price = sweep_found
                return_idx = None
                max_return = min(idx + request.return_within_bars, session_end_idx)

                for j in range(idx, max_return + 1):
                    close = float(df.iloc[j]["close"])
                    if direction == "long" and close > level_price:
                        return_idx = j
                        break
                    if direction == "short" and close < level_price:
                        return_idx = j
                        break

                if return_idx is None:
                    pos_ptr += 1
                    continue

                fvg_found = None
                fvg_idx = None
                max_fvg_search = min(return_idx + request.return_within_bars, session_end_idx)

                for k in range(return_idx + 2, max_fvg_search + 1):
                    atr_k = df.iloc[k]["atr"]
                    if atr_k is None or not math.isfinite(atr_k):
                        continue
                    min_gap = request.fvg_min_atr_mult * float(atr_k)
                    fvg = _detect_fvg(df, k, direction, min_gap)
                    if fvg:
                        fvg_found = fvg
                        fvg_idx = k
                        break

                if fvg_found is None or fvg_idx is None:
                    pos_ptr += 1
                    continue

                gap_low, gap_high = fvg_found
                entry_mid = (gap_low + gap_high) / 2
                entry_idx = None
                max_retrace = min(fvg_idx + request.fvg_retrace_window, session_end_idx)

                for m in range(fvg_idx + 1, max_retrace + 1):
                    high = float(df.iloc[m]["high"])
                    low = float(df.iloc[m]["low"])
                    if low <= entry_mid <= high:
                        entry_idx = m
                        break

                if entry_idx is None:
                    pos_ptr += 1
                    continue

                atr_entry = df.iloc[entry_idx]["atr"]
                if atr_entry is None or not math.isfinite(atr_entry) or atr_entry <= 0:
                    pos_ptr += 1
                    continue

                if direction == "long":
                    sweep_extreme = float(df.iloc[idx : return_idx + 1]["low"].min())
                    stop_atr = entry_mid - request.stop_atr_mult * float(atr_entry)
                    stop_price = min(sweep_extreme, stop_atr)
                    risk = entry_mid - stop_price
                    target_price = entry_mid + risk * request.target_rr
                else:
                    sweep_extreme = float(df.iloc[idx : return_idx + 1]["high"].max())
                    stop_atr = entry_mid + request.stop_atr_mult * float(atr_entry)
                    stop_price = max(sweep_extreme, stop_atr)
                    risk = stop_price - entry_mid
                    target_price = entry_mid - risk * request.target_rr

                if risk <= 0:
                    pos_ptr += 1
                    continue

                exit_idx, exit_price = _simulate_exit(
                    df,
                    entry_idx,
                    session_end_idx,
                    direction,
                    stop_price,
                    target_price,
                )

                if direction == "long":
                    r_multiple = (exit_price - entry_mid) / risk
                else:
                    r_multiple = (entry_mid - exit_price) / risk

                result = "breakeven"
                if r_multiple > 0:
                    result = "win"
                elif r_multiple < 0:
                    result = "loss"

                trades.append(
                    BacktestTrade(
                        ticker=ticker,
                        session=session_name,
                        direction=direction,
                        level_name=level_name,
                        sweep_time=int(index[idx].timestamp()),
                        fvg_time=int(index[fvg_idx].timestamp()) if fvg_idx is not None else None,
                        entry_time=int(index[entry_idx].timestamp()),
                        entry_price=float(entry_mid),
                        stop_price=float(stop_price),
                        target_price=float(target_price),
                        exit_time=int(index[exit_idx].timestamp()),
                        exit_price=float(exit_price),
                        result=result,
                        r_multiple=float(r_multiple),
                        pnl=0.0,
                    )
                )

                trades_today += 1
                # Jump ahead to avoid overlapping positions.
                while pos_ptr < len(session_positions) and session_positions[pos_ptr] <= exit_idx:
                    pos_ptr += 1
                continue

            if trades_today >= request.max_trades_per_day:
                break

    return trades


def _summarize_backtest(
    trades: List[BacktestTrade],
    starting_balance: float,
    risk_per_trade: float,
) -> Tuple[BacktestSummary, List[EquityPoint], List[SessionBreakdown]]:
    if not trades:
        summary = BacktestSummary(
            starting_balance=starting_balance,
            ending_balance=starting_balance,
            return_pct=0.0,
            total_trades=0,
            wins=0,
            losses=0,
            breakeven=0,
            win_rate=0.0,
            profit_factor=None,
            max_drawdown=0.0,
        )
        return summary, [], []

    sorted_trades = sorted(trades, key=lambda trade: trade.entry_time)
    equity = starting_balance
    peak = starting_balance
    max_drawdown = 0.0
    wins = 0
    losses = 0
    breakeven = 0
    total_profit = 0.0
    total_loss = 0.0
    equity_curve: List[EquityPoint] = []

    for trade in sorted_trades:
        risk_amount = equity * risk_per_trade
        trade.pnl = float(risk_amount * trade.r_multiple)
        equity += trade.pnl
        equity_curve.append(EquityPoint(time=trade.exit_time, equity=equity))

        if trade.pnl > 0:
            wins += 1
            total_profit += trade.pnl
        elif trade.pnl < 0:
            losses += 1
            total_loss += trade.pnl
        else:
            breakeven += 1

        if equity > peak:
            peak = equity
        drawdown = (peak - equity) / peak if peak > 0 else 0.0
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    total_trades = len(sorted_trades)
    win_rate = wins / total_trades if total_trades else 0.0
    profit_factor = None
    if total_loss < 0:
        profit_factor = total_profit / abs(total_loss)

    summary = BacktestSummary(
        starting_balance=starting_balance,
        ending_balance=equity,
        return_pct=((equity - starting_balance) / starting_balance) * 100.0 if starting_balance else 0.0,
        total_trades=total_trades,
        wins=wins,
        losses=losses,
        breakeven=breakeven,
        win_rate=win_rate,
        profit_factor=profit_factor,
        max_drawdown=max_drawdown,
    )

    session_breakdown: List[SessionBreakdown] = []
    for session in ("london", "newyork"):
        session_trades = [trade for trade in sorted_trades if trade.session == session]
        if not session_trades:
            continue
        session_profit = sum(trade.pnl for trade in session_trades if trade.pnl > 0)
        session_loss = sum(trade.pnl for trade in session_trades if trade.pnl < 0)
        session_wins = sum(1 for trade in session_trades if trade.pnl > 0)
        session_losses = sum(1 for trade in session_trades if trade.pnl < 0)
        session_breakeven = sum(1 for trade in session_trades if trade.pnl == 0)
        session_total = len(session_trades)
        session_pf = None if session_loss == 0 else session_profit / abs(session_loss)
        session_breakdown.append(
            SessionBreakdown(
                session=session,
                total_trades=session_total,
                wins=session_wins,
                losses=session_losses,
                breakeven=session_breakeven,
                win_rate=session_wins / session_total if session_total else 0.0,
                profit_factor=session_pf,
                return_pct=(sum(trade.pnl for trade in session_trades) / starting_balance) * 100.0
                if starting_balance
                else 0.0,
            )
        )

    return summary, equity_curve, session_breakdown


def _validate_backtest_request(request: BacktestRequest) -> None:
    if request.interval not in {"1m", "5m"}:
        raise HTTPException(status_code=400, detail="Interval must be 1m or 5m for backtests.")
    if request.risk_per_trade <= 0 or request.risk_per_trade > 0.05:
        raise HTTPException(status_code=400, detail="risk_per_trade must be between 0 and 0.05.")
    if request.max_trades_per_day < 1:
        raise HTTPException(status_code=400, detail="max_trades_per_day must be >= 1.")
    if request.return_within_bars < 1:
        raise HTTPException(status_code=400, detail="return_within_bars must be >= 1.")
    if request.fvg_retrace_window < 1:
        raise HTTPException(status_code=400, detail="fvg_retrace_window must be >= 1.")


def _prepare_backtest_data(
    request: BacktestRequest,
    zone: ZoneInfo,
) -> Tuple[List[str], Dict[str, pd.DataFrame], Dict[str, Dict[str, Optional[int]]]]:
    tickers = [_normalize_fx_ticker(ticker) for ticker in request.tickers if ticker.strip()]
    tickers = [ticker for ticker in tickers if ticker]
    if not tickers:
        raise HTTPException(status_code=400, detail="No tickers provided for backtest.")

    data_by_ticker: Dict[str, pd.DataFrame] = {}
    data_ranges: Dict[str, Dict[str, Optional[int]]] = {}

    for ticker in tickers:
        try:
            df = fetch_ohlcv_backtest(ticker, request)
        except HTTPException as exc:
            _LOGGER.warning("Backtest skipped %s: %s", ticker, exc.detail)
            continue
        except Exception as exc:  # pragma: no cover
            _LOGGER.warning("Backtest failed %s: %s", ticker, exc)
            continue

        df = _filter_df_by_dates(df, zone, request.start, request.end)
        if df.empty:
            continue

        data_by_ticker[ticker] = df
        data_ranges[ticker] = {
            "start": int(df.index[0].timestamp()),
            "end": int(df.index[-1].timestamp()),
        }

    return tickers, data_by_ticker, data_ranges


def _build_year_segments(
    start_date: Optional[date],
    end_date: Optional[date],
    start_year: Optional[int],
    end_year: Optional[int],
) -> List[Tuple[int, date, date]]:
    if not start_date or not end_date:
        raise HTTPException(status_code=400, detail="Batch backtest requires start and end dates.")

    year_start = start_year or start_date.year
    year_end = end_year or end_date.year
    if year_start > year_end:
        raise HTTPException(status_code=400, detail="start_year must be <= end_year.")

    segments: List[Tuple[int, date, date]] = []
    for year in range(year_start, year_end + 1):
        seg_start = max(start_date, date(year, 1, 1))
        seg_end = min(end_date, date(year, 12, 31))
        if seg_start <= seg_end:
            segments.append((year, seg_start, seg_end))

    if not segments:
        raise HTTPException(status_code=400, detail="No valid year segments in the given range.")
    return segments


def summarize_ticker(ticker: str, interval: str) -> ScanResult:
    try:
        df = fetch_ohlcv(ticker, interval)
        return summarize_ticker_from_df(ticker, df, interval)
    except HTTPException as exc:
        return ScanResult(
            ticker=ticker.upper(),
            latest_close=None,
            last_signal_time=None,
            last_signal_type=None,
            last_signal_strength=None,
            bias_type=None,
            bias_time=None,
            signal_count=0,
            status="error",
            error=str(exc.detail),
        )
    except Exception as exc:  # pragma: no cover - safety net
        return ScanResult(
            ticker=ticker.upper(),
            latest_close=None,
            last_signal_time=None,
            last_signal_type=None,
            last_signal_strength=None,
            bias_type=None,
            bias_time=None,
            signal_count=0,
            status="error",
            error=str(exc),
        )


def summarize_ticker_from_df(ticker: str, df: pd.DataFrame, interval: str = "1d") -> ScanResult:
    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data for ticker: {ticker}")

    df = add_indicators(df, interval)

    signals = build_signals(df)
    closes = df["close"].dropna()
    if closes.empty:
        raise HTTPException(status_code=404, detail="Not enough data after indicator calculation.")
    latest_close = float(closes.iloc[-1])
    last_signal = signals[-1] if signals else None

    bias_type: Optional[Literal["buy", "sell", "neutral"]] = None
    bias_time: Optional[int] = None
    latest_row = df.dropna(subset=["close", "ema200", "rsi", "supertrend", "time"]).tail(1)
    if not latest_row.empty:
        row = latest_row.iloc[0]
        trend_up = row["close"] > row["ema200"]
        trend_down = row["close"] < row["ema200"]
        volatility_up = row["close"] > row["supertrend"]
        volatility_down = row["close"] < row["supertrend"]
        momentum_up = row["rsi"] >= 50
        momentum_down = row["rsi"] <= 50

        if trend_up and volatility_up and momentum_up:
            bias_type = "buy"
        elif trend_down and volatility_down and momentum_down:
            bias_type = "sell"
        else:
            bias_type = "neutral"
        bias_time = int(row["time"])

    return ScanResult(
        ticker=ticker.upper(),
        latest_close=latest_close,
        last_signal_time=last_signal.time if last_signal else None,
        last_signal_type=last_signal.type if last_signal else None,
        last_signal_strength=last_signal.strength if last_signal else None,
        bias_type=bias_type,
        bias_time=bias_time,
        signal_count=len(signals),
        status="ok",
        error=None,
    )


@app.get("/analyze/{ticker}/{interval}", response_model=AnalyzeResponse)
def analyze(
    ticker: str,
    interval: str,
    weak: bool = Query(default=False, description="Include weak signals"),
) -> AnalyzeResponse:
    try:
        df = fetch_ohlcv(ticker, interval)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Upstream data error: {exc}") from exc
    df = add_indicators(df, interval)

    candles = build_candles(df)
    if not candles:
        raise HTTPException(status_code=404, detail="Not enough data after indicator calculation.")

    signals = build_signals(df, include_weak=weak)
    indicators = build_indicators(df)

    meta = Meta(ticker=ticker.upper(), interval=interval, rows=len(candles))
    return AnalyzeResponse(meta=meta, candles=candles, signals=signals, indicators=indicators)


@app.get("/scan/{interval}", response_model=ScanResponse)
async def scan(
    interval: str,
    tickers: Optional[str] = Query(default=None, description="Comma-separated tickers"),
) -> ScanResponse:
    if interval not in INTERVAL_MAP:
        raise HTTPException(status_code=400, detail=f"Unsupported interval: {interval}")

    if tickers:
        parsed = [ticker.strip().upper() for ticker in tickers.split(",") if ticker.strip()]
    else:
        parsed = DEFAULT_TICKERS.copy()

    deduped: List[str] = []
    seen = set()
    for ticker in parsed:
        if ticker not in seen:
            deduped.append(ticker)
            seen.add(ticker)

    if not deduped:
        raise HTTPException(status_code=400, detail="No tickers provided.")
    if len(deduped) > MAX_TICKERS:
        raise HTTPException(status_code=400, detail=f"Too many tickers. Max is {MAX_TICKERS}.")

    data_by_ticker = fetch_ohlcv_batch(deduped, interval)
    results: List[ScanResult] = []
    for ticker in deduped:
        try:
            result = summarize_ticker_from_df(ticker, data_by_ticker.get(ticker, pd.DataFrame()), interval)
        except HTTPException as exc:
            results.append(
                ScanResult(
                    ticker=ticker.upper(),
                    latest_close=None,
                    last_signal_time=None,
                    last_signal_type=None,
                    last_signal_strength=None,
                    bias_type=None,
                    bias_time=None,
                    signal_count=0,
                    status="error",
                    error=str(exc.detail),
                )
            )
        except Exception as exc:  # pragma: no cover - safety net
            results.append(
                ScanResult(
                    ticker=ticker.upper(),
                    latest_close=None,
                    last_signal_time=None,
                    last_signal_type=None,
                    last_signal_strength=None,
                    bias_type=None,
                    bias_time=None,
                    signal_count=0,
                    status="error",
                    error=str(exc),
                )
            )
        else:
            results.append(result)

    meta = ScanMeta(interval=interval, tickers=deduped, total=len(deduped))
    return ScanResponse(meta=meta, results=results)


@app.get("/sweeps/run", response_model=SweepScanResponse)
def run_sweeps(
    interval: str = Query(default=SWEEP_INTERVAL, description="Interval to scan"),
    tickers: Optional[str] = Query(default=None, description="Comma-separated tickers"),
    send_sms: bool = Query(default=False, description="Send SMS for new sweeps"),
) -> SweepScanResponse:
    if interval not in INTERVAL_MAP:
        raise HTTPException(status_code=400, detail=f"Unsupported interval: {interval}")

    parsed = _parse_ticker_list(tickers) if tickers else []
    use_tickers = parsed or SWEEP_TICKERS

    events = run_sweep_scan(use_tickers, interval, send_sms=send_sms)
    meta = SweepScanMeta(
        interval=interval,
        tickers=use_tickers,
        timezone=SWEEP_TIMEZONE,
        ny_open_start=SWEEP_NY_OPEN_START.strftime("%H:%M"),
        ny_open_end=SWEEP_NY_OPEN_END.strftime("%H:%M"),
    )
    return SweepScanResponse(meta=meta, events=events)


@app.get("/sweeps/status", response_model=SweepStatusResponse)
def sweeps_status() -> SweepStatusResponse:
    twilio_configured = bool(
        os.getenv("TWILIO_ACCOUNT_SID")
        and os.getenv("TWILIO_AUTH_TOKEN")
        and os.getenv("TWILIO_FROM_NUMBER")
        and os.getenv("TWILIO_TO_NUMBER")
    )
    sms_ready = SWEEP_SMS_ENABLED and twilio_configured and Client is not None

    return SweepStatusResponse(
        alerts_enabled=SWEEP_ALERTS_ENABLED,
        sms_enabled=SWEEP_SMS_ENABLED,
        sms_ready=sms_ready,
        twilio_configured=twilio_configured,
        poll_seconds=SWEEP_POLL_SECONDS,
        interval=SWEEP_INTERVAL,
        timezone=SWEEP_TIMEZONE,
        ny_open_start=SWEEP_NY_OPEN_START.strftime("%H:%M"),
        ny_open_end=SWEEP_NY_OPEN_END.strftime("%H:%M"),
        tickers=SWEEP_TICKERS,
    )


@app.post("/backtest/sweep", response_model=BacktestResponse)
def backtest_sweep(request: BacktestRequest) -> BacktestResponse:
    zone = _get_sweep_zone()
    _validate_backtest_request(request)
    tickers, data_by_ticker, data_ranges = _prepare_backtest_data(request, zone)
    trades: List[BacktestTrade] = []

    for ticker, df in data_by_ticker.items():
        trades.extend(_run_backtest_for_ticker(ticker, df, request, zone))

    sorted_trades = sorted(trades, key=lambda trade: trade.entry_time)
    summary, equity_curve, session_breakdown = _summarize_backtest(
        sorted_trades,
        request.starting_balance,
        request.risk_per_trade,
    )

    meta = {
        "tickers": tickers,
        "interval": request.interval,
        "session": request.session,
        "timezone": SWEEP_TIMEZONE,
        "ny_open_start": SWEEP_NY_OPEN_START.strftime("%H:%M"),
        "ny_open_end": SWEEP_NY_OPEN_END.strftime("%H:%M"),
        "london_start": SWEEP_LONDON_START.strftime("%H:%M"),
        "london_end": SWEEP_LONDON_END.strftime("%H:%M"),
        "data_ranges": data_ranges,
        "notes": "Intraday Yahoo data is limited (1m ~7d, 5m ~60d).",
    }
    return BacktestResponse(
        meta=meta,
        summary=summary,
        session_breakdown=session_breakdown,
        equity_curve=equity_curve,
        trades=sorted_trades,
    )


@app.post("/backtest/sweep/csv")
def backtest_sweep_csv(request: BacktestRequest) -> Response:
    result = backtest_sweep(request)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "ticker",
            "session",
            "direction",
            "level_name",
            "sweep_time",
            "fvg_time",
            "entry_time",
            "entry_price",
            "stop_price",
            "target_price",
            "exit_time",
            "exit_price",
            "result",
            "r_multiple",
            "pnl",
        ]
    )
    for trade in result.trades:
        writer.writerow(
            [
                trade.ticker,
                trade.session,
                trade.direction,
                trade.level_name,
                trade.sweep_time,
                trade.fvg_time or "",
                trade.entry_time,
                trade.entry_price,
                trade.stop_price,
                trade.target_price,
                trade.exit_time,
                trade.exit_price,
                trade.result,
                trade.r_multiple,
                trade.pnl,
            ]
        )
    return Response(content=output.getvalue(), media_type="text/csv")


@app.post("/backtest/sweep/grid", response_model=GridSearchResponse)
def backtest_sweep_grid(request: GridSearchRequest) -> GridSearchResponse:
    zone = _get_sweep_zone()
    _validate_backtest_request(request.base)
    tickers, data_by_ticker, data_ranges = _prepare_backtest_data(request.base, zone)

    sweep_atr_mults = request.sweep_atr_mults or [request.base.sweep_atr_mult]
    return_within_bars = request.return_within_bars or [request.base.return_within_bars]
    fvg_min_atr_mults = request.fvg_min_atr_mults or [request.base.fvg_min_atr_mult]
    fvg_retrace_windows = request.fvg_retrace_windows or [request.base.fvg_retrace_window]
    stop_atr_mults = request.stop_atr_mults or [request.base.stop_atr_mult]
    target_rrs = request.target_rrs or [request.base.target_rr]

    results: List[GridSearchResult] = []
    total_tested = 0

    for combo in itertools.product(
        sweep_atr_mults,
        return_within_bars,
        fvg_min_atr_mults,
        fvg_retrace_windows,
        stop_atr_mults,
        target_rrs,
    ):
        if total_tested >= request.max_combinations:
            break

        sweep_mult, return_bars, fvg_min, fvg_retrace, stop_mult, target_rr = combo
        candidate = request.base.model_copy(
            update={
                "sweep_atr_mult": sweep_mult,
                "return_within_bars": return_bars,
                "fvg_min_atr_mult": fvg_min,
                "fvg_retrace_window": fvg_retrace,
                "stop_atr_mult": stop_mult,
                "target_rr": target_rr,
            }
        )

        trades: List[BacktestTrade] = []
        for ticker, df in data_by_ticker.items():
            trades.extend(_run_backtest_for_ticker(ticker, df, candidate, zone))

        sorted_trades = sorted(trades, key=lambda trade: trade.entry_time)
        summary, _, _ = _summarize_backtest(
            sorted_trades,
            candidate.starting_balance,
            candidate.risk_per_trade,
        )

        if request.sort_by == "profit_factor":
            score = summary.profit_factor or 0.0
        elif request.sort_by == "score":
            score = summary.return_pct - (summary.max_drawdown * 100.0)
        else:
            score = summary.return_pct

        results.append(
            GridSearchResult(
                params={
                    "sweep_atr_mult": sweep_mult,
                    "return_within_bars": return_bars,
                    "fvg_min_atr_mult": fvg_min,
                    "fvg_retrace_window": fvg_retrace,
                    "stop_atr_mult": stop_mult,
                    "target_rr": target_rr,
                },
                summary=summary,
                score=score,
            )
        )
        total_tested += 1

    results = sorted(results, key=lambda item: item.score, reverse=True)[: request.top_n]

    meta = {
        "tickers": tickers,
        "interval": request.base.interval,
        "session": request.base.session,
        "timezone": SWEEP_TIMEZONE,
        "combinations_tested": total_tested,
        "max_combinations": request.max_combinations,
        "sort_by": request.sort_by,
        "data_ranges": data_ranges,
        "notes": "Grid search uses cached data from the base request window.",
    }
    return GridSearchResponse(meta=meta, results=results)


@app.post("/backtest/sweep/batch", response_model=BacktestBatchResponse)
def backtest_sweep_batch(request: BacktestBatchRequest) -> BacktestBatchResponse:
    zone = _get_sweep_zone()
    _validate_backtest_request(request.base)

    _BATCH_CONTROL["cancel"] = False

    start_date = _parse_date_only(request.base.start)
    end_date = _parse_date_only(request.base.end)
    segments = _build_year_segments(start_date, end_date, request.start_year, request.end_year)

    _BATCH_PROGRESS.update(
        {
            "status": "running",
            "processed": 0,
            "total": len(segments),
            "current_year": segments[0][0] if segments else None,
            "updated_at": time.time(),
        }
    )

    results: List[BacktestYearResult] = []
    tickers: List[str] = []
    interval = request.base.interval
    session = request.base.session

    for year, seg_start, seg_end in segments:
        if _BATCH_CONTROL["cancel"]:
            break
        _BATCH_PROGRESS.update(
            {
                "status": "running",
                "processed": _BATCH_PROGRESS.get("processed", 0),
                "total": len(segments),
                "current_year": year,
                "updated_at": time.time(),
            }
        )
        segment_request = request.base.model_copy(
            update={"start": seg_start.isoformat(), "end": seg_end.isoformat()}
        )
        tickers, data_by_ticker, data_ranges = _prepare_backtest_data(segment_request, zone)
        trades: List[BacktestTrade] = []
        for ticker, df in data_by_ticker.items():
            trades.extend(_run_backtest_for_ticker(ticker, df, segment_request, zone))

        sorted_trades = sorted(trades, key=lambda trade: trade.entry_time)
        summary, _, _ = _summarize_backtest(
            sorted_trades,
            segment_request.starting_balance,
            segment_request.risk_per_trade,
        )

        results.append(
            BacktestYearResult(
                year=year,
                summary=summary,
                total_trades=len(sorted_trades),
                data_ranges=data_ranges,
            )
        )
        _BATCH_PROGRESS.update(
            {
                "status": "running",
                "processed": _BATCH_PROGRESS.get("processed", 0) + 1,
                "total": len(segments),
                "current_year": year,
                "updated_at": time.time(),
            }
        )

    meta = {
        "tickers": tickers,
        "interval": interval,
        "session": session,
        "timezone": SWEEP_TIMEZONE,
        "start_year": segments[0][0],
        "end_year": segments[-1][0],
        "notes": "Batch backtest runs each year independently to keep downloads manageable.",
    }
    _BATCH_PROGRESS.update(
        {
            "status": "done" if not _BATCH_CONTROL["cancel"] else "idle",
            "processed": _BATCH_PROGRESS.get("processed", 0),
            "total": len(segments),
            "current_year": segments[-1][0] if segments else None,
            "updated_at": time.time(),
        }
    )
    return BacktestBatchResponse(meta=meta, results=results)


@app.get("/dukascopy/progress", response_model=DukascopyProgressResponse)
def dukascopy_progress() -> DukascopyProgressResponse:
    sources: List[dict] = []
    for key, stats in _DUKASCOPY_PROGRESS.items():
        processed = stats.get("processed", 0.0)
        total = stats.get("total", 0.0)
        percent = (processed / total * 100.0) if total else 0.0
        sources.append(
            {
                "source": key,
                "processed": processed,
                "total": total,
                "cached": stats.get("cached", 0.0),
                "downloaded": stats.get("downloaded", 0.0),
                "missing": stats.get("missing", 0.0),
                "retry_attempts": stats.get("retry_attempts", 0.0),
                "skipped": stats.get("skipped", 0.0),
                "speed": stats.get("speed", 0.0),
                "eta_seconds": stats.get("eta_seconds", 0.0),
                "percent": percent,
                "updated_at": stats.get("updated_at", 0.0),
            }
        )
    sources.sort(key=lambda entry: entry["source"])
    return DukascopyProgressResponse(
        paused=_DUKASCOPY_CONTROL["pause"],
        canceled=_DUKASCOPY_CONTROL["cancel"],
        sources=sources,
    )


@app.post("/dukascopy/pause", response_model=DukascopyControlResponse)
def dukascopy_pause() -> DukascopyControlResponse:
    _DUKASCOPY_CONTROL["pause"] = True
    _DUKASCOPY_CONTROL["cancel"] = False
    return DukascopyControlResponse(paused=True, canceled=False)


@app.post("/dukascopy/resume", response_model=DukascopyControlResponse)
def dukascopy_resume() -> DukascopyControlResponse:
    _DUKASCOPY_CONTROL["pause"] = False
    _DUKASCOPY_CONTROL["cancel"] = False
    return DukascopyControlResponse(paused=False, canceled=False)


@app.post("/dukascopy/cancel", response_model=DukascopyControlResponse)
def dukascopy_cancel() -> DukascopyControlResponse:
    _DUKASCOPY_CONTROL["cancel"] = True
    _DUKASCOPY_CONTROL["pause"] = False
    return DukascopyControlResponse(
        paused=_DUKASCOPY_CONTROL["pause"],
        canceled=_DUKASCOPY_CONTROL["cancel"],
    )


@app.get("/backtest/sweep/batch/progress", response_model=BatchProgressResponse)
def backtest_batch_progress() -> BatchProgressResponse:
    return BatchProgressResponse(
        status=str(_BATCH_PROGRESS.get("status", "idle")),
        processed=int(_BATCH_PROGRESS.get("processed", 0)),
        total=int(_BATCH_PROGRESS.get("total", 0)),
        current_year=_BATCH_PROGRESS.get("current_year"),
        updated_at=float(_BATCH_PROGRESS.get("updated_at", 0.0)),
    )


@app.post("/backtest/sweep/batch/cancel")
def backtest_batch_cancel() -> dict:
    _BATCH_CONTROL["cancel"] = True
    _BATCH_PROGRESS.update(
        {
            "status": "idle",
            "updated_at": time.time(),
        }
    )
    return {"canceled": True}


async def _sweep_monitor_loop() -> None:
    while True:
        try:
            await asyncio.to_thread(run_sweep_scan, SWEEP_TICKERS, SWEEP_INTERVAL, True)
        except Exception as exc:  # pragma: no cover - safety net
            _LOGGER.warning("Sweep monitor error: %s", exc)
        await asyncio.sleep(SWEEP_POLL_SECONDS)


@app.on_event("startup")
async def start_sweep_monitor() -> None:
    if not SWEEP_ALERTS_ENABLED:
        return
    asyncio.create_task(_sweep_monitor_loop())


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
