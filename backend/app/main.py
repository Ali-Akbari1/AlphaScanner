from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from datetime import date, time as dt_time, timedelta
from typing import List, Literal, Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
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
    "5m": "5m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
    "1w": "1wk",
    "1mo": "1mo",
}

PERIOD_MAP = {
    "5m": "60d",
    "1h": "2y",
    "4h": "2y",
    "1d": "max",
    "1w": "max",
    "1mo": "max",
}

INTERVAL_SECONDS = {
    "5m": 5 * 60,
    "1h": 60 * 60,
    "4h": 4 * 60 * 60,
    "1d": 24 * 60 * 60,
    "1w": 7 * 24 * 60 * 60,
    "1mo": 30 * 24 * 60 * 60,
}

INTRADAY_PERIODS = ["2y", "1y", "6mo", "3mo", "60d", "30d", "14d", "7d"]

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

_LOGGER = logging.getLogger("alpha_scanner.sweeps")


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

    if interval in {"5m", "1h", "4h"}:
        raw_df = fetch_intraday_ohlcv(ticker, yf_interval, INTERVAL_SECONDS[interval])
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

    if df.empty and interval in {"5m", "1h", "4h"}:
        for fallback in ("2y", "1y", "6mo", "3mo", "60d"):
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


def fetch_intraday_ohlcv(ticker: str, yf_interval: str, expected_seconds: int) -> pd.DataFrame:
    best_df = pd.DataFrame()
    best_step = None

    for period in INTRADAY_PERIODS:
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
