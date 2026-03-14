from __future__ import annotations

import os
import time
from typing import List, Literal, Optional

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

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
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
    "1w": "1wk",
}

PERIOD_MAP = {
    "1h": "60d",
    "4h": "180d",
    "1d": "max",
    "1w": "max",
}

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
    ema9: float
    ema21: float
    ema50: float
    ema200: float
    supertrend: float
    rsi: float
    vwap: float
    bb_upper: float
    bb_middle: float
    bb_lower: float
    macd: float
    macd_signal: float


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

    period = PERIOD_MAP[interval]
    raw_df = _yf_download(
        ticker,
        period=period,
        interval=yf_interval,
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    df = normalize_ohlcv(raw_df)

    if df.empty:
        df = normalize_ohlcv(fetch_ohlcv_history(ticker, period, yf_interval))

    if df.empty and interval == "1d" and period != "1y":
        df = normalize_ohlcv(fetch_ohlcv_history(ticker, "1y", yf_interval))

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No data for ticker: {ticker}. Yahoo returned empty data.",
        )

    df = normalize_ohlcv(df)
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

    downloaded = yf.download(
        " ".join(missing),
        period=period,
        interval=yf_interval,
        group_by="ticker",
        auto_adjust=False,
        progress=False,
        threads=False,
    )

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
            results[ticker] = df
            if not df.empty:
                _ohlcv_cache[(ticker.upper(), interval)] = (time.time(), df.copy())
    else:
        ticker = missing[0]
        df = normalize_ohlcv(downloaded.copy())
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


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
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
    df["time"] = _build_time_index(df.index, len(df))
    return df


def _build_time_index(index: pd.Index, length: int) -> pd.Series:
    times: Optional[np.ndarray] = None

    if isinstance(index, pd.PeriodIndex):
        index = index.to_timestamp()

    if isinstance(index, pd.DatetimeIndex):
        times = (index.view("int64") // 10**9).astype("int64")
    else:
        parsed = pd.to_datetime(index, utc=True, errors="coerce")
        if isinstance(parsed, pd.DatetimeIndex) and not parsed.isna().any():
            times = (parsed.view("int64") // 10**9).astype("int64")

    if times is None:
        # Fallback to a synthetic daily cadence ending now to keep charts usable.
        step = 60 * 60 * 24
        end = int(time.time())
        start = end - max(length - 1, 0) * step
        times = np.arange(start, start + length * step, step, dtype="int64")

    if length > 1 and (np.diff(times) <= 0).any():
        step = 60 * 60 * 24
        end = int(time.time())
        start = end - max(length - 1, 0) * step
        times = np.arange(start, start + length * step, step, dtype="int64")

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
    valid = df.dropna(
        subset=[
            "ema9",
            "ema21",
            "ema50",
            "ema200",
            "supertrend",
            "rsi",
            "vwap",
            "bb_upper",
            "bb_middle",
            "bb_lower",
            "macd",
            "macd_signal",
            "time",
        ]
    )
    valid = valid.sort_values("time")
    for _, row in valid.iterrows():
        indicators.append(
            Indicator(
                time=int(row["time"]),
                ema9=float(row["ema9"]),
                ema21=float(row["ema21"]),
                ema50=float(row["ema50"]),
                ema200=float(row["ema200"]),
                supertrend=float(row["supertrend"]),
                rsi=float(row["rsi"]),
                vwap=float(row["vwap"]),
                bb_upper=float(row["bb_upper"]),
                bb_middle=float(row["bb_middle"]),
                bb_lower=float(row["bb_lower"]),
                macd=float(row["macd"]),
                macd_signal=float(row["macd_signal"]),
            )
        )
    return indicators


def summarize_ticker(ticker: str, interval: str) -> ScanResult:
    try:
        df = fetch_ohlcv(ticker, interval)
        return summarize_ticker_from_df(ticker, df)
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


def summarize_ticker_from_df(ticker: str, df: pd.DataFrame) -> ScanResult:
    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data for ticker: {ticker}")

    df = add_indicators(df)

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
    df = fetch_ohlcv(ticker, interval)
    df = add_indicators(df)

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
    results = [summarize_ticker_from_df(ticker, data_by_ticker.get(ticker, pd.DataFrame())) for ticker in deduped]

    meta = ScanMeta(interval=interval, tickers=deduped, total=len(deduped))
    return ScanResponse(meta=meta, results=results)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
