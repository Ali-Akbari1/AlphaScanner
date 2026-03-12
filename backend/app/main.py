from __future__ import annotations

import asyncio
from typing import List, Literal, Optional

import numpy as np
import pandas as pd
import pandas_ta as ta
import yfinance as yf
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="AlphaScanner API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
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
    "1d": "2y",
    "1w": "5y",
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
    strength: Literal["strong"]
    conditions: List[str]


class Indicator(BaseModel):
    time: int
    ema9: float
    ema21: float
    ema50: float
    ema200: float
    supertrend: float
    rsi: float


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
    signal_count: int
    status: Literal["ok", "error"]
    error: Optional[str] = None


class ScanResponse(BaseModel):
    meta: ScanMeta
    results: List[ScanResult]


def fetch_ohlcv(ticker: str, interval: str) -> pd.DataFrame:
    yf_interval = INTERVAL_MAP.get(interval)
    if not yf_interval:
        raise HTTPException(status_code=400, detail=f"Unsupported interval: {interval}")

    period = PERIOD_MAP[interval]
    df = yf.download(
        ticker,
        period=period,
        interval=yf_interval,
        auto_adjust=False,
        progress=False,
    )

    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data for ticker: {ticker}")

    df = df.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )

    df = df[["open", "high", "low", "close", "volume"]].copy()
    df.index = pd.to_datetime(df.index, utc=True)
    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema9"] = ta.ema(df["close"], length=9)
    df["ema21"] = ta.ema(df["close"], length=21)
    df["ema50"] = ta.ema(df["close"], length=50)
    df["ema200"] = ta.ema(df["close"], length=200)
    df["rsi"] = ta.rsi(df["close"], length=14)

    st = ta.supertrend(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        length=14,
        multiplier=3.0,
    )

    df["supertrend"] = np.nan
    df["supertrend_upper"] = np.nan
    df["supertrend_lower"] = np.nan

    if st is not None and not st.empty:
        df = pd.concat([df, st], axis=1)
        st_col = next((c for c in st.columns if c.startswith("SUPERT_")), None)
        upper_col = next((c for c in st.columns if c.startswith("SUPERTs_")), None)
        lower_col = next((c for c in st.columns if c.startswith("SUPERTl_")), None)
        if st_col:
            df["supertrend"] = df[st_col]
        if upper_col:
            df["supertrend_upper"] = df[upper_col]
        if lower_col:
            df["supertrend_lower"] = df[lower_col]

    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(
        subset=[
            "open",
            "high",
            "low",
            "close",
            "volume",
            "ema9",
            "ema21",
            "ema50",
            "ema200",
            "rsi",
            "supertrend",
        ]
    )

    df["time"] = (df.index.view("int64") // 10**9).astype(int)
    return df


def build_signals(df: pd.DataFrame) -> List[Signal]:
    st_upper = df["supertrend_upper"].fillna(df["supertrend"])
    st_lower = df["supertrend_lower"].fillna(df["supertrend"])

    trend_up = df["close"] > df["ema200"]
    trend_down = df["close"] < df["ema200"]
    volatility_up = df["close"] > st_upper
    volatility_down = df["close"] < st_lower

    momentum_up = (df["rsi"].shift(1) < 30) & (df["rsi"] >= 30)
    momentum_down = (df["rsi"].shift(1) > 70) & (df["rsi"] <= 70)

    buy_mask = trend_up & volatility_up & momentum_up
    sell_mask = trend_down & volatility_down & momentum_down

    signals: List[Signal] = []
    for _, row in df.loc[buy_mask | sell_mask].iterrows():
        signal_type = "buy" if buy_mask.loc[row.name] else "sell"
        signals.append(
            Signal(
                time=int(row["time"]),
                type=signal_type,
                strength="strong",
                conditions=["trend", "volatility", "momentum"],
            )
        )

    return signals


def build_candles(df: pd.DataFrame) -> List[Candle]:
    candles: List[Candle] = []
    for _, row in df.iterrows():
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
    for _, row in df.iterrows():
        indicators.append(
            Indicator(
                time=int(row["time"]),
                ema9=float(row["ema9"]),
                ema21=float(row["ema21"]),
                ema50=float(row["ema50"]),
                ema200=float(row["ema200"]),
                supertrend=float(row["supertrend"]),
                rsi=float(row["rsi"]),
            )
        )
    return indicators


def summarize_ticker(ticker: str, interval: str) -> ScanResult:
    try:
        df = fetch_ohlcv(ticker, interval)
        df = add_indicators(df)

        if df.empty:
            raise HTTPException(status_code=404, detail="Not enough data after indicator calculation.")

        signals = build_signals(df)
        latest_close = float(df["close"].iloc[-1])
        last_signal = signals[-1] if signals else None

        return ScanResult(
            ticker=ticker.upper(),
            latest_close=latest_close,
            last_signal_time=last_signal.time if last_signal else None,
            last_signal_type=last_signal.type if last_signal else None,
            last_signal_strength=last_signal.strength if last_signal else None,
            signal_count=len(signals),
            status="ok",
            error=None,
        )
    except HTTPException as exc:
        return ScanResult(
            ticker=ticker.upper(),
            latest_close=None,
            last_signal_time=None,
            last_signal_type=None,
            last_signal_strength=None,
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
            signal_count=0,
            status="error",
            error=str(exc),
        )


@app.get("/analyze/{ticker}/{interval}", response_model=AnalyzeResponse)
def analyze(ticker: str, interval: str) -> AnalyzeResponse:
    df = fetch_ohlcv(ticker, interval)
    df = add_indicators(df)

    if df.empty:
        raise HTTPException(status_code=404, detail="Not enough data after indicator calculation.")

    signals = build_signals(df)
    candles = build_candles(df)
    indicators = build_indicators(df)

    meta = Meta(ticker=ticker.upper(), interval=interval, rows=len(df))
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

    tasks = [asyncio.to_thread(summarize_ticker, ticker, interval) for ticker in deduped]
    results = await asyncio.gather(*tasks)

    meta = ScanMeta(interval=interval, tickers=deduped, total=len(deduped))
    return ScanResponse(meta=meta, results=results)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
