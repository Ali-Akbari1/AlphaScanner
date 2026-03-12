# AlphaScanner Specification

## Project Overview
AlphaScanner is a full-stack technical analysis platform that replicates LuxAlgo/SimpleAlgo-style signals using a consensus-based indicator engine. The system provides multi-timeframe signals, interactive charting, and a multi-ticker scanner to surface high-confidence buy/sell opportunities.

## Tech Constraints
- Frontend: React 18 + TypeScript + Vite
- Charting: TradingView lightweight-charts
- Backend: FastAPI (asynchronous Python)
- Data Science: pandas, numpy, pandas_ta
- Data Source: yfinance
- Deployment: Vercel (frontend) + Railway/Render (backend)
- Optional: Redis cache to avoid yfinance rate limits

## Feature List
- Phase 1 (Engine): `/analyze/{ticker}/{interval}` endpoint, EMA cloud (9/21/50), SuperTrend, RSI transitions, buy/sell detection, timestamps
- Phase 2 (Canvas): candlestick rendering, buy/sell markers, timeframe switcher, API integration
- Phase 3 (Scanner): dashboard view scanning 10-20 tickers with parallel fetch (asyncio or concurrent.futures)

## Indicator Engine Spec
- Supported intervals: `1h`, `4h`, `1d`, `1w`
- Short-term volatility: ATR length 14
- Long-term bias: EMA 200
- SuperTrend: ATR length 14, multiplier 3.0 (LuxAlgo style)
- RSI length: 14
- EMA cloud lines: 9, 21, 50
- Momentum condition: RSI crosses up through 30 (oversold -> neutral)
- Sell conditions (default): Price < EMA200, price breaks below lower SuperTrend line, RSI crosses down through 70 (overbought -> neutral)
- Signal strength: `SignalStrength = Σ(Condition_i * Weight_i)`
- Default weights: all `1.0`
- Strong Buy/Sell: requires trend + volatility + momentum alignment
- Timestamps: UNIX epoch seconds in UTC (aligned with lightweight-charts)

## API Contract (Documented Only)
### Endpoint
- `GET /analyze/{ticker}/{interval}`
- `GET /scan/{interval}?tickers=BTC-USD,AAPL,NVDA` (comma-separated optional list; default universe if omitted)

### Response Shape
- `meta`: basic request context
- `candles[]`: OHLCV time series
- `signals[]`: buy/sell events with strength and conditions
- `indicators[]`: aligned indicator values

### Scan Response Shape
- `meta`: scan context (interval, tickers, total)
- `results[]`: per-ticker summary (latest_close, last_signal_* fields, signal_count, status)

### Response Example
```json
{
  "meta": {
    "ticker": "AAPL",
    "interval": "1d"
  },
  "candles": [
    {
      "time": 1700000000,
      "open": 0,
      "high": 0,
      "low": 0,
      "close": 0,
      "volume": 0
    }
  ],
  "signals": [
    {
      "time": 1700000000,
      "type": "buy",
      "strength": "strong",
      "conditions": ["trend", "volatility", "momentum"]
    }
  ],
  "indicators": [
    {
      "time": 1700000000,
      "ema9": 0,
      "ema21": 0,
      "ema50": 0,
      "ema200": 0,
      "supertrend": 0,
      "rsi": 0
    }
  ]
}
```

## UI/UX Style Tokens
### Typography
- Headings: Space Grotesk
- Body: IBM Plex Sans
- Data labels: IBM Plex Mono

### Color Palette (Hex)
| Token | Hex |
| --- | --- |
| background | `#0B0F14` |
| surface | `#121826` |
| surface-2 | `#171F2E` |
| gridline | `#1F2A3A` |
| text-primary | `#E6EDF3` |
| text-secondary | `#9FB0C3` |
| accent-cyan | `#3EE7F7` |
| accent-green (buy) | `#2EEA8C` |
| accent-red (sell) | `#FF4D6D` |
| accent-yellow | `#F6C453` |
| ema9 | `#7CFFB2` |
| ema21 | `#66B2FF` |
| ema50 | `#F6C453` |
| ema200 | `#FF8C42` |
| supertrend-up | `#3EE7F7` |
| supertrend-down | `#FF7A9E` |

## Roadmap + PM Checklist (Verbatim)
### Phase 1: The "Engine" (Backend)
Core Task: Build a FastAPI endpoint GET /analyze/{ticker}/{interval}.
Logic: * Fetch OHLCV data via yfinance.Calculate EMA Cloud (9/21/50).Calculate SuperTrend (LuxAlgo style).Identify "Crossover" points in the data frame to generate Buy/Sell timestamps.AI Prompt Tip: "Write a Python service using pandas_ta that identifies every instance where a 9 EMA crosses a 21 EMA while the price is above a 200 EMA."

### Phase 2: The "Canvas" (Frontend)
Core Task: Integrate lightweight-charts.Feature: * Render Candlestick data.Markers: Use the setMarkers() function to place "BUY" and "SELL" icons directly on the price bars based on backend timestamps.Timeframe Switcher: A button group that re-fetches data for the current ticker.

### Phase 3: The "Scanner" (Multi-Stock)
Core Task: Create a dashboard view that scans a list of 10-20 popular tickers (BTC, NVDA, AAPL, etc.) simultaneously.Engineering Challenge: Use Python’s asyncio or concurrent.futures to fetch all 20 tickers in parallel so the user doesn't wait 30 seconds for a page load.

### Project Manager Checklist (Your Next Steps)
[ ] Initialize Repo: Create the /docs folder and the spec.md.
[ ] Backend MVP: Get a single FastAPI endpoint returning "Buy/Sell" signals in JSON for a single ticker.
[ ] Frontend MVP: Get a React chart to display candles for "AAPL."
[ ] The "Bridge": Connect the two so clicking a button on the chart updates the indicators.

## Resume-Ready Descriptions (Verbatim)
AlphaScanner | Lead Developer & Architect
Engineered a full-stack financial dashboard using React, TypeScript, and FastAPI to provide real-time technical analysis for 50+ equities.
Developed a multi-factor signal engine incorporating LuxAlgo-inspired volatility markers (SuperTrend) and SimpleAlgo trend clouds.
Optimized data retrieval using asynchronous Python (asyncio) and vectorized Pandas operations, reducing signal calculation latency by 60%.
Implemented high-performance data visualization using TradingView’s Lightweight Charts, rendering 1,000+ data points with interactive buy/sell markers.
