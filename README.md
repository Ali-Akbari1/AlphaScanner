# AlphaScanner

AlphaScanner is a full-stack technical analysis dashboard that combines EMA clouds, SuperTrend volatility signals, and RSI momentum transitions to surface high-confidence trade markers across multiple timeframes.

## Highlights
- Consensus-based signal engine with EMA cloud, SuperTrend, and RSI conditions
- Multi-timeframe analysis for `1h`, `4h`, `1d`, and `1w`
- TradingView lightweight-charts candlestick rendering with buy/sell markers
- Async-friendly backend for multi-ticker scanning

## Tech Stack
- Frontend: React 18, TypeScript, Vite, lightweight-charts
- Backend: FastAPI (async), pandas, numpy, pandas_ta, yfinance
- Optional: Redis cache
- Deployment: Vercel (frontend) + Railway/Render (backend)

## Project Structure
- `docs/` memory layer (spec, architecture, prompts, context log)
- `backend/` FastAPI service
- `frontend/` React app

## Quickstart

### Backend
```powershell
cd backend
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

### Frontend
```powershell
cd frontend
npm install
npm run dev
```

### Run Both (Single Command)
```powershell
cd frontend
npm run dev:all
```

### Environment
- Copy `frontend/.env.example` to `frontend/.env` and update `VITE_API_BASE` if needed.

## API
- `GET /analyze/{ticker}/{interval}` returns `meta`, `candles[]`, `signals[]`, and `indicators[]`.
- `GET /scan/{interval}?tickers=BTC-USD,AAPL,NVDA` returns a per-ticker summary for the scanner dashboard.

## Tests
```powershell
cd backend
pip install -r requirements-dev.txt
pytest
```

## Notes
- Timestamps are UNIX epoch seconds in UTC.
- This project is for educational purposes and not financial advice.

## Documentation
- `docs/spec.md`
- `docs/architecture.md`
- `docs/context.md`
- `docs/prompts.md`
