# Prompt Library

## Canonical Prompt Tip (Verbatim)
"Write a Python service using pandas_ta that identifies every instance where a 9 EMA crosses a 21 EMA while the price is above a 200 EMA."

## Additional High-Signal Prompts
- "Implement SuperTrend in pandas_ta and return a DataFrame with trend direction and upper/lower bands for each candle."
- "Given OHLCV data, detect all timestamps where EMA9 crosses EMA21 while close is above EMA200, and return them as buy/sell markers."
- "Write an async FastAPI endpoint that fetches 20 tickers concurrently (asyncio or concurrent.futures) and returns aggregated signal summaries."
