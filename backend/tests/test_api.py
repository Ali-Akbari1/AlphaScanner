import pandas as pd
from fastapi.testclient import TestClient

from app import main


def test_analyze_response_shape(monkeypatch):
    df = pd.DataFrame(
        {
            "open": [1.0, 2.0],
            "high": [2.0, 3.0],
            "low": [0.5, 1.5],
            "close": [1.5, 2.5],
            "volume": [100.0, 200.0],
            "ema9": [1.4, 2.4],
            "ema21": [1.3, 2.3],
            "ema50": [1.2, 2.2],
            "ema200": [1.0, 2.0],
            "rsi": [25.0, 35.0],
            "supertrend": [1.1, 2.1],
            "supertrend_upper": [1.1, 2.1],
            "supertrend_lower": [0.9, 1.9],
            "time": [1000, 2000],
        },
        index=pd.to_datetime([1000, 2000], unit="s", utc=True),
    )

    monkeypatch.setattr(main, "fetch_ohlcv", lambda ticker, interval: df)
    monkeypatch.setattr(main, "add_indicators", lambda frame: df)

    client = TestClient(main.app)
    response = client.get("/analyze/AAPL/1d")

    assert response.status_code == 200
    payload = response.json()

    assert set(payload.keys()) == {"meta", "candles", "signals", "indicators"}
    assert payload["meta"]["ticker"] == "AAPL"
    assert payload["meta"]["interval"] == "1d"
    assert len(payload["candles"]) == 2
    assert len(payload["indicators"]) == 2
    assert payload["signals"][0]["type"] == "buy"
    assert payload["signals"][0]["time"] == 2000
