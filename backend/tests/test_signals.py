import pandas as pd

from app.main import build_signals


def test_build_signals_buy():
    df = pd.DataFrame(
        [
            {
                "close": 90.0,
                "ema200": 100.0,
                "supertrend": 95.0,
                "supertrend_upper": 95.0,
                "supertrend_lower": 85.0,
                "rsi": 25.0,
                "time": 1,
            },
            {
                "close": 120.0,
                "ema200": 100.0,
                "supertrend": 110.0,
                "supertrend_upper": 110.0,
                "supertrend_lower": 100.0,
                "rsi": 35.0,
                "time": 2,
            },
        ]
    )

    signals = build_signals(df)

    assert len(signals) == 1
    assert signals[0].type == "buy"
    assert signals[0].time == 2


def test_build_signals_sell():
    df = pd.DataFrame(
        [
            {
                "close": 120.0,
                "ema200": 100.0,
                "supertrend": 110.0,
                "supertrend_upper": 130.0,
                "supertrend_lower": 115.0,
                "rsi": 75.0,
                "time": 1,
            },
            {
                "close": 80.0,
                "ema200": 100.0,
                "supertrend": 90.0,
                "supertrend_upper": 95.0,
                "supertrend_lower": 85.0,
                "rsi": 65.0,
                "time": 2,
            },
        ]
    )

    signals = build_signals(df)

    assert len(signals) == 1
    assert signals[0].type == "sell"
    assert signals[0].time == 2
