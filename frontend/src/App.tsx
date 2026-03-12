import { useEffect, useRef, useState } from "react";
import {
  ColorType,
  createChart,
  CrosshairMode,
  IChartApi,
  ISeriesApi,
  UTCTimestamp,
} from "lightweight-charts";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";
const INTERVALS = ["1h", "4h", "1d", "1w"] as const;
const DEFAULT_SCAN_TICKERS = [
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
];

type Interval = (typeof INTERVALS)[number];

type Candle = {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

type Signal = {
  time: number;
  type: "buy" | "sell";
  strength: "strong";
  conditions: string[];
};

type Indicator = {
  time: number;
  ema9: number;
  ema21: number;
  ema50: number;
  ema200: number;
  supertrend: number;
  rsi: number;
};

type AnalyzeResponse = {
  meta: {
    ticker: string;
    interval: string;
    rows: number;
  };
  candles: Candle[];
  signals: Signal[];
  indicators: Indicator[];
};

type ScanResult = {
  ticker: string;
  latest_close: number | null;
  last_signal_time: number | null;
  last_signal_type: "buy" | "sell" | null;
  last_signal_strength: "strong" | null;
  signal_count: number;
  status: "ok" | "error";
  error?: string | null;
};

type ScanResponse = {
  meta: {
    interval: string;
    tickers: string[];
    total: number;
  };
  results: ScanResult[];
};

const formatTime = (timestamp?: number | null) => {
  if (!timestamp) {
    return "-";
  }
  return new Date(timestamp * 1000).toLocaleString();
};

const formatPrice = (value?: number | null) => {
  if (value === null || value === undefined) {
    return "-";
  }
  return value.toFixed(2);
};

export default function App() {
  const chartContainerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);

  const [mode, setMode] = useState<"chart" | "scanner">("chart");
  const [tickerInput, setTickerInput] = useState("AAPL");
  const [ticker, setTicker] = useState("AAPL");
  const [interval, setInterval] = useState<Interval>("1d");
  const [meta, setMeta] = useState<AnalyzeResponse["meta"] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [scanMeta, setScanMeta] = useState<ScanResponse["meta"] | null>(null);
  const [scanResults, setScanResults] = useState<ScanResult[]>([]);
  const [scanLoading, setScanLoading] = useState(false);
  const [scanError, setScanError] = useState<string | null>(null);
  const [scanRefresh, setScanRefresh] = useState(0);

  useEffect(() => {
    if (mode !== "chart" || !chartContainerRef.current) {
      return;
    }

    const chart = createChart(chartContainerRef.current, {
      height: 520,
      layout: {
        background: { type: ColorType.Solid, color: "#0B0F14" },
        textColor: "#E6EDF3",
        fontFamily: "IBM Plex Sans, sans-serif",
      },
      grid: {
        vertLines: { color: "#1F2A3A" },
        horzLines: { color: "#1F2A3A" },
      },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: { borderColor: "#1F2A3A" },
      timeScale: { borderColor: "#1F2A3A" },
    });

    const series = chart.addCandlestickSeries({
      upColor: "#2EEA8C",
      downColor: "#FF4D6D",
      borderUpColor: "#2EEA8C",
      borderDownColor: "#FF4D6D",
      wickUpColor: "#2EEA8C",
      wickDownColor: "#FF4D6D",
    });

    chartRef.current = chart;
    seriesRef.current = series;

    const handleResize = () => {
      if (chartContainerRef.current) {
        chart.applyOptions({ width: chartContainerRef.current.clientWidth });
      }
    };

    handleResize();
    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
    };
  }, [mode]);

  useEffect(() => {
    if (mode !== "chart") {
      return;
    }

    const load = async () => {
      if (!seriesRef.current) {
        return;
      }

      setLoading(true);
      setError(null);

      try {
        const response = await fetch(`${API_BASE}/analyze/${ticker}/${interval}`);
        if (!response.ok) {
          throw new Error(`API error: ${response.status}`);
        }

        const data = (await response.json()) as AnalyzeResponse;
        setMeta(data.meta);

        const candles = data.candles.map((candle) => ({
          time: candle.time as UTCTimestamp,
          open: candle.open,
          high: candle.high,
          low: candle.low,
          close: candle.close,
        }));

        seriesRef.current.setData(candles);
        seriesRef.current.setMarkers(
          data.signals.map((signal) => ({
            time: signal.time as UTCTimestamp,
            position: signal.type === "buy" ? "belowBar" : "aboveBar",
            color: signal.type === "buy" ? "#2EEA8C" : "#FF4D6D",
            shape: signal.type === "buy" ? "arrowUp" : "arrowDown",
            text: signal.type === "buy" ? "BUY" : "SELL",
          }))
        );

        chartRef.current?.timeScale().fitContent();
      } catch (err) {
        const message = err instanceof Error ? err.message : "Unknown error";
        setError(message);
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [ticker, interval, mode]);

  useEffect(() => {
    if (mode !== "scanner") {
      return;
    }

    const controller = new AbortController();

    const loadScan = async () => {
      setScanLoading(true);
      setScanError(null);

      try {
        const tickers = DEFAULT_SCAN_TICKERS.join(",");
        const response = await fetch(
          `${API_BASE}/scan/${interval}?tickers=${encodeURIComponent(tickers)}`,
          { signal: controller.signal }
        );

        if (!response.ok) {
          throw new Error(`API error: ${response.status}`);
        }

        const data = (await response.json()) as ScanResponse;
        setScanMeta(data.meta);
        setScanResults(data.results);
      } catch (err) {
        if ((err as Error).name === "AbortError") {
          return;
        }
        const message = err instanceof Error ? err.message : "Unknown error";
        setScanError(message);
      } finally {
        setScanLoading(false);
      }
    };

    loadScan();

    return () => controller.abort();
  }, [mode, interval, scanRefresh]);

  const statusText = mode === "chart" ? (loading ? "Loading signals..." : "Ready") : scanLoading ? "Scanning tickers..." : "Ready";
  const statusError = mode === "chart" ? error : scanError;

  const metaCards = mode === "chart"
    ? [
        { label: "Ticker", value: meta?.ticker ?? ticker },
        { label: "Interval", value: meta?.interval ?? interval },
        { label: "Rows", value: meta?.rows ?? "-" },
      ]
    : [
        { label: "Mode", value: "Scanner" },
        { label: "Interval", value: scanMeta?.interval ?? interval },
        { label: "Tickers", value: scanMeta?.total ?? DEFAULT_SCAN_TICKERS.length },
      ];

  return (
    <div className="app">
      <header className="header">
        <div>
          <p className="eyebrow">AlphaScanner</p>
          <h1 className="title">Consensus Signal Dashboard</h1>
          <p className="subtitle">
            Multi-factor analysis using EMA clouds, SuperTrend, and RSI transitions.
          </p>
        </div>
        <div className="meta">
          {metaCards.map((card) => (
            <div key={card.label}>
              <span className="meta-label">{card.label}</span>
              <span className="meta-value">{card.value}</span>
            </div>
          ))}
        </div>
      </header>

      <section className="controls">
        <div className="control-group">
          <span className="control-label">Mode</span>
          <div className="button-group">
            <button
              className={`button ${mode === "chart" ? "active" : ""}`}
              onClick={() => setMode("chart")}
            >
              Chart
            </button>
            <button
              className={`button ${mode === "scanner" ? "active" : ""}`}
              onClick={() => setMode("scanner")}
            >
              Scanner
            </button>
          </div>
        </div>

        {mode === "chart" ? (
          <div className="control-group">
            <label className="control-label" htmlFor="ticker">
              Ticker
            </label>
            <div className="control-row">
              <input
                id="ticker"
                className="input"
                value={tickerInput}
                onChange={(event) => setTickerInput(event.target.value.toUpperCase())}
              />
              <button
                className="button"
                onClick={() => setTicker(tickerInput.trim() || "AAPL")}
              >
                Load
              </button>
            </div>
          </div>
        ) : null}

        <div className="control-group">
          <span className="control-label">Timeframe</span>
          <div className="button-group">
            {INTERVALS.map((value) => (
              <button
                key={value}
                className={`button ${interval === value ? "active" : ""}`}
                onClick={() => setInterval(value)}
              >
                {value}
              </button>
            ))}
          </div>
        </div>

        {mode === "scanner" ? (
          <div className="control-group">
            <span className="control-label">Universe</span>
            <div className="control-row">
              <span className="chip">{DEFAULT_SCAN_TICKERS.length} tickers</span>
              <button className="button" onClick={() => setScanRefresh((v) => v + 1)}>
                Refresh
              </button>
            </div>
          </div>
        ) : null}

        <div className="status">
          {statusText}
          {statusError ? ` | ${statusError}` : null}
        </div>
      </section>

      {mode === "chart" ? (
        <section className="chart-shell">
          <div ref={chartContainerRef} className="chart" />
        </section>
      ) : (
        <section className="scanner-shell">
          {scanResults.length === 0 && !scanLoading ? (
            <div className="scanner-empty">No scan results yet.</div>
          ) : null}
          <div className="scanner-grid">
            {scanResults.map((result) => {
              const badgeLabel =
                result.status === "error"
                  ? "ERROR"
                  : result.last_signal_type
                    ? result.last_signal_type.toUpperCase()
                    : "NO SIGNAL";
              const badgeClass =
                result.status === "error"
                  ? "error"
                  : result.last_signal_type
                    ? result.last_signal_type
                    : "neutral";
              const lastSignalText = result.last_signal_time
                ? `${badgeLabel} • ${formatTime(result.last_signal_time)}`
                : "None";

              return (
                <article
                  key={result.ticker}
                  className={`scanner-card ${result.status === "error" ? "is-error" : ""}`}
                >
                  <div className="scanner-card-header">
                    <span className="scanner-ticker">{result.ticker}</span>
                    <span className={`badge ${badgeClass}`}>{badgeLabel}</span>
                  </div>
                  <div className="scanner-card-body">
                    <div className="scanner-item">
                      <span className="scanner-label">Latest Close</span>
                      <span className="scanner-value">{formatPrice(result.latest_close)}</span>
                    </div>
                    <div className="scanner-item">
                      <span className="scanner-label">Last Signal</span>
                      <span className="scanner-value">{lastSignalText}</span>
                    </div>
                    <div className="scanner-item">
                      <span className="scanner-label">Signals</span>
                      <span className="scanner-value">{result.signal_count}</span>
                    </div>
                    {result.status === "error" ? (
                      <div className="scanner-error">{result.error ?? "Unknown error"}</div>
                    ) : null}
                  </div>
                </article>
              );
            })}
          </div>
        </section>
      )}
    </div>
  );
}
