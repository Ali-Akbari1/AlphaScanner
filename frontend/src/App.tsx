import { useEffect, useMemo, useRef, useState } from "react";
import {
  ColorType,
  createChart,
  CrosshairMode,
  IChartApi,
  ISeriesApi,
  LineStyle,
  Time,
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
type DrawMode = "none" | "trendline" | "horizontal" | "fibonacci";
type IndicatorKey = "ema9" | "ema21" | "ema50" | "ema200" | "supertrend" | "rsi";

const INDICATOR_OPTIONS: { key: IndicatorKey; label: string; color: string }[] = [
  { key: "ema9", label: "EMA 9", color: "#3EE7F7" },
  { key: "ema21", label: "EMA 21", color: "#F6C453" },
  { key: "ema50", label: "EMA 50", color: "#8BD3FF" },
  { key: "ema200", label: "EMA 200", color: "#FF9B72" },
  { key: "supertrend", label: "SuperTrend", color: "#2EEA8C" },
  { key: "rsi", label: "RSI", color: "#9FB0C3" },
];

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

type IndicatorVisibility = Record<IndicatorKey, boolean>;

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
  bias_type?: "buy" | "sell" | "neutral" | null;
  bias_time?: number | null;
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

const timeToSeconds = (time: Time) => {
  if (typeof time === "number") {
    return time;
  }
  return Math.floor(Date.UTC(time.year, time.month - 1, time.day) / 1000);
};

export default function App() {
  const chartContainerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const indicatorSeriesRef = useRef<Record<IndicatorKey, ISeriesApi<"Line"> | null>>({
    ema9: null,
    ema21: null,
    ema50: null,
    ema200: null,
    supertrend: null,
    rsi: null,
  });
  const drawingsRef = useRef({
    trendlines: [] as ISeriesApi<"Line">[],
    horizontals: [] as ReturnType<ISeriesApi<"Candlestick">["createPriceLine"]>[],
    fibs: [] as ReturnType<ISeriesApi<"Candlestick">["createPriceLine"]>[],
  });
  const rsiLinesRef = useRef<ReturnType<ISeriesApi<"Line">["createPriceLine"]>[]>([]);
  const drawPointsRef = useRef<{ time: Time; price: number }[]>([]);

  const [mode, setMode] = useState<"chart" | "scanner">("chart");
  const [tickerInput, setTickerInput] = useState("AAPL");
  const [ticker, setTicker] = useState("AAPL");
  const [interval, setInterval] = useState<Interval>("1d");
  const [indicatorVisibility, setIndicatorVisibility] = useState<IndicatorVisibility>({
    ema9: true,
    ema21: true,
    ema50: true,
    ema200: true,
    supertrend: true,
    rsi: true,
  });
  const [drawMode, setDrawMode] = useState<DrawMode>("none");
  const [meta, setMeta] = useState<AnalyzeResponse["meta"] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [scanMeta, setScanMeta] = useState<ScanResponse["meta"] | null>(null);
  const [scanResults, setScanResults] = useState<ScanResult[]>([]);
  const [scanLoading, setScanLoading] = useState(false);
  const [scanError, setScanError] = useState<string | null>(null);
  const [scanRefresh, setScanRefresh] = useState(0);
  const [scanTickersInput, setScanTickersInput] = useState(
    DEFAULT_SCAN_TICKERS.join(", ")
  );

  const parsedScanTickers = useMemo(
    () =>
      scanTickersInput
        .split(/[\s,]+/)
        .map((tickerValue) => tickerValue.trim().toUpperCase())
        .filter(Boolean),
    [scanTickersInput]
  );

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

    const indicatorSeries: Record<IndicatorKey, ISeriesApi<"Line">> = {
      ema9: chart.addLineSeries({
        color: INDICATOR_OPTIONS.find((option) => option.key === "ema9")?.color ?? "#3EE7F7",
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
      }),
      ema21: chart.addLineSeries({
        color: INDICATOR_OPTIONS.find((option) => option.key === "ema21")?.color ?? "#F6C453",
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
      }),
      ema50: chart.addLineSeries({
        color: INDICATOR_OPTIONS.find((option) => option.key === "ema50")?.color ?? "#8BD3FF",
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
      }),
      ema200: chart.addLineSeries({
        color: INDICATOR_OPTIONS.find((option) => option.key === "ema200")?.color ?? "#FF9B72",
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
      }),
      supertrend: chart.addLineSeries({
        color: INDICATOR_OPTIONS.find((option) => option.key === "supertrend")?.color ?? "#2EEA8C",
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
      }),
      rsi: chart.addLineSeries({
        color: INDICATOR_OPTIONS.find((option) => option.key === "rsi")?.color ?? "#9FB0C3",
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
        priceScaleId: "rsi",
      }),
    };

    chart.priceScale("rsi").applyOptions({
      scaleMargins: { top: 0.75, bottom: 0.05 },
      borderColor: "#1F2A3A",
    });

    rsiLinesRef.current = [
      indicatorSeries.rsi.createPriceLine({
        price: 70,
        color: "#F6C453",
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: true,
        title: "RSI 70",
      }),
      indicatorSeries.rsi.createPriceLine({
        price: 30,
        color: "#FF4D6D",
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: true,
        title: "RSI 30",
      }),
      indicatorSeries.rsi.createPriceLine({
        price: 50,
        color: "#9FB0C3",
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: false,
        title: "RSI 50",
      }),
    ];

    (Object.keys(indicatorSeries) as IndicatorKey[]).forEach((key) => {
      indicatorSeries[key].applyOptions({ visible: indicatorVisibility[key] });
    });
    indicatorSeriesRef.current = indicatorSeries;

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
      indicatorSeriesRef.current = {
        ema9: null,
        ema21: null,
        ema50: null,
        ema200: null,
        supertrend: null,
        rsi: null,
      };
      rsiLinesRef.current = [];
      drawingsRef.current = { trendlines: [], horizontals: [], fibs: [] };
      drawPointsRef.current = [];
    };
  }, [mode]);

  useEffect(() => {
    if (mode !== "chart") {
      return;
    }
    (Object.keys(indicatorSeriesRef.current) as IndicatorKey[]).forEach((key) => {
      const series = indicatorSeriesRef.current[key];
      if (series) {
        series.applyOptions({ visible: indicatorVisibility[key] });
      }
    });
  }, [indicatorVisibility, mode]);

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
          data.signals
            .map((signal) => ({
              time: signal.time as UTCTimestamp,
              position: signal.type === "buy" ? "belowBar" : "aboveBar",
              color: signal.type === "buy" ? "#2EEA8C" : "#FF4D6D",
              shape: signal.type === "buy" ? "arrowUp" : "arrowDown",
              text: signal.type === "buy" ? "BUY" : "SELL",
            }))
            .sort((a, b) => timeToSeconds(a.time) - timeToSeconds(b.time))
        );

        const indicatorRows = data.indicators.map((indicator) => ({
          time: indicator.time as UTCTimestamp,
          ema9: indicator.ema9,
          ema21: indicator.ema21,
          ema50: indicator.ema50,
          ema200: indicator.ema200,
          supertrend: indicator.supertrend,
          rsi: indicator.rsi,
        }));

        const buildLineData = (values: { time: Time; value: number }[]) =>
          values
            .filter((point) => Number.isFinite(point.value))
            .sort((a, b) => timeToSeconds(a.time) - timeToSeconds(b.time));

        indicatorSeriesRef.current.ema9?.setData(
          buildLineData(indicatorRows.map((row) => ({ time: row.time, value: row.ema9 })))
        );
        indicatorSeriesRef.current.ema21?.setData(
          buildLineData(indicatorRows.map((row) => ({ time: row.time, value: row.ema21 })))
        );
        indicatorSeriesRef.current.ema50?.setData(
          buildLineData(indicatorRows.map((row) => ({ time: row.time, value: row.ema50 })))
        );
        indicatorSeriesRef.current.ema200?.setData(
          buildLineData(indicatorRows.map((row) => ({ time: row.time, value: row.ema200 })))
        );
        indicatorSeriesRef.current.supertrend?.setData(
          buildLineData(indicatorRows.map((row) => ({ time: row.time, value: row.supertrend })))
        );
        indicatorSeriesRef.current.rsi?.setData(
          buildLineData(indicatorRows.map((row) => ({ time: row.time, value: row.rsi })))
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
        const tickers = parsedScanTickers.length > 0
          ? parsedScanTickers.join(",")
          : DEFAULT_SCAN_TICKERS.join(",");
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

  useEffect(() => {
    if (drawMode === "none") {
      drawPointsRef.current = [];
    }
  }, [drawMode]);

  useEffect(() => {
    if (mode !== "chart") {
      return;
    }
    const chart = chartRef.current;
    const series = seriesRef.current;
    if (!chart || !series) {
      return;
    }

    const handleClick = (param: any) => {
      if (drawMode === "none") {
        return;
      }
      if (!param.point) {
        return;
      }

      const time =
        param.time ?? (chart.timeScale().coordinateToTime(param.point.x) as Time | null);
      if (time === null || time === undefined) {
        return;
      }

      const price = series.coordinateToPrice(param.point.y);
      if (price === null) {
        return;
      }

      if (drawMode === "horizontal") {
        const line = series.createPriceLine({
          price,
          color: "#3EE7F7",
          lineWidth: 2,
          lineStyle: LineStyle.Solid,
          axisLabelVisible: true,
          title: "H",
        });
        drawingsRef.current.horizontals.push(line);
        return;
      }

      drawPointsRef.current = [...drawPointsRef.current, { time, price }];
      if (drawPointsRef.current.length < 2) {
        return;
      }

      const [pointA, pointB] = drawPointsRef.current.slice(-2);
      drawPointsRef.current = [];
      const [start, end] =
        timeToSeconds(pointA.time) <= timeToSeconds(pointB.time) ? [pointA, pointB] : [pointB, pointA];

      if (drawMode === "trendline") {
        const lineSeries = chart.addLineSeries({
          color: "#F6C453",
          lineWidth: 2,
          priceLineVisible: false,
          lastValueVisible: false,
        });
        lineSeries.setData([
          { time: start.time, value: start.price },
          { time: end.time, value: end.price },
        ]);
        drawingsRef.current.trendlines.push(lineSeries);
        return;
      }

      if (drawMode === "fibonacci") {
        const diff = end.price - start.price;
        const levels = [
          { level: 0, label: "0" },
          { level: 0.236, label: "0.236" },
          { level: 0.382, label: "0.382" },
          { level: 0.5, label: "0.5" },
          { level: 0.618, label: "0.618" },
          { level: 0.786, label: "0.786" },
          { level: 1, label: "1" },
        ];

        levels.forEach((item) => {
          const line = series.createPriceLine({
            price: start.price + diff * item.level,
            color: item.level === 0 || item.level === 1 ? "#F6C453" : "#9FB0C3",
            lineWidth: item.level === 0 || item.level === 1 ? 2 : 1,
            lineStyle: item.level === 0 || item.level === 1 ? LineStyle.Solid : LineStyle.Dashed,
            axisLabelVisible: true,
            title: `Fib ${item.label}`,
          });
          drawingsRef.current.fibs.push(line);
        });
      }
    };

    chart.subscribeClick(handleClick);
    return () => chart.unsubscribeClick(handleClick);
  }, [drawMode, mode]);

  const toggleIndicator = (key: IndicatorKey) => {
    setIndicatorVisibility((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  const clearDrawings = () => {
    const chart = chartRef.current;
    const series = seriesRef.current;
    if (!chart || !series) {
      return;
    }

    drawingsRef.current.trendlines.forEach((line) => chart.removeSeries(line));
    drawingsRef.current.horizontals.forEach((line) => series.removePriceLine(line));
    drawingsRef.current.fibs.forEach((line) => series.removePriceLine(line));
    drawingsRef.current = { trendlines: [], horizontals: [], fibs: [] };
    drawPointsRef.current = [];
  };

  const drawHint =
    mode === "chart" && drawMode !== "none"
      ? drawMode === "horizontal"
        ? "Draw: click once for horizontal line"
        : drawMode === "trendline"
          ? "Draw: click two points for trendline"
          : "Draw: click two points for Fibonacci"
      : null;

  const statusText =
    mode === "chart" ? (loading ? "Loading signals..." : "Ready") : scanLoading ? "Scanning tickers..." : "Ready";
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
        {
          label: "Tickers",
          value: scanMeta?.total ?? (parsedScanTickers.length || DEFAULT_SCAN_TICKERS.length),
        },
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

        {mode === "chart" ? (
          <div className="control-group">
            <span className="control-label">Indicators</span>
            <div className="button-group">
              {INDICATOR_OPTIONS.map((option) => (
                <button
                  key={option.key}
                  className={`button ${indicatorVisibility[option.key] ? "active" : ""}`}
                  onClick={() => toggleIndicator(option.key)}
                >
                  {option.label}
                </button>
              ))}
            </div>
          </div>
        ) : null}

        {mode === "chart" ? (
          <div className="control-group">
            <span className="control-label">Draw</span>
            <div className="button-group">
              {[
                { value: "none" as DrawMode, label: "Off" },
                { value: "trendline" as DrawMode, label: "Trendline" },
                { value: "horizontal" as DrawMode, label: "Horizontal" },
                { value: "fibonacci" as DrawMode, label: "Fibonacci" },
              ].map((option) => (
                <button
                  key={option.value}
                  className={`button ${drawMode === option.value ? "active" : ""}`}
                  onClick={() => setDrawMode(option.value)}
                >
                  {option.label}
                </button>
              ))}
            </div>
            <div className="control-row">
              <button className="button" onClick={clearDrawings}>
                Clear
              </button>
            </div>
          </div>
        ) : null}

        {mode === "scanner" ? (
          <div className="control-group">
            <label className="control-label" htmlFor="scan-tickers">
              Tickers
            </label>
            <textarea
              id="scan-tickers"
              className="input input-area"
              value={scanTickersInput}
              onChange={(event) => setScanTickersInput(event.target.value)}
              placeholder="AAPL, MSFT, NVDA..."
            />
            <div className="control-row">
              <span className="chip">
                {(parsedScanTickers.length || DEFAULT_SCAN_TICKERS.length)} tickers
              </span>
              <button
                className="button"
                onClick={() => setScanTickersInput(DEFAULT_SCAN_TICKERS.join(", "))}
              >
                Defaults
              </button>
              <button className="button" onClick={() => setScanRefresh((v) => v + 1)}>
                Scan
              </button>
            </div>
          </div>
        ) : null}

        <div className="status">
          {statusText}
          {drawHint ? ` | ${drawHint}` : null}
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
              const biasType = result.bias_type ?? null;
              const signalType =
                result.last_signal_type ??
                (biasType && biasType !== "neutral" ? biasType : null);
              const badgeLabel =
                result.status === "error"
                  ? "ERROR"
                  : signalType
                    ? signalType.toUpperCase()
                    : biasType === "neutral"
                      ? "NEUTRAL"
                      : "NO SIGNAL";
              const badgeClass =
                result.status === "error"
                  ? "error"
                  : signalType
                    ? signalType
                    : "neutral";
              const lastSignalText = result.last_signal_time
                ? `${badgeLabel} • ${formatTime(result.last_signal_time)}`
                : biasType
                  ? `BIAS ${biasType.toUpperCase()} • ${formatTime(result.bias_time ?? null)}`
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
