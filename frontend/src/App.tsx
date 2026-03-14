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
const DEFAULT_TICKER = "BTC-USD";
const SETTINGS_KEY = "alphascanner:settings";
const DRAWINGS_KEY = "alphascanner:drawings";
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
const FIB_LEVELS = [
  { level: 0, label: "0" },
  { level: 0.236, label: "0.236" },
  { level: 0.382, label: "0.382" },
  { level: 0.5, label: "0.5" },
  { level: 0.618, label: "0.618" },
  { level: 0.786, label: "0.786" },
  { level: 1, label: "1" },
];

type Interval = (typeof INTERVALS)[number];
type DrawMode = "none" | "trendline" | "horizontal" | "fibonacci" | "delete";
type IndicatorKey =
  | "ema9"
  | "ema21"
  | "ema50"
  | "ema200"
  | "supertrend"
  | "rsi"
  | "vwap"
  | "bb_upper"
  | "bb_middle"
  | "bb_lower"
  | "macd"
  | "macd_signal";

type IndicatorGroup = {
  label: string;
  keys: IndicatorKey[];
};

const INDICATOR_COLORS: Record<IndicatorKey, string> = {
  ema9: "#3EE7F7",
  ema21: "#F6C453",
  ema50: "#8BD3FF",
  ema200: "#FF9B72",
  supertrend: "#2EEA8C",
  rsi: "#9FB0C3",
  vwap: "#B6FF5C",
  bb_upper: "#6A7A8C",
  bb_middle: "#9FB0C3",
  bb_lower: "#6A7A8C",
  macd: "#F6C453",
  macd_signal: "#FF9B72",
};

const INDICATOR_GROUPS: IndicatorGroup[] = [
  { label: "EMA 9", keys: ["ema9"] },
  { label: "EMA 21", keys: ["ema21"] },
  { label: "EMA 50", keys: ["ema50"] },
  { label: "EMA 200", keys: ["ema200"] },
  { label: "SuperTrend", keys: ["supertrend"] },
  { label: "VWAP", keys: ["vwap"] },
  { label: "Bollinger Bands", keys: ["bb_upper", "bb_middle", "bb_lower"] },
  { label: "RSI", keys: ["rsi"] },
  { label: "MACD", keys: ["macd", "macd_signal"] },
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
  strength: "strong" | "weak";
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
  vwap: number;
  bb_upper: number;
  bb_middle: number;
  bb_lower: number;
  macd: number;
  macd_signal: number;
};

type IndicatorVisibility = Record<IndicatorKey, boolean>;

type DrawPoint = {
  time: UTCTimestamp;
  price: number;
};

type DrawingsState = {
  trendlines: { start: DrawPoint; end: DrawPoint }[];
  horizontals: { price: number }[];
  fibs: { start: DrawPoint; end: DrawPoint }[];
};

type StoredSettings = {
  mode: "chart" | "scanner";
  ticker: string;
  tickerInput: string;
  interval: Interval;
  indicatorVisibility: Partial<IndicatorVisibility>;
  scanTickersInput: string;
  drawMode?: DrawMode;
  showWeakSignals?: boolean;
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

const DEFAULT_INDICATOR_VISIBILITY: IndicatorVisibility = {
  ema9: true,
  ema21: true,
  ema50: true,
  ema200: true,
  supertrend: true,
  rsi: true,
  vwap: true,
  bb_upper: false,
  bb_middle: false,
  bb_lower: false,
  macd: false,
  macd_signal: false,
};

const loadStoredJson = <T,>(key: string, fallback: T): T => {
  if (typeof window === "undefined") {
    return fallback;
  }
  try {
    const raw = window.localStorage.getItem(key);
    if (!raw) {
      return fallback;
    }
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
};

const normalizeIndicatorVisibility = (
  stored: Partial<IndicatorVisibility> | undefined
): IndicatorVisibility => ({
  ...DEFAULT_INDICATOR_VISIBILITY,
  ...(stored ?? {}),
});

const timeToSeconds = (time: Time) => {
  if (typeof time === "number") {
    return time;
  }
  return Math.floor(Date.UTC(time.year, time.month - 1, time.day) / 1000);
};

const toTimestamp = (time: Time): UTCTimestamp => timeToSeconds(time) as UTCTimestamp;

const ensureDistinctTimes = (start: DrawPoint, end: DrawPoint) => {
  if (start.time === end.time) {
    return { start, end: { ...end, time: (end.time + 1) as UTCTimestamp } };
  }
  return { start, end };
};

const medianStepSeconds = (times: UTCTimestamp[]) => {
  if (times.length < 2) {
    return 60 * 60 * 24;
  }
  const diffs = times
    .slice(1)
    .map((time, idx) => time - times[idx])
    .filter((diff) => diff > 0)
    .sort((a, b) => a - b);
  if (diffs.length === 0) {
    return 60 * 60 * 24;
  }
  return diffs[Math.floor(diffs.length / 2)];
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
    vwap: null,
    bb_upper: null,
    bb_middle: null,
    bb_lower: null,
    macd: null,
    macd_signal: null,
  });
  const drawingsRef = useRef({
    trendlines: [] as ISeriesApi<"Line">[],
    horizontals: [] as ReturnType<ISeriesApi<"Candlestick">["createPriceLine"]>[],
    fibs: [] as ReturnType<ISeriesApi<"Candlestick">["createPriceLine"]>[],
  });
  const rsiLinesRef = useRef<ReturnType<ISeriesApi<"Line">["createPriceLine"]>[]>([]);
  const drawPointsRef = useRef<DrawPoint[]>([]);
  const previewRef = useRef<{
    horizontal: ReturnType<ISeriesApi<"Candlestick">["createPriceLine"]> | null;
    trendline: ISeriesApi<"Line"> | null;
    fibs: ReturnType<ISeriesApi<"Candlestick">["createPriceLine"]>[];
  }>({
    horizontal: null,
    trendline: null,
    fibs: [],
  });
  const previewFrameRef = useRef<number | null>(null);
  const pendingMoveRef = useRef<any>(null);
  const previewCacheRef = useRef<{
    horizontal?: number;
    trendline?: { start: DrawPoint; end: DrawPoint };
    fibEnd?: number;
  }>({});
  const candlesRef = useRef<{ times: UTCTimestamp[]; step: number } | null>(null);
  const hoverRef = useRef<{
    kind: "trendline" | "horizontal" | "fib";
    index: number;
    series?: ISeriesApi<"Line">;
    line?: ReturnType<ISeriesApi<"Candlestick">["createPriceLine"]>;
  } | null>(null);

  const storedSettings = useMemo(
    () => loadStoredJson<StoredSettings | null>(SETTINGS_KEY, null),
    []
  );
  const storedDrawings = useMemo(
    () =>
      loadStoredJson<DrawingsState>(DRAWINGS_KEY, {
        trendlines: [],
        horizontals: [],
        fibs: [],
      }),
    []
  );

  const [mode, setMode] = useState<"chart" | "scanner">(
    storedSettings?.mode ?? "chart"
  );
  const [tickerInput, setTickerInput] = useState(
    storedSettings?.tickerInput ?? DEFAULT_TICKER
  );
  const [ticker, setTicker] = useState(storedSettings?.ticker ?? DEFAULT_TICKER);
  const [interval, setInterval] = useState<Interval>(
    storedSettings?.interval ?? "1d"
  );
  const [indicatorVisibility, setIndicatorVisibility] = useState<IndicatorVisibility>(
    normalizeIndicatorVisibility(storedSettings?.indicatorVisibility)
  );
  const [drawMode, setDrawMode] = useState<DrawMode>(
    storedSettings?.drawMode ?? "none"
  );
  const [drawings, setDrawings] = useState<DrawingsState>(storedDrawings);
  const [showWeakSignals, setShowWeakSignals] = useState(
    storedSettings?.showWeakSignals ?? false
  );
  const [meta, setMeta] = useState<AnalyzeResponse["meta"] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [scanMeta, setScanMeta] = useState<ScanResponse["meta"] | null>(null);
  const [scanResults, setScanResults] = useState<ScanResult[]>([]);
  const [scanLoading, setScanLoading] = useState(false);
  const [scanError, setScanError] = useState<string | null>(null);
  const [scanRefresh, setScanRefresh] = useState(0);
  const [scanTickersInput, setScanTickersInput] = useState(
    storedSettings?.scanTickersInput ?? DEFAULT_SCAN_TICKERS.join(", ")
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
    if (typeof window === "undefined") {
      return;
    }
    const settings: StoredSettings = {
      mode,
      ticker,
      tickerInput,
      interval,
      indicatorVisibility,
      scanTickersInput,
      drawMode,
      showWeakSignals,
    };
    window.localStorage.setItem(SETTINGS_KEY, JSON.stringify(settings));
  }, [
    mode,
    ticker,
    tickerInput,
    interval,
    indicatorVisibility,
    scanTickersInput,
    drawMode,
    showWeakSignals,
  ]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    window.localStorage.setItem(DRAWINGS_KEY, JSON.stringify(drawings));
  }, [drawings]);

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
        color: INDICATOR_COLORS.ema9,
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
      }),
      ema21: chart.addLineSeries({
        color: INDICATOR_COLORS.ema21,
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
      }),
      ema50: chart.addLineSeries({
        color: INDICATOR_COLORS.ema50,
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
      }),
      ema200: chart.addLineSeries({
        color: INDICATOR_COLORS.ema200,
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
      }),
      supertrend: chart.addLineSeries({
        color: INDICATOR_COLORS.supertrend,
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
      }),
      vwap: chart.addLineSeries({
        color: INDICATOR_COLORS.vwap,
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
      }),
      bb_upper: chart.addLineSeries({
        color: INDICATOR_COLORS.bb_upper,
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        priceLineVisible: false,
        lastValueVisible: false,
      }),
      bb_middle: chart.addLineSeries({
        color: INDICATOR_COLORS.bb_middle,
        lineWidth: 1,
        lineStyle: LineStyle.Dotted,
        priceLineVisible: false,
        lastValueVisible: false,
      }),
      bb_lower: chart.addLineSeries({
        color: INDICATOR_COLORS.bb_lower,
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        priceLineVisible: false,
        lastValueVisible: false,
      }),
      rsi: chart.addLineSeries({
        color: INDICATOR_COLORS.rsi,
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
        priceScaleId: "rsi",
      }),
      macd: chart.addLineSeries({
        color: INDICATOR_COLORS.macd,
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
        priceScaleId: "macd",
      }),
      macd_signal: chart.addLineSeries({
        color: INDICATOR_COLORS.macd_signal,
        lineWidth: 2,
        lineStyle: LineStyle.Dashed,
        priceLineVisible: false,
        lastValueVisible: false,
        priceScaleId: "macd",
      }),
    };

    chart.priceScale("rsi").applyOptions({
      scaleMargins: { top: 0.75, bottom: 0.05 },
      borderColor: "#1F2A3A",
    });
    chart.priceScale("macd").applyOptions({
      scaleMargins: { top: 0.55, bottom: 0.25 },
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
        vwap: null,
        bb_upper: null,
        bb_middle: null,
        bb_lower: null,
        macd: null,
        macd_signal: null,
      };
      rsiLinesRef.current = [];
      drawingsRef.current = { trendlines: [], horizontals: [], fibs: [] };
      drawPointsRef.current = [];
      previewRef.current = { horizontal: null, trendline: null, fibs: [] };
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
    const rsiVisible = indicatorVisibility.rsi;
    rsiLinesRef.current.forEach((line) =>
      line.applyOptions({ lineVisible: rsiVisible, axisLabelVisible: rsiVisible })
    );
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
        const response = await fetch(
          `${API_BASE}/analyze/${ticker}/${interval}?weak=${showWeakSignals ? "true" : "false"}`
        );
        if (!response.ok) {
          let detail = "";
          try {
            const payload = await response.json();
            if (payload && typeof payload.detail === "string") {
              detail = payload.detail;
            }
          } catch {
            detail = "";
          }
          throw new Error(detail || `API error: ${response.status}`);
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
        const candleTimes = candles.map((candle) => candle.time);
        candlesRef.current = {
          times: candleTimes,
          step: medianStepSeconds(candleTimes),
        };
        seriesRef.current.setMarkers(
          data.signals
            .map((signal) => ({
              time: signal.time as UTCTimestamp,
              position: signal.type === "buy" ? "belowBar" : "aboveBar",
              color:
                signal.type === "buy"
                  ? signal.strength === "weak"
                    ? "rgba(46, 234, 140, 0.55)"
                    : "#2EEA8C"
                  : signal.strength === "weak"
                    ? "rgba(255, 77, 109, 0.55)"
                    : "#FF4D6D",
              shape: signal.type === "buy" ? "arrowUp" : "arrowDown",
              text:
                signal.strength === "weak"
                  ? signal.type === "buy"
                    ? "W BUY"
                    : "W SELL"
                  : signal.type === "buy"
                    ? "BUY"
                    : "SELL",
              size: signal.strength === "weak" ? 1 : 2,
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
          vwap: indicator.vwap,
          bb_upper: indicator.bb_upper,
          bb_middle: indicator.bb_middle,
          bb_lower: indicator.bb_lower,
          macd: indicator.macd,
          macd_signal: indicator.macd_signal,
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
        indicatorSeriesRef.current.vwap?.setData(
          buildLineData(indicatorRows.map((row) => ({ time: row.time, value: row.vwap })))
        );
        indicatorSeriesRef.current.bb_upper?.setData(
          buildLineData(indicatorRows.map((row) => ({ time: row.time, value: row.bb_upper })))
        );
        indicatorSeriesRef.current.bb_middle?.setData(
          buildLineData(indicatorRows.map((row) => ({ time: row.time, value: row.bb_middle })))
        );
        indicatorSeriesRef.current.bb_lower?.setData(
          buildLineData(indicatorRows.map((row) => ({ time: row.time, value: row.bb_lower })))
        );
        indicatorSeriesRef.current.rsi?.setData(
          buildLineData(indicatorRows.map((row) => ({ time: row.time, value: row.rsi })))
        );
        indicatorSeriesRef.current.macd?.setData(
          buildLineData(indicatorRows.map((row) => ({ time: row.time, value: row.macd })))
        );
        indicatorSeriesRef.current.macd_signal?.setData(
          buildLineData(indicatorRows.map((row) => ({ time: row.time, value: row.macd_signal })))
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
  }, [ticker, interval, mode, showWeakSignals]);

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
          let detail = "";
          try {
            const payload = await response.json();
            if (payload && typeof payload.detail === "string") {
              detail = payload.detail;
            }
          } catch {
            detail = "";
          }
          throw new Error(detail || `API error: ${response.status}`);
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
    drawPointsRef.current = [];
    if (previewFrameRef.current !== null) {
      cancelAnimationFrame(previewFrameRef.current);
      previewFrameRef.current = null;
    }
    pendingMoveRef.current = null;
    previewCacheRef.current = {};
    const series = seriesRef.current;
    const chart = chartRef.current;
    if (series && chart) {
      if (hoverRef.current?.series) {
        chart.removeSeries(hoverRef.current.series);
      }
      if (hoverRef.current?.line) {
        series.removePriceLine(hoverRef.current.line);
      }
      hoverRef.current = null;
      if (previewRef.current.horizontal) {
        series.removePriceLine(previewRef.current.horizontal);
        previewRef.current.horizontal = null;
      }
      if (previewRef.current.trendline) {
        chart.removeSeries(previewRef.current.trendline);
        previewRef.current.trendline = null;
      }
      previewRef.current.fibs.forEach((line) => series.removePriceLine(line));
      previewRef.current.fibs = [];
    }
  }, [drawMode]);

  const renderDrawings = (state: DrawingsState) => {
    const chart = chartRef.current;
    const series = seriesRef.current;
    if (!chart || !series) {
      return;
    }

    drawingsRef.current.trendlines.forEach((line) => chart.removeSeries(line));
    drawingsRef.current.horizontals.forEach((line) => series.removePriceLine(line));
    drawingsRef.current.fibs.forEach((line) => series.removePriceLine(line));
    drawingsRef.current = { trendlines: [], horizontals: [], fibs: [] };

    state.trendlines.forEach((trend) => {
      const normalized = ensureDistinctTimes(trend.start, trend.end);
      const lineSeries = chart.addLineSeries({
        color: "#F6C453",
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
      });
      lineSeries.setData([
        { time: normalized.start.time, value: normalized.start.price },
        { time: normalized.end.time, value: normalized.end.price },
      ]);
      drawingsRef.current.trendlines.push(lineSeries);
    });

    state.horizontals.forEach((horizontal) => {
      const line = series.createPriceLine({
        price: horizontal.price,
        color: "#3EE7F7",
        lineWidth: 2,
        lineStyle: LineStyle.Solid,
        axisLabelVisible: true,
        title: "H",
      });
      drawingsRef.current.horizontals.push(line);
    });

    state.fibs.forEach((fib) => {
      const diff = fib.end.price - fib.start.price;
      FIB_LEVELS.forEach((item) => {
        const line = series.createPriceLine({
          price: fib.start.price + diff * item.level,
          color: item.level === 0 || item.level === 1 ? "#F6C453" : "#9FB0C3",
          lineWidth: item.level === 0 || item.level === 1 ? 2 : 1,
          lineStyle: item.level === 0 || item.level === 1 ? LineStyle.Solid : LineStyle.Dashed,
          axisLabelVisible: true,
          title: `Fib ${item.label}`,
        });
        drawingsRef.current.fibs.push(line);
      });
    });
  };

  useEffect(() => {
    if (mode !== "chart") {
      return;
    }
    renderDrawings(drawings);
  }, [drawings, mode]);

  useEffect(() => {
    if (mode !== "chart") {
      return;
    }
    const chart = chartRef.current;
    const series = seriesRef.current;
    if (!chart || !series) {
      return;
    }

    const renderPreview = (param: any) => {
      try {
        if (drawMode === "none" || drawMode === "delete") {
          return;
        }
        if (!param.point) {
          return;
        }

        const time =
          param.time ??
          (chart.timeScale().coordinateToTime(param.point.x) as Time | null) ??
          null;
        let resolvedTime = time;
        if (resolvedTime === null || resolvedTime === undefined) {
          const logical = chart.timeScale().coordinateToLogical(param.point.x);
          const ref = candlesRef.current;
          if (logical !== null && ref && ref.times.length > 0) {
            const base = ref.times[0];
            resolvedTime = (base + Math.round(logical) * ref.step) as UTCTimestamp;
          }
        }
        if (resolvedTime === null || resolvedTime === undefined) {
          return;
        }

        const price = series.coordinateToPrice(param.point.y);
        if (price === null || !Number.isFinite(price)) {
          return;
        }

        if (drawMode === "horizontal") {
          const lastPrice = previewCacheRef.current.horizontal;
          if (lastPrice !== undefined && Math.abs(lastPrice - price) < 1e-6) {
            return;
          }
          if (!previewRef.current.horizontal) {
            previewRef.current.horizontal = series.createPriceLine({
              price,
              color: "#3EE7F7",
              lineWidth: 1,
              lineStyle: LineStyle.Dashed,
              axisLabelVisible: true,
              title: "H",
            });
          } else {
            previewRef.current.horizontal.applyOptions({ price });
          }
          previewCacheRef.current.horizontal = price;
        } else if (previewRef.current.horizontal) {
          series.removePriceLine(previewRef.current.horizontal);
          previewRef.current.horizontal = null;
          delete previewCacheRef.current.horizontal;
        }

        if (drawMode === "trendline" && drawPointsRef.current.length === 1) {
          const start = drawPointsRef.current[0];
          const end: DrawPoint = { time: toTimestamp(resolvedTime), price };
          const ordered = start.time <= end.time ? [start, end] : [end, start];
          const normalized = ensureDistinctTimes(ordered[0], ordered[1]);
          const lastTrend = previewCacheRef.current.trendline;
          if (
            lastTrend &&
            lastTrend.start.time === normalized.start.time &&
            lastTrend.end.time === normalized.end.time &&
            Math.abs(lastTrend.start.price - normalized.start.price) < 1e-6 &&
            Math.abs(lastTrend.end.price - normalized.end.price) < 1e-6
          ) {
            return;
          }
          if (!previewRef.current.trendline) {
            previewRef.current.trendline = chart.addLineSeries({
              color: "#F6C453",
              lineWidth: 2,
              lineStyle: LineStyle.Dashed,
              priceLineVisible: false,
              lastValueVisible: false,
            });
          }
          previewRef.current.trendline.setData([
            { time: normalized.start.time, value: normalized.start.price },
            { time: normalized.end.time, value: normalized.end.price },
          ]);
          previewCacheRef.current.trendline = { start: normalized.start, end: normalized.end };
        } else if (previewRef.current.trendline) {
          chart.removeSeries(previewRef.current.trendline);
          previewRef.current.trendline = null;
          delete previewCacheRef.current.trendline;
        }

        if (drawMode === "fibonacci" && drawPointsRef.current.length === 1) {
          const start = drawPointsRef.current[0];
          const end: DrawPoint = { time: toTimestamp(resolvedTime), price };
          const diff = end.price - start.price;
          const lastEnd = previewCacheRef.current.fibEnd;
          if (lastEnd !== undefined && Math.abs(lastEnd - end.price) < 1e-6) {
            return;
          }
          if (previewRef.current.fibs.length === 0) {
            previewRef.current.fibs = FIB_LEVELS.map((item) =>
              series.createPriceLine({
                price: start.price + diff * item.level,
                color: item.level === 0 || item.level === 1 ? "#F6C453" : "#9FB0C3",
                lineWidth: item.level === 0 || item.level === 1 ? 2 : 1,
                lineStyle: LineStyle.Dashed,
                axisLabelVisible: true,
                title: `Fib ${item.label}`,
              })
            );
          } else {
            previewRef.current.fibs.forEach((line, idx) => {
              const item = FIB_LEVELS[idx];
              line.applyOptions({ price: start.price + diff * item.level });
            });
          }
          previewCacheRef.current.fibEnd = end.price;
        } else if (previewRef.current.fibs.length > 0) {
          previewRef.current.fibs.forEach((line) => series.removePriceLine(line));
          previewRef.current.fibs = [];
          delete previewCacheRef.current.fibEnd;
        }
      } catch (err) {
        console.warn("Preview draw error", err);
      }
    };

    const clearHover = () => {
      if (!hoverRef.current) {
        return;
      }
      if (hoverRef.current.series) {
        chart.removeSeries(hoverRef.current.series);
      }
      if (hoverRef.current.line) {
        series.removePriceLine(hoverRef.current.line);
      }
      hoverRef.current = null;
    };

    const renderDeleteHover = (param: any) => {
      if (drawMode !== "delete") {
        return;
      }
      if (!param.point) {
        clearHover();
        return;
      }
      const target = findDeletionTarget(param.point.x, param.point.y);
      if (!target) {
        clearHover();
        return;
      }
      if (hoverRef.current && hoverRef.current.kind === target.kind && hoverRef.current.index === target.index) {
        return;
      }
      clearHover();
      if (target.kind === "trendline") {
        const trend = drawings.trendlines[target.index];
        if (!trend) {
          return;
        }
        const normalized = ensureDistinctTimes(trend.start, trend.end);
        const highlight = chart.addLineSeries({
          color: "#FFD166",
          lineWidth: 3,
          priceLineVisible: false,
          lastValueVisible: false,
        });
        highlight.setData([
          { time: normalized.start.time, value: normalized.start.price },
          { time: normalized.end.time, value: normalized.end.price },
        ]);
        hoverRef.current = { kind: "trendline", index: target.index, series: highlight };
        return;
      }

      if (target.kind === "horizontal") {
        const horizontal = drawings.horizontals[target.index];
        if (!horizontal) {
          return;
        }
        const line = series.createPriceLine({
          price: horizontal.price,
          color: "#FFD166",
          lineWidth: 3,
          lineStyle: LineStyle.Solid,
          axisLabelVisible: false,
          title: "Delete",
        });
        hoverRef.current = { kind: "horizontal", index: target.index, line };
        return;
      }

      const fib = drawings.fibs[target.index];
      if (!fib || target.price === undefined) {
        return;
      }
      const line = series.createPriceLine({
        price: target.price,
        color: "#FFD166",
        lineWidth: 3,
        lineStyle: LineStyle.Solid,
        axisLabelVisible: false,
        title: "Delete",
      });
      hoverRef.current = { kind: "fib", index: target.index, line };
    };

    const handleMove = (param: any) => {
      pendingMoveRef.current = param;
      if (previewFrameRef.current !== null) {
        return;
      }
      previewFrameRef.current = requestAnimationFrame(() => {
        previewFrameRef.current = null;
        const pending = pendingMoveRef.current;
        pendingMoveRef.current = null;
        if (pending) {
          if (drawMode === "delete") {
            renderDeleteHover(pending);
          } else {
            renderPreview(pending);
          }
        }
      });
    };

    chart.subscribeCrosshairMove(handleMove);
    return () => chart.unsubscribeCrosshairMove(handleMove);
  }, [drawMode, mode, drawings]);

  useEffect(() => {
    if (mode !== "chart") {
      return;
    }
    const chart = chartRef.current;
    const series = seriesRef.current;
    if (!chart || !series) {
      return;
    }

    const pixelTolerance = 10;

    const distanceToSegment = (
      px: number,
      py: number,
      x1: number,
      y1: number,
      x2: number,
      y2: number
    ) => {
      const dx = x2 - x1;
      const dy = y2 - y1;
      if (dx === 0 && dy === 0) {
        return Math.hypot(px - x1, py - y1);
      }
      const t = ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy);
      const clamped = Math.max(0, Math.min(1, t));
      const cx = x1 + clamped * dx;
      const cy = y1 + clamped * dy;
      return Math.hypot(px - cx, py - cy);
    };

    const findDeletionTarget = (clickX: number, clickY: number) => {

      let best:
        | { kind: "trendline"; index: number; distance: number }
        | { kind: "horizontal"; index: number; distance: number; price: number }
        | { kind: "fib"; index: number; distance: number; price: number }
        | null = null;

      drawings.trendlines.forEach((trend, index) => {
        const normalized = ensureDistinctTimes(trend.start, trend.end);
        const x1 = chart.timeScale().timeToCoordinate(normalized.start.time);
        const x2 = chart.timeScale().timeToCoordinate(normalized.end.time);
        const y1 = series.priceToCoordinate(normalized.start.price);
        const y2 = series.priceToCoordinate(normalized.end.price);
        if (x1 === null || x2 === null || y1 === null || y2 === null) {
          return;
        }
        const distance = distanceToSegment(clickX, clickY, x1, y1, x2, y2);
        if (distance <= pixelTolerance && (!best || distance < best.distance)) {
          best = { kind: "trendline", index, distance };
        }
      });

      drawings.horizontals.forEach((horizontal, index) => {
        const y = series.priceToCoordinate(horizontal.price);
        if (y === null) {
          return;
        }
        const distance = Math.abs(clickY - y);
        if (distance <= pixelTolerance && (!best || distance < best.distance)) {
          best = { kind: "horizontal", index, distance, price: horizontal.price };
        }
      });

      drawings.fibs.forEach((fib, index) => {
        const diff = fib.end.price - fib.start.price;
        FIB_LEVELS.forEach((item) => {
          const y = series.priceToCoordinate(fib.start.price + diff * item.level);
          if (y === null) {
            return;
          }
          const distance = Math.abs(clickY - y);
          if (distance <= pixelTolerance && (!best || distance < best.distance)) {
            best = {
              kind: "fib",
              index,
              distance,
              price: fib.start.price + diff * item.level,
            };
          }
        });
      });

      return best;
    };

    const handleClick = (param: any) => {
      try {
        if (drawMode === "none" || drawMode === "delete") {
          if (!param.point) {
            return;
          }
          const target = findDeletionTarget(param.point.x, param.point.y);
          if (!target) {
            return;
          }
          if (target.kind === "trendline") {
            setDrawings((prev) => ({
              ...prev,
              trendlines: prev.trendlines.filter((_, idx) => idx !== target.index),
            }));
          } else if (target.kind === "horizontal") {
            setDrawings((prev) => ({
              ...prev,
              horizontals: prev.horizontals.filter((_, idx) => idx !== target.index),
            }));
          } else {
            setDrawings((prev) => ({
              ...prev,
              fibs: prev.fibs.filter((_, idx) => idx !== target.index),
            }));
          }
          return;
        }
        if (!param.point) {
          return;
        }

        const time =
          param.time ??
          (chart.timeScale().coordinateToTime(param.point.x) as Time | null) ??
          null;
        let resolvedTime = time;
        if (resolvedTime === null || resolvedTime === undefined) {
          const logical = chart.timeScale().coordinateToLogical(param.point.x);
          const ref = candlesRef.current;
          if (logical !== null && ref && ref.times.length > 0) {
            const base = ref.times[0];
            resolvedTime = (base + Math.round(logical) * ref.step) as UTCTimestamp;
          }
        }
        if (resolvedTime === null || resolvedTime === undefined) {
          return;
        }

        const price = series.coordinateToPrice(param.point.y);
        if (price === null || !Number.isFinite(price)) {
          return;
        }

        if (drawMode === "horizontal") {
          setDrawings((prev) => ({
            ...prev,
            horizontals: [...prev.horizontals, { price }],
          }));
          return;
        }

        const nextPoint: DrawPoint = { time: toTimestamp(resolvedTime), price };
        drawPointsRef.current = [...drawPointsRef.current, nextPoint];
        if (drawPointsRef.current.length < 2) {
          return;
        }

        const [pointA, pointB] = drawPointsRef.current.slice(-2);
        drawPointsRef.current = [];
        const [startRaw, endRaw] = pointA.time <= pointB.time ? [pointA, pointB] : [pointB, pointA];
        const { start, end } = ensureDistinctTimes(startRaw, endRaw);

        if (drawMode === "trendline") {
          setDrawings((prev) => ({
            ...prev,
            trendlines: [...prev.trendlines, { start, end }],
          }));
          if (previewRef.current.trendline) {
            chart.removeSeries(previewRef.current.trendline);
            previewRef.current.trendline = null;
          }
          return;
        }

        if (drawMode === "fibonacci") {
          setDrawings((prev) => ({
            ...prev,
            fibs: [...prev.fibs, { start, end }],
          }));
          previewRef.current.fibs.forEach((line) => series.removePriceLine(line));
          previewRef.current.fibs = [];
        }
      } catch (err) {
        console.warn("Draw click error", err);
      }
    };

    chart.subscribeClick(handleClick);
    return () => chart.unsubscribeClick(handleClick);
  }, [drawMode, mode]);

  const toggleIndicatorGroup = (keys: IndicatorKey[]) => {
    setIndicatorVisibility((prev) => {
      const isActive = keys.every((key) => prev[key]);
      const next = { ...prev };
      keys.forEach((key) => {
        next[key] = !isActive;
      });
      return next;
    });
  };

  const clearDrawings = () => {
    setDrawings({ trendlines: [], horizontals: [], fibs: [] });
    drawPointsRef.current = [];
  };

  const handleLoadTicker = () => {
    const next = tickerInput.trim().toUpperCase() || DEFAULT_TICKER;
    setTickerInput(next);
    setTicker(next);
  };

  const drawHint =
    mode === "chart" && drawMode !== "none"
      ? drawMode === "delete"
        ? "Delete: click a line to remove"
        : drawMode === "horizontal"
          ? "Draw: click once for horizontal line"
          : drawMode === "trendline"
            ? "Draw: click two points for trendline"
            : "Draw: click two points for Fibonacci"
      : null;

  const isIndicatorGroupActive = (group: IndicatorGroup) =>
    group.keys.every((key) => indicatorVisibility[key]);

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
                onKeyDown={(event) => {
                  if (event.key === "Enter") {
                    handleLoadTicker();
                  }
                }}
              />
              <button
                className="button"
                onClick={handleLoadTicker}
                disabled={loading}
              >
                {loading ? "Loading..." : "Load"}
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
              {INDICATOR_GROUPS.map((group) => (
                <button
                  key={group.label}
                  className={`button ${isIndicatorGroupActive(group) ? "active" : ""}`}
                  onClick={() => toggleIndicatorGroup(group.keys)}
                >
                  {group.label}
                </button>
              ))}
            </div>
          </div>
        ) : null}

        {mode === "chart" ? (
          <div className="control-group">
            <span className="control-label">Signals</span>
            <div className="button-group">
              <button
                className={`button ${showWeakSignals ? "active" : ""}`}
                onClick={() => setShowWeakSignals((prev) => !prev)}
              >
                Weak Signals
              </button>
            </div>
          </div>
        ) : null}

        {mode === "chart" ? (
          <div className="control-group">
            <span className="control-label">Draw</span>
            <div className="button-group">
              {[
                { value: "none" as DrawMode, label: "Off" },
                { value: "delete" as DrawMode, label: "Delete" },
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
              <button
                className="button"
                onClick={() => setScanRefresh((v) => v + 1)}
                disabled={scanLoading}
              >
                {scanLoading ? "Scanning..." : "Scan"}
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
