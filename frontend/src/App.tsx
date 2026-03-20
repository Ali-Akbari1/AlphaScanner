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
const INTERVALS = ["1h", "4h", "1d", "1w", "1mo"] as const;
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
const TICKER_SUGGESTIONS = Array.from(
  new Set([
    DEFAULT_TICKER,
    ...DEFAULT_SCAN_TICKERS,
    "AMD",
    "INTC",
    "ORCL",
    "CRM",
    "ADBE",
    "PYPL",
    "DIS",
    "NFLX",
    "KO",
    "PEP",
    "MCD",
    "COST",
    "WMT",
    "HD",
    "LOW",
    "NKE",
    "JPM",
    "BAC",
    "WFC",
    "GS",
    "V",
    "MA",
    "SQ",
    "SHOP",
    "UBER",
    "LYFT",
    "SNAP",
    "ROKU",
    "PLTR",
    "SOFI",
    "RIVN",
    "LCID",
    "XOM",
    "CVX",
    "COP",
    "CAT",
    "GE",
    "BA",
    "UNH",
    "JNJ",
    "PFE",
    "LLY",
    "MRK",
    "ABBV",
    "TMO",
    "GOOG",
    "USO",
    "GLD",
    "SLV",
    "IWM",
    "DIA",
    "EEM",
    "IEMG",
    "ARKQ",
    "ARKG",
    "SMH",
    "SOXX",
    "XBI",
    "XLV",
    "XLY",
    "XLP",
    "XLI",
    "XLU",
    "XLF",
    "XLE",
    "XLK",
    "VTI",
    "ARKK",
    "TSM",
    "AVGO",
  ])
).sort();
const FIB_LEVELS = [
  { level: 0, label: "0" },
  { level: 0.236, label: "0.236" },
  { level: 0.382, label: "0.382" },
  { level: 0.5, label: "0.5" },
  { level: 0.618, label: "0.618" },
  { level: 0.786, label: "0.786" },
  { level: 1, label: "1" },
];
const DELETE_TOLERANCE_HOVER = 14;
const DELETE_TOLERANCE_CLICK = 12;

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
  ema9: number | null;
  ema21: number | null;
  ema50: number | null;
  ema200: number | null;
  supertrend: number | null;
  rsi: number | null;
  vwap: number | null;
  bb_upper: number | null;
  bb_middle: number | null;
  bb_lower: number | null;
  macd: number | null;
  macd_signal: number | null;
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
type DeleteTarget =
  | { kind: "trendline"; index: number; distance: number }
  | { kind: "horizontal"; index: number; distance: number; price: number }
  | { kind: "fib"; index: number; distance: number; price: number };

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

type SweepEvent = {
  ticker: string;
  time: number;
  ny_time: string;
  direction: "bull" | "bear";
  level_name: string;
  level_price: number;
  close: number;
  high: number;
  low: number;
  sent: boolean;
};

type SweepScanResponse = {
  meta: {
    interval: string;
    tickers: string[];
    timezone: string;
    ny_open_start: string;
    ny_open_end: string;
  };
  events: SweepEvent[];
};

type SweepStatus = {
  alerts_enabled: boolean;
  sms_enabled: boolean;
  sms_ready: boolean;
  twilio_configured: boolean;
  poll_seconds: number;
  interval: string;
  timezone: string;
  ny_open_start: string;
  ny_open_end: string;
  tickers: string[];
};

type BacktestTrade = {
  ticker: string;
  session: "london" | "newyork";
  direction: "long" | "short";
  level_name: string;
  sweep_time: number;
  fvg_time?: number | null;
  entry_time: number;
  entry_price: number;
  stop_price: number;
  target_price: number;
  exit_time: number;
  exit_price: number;
  result: "win" | "loss" | "breakeven";
  r_multiple: number;
  pnl: number;
};

type BacktestSummary = {
  starting_balance: number;
  ending_balance: number;
  return_pct: number;
  total_trades: number;
  wins: number;
  losses: number;
  breakeven: number;
  win_rate: number;
  profit_factor: number | null;
  max_drawdown: number;
};

type EquityPoint = {
  time: number;
  equity: number;
};

type SessionBreakdown = {
  session: "london" | "newyork";
  total_trades: number;
  wins: number;
  losses: number;
  breakeven: number;
  win_rate: number;
  profit_factor: number | null;
  return_pct: number;
};

type BacktestResponse = {
  meta: {
    tickers: string[];
    interval: string;
    session: string;
    timezone: string;
    ny_open_start: string;
    ny_open_end: string;
    london_start: string;
    london_end: string;
    data_ranges: Record<string, { start: number; end: number }>;
    notes: string;
  };
  summary: BacktestSummary;
  session_breakdown: SessionBreakdown[];
  equity_curve: EquityPoint[];
  trades: BacktestTrade[];
};

type GridSearchResult = {
  params: Record<string, number>;
  summary: BacktestSummary;
  score: number;
};

type GridSearchResponse = {
  meta: {
    tickers: string[];
    interval: string;
    session: string;
    timezone: string;
    combinations_tested: number;
    max_combinations: number;
    sort_by: string;
    data_ranges: Record<string, { start: number; end: number }>;
    notes: string;
  };
  results: GridSearchResult[];
};

type DukascopyProgress = {
  source: string;
  processed: number;
  total: number;
  cached: number;
  downloaded: number;
  missing: number;
  retry_attempts: number;
  skipped: number;
  speed: number;
  eta_seconds: number;
  percent: number;
  updated_at: number;
};

type DukascopyProgressResponse = {
  paused: boolean;
  canceled: boolean;
  sources: DukascopyProgress[];
};

type BatchBacktestResponse = {
  meta: {
    tickers: string[];
    interval: string;
    session: string;
    timezone: string;
    start_year: number;
    end_year: number;
    notes: string;
  };
  results: {
    year: number;
    summary: BacktestSummary;
    total_trades: number;
  }[];
};

type BatchProgressResponse = {
  status: "idle" | "running" | "done";
  processed: number;
  total: number;
  current_year: number | null;
  updated_at: number;
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

const formatLocalTime = (timestamp?: number | null) => {
  if (!timestamp) {
    return "-";
  }
  return new Date(timestamp).toLocaleString();
};

const formatEta = (seconds?: number | null) => {
  if (!seconds || !Number.isFinite(seconds) || seconds <= 0) {
    return "-";
  }
  const total = Math.max(0, Math.round(seconds));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const secs = total % 60;
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  if (minutes > 0) {
    return `${minutes}m ${secs}s`;
  }
  return `${secs}s`;
};

const formatDate = (value: Date) => value.toISOString().slice(0, 10);

const parseTickers = (value: string) =>
  value
    .split(/[\s,]+/)
    .map((ticker) => ticker.trim().toUpperCase())
    .filter(Boolean);

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

const formatAxisTime = (time: Time, showTime: boolean) => {
  let date: Date;
  if (typeof time === "number") {
    date = new Date(time * 1000);
  } else {
    date = new Date(Date.UTC(time.year, time.month - 1, time.day));
  }
  if (showTime) {
    return date.toISOString().replace("T", " ").slice(0, 16);
  }
  return date.toISOString().slice(0, 10);
};

const inferResolution = (times: UTCTimestamp[]) => {
  if (times.length < 2) {
    return null;
  }
  const diffs = times
    .slice(1)
    .map((time, idx) => time - times[idx])
    .filter((diff) => diff > 0)
    .sort((a, b) => a - b);
  if (diffs.length === 0) {
    return null;
  }
  const median = diffs[Math.floor(diffs.length / 2)];
  const candidates = [
    { label: "1h", seconds: 60 * 60 },
    { label: "4h", seconds: 4 * 60 * 60 },
    { label: "1d", seconds: 24 * 60 * 60 },
    { label: "1w", seconds: 7 * 24 * 60 * 60 },
    { label: "1mo", seconds: 30 * 24 * 60 * 60 },
  ];
  const closest = candidates.reduce((best, candidate) => {
    const diff = Math.abs(median - candidate.seconds);
    if (!best || diff < best.diff) {
      return { label: candidate.label, diff };
    }
    return best;
  }, null as { label: string; diff: number } | null);
  if (!closest) {
    return null;
  }
  if (closest.diff > candidates.find((c) => c.label === closest.label)!.seconds * 0.2) {
    const minutes = Math.max(1, Math.round(median / 60));
    return `${minutes}m`;
  }
  return closest.label;
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
  const rsiChartContainerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const rsiChartRef = useRef<IChartApi | null>(null);
  const mainVisibleRangeRef = useRef<any>(null);
  const seriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const rsiSeriesRef = useRef<ISeriesApi<"Line"> | null>(null);
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
  const [resolutionLabel, setResolutionLabel] = useState<string | null>(null);
  const [scanTickersInput, setScanTickersInput] = useState(
    storedSettings?.scanTickersInput ?? DEFAULT_SCAN_TICKERS.join(", ")
  );
  const [sweepStatus, setSweepStatus] = useState<SweepStatus | null>(null);
  const [sweepStatusLoading, setSweepStatusLoading] = useState(false);
  const [sweepStatusError, setSweepStatusError] = useState<string | null>(null);
  const [sweepRunLoading, setSweepRunLoading] = useState(false);
  const [sweepRunError, setSweepRunError] = useState<string | null>(null);
  const [sweepRunResult, setSweepRunResult] = useState<SweepScanResponse | null>(null);
  const [sweepLastRun, setSweepLastRun] = useState<number | null>(null);
  const [backtestLoading, setBacktestLoading] = useState(false);
  const [backtestError, setBacktestError] = useState<string | null>(null);
  const [backtestResult, setBacktestResult] = useState<BacktestResponse | null>(null);
  const [csvLoading, setCsvLoading] = useState(false);
  const [gridLoading, setGridLoading] = useState(false);
  const [gridError, setGridError] = useState<string | null>(null);
  const [gridResult, setGridResult] = useState<GridSearchResponse | null>(null);
  const [batchLoading, setBatchLoading] = useState(false);
  const [batchError, setBatchError] = useState<string | null>(null);
  const [batchResult, setBatchResult] = useState<BatchBacktestResponse | null>(null);
  const [dukasProgress, setDukasProgress] = useState<DukascopyProgress[]>([]);
  const [dukasLoading, setDukasLoading] = useState(false);
  const [dukasPaused, setDukasPaused] = useState(false);
  const [dukasCanceled, setDukasCanceled] = useState(false);
  const [batchProgress, setBatchProgress] = useState<BatchProgressResponse | null>(null);
  const [batchCanceling, setBatchCanceling] = useState(false);

  const parsedScanTickers = useMemo(
    () =>
      scanTickersInput
        .split(/[\s,]+/)
        .map((tickerValue) => tickerValue.trim().toUpperCase())
        .filter(Boolean),
    [scanTickersInput]
  );
  const sweepTickersLabel = useMemo(() => {
    if (!sweepStatus?.tickers?.length) {
      return "-";
    }
    const joined = sweepStatus.tickers.join(", ");
    return joined.length > 80 ? `${joined.slice(0, 77)}...` : joined;
  }, [sweepStatus?.tickers]);
  const lastSweepEvent = useMemo(() => {
    if (!sweepRunResult?.events?.length) {
      return null;
    }
    return [...sweepRunResult.events].sort((a, b) => b.time - a.time)[0] ?? null;
  }, [sweepRunResult]);
  const [backtestTickersInput, setBacktestTickersInput] = useState(
    "EURUSD, GBPUSD, XAUUSD"
  );
  const [backtestInterval, setBacktestInterval] = useState<"1m" | "5m">("5m");
  const [backtestSession, setBacktestSession] = useState<"london" | "newyork" | "both">(
    "both"
  );
  const [backtestStart, setBacktestStart] = useState(() => {
    const end = new Date();
    const start = new Date(end.getTime() - 1000 * 60 * 60 * 24 * 60);
    return formatDate(start);
  });
  const [backtestEnd, setBacktestEnd] = useState(() => formatDate(new Date()));

  const backtestTickers = useMemo(
    () => parseTickers(backtestTickersInput),
    [backtestTickersInput]
  );
  const backtestPayload = useMemo(
    () => ({
      tickers: backtestTickers,
      interval: backtestInterval,
      session: backtestSession,
      start: backtestStart,
      end: backtestEnd,
      starting_balance: 10000,
      risk_per_trade: 0.005,
      max_trades_per_day: 2,
      sweep_atr_mult: 0.6,
      return_within_bars: 20,
      fvg_min_atr_mult: 0,
      fvg_retrace_window: 12,
      stop_atr_mult: 1.2,
      target_rr: 2,
    }),
    [backtestTickers, backtestInterval, backtestSession, backtestStart, backtestEnd]
  );
  const batchPayload = useMemo(
    () => ({
      base: backtestPayload,
      start_year: backtestStart ? Number(backtestStart.slice(0, 4)) : undefined,
      end_year: backtestEnd ? Number(backtestEnd.slice(0, 4)) : undefined,
    }),
    [backtestPayload, backtestStart, backtestEnd]
  );

  const findDeletionTarget = (
    clickX: number,
    clickY: number,
    tolerance: number = DELETE_TOLERANCE_CLICK
  ): DeleteTarget | null => {
    const chart = chartRef.current;
    const series = seriesRef.current;
    if (!chart || !series) {
      return null;
    }

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

    let best: DeleteTarget | null = null;

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
      if (distance <= tolerance && (!best || distance < best.distance)) {
        best = { kind: "trendline", index, distance };
      }
    });

    drawings.horizontals.forEach((horizontal, index) => {
      const y = series.priceToCoordinate(horizontal.price);
      if (y === null) {
        return;
      }
      const distance = Math.abs(clickY - y);
      if (distance <= tolerance && (!best || distance < best.distance)) {
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
        if (distance <= tolerance && (!best || distance < best.distance)) {
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

    let rsiChart: IChartApi | null = null;
    let rsiSeries: ISeriesApi<"Line"> | null = null;
    let syncMainToRsi: ((range: any) => void) | null = null;

    if (rsiChartContainerRef.current) {
      rsiChart = createChart(rsiChartContainerRef.current, {
        height: 170,
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
        timeScale: { borderColor: "#1F2A3A", visible: false },
        handleScroll: {
          mouseWheel: false,
          pressedMouseMove: false,
          horzTouchDrag: false,
          vertTouchDrag: false,
        },
        handleScale: {
          mouseWheel: false,
          pinch: false,
          axisPressedMouseMove: false,
        },
      });

      rsiChartRef.current = rsiChart;
      rsiSeries = rsiChart.addLineSeries({
        color: INDICATOR_COLORS.rsi,
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
      });
      rsiSeriesRef.current = rsiSeries;

      rsiChart.priceScale("right").applyOptions({
        scaleMargins: { top: 0.2, bottom: 0.2 },
        borderColor: "#1F2A3A",
      });

      rsiLinesRef.current = [
        rsiSeries.createPriceLine({
          price: 70,
          color: "#F6C453",
          lineWidth: 1,
          lineStyle: LineStyle.Dashed,
          axisLabelVisible: true,
          title: "RSI 70",
        }),
        rsiSeries.createPriceLine({
          price: 30,
          color: "#FF4D6D",
          lineWidth: 1,
          lineStyle: LineStyle.Dashed,
          axisLabelVisible: true,
          title: "RSI 30",
        }),
        rsiSeries.createPriceLine({
          price: 50,
          color: "#9FB0C3",
          lineWidth: 1,
          lineStyle: LineStyle.Dashed,
          axisLabelVisible: false,
          title: "RSI 50",
        }),
      ];

      const syncState = { active: false };
      syncMainToRsi = (range) => {
        if (!range) {
          return;
        }
        mainVisibleRangeRef.current = range;
        if (syncState.active) {
          return;
        }
        syncState.active = true;
        rsiChart?.timeScale().setVisibleRange(range);
        syncState.active = false;
      };
      chart.timeScale().subscribeVisibleTimeRangeChange(syncMainToRsi);
    } else {
      rsiLinesRef.current = [];
    }

    const indicatorSeries: Record<IndicatorKey, ISeriesApi<"Line"> | null> = {
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
      rsi: rsiSeries,
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

    chart.priceScale("macd").applyOptions({
      scaleMargins: { top: 0.55, bottom: 0.25 },
      borderColor: "#1F2A3A",
    });

    (Object.keys(indicatorSeries) as IndicatorKey[]).forEach((key) => {
      const series = indicatorSeries[key];
      if (series) {
        series.applyOptions({ visible: indicatorVisibility[key] });
      }
    });
    indicatorSeriesRef.current = indicatorSeries;

    const handleResize = () => {
      if (chartContainerRef.current) {
        chart.applyOptions({ width: chartContainerRef.current.clientWidth });
      }
      if (rsiChartContainerRef.current && rsiChart) {
        rsiChart.applyOptions({ width: rsiChartContainerRef.current.clientWidth });
      }
    };

    handleResize();
    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
      if (syncMainToRsi) {
        chart.timeScale().unsubscribeVisibleTimeRangeChange(syncMainToRsi);
      }
      chart.remove();
      rsiChart?.remove();
      chartRef.current = null;
      rsiChartRef.current = null;
      seriesRef.current = null;
      rsiSeriesRef.current = null;
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
    if (mode !== "chart" || !indicatorVisibility.rsi) {
      return;
    }
    const chart = rsiChartRef.current;
    const container = rsiChartContainerRef.current;
    if (chart && container) {
      chart.applyOptions({ width: container.clientWidth });
      const mainRange = mainVisibleRangeRef.current;
      if (mainRange) {
        chart.timeScale().setVisibleRange(mainRange);
      } else {
        chart.timeScale().fitContent();
      }
    }
  }, [indicatorVisibility.rsi, mode]);

  useEffect(() => {
    if (mode !== "chart") {
      return;
    }
    const chart = chartRef.current;
    if (!chart) {
      return;
    }
    const label = resolutionLabel ?? interval;
    const intraday = label.endsWith("h") || (label.endsWith("m") && !label.endsWith("mo"));
    chart.applyOptions({
      timeScale: {
        timeVisible: intraday,
        secondsVisible: false,
        tickMarkFormatter: (time: Time) => formatAxisTime(time, intraday),
      },
    });
    rsiChartRef.current?.applyOptions({
      timeScale: {
        timeVisible: intraday,
        secondsVisible: false,
        tickMarkFormatter: (time: Time) => formatAxisTime(time, intraday),
      },
    });
  }, [interval, mode, resolutionLabel]);

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
        setResolutionLabel(inferResolution(candleTimes));
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

        const buildLineData = (values: { time: Time; value: number | null }[]) =>
          values
            .filter((point) => typeof point.value === "number" && Number.isFinite(point.value))
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
        const mainRange = chartRef.current?.timeScale().getVisibleRange();
        if (mainRange && rsiChartRef.current) {
          mainVisibleRangeRef.current = mainRange;
          rsiChartRef.current.timeScale().setVisibleRange(mainRange);
        } else {
          rsiChartRef.current?.timeScale().fitContent();
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : "Unknown error";
        setError(message);
        seriesRef.current?.setData([]);
        rsiSeriesRef.current?.setData([]);
        (Object.keys(indicatorSeriesRef.current) as IndicatorKey[]).forEach((key) => {
          indicatorSeriesRef.current[key]?.setData([]);
        });
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

  const parseApiError = async (response: Response) => {
    let detail = `API error: ${response.status}`;
    try {
      const payload = await response.json();
      if (payload && typeof payload.detail === "string") {
        detail = payload.detail;
      }
    } catch {
      detail = `API error: ${response.status}`;
    }
    return detail;
  };

  const fetchSweepStatus = async () => {
    setSweepStatusLoading(true);
    setSweepStatusError(null);

    try {
      const response = await fetch(`${API_BASE}/sweeps/status`);
      if (!response.ok) {
        throw new Error(await parseApiError(response));
      }
      const data = (await response.json()) as SweepStatus;
      setSweepStatus(data);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unknown error";
      setSweepStatusError(message);
    } finally {
      setSweepStatusLoading(false);
    }
  };

  const handleRunSweep = async () => {
    setSweepRunLoading(true);
    setSweepRunError(null);

    try {
      const response = await fetch(`${API_BASE}/sweeps/run?send_sms=true`);
      if (!response.ok) {
        throw new Error(await parseApiError(response));
      }
      const data = (await response.json()) as SweepScanResponse;
      setSweepRunResult(data);
      setSweepLastRun(Date.now());
      fetchSweepStatus();
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unknown error";
      setSweepRunError(message);
    } finally {
      setSweepRunLoading(false);
    }
  };

  const handleRunBacktest = async () => {
    setBacktestLoading(true);
    setBacktestError(null);

    try {
      if (backtestPayload.tickers.length === 0) {
        throw new Error("Add at least one ticker to run a backtest.");
      }
      const response = await fetch(`${API_BASE}/backtest/sweep`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(backtestPayload),
      });
      if (!response.ok) {
        throw new Error(await parseApiError(response));
      }
      const data = (await response.json()) as BacktestResponse;
      setBacktestResult(data);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unknown error";
      setBacktestError(message);
    } finally {
      setBacktestLoading(false);
    }
  };

  const handleDownloadCsv = async () => {
    setCsvLoading(true);
    try {
      if (backtestPayload.tickers.length === 0) {
        throw new Error("Add at least one ticker to export CSV.");
      }
      const response = await fetch(`${API_BASE}/backtest/sweep/csv`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(backtestPayload),
      });
      if (!response.ok) {
        throw new Error(await parseApiError(response));
      }
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = "backtest_trades.csv";
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      window.URL.revokeObjectURL(url);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unknown error";
      setBacktestError(message);
    } finally {
      setCsvLoading(false);
    }
  };

  const handleGridSearch = async () => {
    setGridLoading(true);
    setGridError(null);

    if (backtestPayload.tickers.length === 0) {
      setGridError("Add at least one ticker to run grid search.");
      setGridLoading(false);
      return;
    }
    const gridBody = {
      base: backtestPayload,
      sweep_atr_mults: [0.4, 0.5, 0.6],
      return_within_bars: [10, 20, 30],
      fvg_min_atr_mults: [0, 0.1],
      fvg_retrace_windows: [8, 12, 16],
      stop_atr_mults: [1.0, 1.2],
      target_rrs: [1.5, 2.0],
      max_combinations: 120,
      top_n: 8,
      sort_by: "score",
    };

    try {
      const response = await fetch(`${API_BASE}/backtest/sweep/grid`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(gridBody),
      });
      if (!response.ok) {
        throw new Error(await parseApiError(response));
      }
      const data = (await response.json()) as GridSearchResponse;
      setGridResult(data);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unknown error";
      setGridError(message);
    } finally {
      setGridLoading(false);
    }
  };

  const handleBatchBacktest = async () => {
    setBatchLoading(true);
    setBatchError(null);

    if (backtestPayload.tickers.length === 0) {
      setBatchError("Add at least one ticker to run batch backtest.");
      setBatchLoading(false);
      return;
    }

    try {
      const response = await fetch(`${API_BASE}/backtest/sweep/batch`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(batchPayload),
      });
      if (!response.ok) {
        throw new Error(await parseApiError(response));
      }
      const data = (await response.json()) as BatchBacktestResponse;
      setBatchResult(data);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unknown error";
      setBatchError(message);
    } finally {
      setBatchLoading(false);
    }
  };

  const fetchDukascopyProgress = async () => {
    setDukasLoading(true);
    try {
      const response = await fetch(`${API_BASE}/dukascopy/progress`);
      if (!response.ok) {
        throw new Error(await parseApiError(response));
      }
      const data = (await response.json()) as DukascopyProgressResponse;
      setDukasProgress(data.sources ?? []);
      setDukasPaused(Boolean(data.paused));
      setDukasCanceled(Boolean(data.canceled));
    } catch {
      setDukasProgress([]);
    } finally {
      setDukasLoading(false);
    }
  };

  const sendDukascopyControl = async (action: "pause" | "resume" | "cancel") => {
    try {
      await fetch(`${API_BASE}/dukascopy/${action}`, { method: "POST" });
      fetchDukascopyProgress();
    } catch {
      // ignore UI control errors
    }
  };

  const cancelBatchBacktest = async () => {
    setBatchCanceling(true);
    try {
      await fetch(`${API_BASE}/backtest/sweep/batch/cancel`, { method: "POST" });
      fetchBatchProgress();
    } catch {
      // ignore
    } finally {
      setBatchCanceling(false);
    }
  };

  const fetchBatchProgress = async () => {
    try {
      const response = await fetch(`${API_BASE}/backtest/sweep/batch/progress`);
      if (!response.ok) {
        return;
      }
      const data = (await response.json()) as BatchProgressResponse;
      setBatchProgress(data);
    } catch {
      setBatchProgress(null);
    }
  };

  useEffect(() => {
    fetchDukascopyProgress();
    fetchBatchProgress();
    const intervalId = window.setInterval(() => {
      fetchDukascopyProgress();
      fetchBatchProgress();
    }, 4000);
    return () => window.clearInterval(intervalId);
  }, []);

  useEffect(() => {
    fetchSweepStatus();
  }, []);

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
      const target = findDeletionTarget(
        param.point.x,
        param.point.y,
        DELETE_TOLERANCE_HOVER
      );
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

    const handleClick = (param: any) => {
      try {
        if (drawMode === "none" || drawMode === "delete") {
          if (!param.point) {
            return;
          }
          const target = findDeletionTarget(
            param.point.x,
            param.point.y,
            DELETE_TOLERANCE_CLICK
          );
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
  }, [drawMode, mode, drawings]);

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
  const smsStatusLabel = sweepStatus
    ? sweepStatus.sms_ready
      ? "SMS Ready"
      : sweepStatus.sms_enabled
        ? "SMS Not Ready"
        : "SMS Disabled"
    : "Status Unknown";
  const smsStatusClass = sweepStatus
    ? sweepStatus.sms_ready
      ? "ready"
      : sweepStatus.sms_enabled
        ? "warn"
        : "off"
    : "off";
  const equityPath = useMemo(() => {
    const points = backtestResult?.equity_curve ?? [];
    if (points.length < 2) {
      return "";
    }
    const values = points.map((point) => point.equity);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const range = max - min || 1;
    const width = 520;
    const height = 160;
    return points
      .map((point, idx) => {
        const x = (idx / (points.length - 1)) * width;
        const y = height - ((point.equity - min) / range) * height;
        return `${idx === 0 ? "M" : "L"}${x.toFixed(2)} ${y.toFixed(2)}`;
      })
      .join(" ");
  }, [backtestResult]);

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
                list="ticker-suggestions"
                value={tickerInput}
                onChange={(event) => setTickerInput(event.target.value.toUpperCase())}
                onKeyDown={(event) => {
                  if (event.key === "Enter") {
                    handleLoadTicker();
                  }
                }}
              />
              <datalist id="ticker-suggestions">
                {TICKER_SUGGESTIONS.map((value) => (
                  <option key={value} value={value} />
                ))}
              </datalist>
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

      <section className="sms-panel">
        <div className="sms-header">
          <div>
            <p className="sms-eyebrow">SMS Alerts</p>
            <h2 className="sms-title">Liquidity Sweep Monitor</h2>
            <p className="sms-subtitle">
              Watches NY open for liquidity sweeps and sends alerts when configured.
            </p>
          </div>
          <span className={`sms-pill ${smsStatusClass}`}>{smsStatusLabel}</span>
        </div>

        <div className="sms-grid">
          <div className="sms-item">
            <span className="sms-label">Monitor</span>
            <span className="sms-value">
              {sweepStatus?.alerts_enabled ? "Enabled" : "Disabled"}
            </span>
          </div>
          <div className="sms-item">
            <span className="sms-label">Interval</span>
            <span className="sms-value">{sweepStatus?.interval ?? "-"}</span>
          </div>
          <div className="sms-item">
            <span className="sms-label">Poll</span>
            <span className="sms-value">
              {sweepStatus ? `${sweepStatus.poll_seconds}s` : "-"}
            </span>
          </div>
          <div className="sms-item">
            <span className="sms-label">NY Window</span>
            <span className="sms-value">
              {sweepStatus
                ? `${sweepStatus.ny_open_start}-${sweepStatus.ny_open_end} ${sweepStatus.timezone}`
                : "-"}
            </span>
          </div>
          <div className="sms-item">
            <span className="sms-label">Tickers</span>
            <span className="sms-value">{sweepTickersLabel}</span>
          </div>
          <div className="sms-item">
            <span className="sms-label">Last Run</span>
            <span className="sms-value">{formatLocalTime(sweepLastRun)}</span>
          </div>
          <div className="sms-item">
            <span className="sms-label">Last Result</span>
            <span className="sms-value">
              {sweepRunResult ? `${sweepRunResult.events.length} sweeps` : "-"}
            </span>
          </div>
          <div className="sms-item">
            <span className="sms-label">Last Event</span>
            <span className="sms-value">
              {lastSweepEvent
                ? `${lastSweepEvent.ticker} ${lastSweepEvent.direction.toUpperCase()} ${lastSweepEvent.level_name.replace(
                    /_/g,
                    " "
                  )}`
                : "-"}
            </span>
          </div>
        </div>

        <div className="sms-actions">
          <button className="button" onClick={fetchSweepStatus} disabled={sweepStatusLoading}>
            {sweepStatusLoading ? "Refreshing..." : "Refresh Status"}
          </button>
          <button className="button" onClick={handleRunSweep} disabled={sweepRunLoading}>
            {sweepRunLoading ? "Running..." : "Run Sweep + SMS"}
          </button>
        </div>

        {sweepStatusError ? <div className="sms-error">{sweepStatusError}</div> : null}
        {sweepRunError ? <div className="sms-error">{sweepRunError}</div> : null}
      </section>

      <section className="backtest-panel">
        <div className="backtest-header">
          <div>
            <p className="backtest-eyebrow">Backtest Lab</p>
            <h2 className="backtest-title">Sweep + FVG Strategy</h2>
            <p className="backtest-subtitle">
              Runs the liquidity sweep + FVG retrace rules across London and NY sessions.
            </p>
          </div>
          <div className="backtest-actions">
            <button className="button" onClick={handleRunBacktest} disabled={backtestLoading}>
              {backtestLoading ? "Running..." : "Run Backtest"}
            </button>
            <button className="button" onClick={handleDownloadCsv} disabled={csvLoading}>
              {csvLoading ? "Exporting..." : "Export CSV"}
            </button>
            <button className="button" onClick={handleGridSearch} disabled={gridLoading}>
              {gridLoading ? "Searching..." : "Run Grid Search"}
            </button>
            <button className="button" onClick={handleBatchBacktest} disabled={batchLoading}>
              {batchLoading ? "Batching..." : "Run Yearly Batch"}
            </button>
          </div>
        </div>

        <div className="backtest-form">
          <div className="backtest-field">
            <span className="backtest-field-label">Tickers</span>
            <input
              className="input"
              value={backtestTickersInput}
              onChange={(event) => setBacktestTickersInput(event.target.value)}
              placeholder="EURUSD, GBPUSD, XAUUSD"
            />
          </div>
          <div className="backtest-field">
            <span className="backtest-field-label">Interval</span>
            <select
              className="input"
              value={backtestInterval}
              onChange={(event) => setBacktestInterval(event.target.value as "1m" | "5m")}
            >
              <option value="5m">5m</option>
              <option value="1m">1m</option>
            </select>
          </div>
          <div className="backtest-field">
            <span className="backtest-field-label">Session</span>
            <select
              className="input"
              value={backtestSession}
              onChange={(event) =>
                setBacktestSession(event.target.value as "london" | "newyork" | "both")
              }
            >
              <option value="both">Both</option>
              <option value="london">London</option>
              <option value="newyork">New York</option>
            </select>
          </div>
          <div className="backtest-field">
            <span className="backtest-field-label">Start</span>
            <input
              className="input"
              type="date"
              value={backtestStart}
              onChange={(event) => setBacktestStart(event.target.value)}
            />
          </div>
          <div className="backtest-field">
            <span className="backtest-field-label">End</span>
            <input
              className="input"
              type="date"
              value={backtestEnd}
              onChange={(event) => setBacktestEnd(event.target.value)}
            />
          </div>
        </div>

        <div className="backtest-meta">
          <div className="backtest-chip">
            Tickers: {backtestTickers.length ? backtestTickers.join(", ") : "-"}
          </div>
          <div className="backtest-chip">Interval: {backtestInterval}</div>
          <div className="backtest-chip">Session: {backtestSession}</div>
          <div className="backtest-chip">
            Window: {backtestStart} → {backtestEnd}
          </div>
        </div>

        <div className="backtest-grid">
          <div className="backtest-card">
            <span className="backtest-label">Return</span>
            <span className="backtest-value">
              {backtestResult ? `${backtestResult.summary.return_pct.toFixed(2)}%` : "-"}
            </span>
          </div>
          <div className="backtest-card">
            <span className="backtest-label">Trades</span>
            <span className="backtest-value">
              {backtestResult ? backtestResult.summary.total_trades : "-"}
            </span>
          </div>
          <div className="backtest-card">
            <span className="backtest-label">Win Rate</span>
            <span className="backtest-value">
              {backtestResult ? `${(backtestResult.summary.win_rate * 100).toFixed(1)}%` : "-"}
            </span>
          </div>
          <div className="backtest-card">
            <span className="backtest-label">Profit Factor</span>
            <span className="backtest-value">
              {backtestResult && backtestResult.summary.profit_factor !== null
                ? backtestResult.summary.profit_factor.toFixed(2)
                : "-"}
            </span>
          </div>
          <div className="backtest-card">
            <span className="backtest-label">Max Drawdown</span>
            <span className="backtest-value">
              {backtestResult ? `${(backtestResult.summary.max_drawdown * 100).toFixed(2)}%` : "-"}
            </span>
          </div>
        </div>

        <div className="backtest-chart">
          {equityPath ? (
            <svg viewBox="0 0 520 160" className="equity-chart" role="img">
              <path d={equityPath} className="equity-line" />
            </svg>
          ) : (
            <div className="backtest-placeholder">Run a backtest to see the equity curve.</div>
          )}
        </div>

        <div className="dukascopy-progress">
          <div className="progress-header">
            <span>Dukascopy Download Progress</span>
            <div className="progress-actions">
              <button className="button" onClick={fetchDukascopyProgress} disabled={dukasLoading}>
                {dukasLoading ? "Refreshing..." : "Refresh"}
              </button>
              <button
                className="button"
                onClick={() => sendDukascopyControl(dukasPaused ? "resume" : "pause")}
              >
                {dukasPaused ? "Resume" : "Pause"}
              </button>
              <button className="button" onClick={() => sendDukascopyControl("cancel")}>
                Cancel
              </button>
            </div>
          </div>
          {dukasProgress.length === 0 ? (
            <div className="backtest-placeholder">No active Dukascopy downloads yet.</div>
          ) : (
            <div className="progress-list progress-list-wide">
              {dukasProgress.map((item) => (
                <div key={item.source} className="progress-row">
                  <div className="progress-label">{item.source}</div>
                  <div className="progress-bar">
                    <div
                      className="progress-fill"
                      style={{ width: `${Math.min(item.percent, 100).toFixed(1)}%` }}
                    />
                  </div>
                  <div className="progress-meta">
                    {item.percent.toFixed(1)}% · {item.processed}/{item.total} hours
                  </div>
                  <div className="progress-meta">
                    Speed {item.speed.toFixed(2)} h/s · Retry {item.retry_attempts} · ETA{" "}
                    {formatEta(item.eta_seconds)}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {batchProgress ? (
          <div className="batch-progress">
            <div className="progress-header">
              <span>Batch Backtest Progress</span>
              <div className="progress-actions">
                <span className="progress-state">{batchProgress.status}</span>
                <button
                  className="button"
                  onClick={cancelBatchBacktest}
                  disabled={batchCanceling}
                >
                  {batchCanceling ? "Canceling..." : "Cancel"}
                </button>
              </div>
            </div>
            <div className="progress-row">
              <div className="progress-label">
                {batchProgress.current_year
                  ? `Year ${batchProgress.current_year}`
                  : "Waiting"}
              </div>
              <div className="progress-bar">
                <div
                  className="progress-fill"
                  style={{
                    width:
                      batchProgress.total > 0
                        ? `${(batchProgress.processed / batchProgress.total) * 100}%`
                        : "0%",
                  }}
                />
              </div>
              <div className="progress-meta">
                {batchProgress.processed}/{batchProgress.total} years
              </div>
            </div>
          </div>
        ) : null}

        <div className="backtest-sessions">
          {(backtestResult?.session_breakdown ?? []).map((session) => (
            <div className="session-card" key={session.session}>
              <div className="session-title">{session.session.toUpperCase()}</div>
              <div className="session-row">
                <span>Trades</span>
                <span>{session.total_trades}</span>
              </div>
              <div className="session-row">
                <span>Win Rate</span>
                <span>{(session.win_rate * 100).toFixed(1)}%</span>
              </div>
              <div className="session-row">
                <span>Profit Factor</span>
                <span>{session.profit_factor ? session.profit_factor.toFixed(2) : "-"}</span>
              </div>
              <div className="session-row">
                <span>Return</span>
                <span>{session.return_pct.toFixed(2)}%</span>
              </div>
            </div>
          ))}
        </div>

        {gridResult ? (
          <div className="grid-results">
            <div className="grid-header">
              Top Grid Results (tested {gridResult.meta.combinations_tested})
            </div>
            <div className="grid-table">
              {gridResult.results.map((result, idx) => (
                <div className="grid-row" key={`${result.score}-${idx}`}>
                  <span className="grid-rank">#{idx + 1}</span>
                  <span className="grid-metric">
                    Return {result.summary.return_pct.toFixed(2)}%
                  </span>
                  <span className="grid-metric">
                    PF {result.summary.profit_factor ? result.summary.profit_factor.toFixed(2) : "-"}
                  </span>
                  <span className="grid-metric">
                    DD {(result.summary.max_drawdown * 100).toFixed(2)}%
                  </span>
                  <span className="grid-params">
                    sweep {result.params.sweep_atr_mult} · return {result.params.return_within_bars} · fvg{" "}
                    {result.params.fvg_min_atr_mult} · retrace {result.params.fvg_retrace_window} · stop{" "}
                    {result.params.stop_atr_mult} · RR {result.params.target_rr}
                  </span>
                </div>
              ))}
            </div>
          </div>
        ) : null}

        {batchResult ? (
          <div className="grid-results">
            <div className="grid-header">
              Yearly Batch Results ({batchResult.meta.start_year}–{batchResult.meta.end_year})
            </div>
            <div className="grid-table">
              {batchResult.results.map((result) => (
                <div className="grid-row" key={result.year}>
                  <span className="grid-rank">{result.year}</span>
                  <span className="grid-metric">
                    Return {result.summary.return_pct.toFixed(2)}%
                  </span>
                  <span className="grid-metric">
                    PF {result.summary.profit_factor ? result.summary.profit_factor.toFixed(2) : "-"}
                  </span>
                  <span className="grid-metric">
                    DD {(result.summary.max_drawdown * 100).toFixed(2)}%
                  </span>
                  <span className="grid-params">
                    Trades {result.total_trades} · Win {(result.summary.win_rate * 100).toFixed(1)}%
                  </span>
                </div>
              ))}
            </div>
          </div>
        ) : null}

        {backtestError ? <div className="backtest-error">{backtestError}</div> : null}
        {gridError ? <div className="backtest-error">{gridError}</div> : null}
        {batchError ? <div className="backtest-error">{batchError}</div> : null}
      </section>

      {mode === "chart" ? (
        <section className="chart-shell">
          <div className="chart-panel">
            <div ref={chartContainerRef} className="chart chart-primary" />
            <div className="chart-overlay">
              {resolutionLabel && resolutionLabel !== interval
                ? `Resolution: ${resolutionLabel} (requested ${interval})`
                : `Resolution: ${resolutionLabel ?? meta?.interval ?? interval}`}
            </div>
          </div>
          <div
            className={`chart-rsi-shell ${indicatorVisibility.rsi ? "" : "is-hidden"}`}
          >
            <div ref={rsiChartContainerRef} className="chart chart-rsi" />
          </div>
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
