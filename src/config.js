function numberFromEnv(rawValue, fallback) {
  const value = Number(rawValue);
  return Number.isFinite(value) ? value : fallback;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

export const APP_NAME = 'Spread Monitor MVP';
export const PORT = clamp(numberFromEnv(process.env.PORT, 3000), 1, 65535);
export const POLL_INTERVAL_MS = clamp(
  numberFromEnv(process.env.POLL_INTERVAL_MS, 10000),
  5000,
  15000
);
export const HISTORY_LIMIT = Math.max(1000, numberFromEnv(process.env.HISTORY_LIMIT, 200000));
export const ROLLING_WINDOW_SIZE = clamp(
  numberFromEnv(process.env.ROLLING_WINDOW_SIZE, 120),
  20,
  HISTORY_LIMIT
);
export const FETCH_TIMEOUT_MS = clamp(
  numberFromEnv(process.env.FETCH_TIMEOUT_MS, 8000),
  2000,
  15000
);
export const FRONTEND_HISTORY_LIMIT = clamp(
  numberFromEnv(process.env.FRONTEND_HISTORY_LIMIT, 5000),
  120,
  HISTORY_LIMIT
);
export const HISTORY_SAMPLE_INTERVAL_MS = Math.max(
  10000,
  numberFromEnv(process.env.HISTORY_SAMPLE_INTERVAL_MS, 60000)
);

// Important: domestic symbols may need adjustment based on your data source.
// This first version is intentionally easy to edit.
export const ASSETS = [
  {
    key: 'gold',
    label: '黄金',
    unitLabel: 'USD / oz (comparable)',
    domestic: {
      source: 'sina-futures',
      symbol: 'nf_AU0',
      label: '沪金主力',
      unit: 'CNY_PER_GRAM'
    },
    external: {
      source: 'binance-futures',
      symbol: 'XAUUSDT',
      label: 'Binance Futures XAUUSDT',
      unit: 'USD_PER_TROY_OUNCE'
    }
  },
  {
    key: 'silver',
    label: '白银',
    unitLabel: 'USD / oz (comparable)',
    domestic: {
      source: 'sina-futures',
      symbol: 'nf_AG0',
      label: '沪银主力',
      unit: 'CNY_PER_KG'
    },
    external: {
      source: 'binance-futures',
      symbol: 'XAGUSDT',
      label: 'Binance Futures XAGUSDT',
      unit: 'USD_PER_TROY_OUNCE'
    }
  },
  {
    key: 'oil',
    label: '原油',
    unitLabel: 'USD / barrel (comparable)',
    domestic: {
      source: 'sina-futures',
      symbol: 'nf_SC0',
      label: '原油主力',
      unit: 'CNY_PER_BARREL'
    },
    external: {
      source: 'hyperliquid',
      symbol: 'xyz:CL',
      label: 'Hyperliquid CL',
      unit: 'USD_PER_BARREL'
    }
  }
];
