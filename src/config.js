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
export const HISTORY_LIMIT = Math.max(100, numberFromEnv(process.env.HISTORY_LIMIT, 1500));
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
  numberFromEnv(process.env.FRONTEND_HISTORY_LIMIT, 180),
  60,
  HISTORY_LIMIT
);

// Important: domestic symbols may need adjustment based on your data source.
// This first version is intentionally easy to edit.
export const ASSETS = [
  {
    key: 'gold',
    label: '黄金',
    unitLabel: 'CNY / g (normalized)',
    domestic: {
      source: 'sina-futures',
      symbol: 'au0',
      label: '沪金主力',
      unit: 'CNY_PER_GRAM'
    },
    external: {
      source: 'binance',
      symbol: 'PAXGUSDT',
      label: 'Binance PAXG/USDT proxy',
      unit: 'USD_PER_TROY_OUNCE'
    }
  },
  {
    key: 'silver',
    label: '白银',
    unitLabel: 'CNY / kg (normalized)',
    domestic: {
      source: 'sina-futures',
      symbol: 'ag0',
      label: '沪银主力',
      unit: 'CNY_PER_KG'
    },
    external: {
      source: 'binance',
      symbol: 'XAGUSDT',
      label: 'Binance XAG/USDT proxy',
      unit: 'USD_PER_TROY_OUNCE'
    }
  },
  {
    key: 'oil',
    label: '原油',
    unitLabel: 'CNY / barrel (normalized)',
    domestic: {
      source: 'sina-futures',
      symbol: 'sc0',
      label: '原油主力',
      unit: 'CNY_PER_BARREL'
    },
    external: {
      source: 'binance',
      symbol: 'WTIUSDT',
      label: 'Binance WTI/USDT proxy',
      unit: 'USD_PER_BARREL'
    }
  }
];
