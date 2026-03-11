import { FETCH_TIMEOUT_MS } from './config.js';

const BINANCE_BASE = 'https://api.binance.com';
const USD_CNY_PAIR = 'USDCNY';

async function fetchWithTimeout(url, options = {}) {
  const { timeoutMs: timeoutOverride, ...fetchOptions } = options;
  const controller = new AbortController();
  const timeoutMs = timeoutOverride ?? FETCH_TIMEOUT_MS;
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, {
      ...fetchOptions,
      signal: controller.signal
    });
  } catch (error) {
    if (error?.name === 'AbortError') {
      throw new Error(`Request timed out after ${timeoutMs}ms`);
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

export async function fetchUsdCny() {
  try {
    const res = await fetchWithTimeout(`${BINANCE_BASE}/api/v3/ticker/price?symbol=${USD_CNY_PAIR}`);
    if (!res.ok) throw new Error(`Binance FX ${res.status}`);
    const json = await res.json();
    const price = Number(json.price);
    if (!Number.isFinite(price) || price <= 0) {
      throw new Error('Invalid FX price');
    }
    return price;
  } catch {
    // fallback: a sane static value keeps the dashboard usable if FX endpoint is unavailable
    return 7.2;
  }
}

export async function fetchBinanceTicker(symbol) {
  const res = await fetchWithTimeout(`${BINANCE_BASE}/api/v3/ticker/price?symbol=${symbol}`);
  if (!res.ok) throw new Error(`Binance ticker ${symbol} failed with ${res.status}`);
  const json = await res.json();
  const price = Number(json.price);
  if (!Number.isFinite(price) || price <= 0) {
    throw new Error(`Binance ticker ${symbol} returned invalid price`);
  }
  return {
    price,
    sourceSymbol: json.symbol,
    at: new Date().toISOString()
  };
}

export async function fetchSinaFutures(symbol) {
  const url = `https://hq.sinajs.cn/list=${symbol}`;
  const res = await fetchWithTimeout(url, {
    headers: {
      Referer: 'https://finance.sina.com.cn',
      'User-Agent': 'Mozilla/5.0'
    }
  });

  if (!res.ok) throw new Error(`Sina futures ${symbol} failed with ${res.status}`);
  const text = await res.text();
  const match = text.match(/="([^"]*)"/);
  if (!match) throw new Error(`Unexpected Sina response for ${symbol}`);
  const parts = match[1].split(',');
  if (!parts.length || parts.every((item) => item === '')) {
    throw new Error(`Empty Sina payload for ${symbol}`);
  }

  // Sina futures payloads vary a bit by contract family. For the "0" continuous symbols,
  // latest price is usually at index 8 or 9; we try a few candidates.
  const candidates = [8, 9, 10, 3, 1]
    .map((index) => Number(parts[index]))
    .filter((value) => Number.isFinite(value) && value > 0);

  if (!candidates.length) throw new Error(`Could not parse price for ${symbol}`);

  return {
    price: candidates[0],
    raw: parts,
    at: new Date().toISOString()
  };
}

export async function fetchQuote(source, symbol) {
  if (source === 'binance') return fetchBinanceTicker(symbol);
  if (source === 'sina-futures') return fetchSinaFutures(symbol);
  throw new Error(`Unsupported source: ${source}`);
}
