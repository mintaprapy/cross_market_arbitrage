import {
  APP_NAME,
  ASSETS,
  HISTORY_LIMIT,
  POLL_INTERVAL_MS,
  ROLLING_WINDOW_SIZE
} from './config.js';
import { fetchQuote, fetchUsdCny } from './dataSources.js';
import {
  computeSpread,
  convertExternalToComparableUnit,
  isFiniteNumber,
  percentile,
  takeLastNumeric,
  zScore
} from './normalization.js';

function errorMessage(error) {
  if (error instanceof Error) return error.message;
  return String(error);
}

function cloneSample(sample) {
  return sample ? JSON.parse(JSON.stringify(sample)) : null;
}

function createEmptySample(asset) {
  return {
    asset: asset.key,
    label: asset.label,
    unit_label: asset.unitLabel,
    timestamp: null,
    status: 'waiting',
    usd_cny: null,
    domestic_price: null,
    external_price: null,
    external_comparable_price: null,
    spread_abs: null,
    spread_pct: null,
    z_score: null,
    percentile: null,
    errors: [],
    domestic: {
      ...asset.domestic,
      price: null,
      at: null,
      error: null
    },
    external: {
      ...asset.external,
      price: null,
      at: null,
      error: null
    }
  };
}

function quoteFromResult(config, result) {
  if (result.status === 'fulfilled') {
    return {
      ...config,
      price: isFiniteNumber(result.value.price) ? result.value.price : null,
      at: result.value.at ?? new Date().toISOString(),
      error: null
    };
  }

  return {
    ...config,
    price: null,
    at: null,
    error: errorMessage(result.reason)
  };
}

function sampleStatus(domestic, external, spread) {
  if (isFiniteNumber(spread.absolute)) return 'ok';
  if (isFiniteNumber(domestic.price) || isFiniteNumber(external.price)) return 'partial';
  return 'error';
}

function trimHistory(history, limit) {
  if (history.length <= limit) return history;
  history.splice(0, history.length - limit);
  return history;
}

export class SpreadMonitor {
  constructor(options = {}) {
    this.assets = options.assets ?? ASSETS;
    this.historyLimit = options.historyLimit ?? HISTORY_LIMIT;
    this.pollIntervalMs = options.pollIntervalMs ?? POLL_INTERVAL_MS;
    this.rollingWindowSize = options.rollingWindowSize ?? ROLLING_WINDOW_SIZE;
    this.startedAt = new Date().toISOString();
    this.lastPollAt = null;
    this.lastPollFinishedAt = null;
    this.latestUsdCny = null;
    this.isPolling = false;
    this.timer = null;
    this.state = new Map(
      this.assets.map((asset) => [
        asset.key,
        {
          asset,
          latest: createEmptySample(asset),
          history: [],
          pollCount: 0,
          successCount: 0,
          lastError: null
        }
      ])
    );
  }

  start() {
    if (this.timer) return;
    this.pollOnce().catch((error) => {
      console.error('Initial poll failed:', errorMessage(error));
    });
    this.timer = setInterval(() => {
      this.pollOnce().catch((error) => {
        console.error('Polling cycle failed:', errorMessage(error));
      });
    }, this.pollIntervalMs);
  }

  stop() {
    if (!this.timer) return;
    clearInterval(this.timer);
    this.timer = null;
  }

  async pollOnce() {
    if (this.isPolling) return;
    this.isPolling = true;
    this.lastPollAt = new Date().toISOString();

    try {
      this.latestUsdCny = await fetchUsdCny();
      await Promise.all(this.assets.map((asset) => this.pollAsset(asset, this.latestUsdCny)));
      this.lastPollFinishedAt = new Date().toISOString();
    } finally {
      this.isPolling = false;
    }
  }

  async pollAsset(asset, usdCny) {
    const entry = this.state.get(asset.key);
    const timestamp = new Date().toISOString();

    const [domesticResult, externalResult] = await Promise.allSettled([
      fetchQuote(asset.domestic.source, asset.domestic.symbol),
      fetchQuote(asset.external.source, asset.external.symbol)
    ]);

    const domestic = quoteFromResult(asset.domestic, domesticResult);
    const external = quoteFromResult(asset.external, externalResult);
    const domesticPrice = isFiniteNumber(domestic.price) ? domestic.price : null;
    const externalComparablePrice = convertExternalToComparableUnit(
      asset.key,
      external.price,
      asset.external.unit,
      usdCny
    );
    const spread = computeSpread(domesticPrice, externalComparablePrice);
    const historicalSpreads = takeLastNumeric(
      entry.history.map((sample) => sample.spread_abs),
      Math.max(this.rollingWindowSize - 1, 0)
    );
    const rollingSpreads = isFiniteNumber(spread.absolute)
      ? [...historicalSpreads, spread.absolute]
      : historicalSpreads;

    const errors = [domestic.error, external.error].filter(Boolean);
    const sample = {
      asset: asset.key,
      label: asset.label,
      unit_label: asset.unitLabel,
      timestamp,
      status: sampleStatus(domestic, external, spread),
      usd_cny: usdCny,
      domestic_price: domesticPrice,
      external_price: isFiniteNumber(external.price) ? external.price : null,
      external_comparable_price: externalComparablePrice,
      spread_abs: spread.absolute,
      spread_pct: spread.percent,
      z_score: zScore(spread.absolute, rollingSpreads),
      percentile: percentile(spread.absolute, rollingSpreads),
      errors,
      domestic,
      external
    };

    entry.pollCount += 1;
    if (sample.status === 'ok') entry.successCount += 1;
    entry.lastError = errors[0] ?? null;
    entry.latest = sample;
    entry.history.push(sample);
    trimHistory(entry.history, this.historyLimit);
  }

  getHealth() {
    const uptimeMs = Date.now() - new Date(this.startedAt).getTime();

    return {
      ok: true,
      service: APP_NAME,
      started_at: this.startedAt,
      uptime_sec: Math.round(uptimeMs / 1000),
      poll_interval_ms: this.pollIntervalMs,
      rolling_window_size: this.rollingWindowSize,
      history_limit: this.historyLimit,
      is_polling: this.isPolling,
      last_poll_at: this.lastPollAt,
      last_poll_finished_at: this.lastPollFinishedAt,
      usd_cny: this.latestUsdCny,
      assets: this.assets.map((asset) => {
        const entry = this.state.get(asset.key);
        return {
          key: asset.key,
          label: asset.label,
          status: entry.latest.status,
          last_sample_at: entry.latest.timestamp,
          sample_count: entry.history.length,
          ok_count: entry.successCount,
          last_error: entry.lastError
        };
      })
    };
  }

  getSnapshot() {
    return {
      as_of: this.lastPollFinishedAt ?? this.lastPollAt ?? new Date().toISOString(),
      poll_interval_ms: this.pollIntervalMs,
      rolling_window_size: this.rollingWindowSize,
      history_limit: this.historyLimit,
      usd_cny: this.latestUsdCny,
      assets: this.assets.map((asset) => cloneSample(this.state.get(asset.key).latest))
    };
  }

  getHistory(assetKey, limit = 300) {
    const entry = this.state.get(assetKey);
    if (!entry) return null;

    const safeLimit = Math.max(1, Math.min(Number(limit) || 300, this.historyLimit));
    return {
      asset: {
        key: entry.asset.key,
        label: entry.asset.label,
        unit_label: entry.asset.unitLabel,
        domestic: entry.asset.domestic,
        external: entry.asset.external
      },
      limit: safeLimit,
      points: entry.history.slice(-safeLimit).map((sample) => cloneSample(sample))
    };
  }
}
