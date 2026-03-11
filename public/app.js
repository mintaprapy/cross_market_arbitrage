const assetGrid = document.getElementById('asset-grid');
const statusStrip = document.getElementById('status-strip');
const pollIntervalEl = document.getElementById('poll-interval');
const usdCnyEl = document.getElementById('usd-cny');
const lastRefreshEl = document.getElementById('last-refresh');
const assetTemplate = document.getElementById('asset-template');

const REFRESH_EVERY_MS = 5000;
const HISTORY_LIMIT = 180;
let refreshTimer = null;
let refreshInFlight = false;

function formatNumber(value, digits = 2) {
  if (!Number.isFinite(value)) return '--';
  return value.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  });
}

function formatPercent(value) {
  if (!Number.isFinite(value)) return '--';
  return `${(value * 100).toFixed(2)}%`;
}

function formatPercentile(value) {
  if (!Number.isFinite(value)) return '--';
  return `${(value * 100).toFixed(0)}%`;
}

function formatTimestamp(value) {
  if (!value) return 'Waiting';
  return new Date(value).toLocaleString();
}

function statusLabel(status) {
  switch (status) {
    case 'ok':
      return 'Live';
    case 'partial':
      return 'Partial';
    case 'error':
      return 'Missing';
    default:
      return 'Waiting';
  }
}

async function fetchJson(url) {
  const response = await fetch(url, { headers: { Accept: 'application/json' } });
  if (!response.ok) {
    throw new Error(`${url} returned ${response.status}`);
  }
  return response.json();
}

function clearTimer() {
  if (refreshTimer) {
    clearTimeout(refreshTimer);
    refreshTimer = null;
  }
}

function scheduleRefresh(delay = REFRESH_EVERY_MS) {
  clearTimer();
  refreshTimer = setTimeout(() => {
    refreshDashboard().catch((error) => {
      console.error(error);
    });
  }, delay);
}

function renderStatusPills(snapshot, health) {
  const pills = [];
  pills.push(
    `<span class="status-pill muted">Monitor ${health.is_polling ? 'polling' : 'idle'}</span>`
  );

  for (const asset of snapshot.assets) {
    pills.push(
      `<span class="status-pill ${asset.status}">${asset.label} ${statusLabel(asset.status)}</span>`
    );
  }

  statusStrip.innerHTML = pills.join('');
}

function metricBox(label, value, note = '') {
  return `
    <div class="metric-box">
      <span>${label}</span>
      <strong>${value}</strong>
      <small>${note}</small>
    </div>
  `;
}

function createAssetCard(asset, history) {
  const fragment = assetTemplate.content.cloneNode(true);
  const card = fragment.querySelector('.asset-card');
  const title = fragment.querySelector('h2');
  const eyebrow = fragment.querySelector('.asset-eyebrow');
  const pill = fragment.querySelector('.status-pill');
  const metricsGrid = fragment.querySelector('.metrics-grid');
  const domesticPrice = fragment.querySelector('.domestic-price');
  const externalPrice = fragment.querySelector('.external-price');
  const domesticMeta = fragment.querySelector('.domestic-meta');
  const externalMeta = fragment.querySelector('.external-meta');
  const errorBox = fragment.querySelector('.error-box');
  const priceChart = fragment.querySelector('.price-chart');
  const spreadChart = fragment.querySelector('.spread-chart');

  eyebrow.textContent = asset.unit_label;
  title.textContent = asset.label;
  pill.classList.add(asset.status || 'waiting');
  pill.textContent = statusLabel(asset.status);

  metricsGrid.innerHTML = [
    metricBox('Spread abs', formatNumber(asset.spread_abs, 3), asset.unit_label),
    metricBox('Spread pct', formatPercent(asset.spread_pct), 'Domestic vs comparable'),
    metricBox('Z-score', formatNumber(asset.z_score, 2), 'Rolling spread'),
    metricBox('Percentile', formatPercentile(asset.percentile), 'Rolling spread rank')
  ].join('');

  domesticPrice.textContent = formatNumber(asset.domestic_price, 3);
  externalPrice.textContent = formatNumber(asset.external_comparable_price, 3);
  domesticMeta.textContent = `${asset.domestic.label} · ${formatTimestamp(asset.domestic.at)}`;
  externalMeta.textContent =
    `${asset.external.label} · raw ${formatNumber(asset.external_price, 3)} · ${formatTimestamp(asset.external.at)}`;

  if (Array.isArray(asset.errors) && asset.errors.length) {
    errorBox.classList.remove('hidden');
    errorBox.textContent = asset.errors.join(' | ');
  }

  drawChart(priceChart, history.points, [
    {
      key: 'domestic_price',
      color: '#b7791f'
    },
    {
      key: 'external_comparable_price',
      color: '#245c46'
    }
  ]);

  drawChart(
    spreadChart,
    history.points,
    [
      {
        key: 'spread_abs',
        color: '#56747f'
      }
    ],
    { drawZeroLine: true }
  );

  return card;
}

function drawChart(canvas, points, series, options = {}) {
  const ratio = window.devicePixelRatio || 1;
  const width = canvas.clientWidth || canvas.width;
  const height = canvas.clientHeight || canvas.height;
  canvas.width = Math.round(width * ratio);
  canvas.height = Math.round(height * ratio);

  const ctx = canvas.getContext('2d');
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
  ctx.clearRect(0, 0, width, height);

  const padding = { top: 16, right: 14, bottom: 24, left: 14 };
  const drawableWidth = width - padding.left - padding.right;
  const drawableHeight = height - padding.top - padding.bottom;
  const values = points.flatMap((point) =>
    series
      .map((item) => point[item.key])
      .filter((value) => Number.isFinite(value))
  );

  ctx.lineWidth = 1;
  ctx.strokeStyle = 'rgba(21, 34, 42, 0.1)';
  for (let step = 0; step <= 4; step += 1) {
    const y = padding.top + (drawableHeight / 4) * step;
    ctx.beginPath();
    ctx.moveTo(padding.left, y);
    ctx.lineTo(width - padding.right, y);
    ctx.stroke();
  }

  if (!values.length) {
    ctx.fillStyle = '#5a6a72';
    ctx.font = '13px "Avenir Next", sans-serif';
    ctx.fillText('No comparable data yet', padding.left, height / 2);
    return;
  }

  let minValue = Math.min(...values);
  let maxValue = Math.max(...values);

  if (options.drawZeroLine) {
    minValue = Math.min(minValue, 0);
    maxValue = Math.max(maxValue, 0);
  }

  if (minValue === maxValue) {
    minValue -= 1;
    maxValue += 1;
  }

  const spread = maxValue - minValue;
  const yMin = minValue - spread * 0.08;
  const yMax = maxValue + spread * 0.08;

  function xForIndex(index) {
    if (points.length <= 1) return padding.left + drawableWidth / 2;
    return padding.left + (drawableWidth * index) / (points.length - 1);
  }

  function yForValue(value) {
    return padding.top + ((yMax - value) / (yMax - yMin)) * drawableHeight;
  }

  if (options.drawZeroLine && yMin <= 0 && yMax >= 0) {
    ctx.strokeStyle = 'rgba(161, 58, 50, 0.18)';
    ctx.beginPath();
    ctx.moveTo(padding.left, yForValue(0));
    ctx.lineTo(width - padding.right, yForValue(0));
    ctx.stroke();
  }

  for (const item of series) {
    ctx.strokeStyle = item.color;
    ctx.lineWidth = 2.2;
    ctx.beginPath();

    let started = false;
    points.forEach((point, index) => {
      const value = point[item.key];
      if (!Number.isFinite(value)) {
        started = false;
        return;
      }

      const x = xForIndex(index);
      const y = yForValue(value);
      if (!started) {
        ctx.moveTo(x, y);
        started = true;
      } else {
        ctx.lineTo(x, y);
      }
    });

    ctx.stroke();
  }

  const firstPoint = points[0]?.timestamp;
  const lastPoint = points.at(-1)?.timestamp;
  ctx.fillStyle = '#5a6a72';
  ctx.font = '11px "Avenir Next", sans-serif';
  ctx.fillText(firstPoint ? new Date(firstPoint).toLocaleTimeString() : '', padding.left, height - 8);
  const lastLabel = lastPoint ? new Date(lastPoint).toLocaleTimeString() : '';
  const labelWidth = ctx.measureText(lastLabel).width;
  ctx.fillText(lastLabel, width - padding.right - labelWidth, height - 8);
}

function renderDashboard(snapshot, health, histories) {
  pollIntervalEl.textContent = `${Math.round(snapshot.poll_interval_ms / 1000)}s`;
  usdCnyEl.textContent = formatNumber(snapshot.usd_cny, 4);
  lastRefreshEl.textContent = formatTimestamp(snapshot.as_of);
  renderStatusPills(snapshot, health);

  assetGrid.innerHTML = '';
  for (const asset of snapshot.assets) {
    const history = histories.find((item) => item.asset.key === asset.asset) || { points: [] };
    assetGrid.appendChild(createAssetCard(asset, history));
  }
}

async function refreshDashboard() {
  if (refreshInFlight) return;
  refreshInFlight = true;

  try {
    const [snapshot, health] = await Promise.all([
      fetchJson('/api/snapshot'),
      fetchJson('/api/health')
    ]);

    const histories = await Promise.all(
      snapshot.assets.map((asset) =>
        fetchJson(`/api/history?asset=${encodeURIComponent(asset.asset)}&limit=${HISTORY_LIMIT}`)
      )
    );

    renderDashboard(snapshot, health, histories);
  } catch (error) {
    console.error(error);
    statusStrip.innerHTML = `<span class="status-pill error">Refresh failed</span>`;
  } finally {
    refreshInFlight = false;
    scheduleRefresh();
  }
}

window.addEventListener('resize', () => {
  scheduleRefresh(150);
});

refreshDashboard().catch((error) => {
  console.error(error);
  scheduleRefresh();
});
