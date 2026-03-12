const assetGrid = document.getElementById('asset-grid');
const statusStrip = document.getElementById('status-strip');
const pollIntervalEl = document.getElementById('poll-interval');
const usdCnyEl = document.getElementById('usd-cny');
const lastRefreshEl = document.getElementById('last-refresh');
const assetTemplate = document.getElementById('asset-template');
const rangeControls = document.getElementById('range-controls');

const REFRESH_EVERY_MS = 5000;
const HISTORY_LIMIT = 200000;
let selectedRangeHours = 24;
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
  domesticMeta.textContent = `${asset.domestic.label} · raw ${formatNumber(asset.domestic_raw_price, 3)} (${asset.domestic.unit}) · ${formatTimestamp(asset.domestic.at)}`;
  externalMeta.textContent =
    `${asset.external.label} · ${formatNumber(asset.external_price, 3)} (${asset.external.unit}) · ${formatTimestamp(asset.external.at)}`;

  if (Array.isArray(asset.errors) && asset.errors.length) {
    errorBox.classList.remove('hidden');
    errorBox.textContent = asset.errors.join(' | ');
  }

  drawSingleAxisChart(priceChart, history.points, [
    { key: 'domestic_price', color: '#b7791f', label: '大陆换算价' },
    { key: 'external_comparable_price', color: '#245c46', label: '非大陆市场价' }
  ]);

  drawDualAxisChart(
    spreadChart,
    history.points,
    { key: 'spread_abs', color: '#56747f', label: 'Spread Abs' },
    { key: 'spread_pct', color: '#7a3fb0', label: 'Spread Pct' },
    { drawZeroLine: true, rightAxisPercent: true, rightAxisTight: true }
  );

  attachChartHover(priceChart, history.points, (point) => {
    return [
      `时间: ${formatTimestamp(point.timestamp)}`,
      `大陆换算价: ${formatNumber(point.domestic_price, 3)}`,
      `非大陆市场价: ${formatNumber(point.external_comparable_price, 3)}`
    ];
  });

  attachChartHover(spreadChart, history.points, (point) => {
    return [
      `时间: ${formatTimestamp(point.timestamp)}`,
      `Spread Abs: ${formatNumber(point.spread_abs, 3)}`,
      `Spread Pct: ${formatPercent(point.spread_pct)}`
    ];
  });

  return card;
}

function drawXAxisTicks(ctx, points, width, height, padding, drawableWidth) {
  if (!points.length) return;
  const tickCount = 5;
  ctx.fillStyle = '#5a6a72';
  ctx.font = '11px "Avenir Next", sans-serif';

  for (let i = 0; i < tickCount; i += 1) {
    const ratio = tickCount === 1 ? 0 : i / (tickCount - 1);
    const idx = Math.round((points.length - 1) * ratio);
    const x = padding.left + drawableWidth * ratio;
    const ts = points[idx]?.timestamp;
    const label = ts ? new Date(ts).toLocaleTimeString() : '--';
    const w = ctx.measureText(label).width;
    const drawX = i === 0 ? x : i === tickCount - 1 ? x - w : x - w / 2;
    ctx.fillText(label, drawX, height - 8);
  }
}

function drawYAxisTicks(ctx, min, max, x, top, height, color, formatter) {
  const tickCount = 5;
  ctx.fillStyle = color;
  ctx.font = '11px "Avenir Next", sans-serif';
  for (let i = 0; i < tickCount; i += 1) {
    const ratio = i / (tickCount - 1);
    const value = max - (max - min) * ratio;
    const y = top + height * ratio;
    const label = formatter(value);
    ctx.fillText(label, x - ctx.measureText(label).width / 2, y + 3);
  }
}

function drawSingleAxisChart(canvas, points, series) {
  const ratio = window.devicePixelRatio || 1;
  const width = canvas.clientWidth || canvas.width;
  const height = canvas.clientHeight || canvas.height;
  canvas.width = Math.round(width * ratio);
  canvas.height = Math.round(height * ratio);

  const ctx = canvas.getContext('2d');
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
  ctx.clearRect(0, 0, width, height);

  const padding = { top: 16, right: 16, bottom: 24, left: 50 };
  const drawableWidth = width - padding.left - padding.right;
  const drawableHeight = height - padding.top - padding.bottom;

  const values = points.flatMap((point) =>
    series.map((item) => point[item.key]).filter((value) => Number.isFinite(value))
  );

  if (!values.length) {
    ctx.fillStyle = '#5a6a72';
    ctx.font = '13px "Avenir Next", sans-serif';
    ctx.fillText('No comparable data yet', padding.left, height / 2);
    return;
  }

  let min = Math.min(...values);
  let max = Math.max(...values);
  if (min === max) {
    min -= 1;
    max += 1;
  }
  const span = max - min;
  const yMin = min - span * 0.08;
  const yMax = max + span * 0.08;

  const xForIndex = (index) =>
    points.length <= 1 ? padding.left + drawableWidth / 2 : padding.left + (drawableWidth * index) / (points.length - 1);
  const yForValue = (value) => padding.top + ((yMax - value) / (yMax - yMin)) * drawableHeight;

  ctx.lineWidth = 1;
  ctx.strokeStyle = 'rgba(21, 34, 42, 0.1)';
  for (let step = 0; step <= 4; step += 1) {
    const y = padding.top + (drawableHeight / 4) * step;
    ctx.beginPath();
    ctx.moveTo(padding.left, y);
    ctx.lineTo(width - padding.right, y);
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

  drawYAxisTicks(ctx, yMin, yMax, 26, padding.top, drawableHeight, '#5a6a72', (v) => v.toFixed(2));
  drawXAxisTicks(ctx, points, width, height, padding, drawableWidth);

  // legend
  ctx.font = '11px "Avenir Next", sans-serif';
  let legendX = 8;
  for (const item of series) {
    ctx.fillStyle = item.color;
    ctx.fillText(item.label, legendX, 16);
    legendX += ctx.measureText(item.label).width + 14;
  }
}

function drawDualAxisChart(canvas, points, leftSeries, rightSeries, options = {}) {
  const ratio = window.devicePixelRatio || 1;
  const width = canvas.clientWidth || canvas.width;
  const height = canvas.clientHeight || canvas.height;
  canvas.width = Math.round(width * ratio);
  canvas.height = Math.round(height * ratio);

  const ctx = canvas.getContext('2d');
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
  ctx.clearRect(0, 0, width, height);

  const padding = { top: 16, right: 50, bottom: 24, left: 50 };
  const drawableWidth = width - padding.left - padding.right;
  const drawableHeight = height - padding.top - padding.bottom;

  const leftValues = points.map((p) => p[leftSeries.key]).filter((v) => Number.isFinite(v));
  const rightValues = points.map((p) => p[rightSeries.key]).filter((v) => Number.isFinite(v));

  if (!leftValues.length && !rightValues.length) {
    ctx.fillStyle = '#5a6a72';
    ctx.font = '13px "Avenir Next", sans-serif';
    ctx.fillText('No comparable data yet', padding.left, height / 2);
    return;
  }

  function axisRange(values, { tight = false, includeZero = false } = {}) {
    if (!values.length) return { min: -1, max: 1 };
    let min = Math.min(...values);
    let max = Math.max(...values);

    if (includeZero) {
      min = Math.min(min, 0);
      max = Math.max(max, 0);
    }

    let span = max - min;
    if (span === 0) {
      const base = Math.max(Math.abs(max), 1);
      span = tight ? base * 0.02 : 2;
      min -= span / 2;
      max += span / 2;
    }

    const pad = tight ? Math.max(span * 0.2, 0.0008) : span * 0.08;
    return { min: min - pad, max: max + pad };
  }

  const leftRange = axisRange(leftValues, { includeZero: !!options.drawZeroLine });
  const rightRange = axisRange(rightValues, {
    includeZero: !!options.drawZeroLine,
    tight: !!options.rightAxisTight
  });

  const xForIndex = (index) =>
    points.length <= 1 ? padding.left + drawableWidth / 2 : padding.left + (drawableWidth * index) / (points.length - 1);

  const yForLeft = (value) => padding.top + ((leftRange.max - value) / (leftRange.max - leftRange.min)) * drawableHeight;
  const yForRight = (value) => padding.top + ((rightRange.max - value) / (rightRange.max - rightRange.min)) * drawableHeight;

  ctx.lineWidth = 1;
  ctx.strokeStyle = 'rgba(21, 34, 42, 0.1)';
  for (let step = 0; step <= 4; step += 1) {
    const y = padding.top + (drawableHeight / 4) * step;
    ctx.beginPath();
    ctx.moveTo(padding.left, y);
    ctx.lineTo(width - padding.right, y);
    ctx.stroke();
  }

  if (options.drawZeroLine) {
    if (leftRange.min <= 0 && leftRange.max >= 0) {
      ctx.strokeStyle = 'rgba(161, 58, 50, 0.18)';
      ctx.beginPath();
      ctx.moveTo(padding.left, yForLeft(0));
      ctx.lineTo(width - padding.right, yForLeft(0));
      ctx.stroke();
    }
  }

  function drawLine(series, yFn) {
    ctx.strokeStyle = series.color;
    ctx.lineWidth = 2.2;
    ctx.beginPath();
    let started = false;
    points.forEach((point, index) => {
      const value = point[series.key];
      if (!Number.isFinite(value)) {
        started = false;
        return;
      }
      const x = xForIndex(index);
      const y = yFn(value);
      if (!started) {
        ctx.moveTo(x, y);
        started = true;
      } else {
        ctx.lineTo(x, y);
      }
    });
    ctx.stroke();
  }

  drawLine(leftSeries, yForLeft);
  drawLine(rightSeries, yForRight);

  // axis labels
  ctx.font = '11px "Avenir Next", sans-serif';
  ctx.fillStyle = leftSeries.color;
  ctx.fillText(leftSeries.label, 8, 16);
  ctx.fillStyle = rightSeries.color;
  const rightLabelWidth = ctx.measureText(rightSeries.label).width;
  ctx.fillText(rightSeries.label, width - rightLabelWidth - 8, 16);

  const formatRightAxis = (value) => {
    if (!Number.isFinite(value)) return '--';
    if (options.rightAxisPercent) return `${(value * 100).toFixed(2)}%`;
    return value.toFixed(2);
  };

  drawYAxisTicks(ctx, leftRange.min, leftRange.max, 26, padding.top, drawableHeight, leftSeries.color, (v) => v.toFixed(2));
  drawYAxisTicks(
    ctx,
    rightRange.min,
    rightRange.max,
    width - 26,
    padding.top,
    drawableHeight,
    rightSeries.color,
    formatRightAxis
  );
  drawXAxisTicks(ctx, points, width, height, padding, drawableWidth);
}

function attachChartHover(canvas, points, linesForPoint) {
  const panel = canvas.closest('.chart-panel');
  if (!panel) return;

  let tooltip = panel.querySelector('.chart-tooltip');
  if (!tooltip) {
    tooltip = document.createElement('div');
    tooltip.className = 'chart-tooltip hidden';
    panel.appendChild(tooltip);
  }

  const hide = () => tooltip.classList.add('hidden');

  canvas.onmousemove = (event) => {
    if (!Array.isArray(points) || points.length === 0) {
      hide();
      return;
    }

    const rect = canvas.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const ratio = rect.width <= 0 ? 0 : x / rect.width;
    const index = Math.max(0, Math.min(points.length - 1, Math.round(ratio * (points.length - 1))));
    const point = points[index];
    if (!point) {
      hide();
      return;
    }

    const rows = linesForPoint(point).filter(Boolean);
    tooltip.innerHTML = rows.join('<br/>');
    tooltip.classList.remove('hidden');
  };

  canvas.onmouseleave = hide;
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
        fetchJson(
          `/api/history?asset=${encodeURIComponent(asset.asset)}&limit=${HISTORY_LIMIT}&hours=${selectedRangeHours}`
        )
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

function setActiveRangeButton() {
  if (!rangeControls) return;
  rangeControls.querySelectorAll('button[data-hours]').forEach((btn) => {
    btn.classList.toggle('active', Number(btn.dataset.hours) === selectedRangeHours);
  });
}

if (rangeControls) {
  rangeControls.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-hours]');
    if (!button) return;
    const hours = Number(button.dataset.hours);
    if (!Number.isFinite(hours) || hours <= 0) return;
    selectedRangeHours = hours;
    setActiveRangeButton();
    refreshDashboard().catch((error) => console.error(error));
  });
}

window.addEventListener('resize', () => {
  scheduleRefresh(150);
});

setActiveRangeButton();
refreshDashboard().catch((error) => {
  console.error(error);
  scheduleRefresh();
});
