    async function fetchJson(url) {
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`Request failed: ${response.status}`);
      }
      return await response.json();
    }

    async function postJson(url, payload) {
      const response = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      });
      if (!response.ok) {
        throw new Error(`Request failed: ${response.status}`);
      }
      return await response.json();
    }

    function toFiniteNumber(value) {
      if (value === null || value === undefined || value === "") return null;
      const numeric = Number(value);
      return Number.isFinite(numeric) ? numeric : null;
    }

    function formatNumber(value, digits = 2) {
      const numeric = toFiniteNumber(value);
      return numeric === null ? "--" : numeric.toFixed(digits);
    }

    function formatSignedNumber(value, digits = 2) {
      const numeric = toFiniteNumber(value);
      if (numeric === null) return "--";
      const prefix = numeric > 0 ? "+" : "";
      return `${prefix}${numeric.toFixed(digits)}`;
    }

    function formatPct(value) {
      const numeric = toFiniteNumber(value);
      return numeric === null ? "--" : `${(numeric * 100).toFixed(2)}%`;
    }

    function formatPctPoint(value) {
      const numeric = toFiniteNumber(value);
      return numeric === null ? "--" : `${numeric.toFixed(2)}%`;
    }

    function formatTableNumber(value, digits = 2) {
      const numeric = toFiniteNumber(value);
      if (numeric === null) return "--";
      return numeric.toLocaleString("en-US", {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
      });
    }

    function formatTableSignedNumber(value, digits = 2) {
      const numeric = toFiniteNumber(value);
      if (numeric === null) return "--";
      const prefix = numeric > 0 ? "+" : "";
      return `${prefix}${formatTableNumber(numeric, digits)}`;
    }

    function formatTablePct(value, digits = 2) {
      const numeric = toFiniteNumber(value);
      if (numeric === null) return "--";
      return `${(numeric * 100).toLocaleString("en-US", {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
      })}%`;
    }

    function escapeHtml(value) {
      if (value === null || value === undefined) return "";
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
    }

    const DISPLAY_NAME_MAP = {
      AU_XAU: "黄金 AU / XAU",
      AG_XAG: "白银 AG / XAG",
      AG_XAG_GROSS: "白银 AG / XAG",
      AG_XAG_NET: "白银 AG / XAG",
      CU_COPPER: "铜 CU / COPPER",
      CU_COPPER_GROSS: "铜 CU / COPPER",
      CU_COPPER_NET: "铜 CU / COPPER",
      BC_COPPER: "国际铜 BC / COPPER",
      SC_CL: "原油 SC / CL",
    };

    const STATUS_LABELS = {
      ok: "正常",
      partial: "部分可用",
      stale: "数据过期",
      error: "异常",
      paused: "已暂停",
      waiting: "等待中",
    };

    const SIGNAL_STATE_LABELS = {
      active: "运行中",
      paused: "暂停中",
    };

    const SEVERITY_LABELS = {
      info: "提示",
      warning: "警告",
      critical: "严重",
    };

    const ALERT_CATEGORY_LABELS = {
      spread_pct: "价差百分比",
      spread_level: "价差阈值",
      zscore: "Z 分数",
      data_quality: "数据质量",
      fx: "汇率",
    };

    const SOURCE_KIND_LABELS = {
      binance_futures: "Binance 永续",
      cme_reference: "CME 参考",
      frankfurter_fx: "Frankfurter 汇率",
      hyperliquid: "Hyperliquid",
      open_er_api_fx: "Open ER 汇率",
      okx_swap: "OKX 永续",
      shfe_delaymarket: "上期所/能源中心延时",
      sina_fx: "新浪外汇",
      sina_futures: "新浪期货",
      tqsdk_main: "TqSdk 主连",
    };

    const FORMULA_LABELS = {
      gold: "黄金 -> USD/oz",
      silver: "白银 -> USD/oz",
      copper: "铜 -> USD/lb",
      crude_oil: "原油 -> USD/bbl",
    };

    const TARGET_UNIT_ABBREVIATIONS = {
      USD_PER_OUNCE: "oz",
      USD_PER_POUND: "lb",
      USD_PER_BARREL: "bbl",
    };

    const CARD_VARIANT_GROUPS = {
      AG_XAG: {
        displayName: DISPLAY_NAME_MAP.AG_XAG,
        variants: ["AG_XAG_GROSS", "AG_XAG_NET"],
      },
      CU_COPPER: {
        displayName: DISPLAY_NAME_MAP.CU_COPPER,
        variants: ["CU_COPPER_GROSS", "CU_COPPER_NET"],
      },
    };

    const CARD_DISPLAY_ORDER = ["AU_XAU", "AG_XAG", "CU_COPPER", "BC_COPPER", "SC_CL"];

    const CARD_SELECTION_STORAGE_KEY = "cross-market-card-selection-v5";
    const HISTORY_RANGE_STORAGE_KEY = "cross-market-history-range-v1";
    const CHART_VISIBILITY_STORAGE_KEY = "cross-market-chart-visibility-v1";
    const DEFAULT_HISTORY_RANGE_KEY = "24h";
    let DASHBOARD_BOOTSTRAPPED = false;
    const HISTORY_RANGE_OPTIONS = [
      { key: "24h", label: "24h" },
      { key: "7d", label: "7天" },
      { key: "30d", label: "30天" },
      { key: "90d", label: "90天" },
      { key: "1y", label: "1年" },
      { key: "all", label: "全部" },
    ];
    const CARD_CHART_STATE = {};

    function loadCardSelections() {
      try {
        return JSON.parse(window.localStorage.getItem(CARD_SELECTION_STORAGE_KEY) || "{}");
      } catch (_error) {
        return {};
      }
    }

    function saveCardSelection(cardKey, groupName) {
      const selections = loadCardSelections();
      selections[cardKey] = groupName;
      window.localStorage.setItem(CARD_SELECTION_STORAGE_KEY, JSON.stringify(selections));
    }

    function loadHistoryRangeSelections() {
      try {
        return JSON.parse(window.localStorage.getItem(HISTORY_RANGE_STORAGE_KEY) || "{}");
      } catch (_error) {
        return {};
      }
    }

    function selectedHistoryRange(cardKey) {
      const selections = loadHistoryRangeSelections();
      return selections[cardKey] || DEFAULT_HISTORY_RANGE_KEY;
    }

    function saveHistoryRangeSelection(cardKey, rangeKey) {
      const selections = loadHistoryRangeSelections();
      selections[cardKey] = rangeKey;
      window.localStorage.setItem(HISTORY_RANGE_STORAGE_KEY, JSON.stringify(selections));
    }

    function loadChartVisibilitySelections() {
      try {
        return JSON.parse(window.localStorage.getItem(CHART_VISIBILITY_STORAGE_KEY) || "{}");
      } catch (_error) {
        return {};
      }
    }

    function isSeriesVisible(cardKey, chartKind, seriesName) {
      const selections = loadChartVisibilitySelections();
      const key = `${cardKey}::${chartKind}::${seriesName}`;
      return selections[key] !== false;
    }

    function toggleSeriesVisibility(cardKey, chartKind, seriesName) {
      const selections = loadChartVisibilitySelections();
      const key = `${cardKey}::${chartKind}::${seriesName}`;
      selections[key] = selections[key] === false;
      window.localStorage.setItem(CHART_VISIBILITY_STORAGE_KEY, JSON.stringify(selections));
    }

    function cardKeyForGroup(groupName) {
      for (const [cardKey, config] of Object.entries(CARD_VARIANT_GROUPS)) {
        if ((config.variants || []).includes(groupName)) {
          return cardKey;
        }
      }
      return groupName;
    }

    function sanitizeDomId(value) {
      return String(value).replace(/[^a-zA-Z0-9_-]/g, "-");
    }

    function buildCardElementId(cardKey) {
      return `card-${sanitizeDomId(cardKey)}`;
    }

    function buildReplayRowId(cardKey) {
      return `replay-row-${sanitizeDomId(cardKey)}`;
    }

    function buildInstrumentRowId(cardKey) {
      return `instrument-row-${sanitizeDomId(cardKey)}`;
    }

    function buildChartSvgId(cardKey, chartKind) {
      return `chart-svg-${sanitizeDomId(cardKey)}-${sanitizeDomId(chartKind)}`;
    }

    function buildChartFrameId(cardKey, chartKind) {
      return `chart-frame-${sanitizeDomId(cardKey)}-${sanitizeDomId(chartKind)}`;
    }

    function buildChartTooltipId(cardKey, chartKind) {
      return `chart-tooltip-${sanitizeDomId(cardKey)}-${sanitizeDomId(chartKind)}`;
    }

    function buildChartCursorId(cardKey, chartKind) {
      return `chart-cursor-${sanitizeDomId(cardKey)}-${sanitizeDomId(chartKind)}`;
    }

    function displayNameForGroup(groupName) {
      return DISPLAY_NAME_MAP[groupName] || groupName;
    }

    function taxModeForGroup(groupName) {
      return groupName.endsWith("_NET") ? "去税" : "含税";
    }

    function taxModeBooleanLabel(groupName) {
      return groupName.endsWith("_NET") ? "否" : "是";
    }

    function statusLabel(status) {
      return STATUS_LABELS[status] || status || "--";
    }

    function signalStateLabel(signalState) {
      return SIGNAL_STATE_LABELS[signalState] || signalState || "--";
    }

    function severityLabel(value) {
      return SEVERITY_LABELS[value] || value || "--";
    }

    function alertCategoryLabel(value) {
      return ALERT_CATEGORY_LABELS[value] || value || "--";
    }

    function sourceKindLabel(value) {
      return SOURCE_KIND_LABELS[value] || value || "--";
    }

    function sourceDisplayName(value) {
      if (!value) return "--";
      if (value.includes("sina_fx")) return "新浪外汇";
      if (value.includes("sina")) return "新浪期货";
      if (value.includes("tqsdk")) return "TqSdk 主连";
      if (value.includes("binance")) return "Binance 永续";
      if (value.includes("okx")) return "OKX 永续";
      if (value.includes("hyperliquid")) return "Hyperliquid";
      if (value.includes("cme")) return "CME 参考";
      if (value.includes("frankfurter")) return "Frankfurter 汇率";
      if (value.includes("open_er")) return "Open ER 汇率";
      return value;
    }

    function formulaLabel(value) {
      return FORMULA_LABELS[value] || value || "--";
    }

    function formatDateTime(value) {
      if (!value) return "--";
      const date = new Date(value);
      if (!Number.isNaN(date.getTime())) {
        return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")} ${
          String(date.getHours()).padStart(2, "0")
        }:${String(date.getMinutes()).padStart(2, "0")}:${String(date.getSeconds()).padStart(2, "0")}`;
      }
      return escapeHtml(String(value).replace("T", " ").replace("Z", ""));
    }

    function formatTimeShort(value) {
      if (!value) return "--";
      const date = new Date(value);
      if (!Number.isNaN(date.getTime())) {
        return `${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
      }
      return String(value).slice(11, 16);
    }

    function formatMonthDayTime(value) {
      if (!value) return "--";
      const date = new Date(value);
      if (!Number.isNaN(date.getTime())) {
        return `${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")} ${
          String(date.getHours()).padStart(2, "0")
        }:${String(date.getMinutes()).padStart(2, "0")}`;
      }
      return String(value).slice(5, 16).replace("T", " ");
    }

    function rangeLabel(rangeKey) {
      return HISTORY_RANGE_OPTIONS.find((item) => item.key === rangeKey)?.label || rangeKey || DEFAULT_HISTORY_RANGE_KEY;
    }

    function median(values) {
      if (!values.length) return null;
      const sorted = [...values].sort((left, right) => left - right);
      return sorted[Math.floor(sorted.length / 2)];
    }

    function estimateGranularityLabel(history) {
      const timestamps = (history || [])
        .map((row) => new Date(row.ts_local || row.ts || row.ts_utc))
        .filter((date) => !Number.isNaN(date.getTime()))
        .map((date) => date.getTime());
      if (timestamps.length < 2) return null;
      const deltas = [];
      for (let index = 1; index < timestamps.length; index += 1) {
        const delta = Math.round((timestamps[index] - timestamps[index - 1]) / 1000);
        if (delta > 0) {
          deltas.push(delta);
        }
      }
      const medianSeconds = median(deltas);
      if (!medianSeconds) return null;
      if (medianSeconds < 90) return "约 1 分钟";
      if (medianSeconds < 3600) return `约 ${Math.max(1, Math.round(medianSeconds / 60))} 分钟`;
      if (medianSeconds < 86400) return `约 ${Math.max(1, Math.round(medianSeconds / 3600))} 小时`;
      return `约 ${Math.max(1, Math.round(medianSeconds / 86400))} 天`;
    }

    function buildHistoryCoverage(history, rangeKey) {
      if (!history || !history.length) {
        return `已选 ${rangeLabel(rangeKey)}｜暂无可获取历史`;
      }
      const start = history[0].ts_local || history[0].ts || history[0].ts_utc;
      const end = history[history.length - 1].ts_local || history[history.length - 1].ts || history[history.length - 1].ts_utc;
      const parts = [
        `已选 ${rangeLabel(rangeKey)}`,
        `可获取：${formatMonthDayTime(start)} 至 ${formatMonthDayTime(end)}`,
      ];
      const granularity = estimateGranularityLabel(history);
      if (granularity) {
        parts.push(`颗粒度：${granularity}`);
      }
      return parts.join("｜");
    }

    function latestDefined(values) {
      for (let index = values.length - 1; index >= 0; index -= 1) {
        const numeric = toFiniteNumber(values[index]);
        if (numeric !== null) return numeric;
      }
      return null;
    }

    function clamp(value, minValue, maxValue) {
      return Math.min(maxValue, Math.max(minValue, value));
    }

    function buildLinePath(values, minValue, maxValue, width, height, padding) {
      const usableWidth = width - padding.left - padding.right;
      const usableHeight = height - padding.top - padding.bottom;
      const denominator = maxValue - minValue || 1;
      const path = [];
      let drawing = false;
      values.forEach((rawValue, index) => {
        const value = toFiniteNumber(rawValue);
        if (value === null) {
          drawing = false;
          return;
        }
        const x = padding.left + (values.length <= 1 ? usableWidth / 2 : (index / (values.length - 1)) * usableWidth);
        const y = padding.top + (1 - (value - minValue) / denominator) * usableHeight;
        path.push(`${drawing ? "L" : "M"} ${x.toFixed(1)} ${y.toFixed(1)}`);
        drawing = true;
      });
      return path.join(" ");
    }

    function buildLegend(series, fallbackFormatter = formatNumber, context = null) {
      return series.map((item) => {
        const legendValue = item.legendValue || fallbackFormatter(latestDefined(item.values));
        const swatchStyle = item.dash
          ? `color:${item.color}; border-top-style:dashed; border-top-color:${item.color};`
          : `color:${item.color}; border-top-style:solid; border-top-color:${item.color};`;
        const isVisible = context ? isSeriesVisible(context.cardKey, context.chartKind, item.name) : true;
        const clickAttrs = context
          ? `type="button" onclick="handleSeriesVisibilityToggle('${escapeHtml(context.cardKey)}', '${escapeHtml(context.groupName)}', '${escapeHtml(context.chartKind)}', '${escapeHtml(item.name)}')"`
          : "";
        return `
          <button class="legend-item${context ? " legend-button" : ""}${isVisible ? "" : " inactive"}" ${clickAttrs}>
            <span class="legend-swatch" style="${swatchStyle}"></span>
            <span>${escapeHtml(item.name)}</span>
            <strong>${escapeHtml(legendValue || "--")}</strong>
          </button>
        `;
      }).join("");
    }

    function buildHoverReadoutHtml(timestamp, entries) {
      const rows = (entries || []).map((item) => `
        <div class="chart-tooltip-entry">
          <span class="chart-tooltip-label">
            <span class="chart-tooltip-dot" style="background:${escapeHtml(item.color || "#7fc8f8")};"></span>
            <span>${escapeHtml(item.name)}</span>
          </span>
          <span class="chart-tooltip-value">${escapeHtml(item.value)}</span>
        </div>
      `).join("");
      return `
        <div class="chart-tooltip-time">${escapeHtml(formatDateTime(timestamp))}</div>
        <div class="chart-tooltip-body">${rows || '<div class="chart-tooltip-entry"><span class="chart-tooltip-label">当前无可显示曲线</span></div>'}</div>
      `;
    }

    function updateCardHover(cardKey, index) {
      const state = CARD_CHART_STATE[cardKey];
      if (!state || !state.timestamps || !state.timestamps.length) {
        return;
      }
      const safeIndex = Math.max(0, Math.min(index, state.timestamps.length - 1));
      const timestamp = state.timestamps[safeIndex];
      Object.values(state.charts || {}).forEach((chart) => {
        const frame = document.getElementById(chart.frameId);
        const tooltip = document.getElementById(chart.tooltipId);
        const cursor = document.getElementById(chart.cursorId);
        if (!frame || !tooltip || !cursor) {
          return;
        }
        const entries = chart.series.map((item) => {
          const rawValue = Array.isArray(item.values) ? item.values[safeIndex] : null;
          return {
            name: item.name,
            color: item.color,
            value: item.formatter ? item.formatter(rawValue) : formatNumber(rawValue, 2),
          };
        });
        const usableWidth = chart.width - chart.padding.left - chart.padding.right;
        const xValue = chart.timestamps.length <= 1
          ? chart.padding.left + usableWidth / 2
          : chart.padding.left + (safeIndex / (chart.timestamps.length - 1)) * usableWidth;
        cursor.setAttribute("x1", xValue.toFixed(1));
        cursor.setAttribute("x2", xValue.toFixed(1));
        cursor.style.display = "block";

        tooltip.innerHTML = buildHoverReadoutHtml(timestamp, entries);
        tooltip.style.display = "block";
        const frameRect = frame.getBoundingClientRect();
        const scaleX = frameRect.width && chart.width ? frameRect.width / chart.width : 1;
        const xPixels = xValue * scaleX;
        const tooltipWidth = tooltip.offsetWidth || 220;
        const preferredLeft = xPixels + 14;
        const maxLeft = Math.max(12, frameRect.width - tooltipWidth - 12);
        const alternateLeft = xPixels - tooltipWidth - 14;
        const left = preferredLeft <= maxLeft ? preferredLeft : alternateLeft;
        tooltip.style.left = `${clamp(left, 12, maxLeft)}px`;
      });
    }

    function clearCardHover(cardKey) {
      const state = CARD_CHART_STATE[cardKey];
      if (!state) {
        return;
      }
      Object.values(state.charts || {}).forEach((chart) => {
        const tooltip = document.getElementById(chart.tooltipId);
        const cursor = document.getElementById(chart.cursorId);
        if (tooltip) {
          tooltip.style.display = "none";
          tooltip.innerHTML = "";
        }
        if (cursor) {
          cursor.style.display = "none";
        }
      });
    }

    function hydrateCardCharts(cardKey) {
      const state = CARD_CHART_STATE[cardKey];
      if (!state || !state.timestamps || !state.timestamps.length) {
        return;
      }
      Object.values(state.charts || {}).forEach((chart) => {
        const svg = document.getElementById(chart.svgId);
        if (!svg || svg.dataset.bound === "1") {
          return;
        }
        svg.dataset.bound = "1";
        svg.addEventListener("mousemove", (event) => {
          const rect = svg.getBoundingClientRect();
          if (!rect.width || !rect.height) {
            return;
          }
          const usableWidth = chart.width - chart.padding.left - chart.padding.right;
          const relativeX = ((event.clientX - rect.left) / rect.width) * chart.width;
          const clampedX = Math.max(chart.padding.left, Math.min(chart.width - chart.padding.right, relativeX));
          const ratio = usableWidth <= 0 ? 0 : (clampedX - chart.padding.left) / usableWidth;
          const index = state.timestamps.length <= 1 ? 0 : Math.round(ratio * (state.timestamps.length - 1));
          updateCardHover(cardKey, index);
        });
        svg.addEventListener("mouseleave", () => clearCardHover(cardKey));
      });
      clearCardHover(cardKey);
    }

    async function handleSeriesVisibilityToggle(cardKey, groupName, chartKind, seriesName) {
      toggleSeriesVisibility(cardKey, chartKind, seriesName);
      try {
        await refreshCardGroup(groupName);
      } catch (error) {
        window.alert(`更新图表显示项失败：${error.message}`);
      }
    }

    function buildLineChart({
      cardKey,
      groupName,
      chartKind,
      timestamps = [],
      title,
      subtitle,
      badge,
      headerAside = "",
      series,
      legendSeries = null,
      formatter = formatNumber,
      compact = false,
      includeZero = false,
      referenceLines = [],
    }) {
      const numericSeries = series.map((item) => ({
        ...item,
        values: item.values.map((value) => toFiniteNumber(value)),
      }));
      const legendPrepared = (legendSeries || series).map((item) => ({
        ...item,
        values: item.values.map((value) => toFiniteNumber(value)),
      }));
      const boundsSeries = numericSeries.length ? numericSeries : legendPrepared;
      const allValues = boundsSeries.flatMap((item) => item.values.filter((value) => value !== null));
      const lineReferenceValues = referenceLines.map((item) => toFiniteNumber(item.value)).filter((value) => value !== null);
      if (includeZero) {
        lineReferenceValues.push(0);
      }
      const valuePool = [...allValues, ...lineReferenceValues];
      if (!valuePool.length) {
        return `
          <section class="chart-panel">
            <div class="chart-head">
              <div class="chart-title">
              <strong>${escapeHtml(title)}</strong>
              <span>${escapeHtml(subtitle)}</span>
            </div>
              ${headerAside || `<span class="route-badge">${escapeHtml(badge || "暂无历史数据")}</span>`}
            </div>
            <div class="chart-empty">暂无可绘制的历史数据</div>
          </section>
        `;
      }

      let minValue = Math.min(...valuePool);
      let maxValue = Math.max(...valuePool);
      if (minValue === maxValue) {
        minValue -= 1;
        maxValue += 1;
      } else {
        const padding = (maxValue - minValue) * 0.1;
        minValue -= padding;
        maxValue += padding;
      }

      const width = 960;
      const height = compact ? 240 : 300;
      const padding = { top: 18, right: 20, bottom: 28, left: 70 };
      const usableWidth = width - padding.left - padding.right;
      const usableHeight = height - padding.top - padding.bottom;
      const ticks = Array.from({ length: 5 }, (_, index) => minValue + ((maxValue - minValue) * index) / 4);
      const grid = ticks.map((value) => {
        const y = padding.top + usableHeight - ((value - minValue) / (maxValue - minValue || 1)) * usableHeight;
        return `
          <g>
            <line x1="${padding.left}" y1="${y.toFixed(1)}" x2="${width - padding.right}" y2="${y.toFixed(1)}" stroke="rgba(157, 201, 255, 0.12)" stroke-width="1" />
            <text class="chart-axis" x="${padding.left - 10}" y="${(y + 4).toFixed(1)}" text-anchor="end">${escapeHtml(formatter(value))}</text>
          </g>
        `;
      }).join("");
      const paths = numericSeries.map((item) => {
        const path = buildLinePath(item.values, minValue, maxValue, width, height, padding);
        if (!path) return "";
        return `<path d="${path}" fill="none" stroke="${item.color}" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" stroke-dasharray="${item.dash || ""}" />`;
      }).join("");
      const markers = referenceLines.map((line) => {
        const numeric = toFiniteNumber(line.value);
        if (numeric === null) return "";
        const y = padding.top + usableHeight - ((numeric - minValue) / (maxValue - minValue || 1)) * usableHeight;
        return `<line x1="${padding.left}" y1="${y.toFixed(1)}" x2="${width - padding.right}" y2="${y.toFixed(1)}" stroke="${line.color || "rgba(157, 201, 255, 0.2)"}" stroke-width="1.5" stroke-dasharray="${line.dash || "6 6"}" />`;
      }).join("");
      const xStartLabel = timestamps.length ? formatMonthDayTime(timestamps[0]) : "--";
      const xEndLabel = timestamps.length ? formatMonthDayTime(timestamps[timestamps.length - 1]) : "--";
      const xLabels = `
        <text class="chart-axis" x="${padding.left}" y="${height - 8}" text-anchor="start">${escapeHtml(xStartLabel)}</text>
        <text class="chart-axis" x="${width - padding.right}" y="${height - 8}" text-anchor="end">${escapeHtml(xEndLabel)}</text>
      `;
      const frameId = buildChartFrameId(cardKey, chartKind);
      const svgId = buildChartSvgId(cardKey, chartKind);
      const tooltipId = buildChartTooltipId(cardKey, chartKind);
      const cursorId = buildChartCursorId(cardKey, chartKind);
      CARD_CHART_STATE[cardKey] = CARD_CHART_STATE[cardKey] || { timestamps: [], charts: {} };
      CARD_CHART_STATE[cardKey].timestamps = timestamps;
      CARD_CHART_STATE[cardKey].charts[chartKind] = {
        frameId,
        svgId,
        tooltipId,
        cursorId,
        width,
        height,
        padding,
        timestamps,
        series: numericSeries.map((item) => ({
          name: item.name,
          color: item.color,
          values: item.values,
          formatter,
        })),
      };
      return `
        <section class="chart-panel">
          <div class="chart-head">
            <div class="chart-title">
              <strong>${escapeHtml(title)}</strong>
              <span>${escapeHtml(subtitle)}</span>
            </div>
            ${headerAside || `<span class="route-badge">${escapeHtml(badge || "")}</span>`}
          </div>
          <div class="chart-frame${compact ? " compact" : ""}" id="${frameId}">
            <svg id="${svgId}" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="${escapeHtml(title)}">
              ${grid}
              ${markers}
              ${paths}
              <line id="${cursorId}" x1="${padding.left}" y1="${padding.top}" x2="${padding.left}" y2="${height - padding.bottom}" stroke="rgba(157, 201, 255, 0.68)" stroke-width="1.5" stroke-dasharray="6 6" style="display:none" />
              ${xLabels}
            </svg>
            <div class="chart-tooltip" id="${tooltipId}"></div>
          </div>
          <div class="chart-legend">${buildLegend(legendPrepared, formatter, { cardKey, groupName, chartKind })}</div>
        </section>
      `;
    }

    function computeBounds(values) {
      const clean = values.map((value) => toFiniteNumber(value)).filter((value) => value !== null);
      if (!clean.length) return null;
      let minValue = Math.min(...clean);
      let maxValue = Math.max(...clean);
      if (minValue === maxValue) {
        minValue -= 1;
        maxValue += 1;
      } else {
        const padding = (maxValue - minValue) * 0.12;
        minValue -= padding;
        maxValue += padding;
      }
      return { minValue, maxValue };
    }

    function buildDualAxisChart({
      cardKey,
      groupName,
      chartKind,
      timestamps = [],
      title,
      subtitle,
      badge,
      headerAside = "",
      leftSeries,
      rightSeries,
      legendSeries = null,
      leftFormatter = formatNumber,
      rightFormatter = formatNumber,
    }) {
      const leftPrepared = leftSeries.map((item) => ({
        ...item,
        values: item.values.map((value) => toFiniteNumber(value)),
      }));
      const rightPrepared = rightSeries.map((item) => ({
        ...item,
        values: item.values.map((value) => toFiniteNumber(value)),
      }));
      const legendPrepared = (legendSeries || [...leftSeries, ...rightSeries]).map((item) => ({
        ...item,
        values: item.values.map((value) => toFiniteNumber(value)),
      }));
      const leftBoundsSource = leftPrepared.length
        ? leftPrepared
        : legendPrepared.filter((item) => item.axis === "left");
      const rightBoundsSource = rightPrepared.length
        ? rightPrepared
        : legendPrepared.filter((item) => item.axis !== "left");
      const leftBounds = computeBounds(leftBoundsSource.flatMap((item) => item.values));
      const rightBounds = computeBounds(rightBoundsSource.flatMap((item) => item.values));
      if (!leftBounds && !rightBounds && !legendPrepared.length) {
        return `
          <section class="chart-panel">
            <div class="chart-head">
              <div class="chart-title">
                <strong>${escapeHtml(title)}</strong>
                <span>${escapeHtml(subtitle)}</span>
              </div>
              ${headerAside || `<span class="route-badge">${escapeHtml(badge || "暂无历史数据")}</span>`}
            </div>
            <div class="chart-empty">暂无可绘制的历史数据</div>
          </section>
        `;
      }

      const width = 960;
      const height = 300;
      const padding = { top: 18, right: 74, bottom: 28, left: 74 };
      const usableHeight = height - padding.top - padding.bottom;
      const positions = Array.from({ length: 5 }, (_, index) => index / 4);
      const grid = positions.map((ratio) => {
        const y = padding.top + usableHeight - ratio * usableHeight;
        const leftLabel = leftBounds
          ? leftFormatter(leftBounds.minValue + ratio * (leftBounds.maxValue - leftBounds.minValue))
          : "";
        const rightLabel = rightBounds
          ? rightFormatter(rightBounds.minValue + ratio * (rightBounds.maxValue - rightBounds.minValue))
          : "";
        return `
          <g>
            <line x1="${padding.left}" y1="${y.toFixed(1)}" x2="${width - padding.right}" y2="${y.toFixed(1)}" stroke="rgba(157, 201, 255, 0.12)" stroke-width="1" />
            ${leftBounds ? `<text class="chart-axis" x="${padding.left - 10}" y="${(y + 4).toFixed(1)}" text-anchor="end">${escapeHtml(leftLabel)}</text>` : ""}
            ${rightBounds ? `<text class="chart-axis" x="${width - padding.right + 10}" y="${(y + 4).toFixed(1)}" text-anchor="start">${escapeHtml(rightLabel)}</text>` : ""}
          </g>
        `;
      }).join("");

      const leftPaths = leftPrepared.map((item) => {
        if (!leftBounds) return "";
        const path = buildLinePath(item.values, leftBounds.minValue, leftBounds.maxValue, width, height, padding);
        return path
          ? `<path d="${path}" fill="none" stroke="${item.color}" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" stroke-dasharray="${item.dash || ""}" />`
          : "";
      }).join("");
      const rightPaths = rightPrepared.map((item) => {
        if (!rightBounds) return "";
        const path = buildLinePath(item.values, rightBounds.minValue, rightBounds.maxValue, width, height, padding);
        return path
          ? `<path d="${path}" fill="none" stroke="${item.color}" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" stroke-dasharray="${item.dash || ""}" />`
          : "";
      }).join("");

      const xStartLabel = timestamps.length ? formatMonthDayTime(timestamps[0]) : "--";
      const xEndLabel = timestamps.length ? formatMonthDayTime(timestamps[timestamps.length - 1]) : "--";
      const xLabels = `
        <text class="chart-axis" x="${padding.left}" y="${height - 8}" text-anchor="start">${escapeHtml(xStartLabel)}</text>
        <text class="chart-axis" x="${width - padding.right}" y="${height - 8}" text-anchor="end">${escapeHtml(xEndLabel)}</text>
      `;
      const frameId = buildChartFrameId(cardKey, chartKind);
      const svgId = buildChartSvgId(cardKey, chartKind);
      const tooltipId = buildChartTooltipId(cardKey, chartKind);
      const cursorId = buildChartCursorId(cardKey, chartKind);
      CARD_CHART_STATE[cardKey] = CARD_CHART_STATE[cardKey] || { timestamps: [], charts: {} };
      CARD_CHART_STATE[cardKey].timestamps = timestamps;
      CARD_CHART_STATE[cardKey].charts[chartKind] = {
        frameId,
        svgId,
        tooltipId,
        cursorId,
        width,
        height,
        padding,
        timestamps,
        series: [
          ...leftPrepared.map((item) => ({
            name: item.name,
            color: item.color,
            values: item.values,
            formatter: leftFormatter,
          })),
          ...rightPrepared.map((item) => ({
            name: item.name,
            color: item.color,
            values: item.values,
            formatter: rightFormatter,
          })),
        ],
      };

      return `
        <section class="chart-panel">
          <div class="chart-head">
            <div class="chart-title">
              <strong>${escapeHtml(title)}</strong>
              <span>${escapeHtml(subtitle)}</span>
            </div>
            ${headerAside || `<span class="route-badge">${escapeHtml(badge || "")}</span>`}
          </div>
          <div class="chart-frame" id="${frameId}">
            <svg id="${svgId}" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="${escapeHtml(title)}">
              ${grid}
              ${leftPaths}
              ${rightPaths}
              <line id="${cursorId}" x1="${padding.left}" y1="${padding.top}" x2="${padding.left}" y2="${height - padding.bottom}" stroke="rgba(157, 201, 255, 0.68)" stroke-width="1.5" stroke-dasharray="6 6" style="display:none" />
              ${xLabels}
            </svg>
            <div class="chart-tooltip" id="${tooltipId}"></div>
          </div>
          <div class="chart-legend">${buildLegend(legendPrepared, formatNumber, { cardKey, groupName, chartKind })}</div>
        </section>
      `;
    }

    function buildTimeRange(history) {
      if (!history || !history.length) return "暂无时间范围";
      const start = history[0].ts_local || history[0].ts || history[0].ts_utc;
      const end = history[history.length - 1].ts_local || history[history.length - 1].ts || history[history.length - 1].ts_utc;
      const startLabel = formatTimeShort(start);
      const endLabel = formatTimeShort(end);
      if (startLabel === endLabel) {
        return `行情时间：${endLabel}`;
      }
      return `${startLabel} - ${endLabel}`;
    }

    function buildHistoryRangeControls(cardGroup, history) {
      const cardKey = cardGroup.card_key;
      const groupName = cardGroup.selected_item.group_name;
      const activeRangeKey = selectedHistoryRange(cardKey);
      const buttons = HISTORY_RANGE_OPTIONS.map((option) => {
        const active = option.key === activeRangeKey ? " active" : "";
        return `
          <button
            type="button"
            class="time-filter-button${active}"
            onclick="handleHistoryRangeChange('${escapeHtml(cardKey)}', '${escapeHtml(groupName)}', '${escapeHtml(option.key)}')"
          >
            ${escapeHtml(option.label)}
          </button>
        `;
      }).join("");
      return `
        <div class="chart-controls">
          <div class="time-filter">${buttons}</div>
          <div class="chart-meta">${escapeHtml(buildHistoryCoverage(history, activeRangeKey))}</div>
        </div>
      `;
    }

    function buildCardGroups(items) {
      const selections = loadCardSelections();
      const assigned = new Set();
      const groups = [];

      Object.entries(CARD_VARIANT_GROUPS).forEach(([cardKey, config]) => {
        const variants = config.variants
          .map((groupName) => items.find((item) => item.group_name === groupName))
          .filter(Boolean);
        if (!variants.length) return;
        variants.forEach((item) => assigned.add(item.group_name));
        const selectedGroupName = selections[cardKey];
        const selectedItem = variants.find((item) => item.group_name === selectedGroupName) || variants[0];
        groups.push({
          card_key: cardKey,
          display_name: config.displayName,
          variants,
          selected_item: selectedItem,
        });
      });

      items.forEach((item) => {
        if (assigned.has(item.group_name)) return;
        groups.push({
          card_key: item.group_name,
          display_name: displayNameForGroup(item.group_name),
          variants: [item],
          selected_item: item,
        });
      });

      return groups.sort((left, right) => cardDisplayOrderIndex(left.card_key) - cardDisplayOrderIndex(right.card_key));
    }

    function cardDisplayOrderIndex(cardKey) {
      const index = CARD_DISPLAY_ORDER.indexOf(cardKey);
      return index === -1 ? CARD_DISPLAY_ORDER.length + 100 : index;
    }

    function taxModeSummary(cardGroup) {
      if (!cardGroup || !cardGroup.variants || !cardGroup.variants.length) {
        return "--";
      }
      if (cardGroup.variants.length === 1) {
        return taxModeForGroup(cardGroup.variants[0].group_name);
      }
      return cardGroup.variants.map((item) => taxModeForGroup(item.group_name)).join(" / ");
    }

    function buildVariantValueLines(cardGroup, formatter) {
      const variants = cardGroup?.variants || [];
      return variants.map((item) => {
        const tag = variants.length > 1 ? taxModeForGroup(item.group_name) : "";
        const value = formatter(item);
        return `
          <div class="summary-line">
            <span class="summary-tag">${escapeHtml(tag)}</span>
            <strong>${escapeHtml(value)}</strong>
          </div>
        `;
      }).join("");
    }

    function domesticPriceDigits(item) {
      if (!item) return 0;
      return item.formula === "gold" || item.formula === "crude_oil" ? 2 : 0;
    }

    function formatHedgePosition(item) {
      const numeric = toFiniteNumber(item?.hedge_contract_size);
      if (numeric === null) return "--";
      const unit = TARGET_UNIT_ABBREVIATIONS[item?.target_unit] || "";
      const formatted = formatTableNumber(numeric, 2);
      return unit ? `${formatted} ${unit}` : formatted;
    }

    function buildInstrumentRow(cardGroup, domesticPreference, overseasPreference) {
      if (!cardGroup || !cardGroup.selected_item) {
        return "";
      }
      const item = cardGroup.selected_item;
      const domesticLabel = domesticPreference?.selected_label || item.domestic_label || item.domestic_symbol || "--";
      const overseasLabel = overseasPreference?.selected_label || item.overseas_label || item.overseas_symbol || "--";
      const cardHref = `#${buildCardElementId(cardGroup.card_key)}`;
      return `
        <tr id="${buildInstrumentRowId(cardGroup.card_key)}">
          <td>
            <a class="summary-link" href="${escapeHtml(cardHref)}">
              <strong>${escapeHtml(cardGroup.display_name || displayNameForGroup(item.group_name))}</strong>
              <span class="summary-sub">${escapeHtml(domesticLabel)} / ${escapeHtml(overseasLabel)}</span>
            </a>
          </td>
          <td class="numeric-cell"><div class="summary-lines">${buildVariantValueLines(cardGroup, (variant) => formatTableNumber(variant.domestic_last_raw, domesticPriceDigits(variant)))}</div></td>
          <td class="numeric-cell"><div class="summary-lines">${buildVariantValueLines(cardGroup, (variant) => formatTableNumber(variant.normalized_last, 2))}</div></td>
          <td class="numeric-cell"><div class="summary-lines">${buildVariantValueLines(cardGroup, (variant) => formatTableNumber(variant.overseas_last, 2))}</div></td>
          <td class="numeric-cell"><div class="summary-lines">${buildVariantValueLines(cardGroup, (variant) => formatHedgePosition(variant))}</div></td>
          <td class="numeric-cell"><div class="summary-lines">${buildVariantValueLines(cardGroup, (variant) => formatTableSignedNumber(variant.spread, 2))}</div></td>
          <td class="numeric-cell"><div class="summary-lines">${buildVariantValueLines(cardGroup, (variant) => formatTablePct(variant.spread_pct))}</div></td>
        </tr>
      `;
    }

    function renderInstrumentSummary(cardGroups, snapshot) {
      const rows = (cardGroups || []).map((cardGroup) =>
        buildInstrumentRow(
          cardGroup,
          snapshot.domestic_route_preferences?.[cardGroup.selected_item.group_name],
          snapshot.overseas_route_preferences?.[cardGroup.selected_item.group_name],
        )
      ).filter(Boolean);
      document.getElementById("instrument-summary").innerHTML = rows.length
        ? rows.join("")
        : `<tr><td colspan="7" class="muted">等待第一轮轮询完成后展示标的概览。</td></tr>`;
    }

    function buildVariantSelector(cardGroup) {
      if (!cardGroup || !cardGroup.variants || cardGroup.variants.length <= 1) {
        return "";
      }
      const buttons = cardGroup.variants.map((item) => {
        const active = item.group_name === cardGroup.selected_item.group_name ? " active" : "";
        return `
          <button
            type="button"
            class="segment-button${active}"
            onclick="handleVariantChange('${escapeHtml(cardGroup.card_key)}', '${escapeHtml(item.group_name)}')"
          >
            <span class="segment-check" aria-hidden="true"></span>
            ${escapeHtml(taxModeForGroup(item.group_name))}
          </button>
        `;
      }).join("");
      return `<div class="segmented-control">${buttons}</div>`;
    }

    function buildRouteSelector(groupName, title, endpoint, preference, headerAddon = "", allowAuto = true) {
      if (!preference || !preference.options || !preference.options.length) {
        return "";
      }
      const options = [
        ...(!allowAuto ? [] : [
          `<option value="__auto__"${preference.selected_symbol ? "" : " selected"}>自动选择</option>`,
        ]),
        ...preference.options.map((option) => `
          <option value="${escapeHtml(option.symbol)}"${option.selected ? " selected" : ""}>
            ${escapeHtml(option.label)}${option.enabled ? "" : " [备用]"}
          </option>
        `),
      ].join("");
      const selectedText = preference.selected_label || (allowAuto ? "自动选择" : "--");
      return `
        <div class="selector">
          <div class="selector-head">
            <label>${escapeHtml(title)}</label>
            ${headerAddon}
          </div>
          <strong>${escapeHtml(selectedText)}</strong>
          <select onchange="${endpoint}('${escapeHtml(groupName)}', this.value)">
            ${options}
          </select>
        </div>
      `;
    }

    function buildDomesticDisplay(title, preference, headerAddon = "") {
      if (!preference) {
        return "";
      }
      const selectedText = preference.selected_label || "--";
      return `
        <div class="selector">
          <div class="selector-head">
            <label>${escapeHtml(title)}</label>
            ${headerAddon}
          </div>
          <strong>${escapeHtml(selectedText)}</strong>
        </div>
      `;
    }

    function filterHistoryForSelection(history, domesticPreference) {
      const rows = history || [];
      const selectedDomesticSymbol = domesticPreference?.selected_symbol;
      if (!selectedDomesticSymbol) {
        return rows;
      }
      return rows.filter((row) => row.domestic_symbol === selectedDomesticSymbol);
    }

    function timelineTimestamp(row) {
      return row?.ts_local || row?.ts || row?.ts_utc || null;
    }

    function buildInfoModule(item) {
      const fxModeLabel = item?.route_detail?.fx_is_frozen ? "冻结" : (item?.route_detail?.fx_is_live ? "实时" : "--");
      const pauseLine = item.pause_reason
        ? `<div data-card-field="pause_reason_container">暂停原因：<strong data-card-field="pause_reason">${escapeHtml(item.pause_reason)}</strong></div>`
        : `<div data-card-field="pause_reason_container" hidden></div>`;
      return `
        <div class="selector">
          <div class="detail">
            <div>信号状态：<strong data-card-field="signal_state">${escapeHtml(signalStateLabel(item.signal_state))}</strong></div>
            <div>国内换算价：<strong data-card-field="normalized_last">${formatNumber(item.normalized_last, 4)}</strong> ${escapeHtml(item.target_unit)}</div>
            <div>海外最新价：<strong data-card-field="overseas_last">${formatNumber(item.overseas_last, 4)}</strong> ${escapeHtml(item.target_unit)}</div>
            <div>数据源：<strong data-card-field="domestic_source">${escapeHtml(sourceDisplayName(item.domestic_source))}</strong> / <strong data-card-field="overseas_source">${escapeHtml(sourceDisplayName(item.overseas_source))}</strong></div>
            <div>汇率：<strong data-card-field="fx_source">${escapeHtml(sourceDisplayName(item.fx_source))}</strong> / <strong data-card-field="fx_mode">${escapeHtml(fxModeLabel)}</strong></div>
            <div data-card-field="ages">时效：国内 <strong>${formatNumber(item.domestic_age_sec, 1)}s</strong> / 海外 <strong>${formatNumber(item.overseas_age_sec, 1)}s</strong> / 汇率 <strong>${formatNumber(item.fx_age_sec, 1)}s</strong></div>
            ${pauseLine}
          </div>
        </div>
      `;
    }

    function buildCard(cardGroup, domesticPreference, overseasPreference, history) {
      const item = cardGroup.selected_item;
      const filteredHistory = filterHistoryForSelection(history || [], domesticPreference);
      const cardKey = cardGroup.card_key;
      const groupName = item.group_name;
      const timeline = filteredHistory.map((row) => timelineTimestamp(row));
      const priceHistoryControls = buildHistoryRangeControls(cardGroup, filteredHistory);
      const spreadHistoryControls = buildHistoryRangeControls(cardGroup, filteredHistory);
      const priceLeftSeriesAll = [
        {
          name: "国内原始价",
          axis: "left",
          color: "#7fc8f8",
          values: filteredHistory.map((row) => row.domestic_last_raw),
          legendValue: formatNumber(latestDefined(filteredHistory.map((row) => row.domestic_last_raw)), 4),
        },
      ];
      const priceRightSeriesAll = [
        {
          name: "国内换算价",
          axis: "right",
          color: "#7fc8f8",
          dash: "10 8",
          values: filteredHistory.map((row) => row.normalized_last),
          legendValue: formatNumber(latestDefined(filteredHistory.map((row) => row.normalized_last)), 4),
        },
        {
          name: "海外实际价",
          axis: "right",
          color: "#49dcb1",
          values: filteredHistory.map((row) => row.overseas_last),
          legendValue: formatNumber(latestDefined(filteredHistory.map((row) => row.overseas_last)), 4),
        },
      ];
      const priceLeftSeries = priceLeftSeriesAll.filter((series) => isSeriesVisible(cardKey, "price", series.name));
      const priceRightSeries = priceRightSeriesAll.filter((series) => isSeriesVisible(cardKey, "price", series.name));
      const spreadSeriesAll = [
        {
          name: "价差百分比",
          color: "#ffb454",
          values: filteredHistory.map((row) => {
            const numeric = toFiniteNumber(row.spread_pct);
            return numeric === null ? null : numeric * 100;
          }),
          legendValue: formatPct(item.spread_pct),
        },
      ];
      const spreadSeries = spreadSeriesAll.filter((series) => isSeriesVisible(cardKey, "spread", series.name));
      CARD_CHART_STATE[cardKey] = { timestamps: timeline, charts: {} };
      const priceChart = buildDualAxisChart({
        cardKey,
        groupName,
        chartKind: "price",
        timestamps: timeline,
        title: "价格走势",
        subtitle: "左轴显示国内原始价，右轴显示海外最新价和国内换算价。",
        headerAside: priceHistoryControls,
        leftSeries: priceLeftSeries,
        rightSeries: priceRightSeries,
        legendSeries: [...priceLeftSeriesAll, ...priceRightSeriesAll],
        leftFormatter: (value) => formatNumber(value, 2),
        rightFormatter: (value) => formatNumber(value, 2),
      });
      const spreadPctChart = buildLineChart({
        cardKey,
        groupName,
        chartKind: "spread",
        timestamps: timeline,
        title: "价差百分比",
        subtitle: "直接观察跨市场价差百分比的扩张与收敛。",
        headerAside: spreadHistoryControls,
        series: spreadSeries,
        legendSeries: spreadSeriesAll,
        formatter: (value) => formatPctPoint(value),
        compact: true,
        includeZero: true,
        referenceLines: [
          { value: 0, color: "rgba(157, 201, 255, 0.28)", dash: "4 4" },
        ],
      });
      return `
        <article class="card" id="${buildCardElementId(cardGroup.card_key)}" data-group-name="${escapeHtml(item.group_name)}">
          <div class="head">
            <div class="title">
              <strong>${escapeHtml(cardGroup.display_name)}</strong>
              <span>国内：${escapeHtml(item.domestic_label || item.domestic_symbol)} ｜ 海外：${escapeHtml(item.overseas_label || item.overseas_symbol)}</span>
            </div>
            <span class="status ${escapeHtml(item.status)}" data-card-field="status">${escapeHtml(statusLabel(item.status))}</span>
          </div>
          <div class="metrics">
            <div class="metric">
              <label>理论价差</label>
              <strong data-card-field="spread">${formatNumber(item.spread, 4)}</strong>
            </div>
            <div class="metric">
              <label>价差百分比</label>
              <strong data-card-field="spread_pct">${formatPct(item.spread_pct)}</strong>
            </div>
            <div class="metric">
              <label>Z 分数</label>
              <strong data-card-field="zscore">${formatNumber(item.zscore, 2)}</strong>
            </div>
            <div class="metric">
              <label>汇率跳变</label>
              <strong data-card-field="fx_jump_pct">${formatPct(item.fx_jump_pct)}</strong>
            </div>
          </div>
          <div class="route-grid">
            ${buildDomesticDisplay("国内比对基准", domesticPreference, buildVariantSelector(cardGroup))}
            ${buildRouteSelector(item.group_name, "海外比对基准", "handleOverseasRouteChange", overseasPreference, "", false)}
            ${buildInfoModule(item)}
          </div>
          <div class="chart-grid">
            ${priceChart}
            ${spreadPctChart}
          </div>
        </article>
      `;
    }

    function buildWaitingCard(cardKey, groupName, status) {
      return `
        <article class="card" id="${buildCardElementId(cardKey)}" data-group-name="${escapeHtml(groupName)}">
          <div class="head">
            <div class="title">
              <strong>${escapeHtml(displayNameForGroup(groupName))}</strong>
              <span>正在等待首轮数据采集完成</span>
            </div>
            <span class="status waiting">${escapeHtml(statusLabel(status || "waiting"))}</span>
          </div>
          <div class="detail">
            <div>当前没有可用快照，后台正在拉取国内、海外和汇率数据。</div>
            <div>一旦首轮轮询完成，这张卡片会自动替换成实时监控视图。</div>
          </div>
        </article>
      `;
    }

    function buildReplayRowMarkup(cardGroup, report) {
      const item = cardGroup.selected_item;
      return `
        <tr id="${buildReplayRowId(cardGroup.card_key)}">
          <td>${escapeHtml(cardGroup.display_name || displayNameForGroup(cardGroup.card_key || item.group_name))}</td>
          <td>${report.sample_count}</td>
          <td>${formatNumber(report.spread_mean, 4)}</td>
          <td>${formatNumber(report.hedge_ratio_ols, 4)}</td>
          <td>${formatPct(report.realized_daily_vol_pct)}</td>
          <td>${formatNumber(report.average_round_trip_cost, 4)}</td>
          <td>${formatNumber(report.average_net_edge_after_cost, 4)}</td>
          <td>价差=${report.spread_pct_breach_count} / Z=${report.zscore_breach_count}</td>
        </tr>
      `;
    }

    function buildReplayLoadingRow(cardGroup) {
      return `
        <tr id="${buildReplayRowId(cardGroup.card_key)}">
          <td>${escapeHtml(cardGroup.display_name || displayNameForGroup(cardGroup.card_key || cardGroup.selected_item.group_name))}</td>
          <td colspan="7" class="muted">正在加载回放统计...</td>
        </tr>
      `;
    }

    async function buildReplayRow(cardGroup) {
      if (!cardGroup || !cardGroup.selected_item) {
        return "";
      }
      const report = await fetchJson(`/api/replay/summary?group_name=${encodeURIComponent(cardGroup.selected_item.group_name)}&limit=500`);
      return buildReplayRowMarkup(cardGroup, report);
    }

    function setCardBusy(cardKey, busy) {
      const card = document.getElementById(buildCardElementId(cardKey));
      if (card) {
        card.classList.toggle("loading", busy);
      }
    }

    async function refreshCardGroup(groupName) {
      const cardKey = cardKeyForGroup(groupName);
      const rangeKey = selectedHistoryRange(cardKey);
      setCardBusy(cardKey, true);
      try {
        const payload = await fetchJson(
          `/api/card?group_name=${encodeURIComponent(groupName)}&range_key=${encodeURIComponent(rangeKey)}`
        );
        const cardGroup = payload.card_group;
        if (!cardGroup || !cardGroup.selected_item) {
          await load();
          return;
        }
        cardGroup.display_name = displayNameForGroup(cardGroup.card_key || cardGroup.selected_item.group_name);
        const cardMarkup = buildCard(
          cardGroup,
          payload.domestic_route_preference,
          payload.overseas_route_preference,
          payload.history || [],
        );
        const existingCard = document.getElementById(buildCardElementId(cardGroup.card_key));
        if (existingCard) {
          existingCard.outerHTML = cardMarkup;
        } else {
          await load();
          return;
        }

        const replayMarkup = buildReplayRowMarkup(cardGroup, payload.replay_summary);
        const existingReplayRow = document.getElementById(buildReplayRowId(cardGroup.card_key));
        if (existingReplayRow) {
          existingReplayRow.outerHTML = replayMarkup;
        }

        const instrumentRowMarkup = buildInstrumentRow(
          cardGroup,
          payload.domestic_route_preference,
          payload.overseas_route_preference,
        );
        const existingInstrumentRow = document.getElementById(buildInstrumentRowId(cardGroup.card_key));
        if (existingInstrumentRow) {
          existingInstrumentRow.outerHTML = instrumentRowMarkup;
        }
        hydrateCardCharts(cardGroup.card_key);
      } catch (error) {
        setCardBusy(cardKey, false);
        throw error;
      }
    }

    async function handleHistoryRangeChange(cardKey, groupName, rangeKey) {
      saveHistoryRangeSelection(cardKey, rangeKey);
      try {
        await refreshCardGroup(groupName);
      } catch (error) {
        window.alert(`切换时间段失败：${error.message}`);
      }
    }

    async function handleOverseasRouteChange(groupName, symbol) {
      try {
        await postJson(`/api/overseas-routes/select?group_name=${encodeURIComponent(groupName)}`, {
          symbol: symbol === "__auto__" ? null : symbol,
        });
        await refreshCardGroup(groupName);
      } catch (error) {
        window.alert(`更新海外比对基准失败：${error.message}`);
      }
    }

    async function handleVariantChange(cardKey, groupName) {
      saveCardSelection(cardKey, groupName);
      try {
        await refreshCardGroup(groupName);
      } catch (error) {
        window.alert(`切换税口径失败：${error.message}`);
      }
    }

    async function load() {
      const [snapshot, alerts, deliveries] = await Promise.all([
        fetchJson("/api/snapshot-summary"),
        fetchJson("/api/alerts?limit=12"),
        fetchJson("/api/notification-deliveries?limit=12"),
      ]);

      document.getElementById("meta").innerHTML = `
        <span class="pill">最近刷新：${escapeHtml(formatDateTime(snapshot.as_of))}</span>
        <span class="pill">轮询间隔：${snapshot.health.poll_interval_sec} 秒</span>
        <span class="pill">美元兑人民币：${formatNumber(snapshot.health.latest_fx_rate, 4)}</span>
        <span class="pill">汇率源：${escapeHtml(sourceDisplayName(snapshot.health.latest_fx_source))}</span>
        <span class="pill">汇率模式：${snapshot.health.fx_is_frozen ? "冻结" : (snapshot.health.fx_is_live ? "实时" : "--")}</span>
        <span class="pill">汇率跳变：${formatPct(snapshot.health.latest_fx_jump_pct)}</span>
        <span class="pill">轮询轮次：${snapshot.health.total_cycles}</span>
      `;

      const cardGroups = buildCardGroups(snapshot.snapshots);
      renderInstrumentSummary(cardGroups, snapshot);

      if (!DASHBOARD_BOOTSTRAPPED) {
        const waitingCards = cardGroups.length
          ? cardGroups.map((cardGroup) => buildWaitingCard(
              cardGroup.card_key,
              cardGroup.selected_item.group_name,
              cardGroup.selected_item.status,
            ))
          : (snapshot.health.pairs || []).map((item) => buildWaitingCard(item.group_name, item.group_name, item.status));
        document.getElementById("cards").innerHTML = waitingCards.length
          ? waitingCards.join("")
          : `<article class="card"><div class="muted">等待第一轮轮询完成后展示卡片。</div></article>`;
        document.getElementById("replay").innerHTML = cardGroups.map((cardGroup) => buildReplayLoadingRow(cardGroup)).join("");
        await Promise.allSettled(cardGroups.map((cardGroup) => refreshCardGroup(cardGroup.selected_item.group_name)));
        DASHBOARD_BOOTSTRAPPED = true;
      } else {
        applySnapshotSummaryToCards(cardGroups);
        const missingCards = cardGroups.filter((cardGroup) => !document.getElementById(buildCardElementId(cardGroup.card_key)));
        if (missingCards.length) {
          await Promise.allSettled(missingCards.map((cardGroup) => refreshCardGroup(cardGroup.selected_item.group_name)));
        }
      }

      document.getElementById("sources").innerHTML = (snapshot.health.sources || []).map((item) => `
        <tr>
          <td>${escapeHtml(item.source_name)}</td>
          <td>${escapeHtml(sourceKindLabel(item.kind))}</td>
          <td>${item.success_count}</td>
          <td>${item.failure_count}</td>
          <td>${escapeHtml(formatDateTime(item.last_success_at || "--"))}</td>
          <td>${escapeHtml(item.last_symbol || "--")}</td>
          <td>${formatNumber(item.last_latency_ms, 1)} ms</td>
          <td>${escapeHtml(item.last_error || "--")}</td>
        </tr>
      `).join("");

      document.getElementById("deliveries").innerHTML = deliveries.map((item) => `
        <tr>
          <td>${escapeHtml(formatDateTime(item.ts_local || item.ts || "--"))}</td>
          <td>${escapeHtml(item.notifier_name)}</td>
          <td>${escapeHtml(displayNameForGroup(item.group_name))}</td>
          <td>${escapeHtml(severityLabel(item.severity))}</td>
          <td>${item.success ? "成功" : "失败"}</td>
          <td>${escapeHtml(item.response_message)}</td>
        </tr>
      `).join("");

      document.getElementById("alerts").innerHTML = alerts.map((item) => `
        <tr>
          <td>${escapeHtml(formatDateTime(item.ts_local || item.ts || "--"))}</td>
          <td>${escapeHtml(displayNameForGroup(item.group_name))}</td>
          <td>${escapeHtml(alertCategoryLabel(item.category))}</td>
          <td>${escapeHtml(severityLabel(item.severity))}</td>
          <td>${escapeHtml(item.message)}</td>
        </tr>
      `).join("");
    }

    load().catch((error) => {
      document.getElementById("cards").innerHTML = `<article class="card">页面加载失败：${escapeHtml(error.message)}</article>`;
    });
    setInterval(() => load().catch(() => {}), 10000);

    function cardSummaryForGroup(cardGroups, groupName) {
      for (const cardGroup of cardGroups) {
        const match = (cardGroup.variants || []).find((item) => item.group_name === groupName);
        if (match) {
          return { cardGroup, item: match };
        }
      }
      return null;
    }

    function setCardField(card, fieldName, value, { html = false, hidden = null } = {}) {
      const target = card.querySelector(`[data-card-field="${fieldName}"]`);
      if (!target) return;
      if (hidden !== null) {
        target.hidden = hidden;
      }
      if (html) {
        target.innerHTML = value;
      } else {
        target.textContent = value;
      }
    }

    function agesMarkup(item) {
      return `时效：国内 <strong>${formatNumber(item.domestic_age_sec, 1)}s</strong> / 海外 <strong>${formatNumber(item.overseas_age_sec, 1)}s</strong> / 汇率 <strong>${formatNumber(item.fx_age_sec, 1)}s</strong>`;
    }

    function applySnapshotSummaryToCards(cardGroups) {
      cardGroups.forEach((cardGroup) => {
        const card = document.getElementById(buildCardElementId(cardGroup.card_key));
        if (!card || !cardGroup.selected_item) {
          return;
        }
        const item = cardGroup.selected_item;
        const statusBadge = card.querySelector('[data-card-field="status"]');
        if (statusBadge) {
          statusBadge.className = `status ${item.status}`;
          statusBadge.textContent = statusLabel(item.status);
        }
        setCardField(card, "spread", formatNumber(item.spread, 4));
        setCardField(card, "spread_pct", formatPct(item.spread_pct));
        setCardField(card, "zscore", formatNumber(item.zscore, 2));
        setCardField(card, "fx_jump_pct", formatPct(item.fx_jump_pct));
        setCardField(card, "signal_state", signalStateLabel(item.signal_state));
        setCardField(card, "normalized_last", formatNumber(item.normalized_last, 4));
        setCardField(card, "overseas_last", formatNumber(item.overseas_last, 4));
        setCardField(card, "domestic_source", sourceDisplayName(item.domestic_source));
        setCardField(card, "overseas_source", sourceDisplayName(item.overseas_source));
        setCardField(card, "fx_source", sourceDisplayName(item.fx_source));
        setCardField(card, "fx_mode", item?.route_detail?.fx_is_frozen ? "冻结" : (item?.route_detail?.fx_is_live ? "实时" : "--"));
        setCardField(card, "ages", agesMarkup(item), { html: true });
        const pauseContainer = card.querySelector('[data-card-field="pause_reason_container"]');
        if (pauseContainer) {
          pauseContainer.hidden = !item.pause_reason;
        }
        setCardField(card, "pause_reason", item.pause_reason || "");
      });
    }
  
