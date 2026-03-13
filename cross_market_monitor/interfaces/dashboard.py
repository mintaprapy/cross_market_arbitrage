from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse

from cross_market_monitor.application.service import MonitorRuntime, MonitorService


def create_app(service: MonitorService) -> FastAPI:
    runtime = MonitorRuntime(service)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await runtime.start()
        yield
        await runtime.stop()

    app = FastAPI(title=service.config.app.name, lifespan=lifespan)

    @app.get("/api/health")
    async def health() -> dict:
        return service.get_health()

    @app.get("/api/snapshot")
    async def snapshot() -> dict:
        return service.get_snapshot()

    @app.get("/api/history")
    async def history(group_name: str = Query(...), limit: int = Query(default=300, ge=1, le=5000)) -> list[dict]:
        return service.get_history(group_name, limit)

    @app.get("/api/alerts")
    async def alerts(limit: int = Query(default=100, ge=1, le=500)) -> list[dict]:
        return service.get_alerts(limit)

    @app.get("/api/notification-deliveries")
    async def notification_deliveries(limit: int = Query(default=100, ge=1, le=500)) -> list[dict]:
        return service.get_notification_deliveries(limit)

    @app.get("/api/replay/summary")
    async def replay_summary(
        group_name: str = Query(...),
        limit: int = Query(default=1000, ge=1, le=10000),
        start_ts: str | None = Query(default=None),
        end_ts: str | None = Query(default=None),
    ) -> dict:
        return service.replay_summary(group_name, limit=limit, start_ts=start_ts, end_ts=end_ts)

    @app.get("/", response_class=HTMLResponse)
    async def index() -> HTMLResponse:
        return HTMLResponse(_dashboard_html(service.config.app.name))

    return app


def _dashboard_html(title: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    :root {{
      --bg: #0d1b2a;
      --panel: rgba(18, 38, 57, 0.86);
      --panel-alt: rgba(18, 38, 57, 0.65);
      --border: rgba(157, 201, 255, 0.18);
      --text: #ecf4ff;
      --muted: #95a8bf;
      --good: #49dcb1;
      --warn: #ffb454;
      --bad: #ff6b6b;
      --accent: #79c0ff;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(121, 192, 255, 0.24), transparent 35%),
        radial-gradient(circle at top right, rgba(255, 180, 84, 0.22), transparent 30%),
        linear-gradient(160deg, #08121d 0%, #0d1b2a 100%);
      min-height: 100vh;
    }}
    .shell {{
      max-width: 1400px;
      margin: 0 auto;
      padding: 24px;
    }}
    .hero {{
      display: grid;
      gap: 12px;
      margin-bottom: 22px;
    }}
    .hero h1 {{
      margin: 0;
      font-size: clamp(28px, 4vw, 42px);
      letter-spacing: 0.03em;
    }}
    .hero p {{
      margin: 0;
      color: var(--muted);
      max-width: 780px;
      line-height: 1.6;
    }}
    .meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }}
    .pill {{
      padding: 8px 12px;
      border-radius: 999px;
      border: 1px solid var(--border);
      background: rgba(255,255,255,0.04);
      color: var(--muted);
      font-size: 13px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(290px, 1fr));
      gap: 16px;
    }}
    .card, .panel {{
      border: 1px solid var(--border);
      background: var(--panel);
      border-radius: 20px;
      backdrop-filter: blur(12px);
      box-shadow: 0 20px 40px rgba(0, 0, 0, 0.18);
    }}
    .card {{
      padding: 18px;
      display: grid;
      gap: 12px;
    }}
    .head {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }}
    .title {{
      display: grid;
      gap: 4px;
    }}
    .title strong {{
      font-size: 18px;
      letter-spacing: 0.04em;
    }}
    .title span {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .status {{
      padding: 7px 10px;
      border-radius: 999px;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .status.ok {{ background: rgba(73, 220, 177, 0.14); color: var(--good); }}
    .status.partial, .status.stale {{ background: rgba(255, 180, 84, 0.14); color: var(--warn); }}
    .status.error {{ background: rgba(255, 107, 107, 0.14); color: var(--bad); }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }}
    .metric {{
      padding: 12px;
      border-radius: 14px;
      background: var(--panel-alt);
    }}
    .metric label {{
      display: block;
      font-size: 11px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 6px;
    }}
    .metric strong {{
      font-family: "SF Mono", Menlo, monospace;
      font-size: 18px;
    }}
    .detail {{
      display: grid;
      gap: 8px;
      color: var(--muted);
      font-size: 13px;
    }}
    .sparkline {{
      width: 100%;
      height: 56px;
      background: linear-gradient(180deg, rgba(121,192,255,0.10), rgba(121,192,255,0.02));
      border-radius: 14px;
      overflow: hidden;
    }}
    .panel {{
      margin-top: 18px;
      padding: 18px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
    }}
    th, td {{
      text-align: left;
      padding: 12px 8px;
      border-bottom: 1px solid rgba(255,255,255,0.08);
      font-size: 13px;
    }}
    th {{
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 11px;
    }}
    @media (max-width: 700px) {{
      .shell {{ padding: 16px; }}
      .metrics {{ grid-template-columns: 1fr; }}
      th:nth-child(4), td:nth-child(4) {{ display: none; }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <h1>{title}</h1>
      <p>统一国内外商品价格口径，实时监控理论价差、统计偏离和可成交方向价差。页面每 10 秒刷新一次，并展示最近告警与回放分析摘要。</p>
      <div class="meta" id="meta"></div>
    </section>
    <section class="grid" id="cards"></section>
    <section class="panel">
      <div class="head">
        <div class="title">
          <strong>Replay Summary</strong>
          <span>recent stored snapshot analysis</span>
        </div>
      </div>
      <table>
        <thead>
          <tr>
            <th>Group</th>
            <th>Samples</th>
            <th>Mean Spread</th>
            <th>Conv Ratio</th>
            <th>Spread Breach</th>
            <th>Zscore Breach</th>
          </tr>
        </thead>
        <tbody id="replay"></tbody>
      </table>
    </section>
    <section class="panel">
      <div class="head">
        <div class="title">
          <strong>Recent Alerts</strong>
          <span>latest warning stream</span>
        </div>
      </div>
      <table>
        <thead>
          <tr>
            <th>Time</th>
            <th>Group</th>
            <th>Category</th>
            <th>Severity</th>
            <th>Message</th>
          </tr>
        </thead>
        <tbody id="alerts"></tbody>
      </table>
    </section>
  </div>
  <script>
    async function fetchJson(url) {{
      const res = await fetch(url);
      if (!res.ok) throw new Error(`Request failed: ${{res.status}}`);
      return await res.json();
    }}

    function formatNumber(value, digits = 2) {{
      if (value === null || value === undefined || Number.isNaN(value)) return "--";
      return Number(value).toFixed(digits);
    }}

    function formatPct(value) {{
      if (value === null || value === undefined || Number.isNaN(value)) return "--";
      return `${{(Number(value) * 100).toFixed(2)}}%`;
    }}

    function sparkline(values) {{
      const clean = values.filter((value) => value !== null && value !== undefined);
      if (!clean.length) return "";
      const min = Math.min(...clean);
      const max = Math.max(...clean);
      const range = max - min || 1;
      const points = clean.map((value, index) => {{
        const x = clean.length === 1 ? 0 : (index / (clean.length - 1)) * 100;
        const y = 100 - ((value - min) / range) * 100;
        return `${{x}},${{y}}`;
      }}).join(" ");
      return `<svg viewBox="0 0 100 100" preserveAspectRatio="none" width="100%" height="56">
        <polyline fill="none" stroke="rgba(121,192,255,0.95)" stroke-width="3" points="${{points}}" />
      </svg>`;
    }}

    async function load() {{
      const [snapshot, alerts] = await Promise.all([
        fetchJson("/api/snapshot"),
        fetchJson("/api/alerts?limit=12")
      ]);

      document.getElementById("meta").innerHTML = `
        <span class="pill">Last refresh: ${{snapshot.as_of || "--"}}</span>
        <span class="pill">Poll interval: ${{snapshot.health.poll_interval_sec}}s</span>
        <span class="pill">FX USD/CNY: ${{formatNumber(snapshot.health.latest_fx_rate, 4)}}</span>
        <span class="pill">Total cycles: ${{snapshot.health.total_cycles}}</span>
      `;

      const cards = await Promise.all(snapshot.snapshots.map(async (item) => {{
        const history = await fetchJson(`/api/history?group_name=${{encodeURIComponent(item.group_name)}}&limit=40`);
        const spark = sparkline(history.map((row) => row.spread));
        return `
          <article class="card">
            <div class="head">
              <div class="title">
                <strong>${{item.group_name}}</strong>
                <span>${{item.domestic_symbol}} vs ${{item.overseas_symbol}}</span>
              </div>
              <span class="status ${{item.status}}">${{item.status}}</span>
            </div>
            <div class="metrics">
              <div class="metric">
                <label>Theoretical Spread</label>
                <strong>${{formatNumber(item.spread, 4)}}</strong>
              </div>
              <div class="metric">
                <label>Spread %</label>
                <strong>${{formatPct(item.spread_pct)}}</strong>
              </div>
              <div class="metric">
                <label>Z-score</label>
                <strong>${{formatNumber(item.zscore, 2)}}</strong>
              </div>
              <div class="metric">
                <label>FX</label>
                <strong>${{formatNumber(item.fx_rate, 4)}}</strong>
              </div>
            </div>
            <div class="sparkline">${{spark}}</div>
            <div class="detail">
              <div>Normalized domestic: <strong>${{formatNumber(item.normalized_last, 4)}}</strong> ${{item.target_unit}}</div>
              <div>Overseas last: <strong>${{formatNumber(item.overseas_last, 4)}}</strong> ${{item.target_unit}}</div>
              <div>Buy domestic / sell overseas: <strong>${{formatNumber(item.executable_buy_domestic_sell_overseas, 4)}}</strong></div>
              <div>Buy overseas / sell domestic: <strong>${{formatNumber(item.executable_buy_overseas_sell_domestic, 4)}}</strong></div>
              <div>Data ages: D ${{formatNumber(item.domestic_age_sec, 1)}}s / O ${{formatNumber(item.overseas_age_sec, 1)}}s / FX ${{formatNumber(item.fx_age_sec, 1)}}s</div>
            </div>
          </article>
        `;
      }}));
      document.getElementById("cards").innerHTML = cards.join("");

      const replayRows = await Promise.all(snapshot.snapshots.map(async (item) => {{
        const report = await fetchJson(`/api/replay/summary?group_name=${{encodeURIComponent(item.group_name)}}&limit=300`);
        return `
          <tr>
            <td>${{item.group_name}}</td>
            <td>${{report.sample_count}}</td>
            <td>${{formatNumber(report.spread_mean, 4)}}</td>
            <td>${{report.convergence_ratio === null ? "--" : `${{(report.convergence_ratio * 100).toFixed(1)}}%`}}</td>
            <td>${{report.spread_pct_breach_count}}</td>
            <td>${{report.zscore_breach_count}}</td>
          </tr>
        `;
      }}));
      document.getElementById("replay").innerHTML = replayRows.join("");

      document.getElementById("alerts").innerHTML = alerts.map((item) => `
        <tr>
          <td>${{item.ts}}</td>
          <td>${{item.group_name}}</td>
          <td>${{item.category}}</td>
          <td>${{item.severity}}</td>
          <td>${{item.message}}</td>
        </tr>
      `).join("");
    }}

    load().catch((error) => {{
      document.getElementById("cards").innerHTML = `<article class="card">Dashboard load failed: ${{error.message}}</article>`;
    }});
    setInterval(() => load().catch(() => {{}}), 10000);
  </script>
</body>
</html>
"""
