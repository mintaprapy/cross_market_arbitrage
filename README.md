# Spread Monitor MVP

Simple Node.js + Express dashboard for watching convergence/divergence between China domestic futures main contracts and temporary crypto proxy quotes.

## What it does

- Polls domestic futures and external proxy prices every 5-15 seconds
- Normalizes both legs into the same domestic unit
- Computes `spread_abs`, `spread_pct`, rolling z-score, and rolling percentile
- Stores a bounded in-memory history per asset
- Exposes JSON APIs plus a small vanilla frontend dashboard

Assets included in this MVP:

- Gold
- Silver
- Crude oil

## Requirements

- Node.js 18+ recommended

## Install

```bash
npm install
```

## Run

```bash
npm start
```

Open `http://localhost:3000`.

## Environment variables

- `PORT` default `3000`
- `POLL_INTERVAL_MS` default `10000`, clamped to `5000-15000`
- `HISTORY_LIMIT` default `1500`
- `ROLLING_WINDOW_SIZE` default `120`
- `FETCH_TIMEOUT_MS` default `8000`
- `FRONTEND_HISTORY_LIMIT` default `180`

Example:

```bash
POLL_INTERVAL_MS=5000 ROLLING_WINDOW_SIZE=180 npm start
```

## API

- `GET /api/health`
- `GET /api/snapshot`
- `GET /api/history?asset=gold&limit=300`

## Config-driven mapping

Edit [`src/config.js`](/Users/m2/.openclaw/workspace/src/config.js) to change domestic or external symbols. The source/unit metadata is intentionally kept close to the symbols so the mapping remains easy to update.

## Data and mapping caveats

- Domestic references use Sina futures continuous-style symbols such as `au0`, `ag0`, `sc0`. Depending on your preferred vendor, these may need adjustment.
- External legs are temporary crypto proxies, not perfect cross-market hedges.
- `PAXGUSDT` is a practical gold proxy.
- `XAGUSDT` and `WTIUSDT` may not exist on every venue or may require replacement with symbols that match your exchange/data vendor.
- If one leg is unavailable, the app keeps running, records partial samples, and shows gaps instead of crashing.

## File layout

- [`src/server.js`](/Users/m2/.openclaw/workspace/src/server.js): Express app and API routes
- [`src/monitor.js`](/Users/m2/.openclaw/workspace/src/monitor.js): polling loop, stats, in-memory history
- [`src/dataSources.js`](/Users/m2/.openclaw/workspace/src/dataSources.js): market data adapters
- [`src/normalization.js`](/Users/m2/.openclaw/workspace/src/normalization.js): unit conversion and stats helpers
- [`public/index.html`](/Users/m2/.openclaw/workspace/public/index.html), [`public/app.js`](/Users/m2/.openclaw/workspace/public/app.js), [`public/styles.css`](/Users/m2/.openclaw/workspace/public/styles.css): dashboard
