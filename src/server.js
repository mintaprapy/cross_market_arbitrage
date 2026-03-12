import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { FRONTEND_HISTORY_LIMIT, PORT } from './config.js';
import { SpreadMonitor } from './monitor.js';

const app = express();
const monitor = new SpreadMonitor();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const publicDir = path.join(__dirname, '..', 'public');

app.disable('x-powered-by');
app.use(express.json());
app.use(express.static(publicDir));

app.get('/api/health', (_req, res) => {
  res.json(monitor.getHealth());
});

app.get('/api/snapshot', (_req, res) => {
  res.json(monitor.getSnapshot());
});

app.get('/api/history', (req, res) => {
  const assetKey = String(req.query.asset || '').trim();
  if (!assetKey) {
    res.status(400).json({ ok: false, error: 'Query parameter "asset" is required.' });
    return;
  }

  const history = monitor.getHistory(assetKey, {
    limit: req.query.limit ?? FRONTEND_HISTORY_LIMIT,
    hours: req.query.hours
  });
  if (!history) {
    res.status(404).json({ ok: false, error: `Unknown asset: ${assetKey}` });
    return;
  }

  res.json(history);
});

app.get('*', (_req, res) => {
  res.sendFile(path.join(publicDir, 'index.html'));
});

const server = app.listen(PORT, () => {
  console.log(`Spread monitor listening on http://localhost:${PORT}`);
  monitor.start();
});

function shutdown(signal) {
  console.log(`Received ${signal}, shutting down.`);
  monitor.stop();
  server.close(() => {
    process.exit(0);
  });
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
