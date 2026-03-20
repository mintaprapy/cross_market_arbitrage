#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"
PID_FILE="$LOG_DIR/runtime.pid"
LOG_LINK="$LOG_DIR/runtime_latest.log"
BIND_HOST="${BIND_HOST:-127.0.0.1}"
BIND_PORT="${BIND_PORT:-6080}"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
PIP_CMD=("$PYTHON_BIN" -m pip)
RUN_CMD=(
  "$PYTHON_BIN"
  -m cross_market_monitor.main
  --config "$ROOT_DIR/config/monitor.yaml"
  serve
  --host "$BIND_HOST"
  --port "$BIND_PORT"
)

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "missing virtualenv python: $PYTHON_BIN" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"

if [[ -f "$PID_FILE" ]]; then
  old_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${old_pid:-}" ]] && kill -0 "$old_pid" 2>/dev/null; then
    echo "stopping existing process: $old_pid"
    kill "$old_pid" || true
    sleep 2
  fi
  rm -f "$PID_FILE"
fi

pkill -f "cross_market_monitor.main --config $ROOT_DIR/config/monitor.yaml serve --host $BIND_HOST --port $BIND_PORT" 2>/dev/null || true

echo "pulling latest code..."
git -C "$ROOT_DIR" pull --ff-only origin main

echo "installing/updating package..."
"${PIP_CMD[@]}" install -e ".[tqsdk,parquet]"

ts="$(date +%Y%m%d_%H%M%S)"
log_file="$LOG_DIR/runtime_${ts}.log"
ln -sfn "$log_file" "$LOG_LINK"

echo "starting service on http://$BIND_HOST:$BIND_PORT ..."
nohup "${RUN_CMD[@]}" </dev/null > "$log_file" 2>&1 &
pid="$!"
disown || true
echo "$pid" > "$PID_FILE"

sleep 6

if ! kill -0 "$pid" 2>/dev/null; then
  echo "process exited unexpectedly; check log: $log_file" >&2
  tail -n 80 "$log_file" || true
  exit 1
fi

echo "pid: $pid"
echo "log: $log_file"
echo "health:"
curl -fsS "http://127.0.0.1:$BIND_PORT/api/health" | "$PYTHON_BIN" -m json.tool || true
