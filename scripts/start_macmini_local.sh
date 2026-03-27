#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"
PID_FILE="$LOG_DIR/runtime.pid"
LOG_LINK="$LOG_DIR/runtime_latest.log"
CONFIG_PATH="$ROOT_DIR/config/monitor.yaml"
BIND_HOST="${BIND_HOST:-127.0.0.1}"
BIND_PORT="${BIND_PORT:-6080}"
HEALTH_WAIT_TIMEOUT_SEC="${HEALTH_WAIT_TIMEOUT_SEC:-30}"
HEALTH_WAIT_INTERVAL_SEC="${HEALTH_WAIT_INTERVAL_SEC:-2}"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
RUN_CMD=(
  "$PYTHON_BIN"
  -m cross_market_monitor.main
  --config "$CONFIG_PATH"
  serve
  --host "$BIND_HOST"
  --port "$BIND_PORT"
)

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "missing virtualenv python: $PYTHON_BIN" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"

echo "validating config: $CONFIG_PATH"
CONFIG_PATH_ENV="$CONFIG_PATH" "$PYTHON_BIN" - <<'PY'
import os
from cross_market_monitor.infrastructure.config_loader import load_config

config_path = os.environ["CONFIG_PATH_ENV"]
load_config(config_path)
print(f"config ok: {config_path}")
PY

if [[ -f "$PID_FILE" ]]; then
  old_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${old_pid:-}" ]] && kill -0 "$old_pid" 2>/dev/null; then
    echo "stopping existing process: $old_pid"
    kill "$old_pid" || true
    sleep 2
  fi
  rm -f "$PID_FILE"
fi

pkill -f "cross_market_monitor.main --config $CONFIG_PATH serve --host $BIND_HOST --port $BIND_PORT" 2>/dev/null || true

wait_for_health() {
  local deadline=$((SECONDS + HEALTH_WAIT_TIMEOUT_SEC))
  local health_url="http://127.0.0.1:$BIND_PORT/api/health"

  while (( SECONDS < deadline )); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "process exited unexpectedly; check log: $log_file" >&2
      tail -n 80 "$log_file" || true
      exit 1
    fi
    if curl -fsS "$health_url" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$HEALTH_WAIT_INTERVAL_SEC"
  done

  echo "service is still starting or health check timed out after ${HEALTH_WAIT_TIMEOUT_SEC}s" >&2
  echo "log: $log_file" >&2
  tail -n 40 "$log_file" || true
  exit 1
}

ts="$(date +%Y%m%d_%H%M%S)"
log_file="$LOG_DIR/runtime_${ts}.log"
ln -sfn "$log_file" "$LOG_LINK"

echo "starting service on http://$BIND_HOST:$BIND_PORT without git update..."
nohup "${RUN_CMD[@]}" </dev/null > "$log_file" 2>&1 &
pid="$!"
disown || true
echo "$pid" > "$PID_FILE"

wait_for_health

echo "pid: $pid"
echo "log: $log_file"
echo "health:"
curl -fsS "http://127.0.0.1:$BIND_PORT/api/health" | "$PYTHON_BIN" -m json.tool || true
