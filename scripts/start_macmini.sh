#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"
WORKER_PID_FILE="$LOG_DIR/worker.pid"
API_PID_FILE="$LOG_DIR/api.pid"
WORKER_LOG_LINK="$LOG_DIR/worker_latest.log"
API_LOG_LINK="$LOG_DIR/runtime_latest.log"
CONFIG_PATH="$ROOT_DIR/config/monitor.macmini.yaml"
BIND_HOST="${BIND_HOST:-127.0.0.1}"
BIND_PORT="${BIND_PORT:-6080}"
HEALTH_WAIT_TIMEOUT_SEC="${HEALTH_WAIT_TIMEOUT_SEC:-30}"
HEALTH_WAIT_INTERVAL_SEC="${HEALTH_WAIT_INTERVAL_SEC:-2}"
PORT_RELEASE_TIMEOUT_SEC="${PORT_RELEASE_TIMEOUT_SEC:-15}"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
PIP_CMD=("$PYTHON_BIN" -m pip)
WORKER_CMD=(
  "$PYTHON_BIN"
  -m cross_market_monitor.main
  --config "$CONFIG_PATH"
  run-worker
)
API_CMD=(
  "$PYTHON_BIN"
  -m cross_market_monitor.main
  --config "$CONFIG_PATH"
  run-api
  --host "$BIND_HOST"
  --port "$BIND_PORT"
)

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "missing virtualenv python: $PYTHON_BIN" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"

validate_config() {
  echo "validating config: $CONFIG_PATH"
  CONFIG_PATH_ENV="$CONFIG_PATH" "$PYTHON_BIN" - <<'PY'
import os
from cross_market_monitor.infrastructure.config_loader import load_config

config_path = os.environ["CONFIG_PATH_ENV"]
load_config(config_path)
print(f"config ok: {config_path}")
PY
}

stop_existing() {
  for pid_file in "$WORKER_PID_FILE" "$API_PID_FILE" "$LOG_DIR/runtime.pid"; do
    if [[ -f "$pid_file" ]]; then
      old_pid="$(cat "$pid_file" 2>/dev/null || true)"
      if [[ -n "${old_pid:-}" ]] && kill -0 "$old_pid" 2>/dev/null; then
        echo "stopping existing process: $old_pid"
        kill "$old_pid" || true
        sleep 2
      fi
      rm -f "$pid_file"
    fi
  done
  pkill -f "cross_market_monitor.main --config $CONFIG_PATH serve --host $BIND_HOST --port $BIND_PORT" 2>/dev/null || true
  pkill -f "cross_market_monitor.main --config $CONFIG_PATH run-api --host $BIND_HOST --port $BIND_PORT" 2>/dev/null || true
  pkill -f "cross_market_monitor.main --config $CONFIG_PATH run-worker" 2>/dev/null || true
}

wait_for_port_release() {
  local deadline=$((SECONDS + PORT_RELEASE_TIMEOUT_SEC))

  while (( SECONDS < deadline )); do
    if ! lsof -nP -iTCP:"$BIND_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "port $BIND_PORT is still in use after ${PORT_RELEASE_TIMEOUT_SEC}s" >&2
  lsof -nP -iTCP:"$BIND_PORT" -sTCP:LISTEN || true
  exit 1
}

wait_for_health() {
  local deadline=$((SECONDS + HEALTH_WAIT_TIMEOUT_SEC))
  local health_url="http://127.0.0.1:$BIND_PORT/api/health"

  while (( SECONDS < deadline )); do
    if ! kill -0 "$api_pid" 2>/dev/null; then
      echo "api process exited unexpectedly; check log: $api_log_file" >&2
      tail -n 80 "$api_log_file" || true
      exit 1
    fi
    if ! kill -0 "$worker_pid" 2>/dev/null; then
      echo "worker process exited unexpectedly; check log: $worker_log_file" >&2
      tail -n 80 "$worker_log_file" || true
      exit 1
    fi
    if curl -fsS "$health_url" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$HEALTH_WAIT_INTERVAL_SEC"
  done

  echo "service is still starting or health check timed out after ${HEALTH_WAIT_TIMEOUT_SEC}s" >&2
  echo "api log: $api_log_file" >&2
  tail -n 40 "$api_log_file" || true
  echo "worker log: $worker_log_file" >&2
  tail -n 40 "$worker_log_file" || true
  exit 1
}

echo "pulling latest code..."
git -C "$ROOT_DIR" pull --ff-only origin main

echo "installing/updating package..."
"${PIP_CMD[@]}" install -e ".[tqsdk,parquet]"

validate_config

stop_existing
wait_for_port_release

ts="$(date +%Y%m%d_%H%M%S)"
worker_log_file="$LOG_DIR/worker_${ts}.log"
api_log_file="$LOG_DIR/runtime_${ts}.log"
ln -sfn "$worker_log_file" "$WORKER_LOG_LINK"
ln -sfn "$api_log_file" "$API_LOG_LINK"

echo "starting worker..."
nohup "${WORKER_CMD[@]}" </dev/null > "$worker_log_file" 2>&1 &
worker_pid="$!"
disown || true
echo "$worker_pid" > "$WORKER_PID_FILE"

echo "starting api on http://$BIND_HOST:$BIND_PORT ..."
nohup "${API_CMD[@]}" </dev/null > "$api_log_file" 2>&1 &
api_pid="$!"
disown || true
echo "$api_pid" > "$API_PID_FILE"

wait_for_health

echo "worker pid: $worker_pid"
echo "worker log: $worker_log_file"
echo "api pid: $api_pid"
echo "api log: $api_log_file"
echo "health:"
curl -fsS "http://127.0.0.1:$BIND_PORT/api/health" | "$PYTHON_BIN" -m json.tool || true
