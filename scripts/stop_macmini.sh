#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"
WORKER_PID_FILE="$LOG_DIR/worker.pid"
API_PID_FILE="$LOG_DIR/api.pid"

stop_pid() {
  local pid="$1"
  if ! kill -0 "$pid" 2>/dev/null; then
    return 0
  fi
  echo "stopping process: $pid"
  kill "$pid" 2>/dev/null || true
  for _ in 1 2 3 4 5; do
    if ! kill -0 "$pid" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "forcing process stop: $pid"
  kill -9 "$pid" 2>/dev/null || true
}

for pid_file in "$WORKER_PID_FILE" "$API_PID_FILE" "$LOG_DIR/runtime.pid"; do
  if [[ -f "$pid_file" ]]; then
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ -n "${pid:-}" ]]; then
      stop_pid "$pid"
    fi
    rm -f "$pid_file"
  fi
done

pkill -f "cross_market_monitor.main --config $ROOT_DIR/config/monitor.yaml serve" 2>/dev/null || true
pkill -f "cross_market_monitor.main --config $ROOT_DIR/config/monitor.macmini.yaml serve" 2>/dev/null || true
pkill -f "cross_market_monitor.main --config $ROOT_DIR/config/monitor.macmini.yaml run-api" 2>/dev/null || true
pkill -f "cross_market_monitor.main --config $ROOT_DIR/config/monitor.macmini.yaml run-worker" 2>/dev/null || true

if lsof -iTCP:6080 -sTCP:LISTEN -n -P >/dev/null 2>&1; then
  echo "warning: port 6080 is still listening" >&2
  lsof -iTCP:6080 -sTCP:LISTEN -n -P || true
  exit 1
fi

echo "stopped"
