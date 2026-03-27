#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs/db_growth"
PID_FILE="$LOG_DIR/watch.pid"
LOG_FILE="$LOG_DIR/watch.log"
SAMPLES_FILE="$LOG_DIR/samples.csv"
SIZE_FILE="$LOG_DIR/size_latest.txt"
CONFIG_PATH="${CONFIG_PATH:-$ROOT_DIR/config/monitor.yaml}"
INTERVAL_SEC="${DB_GROWTH_INTERVAL_SEC:-900}"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "missing virtualenv python: $PYTHON_BIN" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"

is_running() {
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      return 0
    fi
  fi
  return 1
}

start_watch() {
  if is_running; then
    echo "db growth watcher already running: $(cat "$PID_FILE")"
    return 0
  fi

  (
    while true; do
      {
        echo "===== $(date '+%Y-%m-%d %H:%M:%S %z') ====="
        "$PYTHON_BIN" "$ROOT_DIR/scripts/check_db_size.py" --config "$CONFIG_PATH" > "$SIZE_FILE"
        "$PYTHON_BIN" "$ROOT_DIR/scripts/check_db_growth.py" --config "$CONFIG_PATH" --samples-path "$SAMPLES_FILE"
        echo
      } >> "$LOG_FILE" 2>&1
      sleep "$INTERVAL_SEC"
    done
  ) &

  local pid="$!"
  disown || true
  echo "$pid" > "$PID_FILE"
  echo "started db growth watcher: $pid"
  echo "log: $LOG_FILE"
  echo "samples: $SAMPLES_FILE"
}

stop_watch() {
  if ! is_running; then
    echo "db growth watcher is not running"
    rm -f "$PID_FILE"
    return 0
  fi
  local pid
  pid="$(cat "$PID_FILE")"
  kill "$pid" || true
  sleep 1
  rm -f "$PID_FILE"
  echo "stopped db growth watcher: $pid"
}

status_watch() {
  if is_running; then
    echo "db growth watcher running: $(cat "$PID_FILE")"
    echo "log: $LOG_FILE"
    echo "samples: $SAMPLES_FILE"
    return 0
  fi
  echo "db growth watcher is not running"
}

tail_watch() {
  touch "$LOG_FILE"
  tail -n 40 "$LOG_FILE"
}

case "${1:-start}" in
  start)
    start_watch
    ;;
  stop)
    stop_watch
    ;;
  restart)
    stop_watch
    start_watch
    ;;
  status)
    status_watch
    ;;
  tail)
    tail_watch
    ;;
  *)
    echo "usage: $0 {start|stop|restart|status|tail}" >&2
    exit 1
    ;;
esac
