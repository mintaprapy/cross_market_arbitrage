#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-6080}"
SERVICE_NAME="${SERVICE_NAME:-cross-market-monitor}"

check_service() {
  local service_name="$1"
  if systemctl is-active --quiet "${service_name}"; then
    echo "[ok] ${service_name} is active"
    return
  fi

  echo "[fail] ${service_name} is not active" >&2
  systemctl status "${service_name}" --no-pager || true
  exit 1
}

check_url() {
  local path="$1"
  local url="http://${HOST}:${PORT}${path}"
  if curl -fsS "${url}" >/dev/null; then
    echo "[ok] ${url}"
    return
  fi

  echo "[fail] ${url}" >&2
  exit 1
}

check_service "${SERVICE_NAME}"
check_url "/api/health"
check_url "/api/snapshot"
check_url "/"

echo "Post-deploy checks completed"
