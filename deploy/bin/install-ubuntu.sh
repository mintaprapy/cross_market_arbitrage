#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
  echo "Run as root: sudo ./deploy/bin/install-ubuntu.sh" >&2
  exit 1
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

APP_DIR="${APP_DIR:-${REPO_DIR}}"
APP_USER="${APP_USER:-${SUDO_USER:-ubuntu}}"
APP_GROUP="${APP_GROUP:-}"
VENV_DIR="${VENV_DIR:-${APP_DIR}/.venv}"
CONFIG_PATH="${CONFIG_PATH:-${APP_DIR}/config/monitor.yaml}"
SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
NGINX_AVAILABLE_DIR="${NGINX_AVAILABLE_DIR:-/etc/nginx/sites-available}"
NGINX_ENABLED_DIR="${NGINX_ENABLED_DIR:-/etc/nginx/sites-enabled}"
NGINX_SITE_NAME="${NGINX_SITE_NAME:-cross-market-monitor}"
SERVICE_NAME="${SERVICE_NAME:-cross-market-monitor}"
SERVER_NAME="${SERVER_NAME:-_}"
API_HOST="${API_HOST:-}"
API_PORT="${API_PORT:-}"
API_BIND="${API_BIND:-}"
PIP_EXTRAS="${PIP_EXTRAS:-tqsdk,parquet}"
INSTALL_NGINX="${INSTALL_NGINX:-1}"
PYTHON_BIN="${PYTHON_BIN:-}"

if ! id -u "${APP_USER}" >/dev/null 2>&1; then
  echo "User ${APP_USER} does not exist" >&2
  exit 1
fi

if [[ -z "${APP_GROUP}" ]]; then
  APP_GROUP="$(id -gn "${APP_USER}")"
fi

if [[ ! -f "${CONFIG_PATH}" ]]; then
  echo "Config file not found: ${CONFIG_PATH}" >&2
  exit 1
fi

choose_python_bin() {
  local candidate=""
  local candidates=()
  if [[ -n "${PYTHON_BIN}" ]]; then
    candidates+=("${PYTHON_BIN}")
  fi
  candidates+=("python3" "python3.12" "python3.11" "python3.10")
  for candidate in "${candidates[@]}"; do
    if ! command -v "${candidate}" >/dev/null 2>&1; then
      continue
    fi
    if "${candidate}" - <<'PY' >/dev/null 2>&1
import sys
raise SystemExit(0 if sys.version_info >= (3, 10) else 1)
PY
    then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  return 1
}

if ! PYTHON_BIN="$(choose_python_bin)"; then
  echo "Python 3.10+ is required. Install python3.10+ and retry." >&2
  exit 1
fi

install -d -o "${APP_USER}" -g "${APP_GROUP}" "${APP_DIR}" "${APP_DIR}/data" "${APP_DIR}/exports"

if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
  chown -R "${APP_USER}:${APP_GROUP}" "${VENV_DIR}"
fi

run_as_app() {
  runuser -u "${APP_USER}" -- bash -lc "$1"
}

run_as_app "cd '${APP_DIR}' && '${VENV_DIR}/bin/python' -m pip install --upgrade pip"
if [[ -n "${PIP_EXTRAS}" ]]; then
  run_as_app "cd '${APP_DIR}' && '${VENV_DIR}/bin/python' -m pip install -e '.[${PIP_EXTRAS}]'"
else
  run_as_app "cd '${APP_DIR}' && '${VENV_DIR}/bin/python' -m pip install -e ."
fi

mapfile -t config_values < <(
  "${VENV_DIR}/bin/python" - <<PY
from pathlib import Path

from cross_market_monitor.infrastructure.config_loader import load_config

config = load_config(r"""${CONFIG_PATH}""")
print(config.app.bind_host)
print(config.app.bind_port)
print(Path(config.app.sqlite_path).resolve().parent)
print(Path(config.app.export_dir).resolve())
PY
)

API_HOST="${API_HOST:-${config_values[0]}}"
API_PORT="${API_PORT:-${config_values[1]}}"
NGINX_UPSTREAM_HOST="${NGINX_UPSTREAM_HOST:-localhost}"
API_BIND="${API_BIND:-${NGINX_UPSTREAM_HOST}:${API_PORT}}"
WRITE_PATHS="${config_values[2]} ${config_values[3]}"

render_unit() {
  local source_file="$1"
  local target_file="$2"
  sed \
    -e "s|/srv/cross_market_arbitrage|${APP_DIR}|g" \
    -e "s|/srv/cross_market_arbitrage/config/monitor.yaml|${CONFIG_PATH}|g" \
    -e "s|User=ubuntu|User=${APP_USER}|g" \
    -e "s|Group=ubuntu|Group=${APP_GROUP}|g" \
    -e "s|ReadWritePaths=/srv/cross_market_arbitrage/data /srv/cross_market_arbitrage/exports|ReadWritePaths=${WRITE_PATHS}|g" \
    -e "s|serve --host 0.0.0.0 --port 6080|serve --host ${API_HOST} --port ${API_PORT}|g" \
    "${source_file}" > "${target_file}"
}

render_unit \
  "${REPO_DIR}/systemd/cross-market-monitor.service" \
  "${SYSTEMD_DIR}/${SERVICE_NAME}.service"

systemctl daemon-reload
for legacy_service in cross-market-monitor-worker cross-market-monitor-api; do
  if systemctl list-unit-files "${legacy_service}.service" --no-legend >/dev/null 2>&1; then
    systemctl disable --now "${legacy_service}" >/dev/null 2>&1 || true
  fi
done
systemctl enable --now "${SERVICE_NAME}"

if [[ "${INSTALL_NGINX}" == "1" ]] && command -v nginx >/dev/null 2>&1; then
  install -d "${NGINX_AVAILABLE_DIR}" "${NGINX_ENABLED_DIR}"
  sed \
    -e "s|server_name _;|server_name ${SERVER_NAME};|g" \
    -e "s|localhost:6080|${API_BIND}|g" \
    "${REPO_DIR}/deploy/nginx/cross-market-monitor.conf" > "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}"
  ln -sf "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}" "${NGINX_ENABLED_DIR}/${NGINX_SITE_NAME}"
  nginx -t
  systemctl reload nginx
fi

echo "Installed service for ${APP_DIR}: ${SYSTEMD_DIR}/${SERVICE_NAME}.service"
if [[ "${INSTALL_NGINX}" == "1" ]] && command -v nginx >/dev/null 2>&1; then
  echo "Nginx site: ${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}"
fi
