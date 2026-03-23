#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

ACTION="${1:-run}"
if [[ $# -gt 0 ]]; then
  shift
fi
EXTRA_ARGS=("$@")

CONFIG_FILE="${CONFIG_FILE:-$ROOT_DIR/config.env}"
GOMODCACHE="${GOMODCACHE:-$ROOT_DIR/.gomodcache}"
GOCACHE="${GOCACHE:-$ROOT_DIR/.gocache}"

GO_BIN=""
DB_PATH=""
HTTP_ADDR=""
COMMON_FLAGS=()

load_config() {
  if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "Missing $CONFIG_FILE. Copy config.env.example -> config.env and fill it."
    exit 1
  fi

  set -a
  source "$CONFIG_FILE"
  set +a

  GO_BIN="${GO_BIN:-/home/shmel/go/go1.24.0/bin/go}"
  DB_PATH="${TINVEST_DB_PATH:-./pnl.sqlite}"
  HTTP_ADDR="${TINVEST_HTTP_ADDR:-:8080}"

  if [[ -z "${TINVEST_TOKEN:-}" ]]; then
    echo "Set TINVEST_TOKEN in $CONFIG_FILE"
    exit 1
  fi

  COMMON_FLAGS=(
    -token "$TINVEST_TOKEN"
    -app-name "${TINVEST_APP_NAME:-tinvest-pnl-report}"
    -timeout "${TINVEST_TIMEOUT:-60s}"
  )

  if [[ -n "${TINVEST_ACCOUNT_ID:-}" ]]; then
    COMMON_FLAGS+=( -account-id "$TINVEST_ACCOUNT_ID" )
  fi
  if [[ -n "${TINVEST_ENDPOINT:-}" ]]; then
    COMMON_FLAGS+=( -endpoint "$TINVEST_ENDPOINT" )
  fi
  if [[ "${TINVEST_SANDBOX:-false}" == "true" ]]; then
    COMMON_FLAGS+=( -sandbox )
  fi
  if [[ -n "${TINVEST_CA_CERT_FILE:-}" ]]; then
    COMMON_FLAGS+=( -ca-cert-file "$TINVEST_CA_CERT_FILE" )
  fi
  if [[ "${TINVEST_INSECURE_SKIP_VERIFY:-false}" == "true" ]]; then
    COMMON_FLAGS+=( -insecure-skip-verify )
  fi
}

require_account() {
  if [[ -z "${TINVEST_ACCOUNT_ID:-}" ]]; then
    echo "Set TINVEST_ACCOUNT_ID in $CONFIG_FILE"
    exit 1
  fi
}

go_run() {
  GOMODCACHE="$GOMODCACHE" GOCACHE="$GOCACHE" \
    "$GO_BIN" run . "$@"
}

http_addr_to_url() {
  local addr="$1"
  local host
  local port

  if [[ "$addr" == :* ]]; then
    host="127.0.0.1"
    port="${addr#:}"
  elif [[ "$addr" == *:* ]]; then
    host="${addr%:*}"
    port="${addr##*:}"
    if [[ -z "$host" || "$host" == "0.0.0.0" ]]; then
      host="127.0.0.1"
    fi
  else
    host="127.0.0.1"
    port="$addr"
  fi

  echo "http://${host}:${port}"
}

wait_http_ready() {
  local url="$1"
  local retries="${2:-80}"
  local i

  if command -v curl >/dev/null 2>&1; then
    for ((i = 1; i <= retries; i++)); do
      if curl -fsS "$url" >/dev/null 2>&1; then
        return 0
      fi
      sleep 0.25
    done
    return 1
  fi

  local host_port="${url#http://}"
  local host="${host_port%:*}"
  local port="${host_port##*:}"

  for ((i = 1; i <= retries; i++)); do
    if (echo >/dev/tcp/"$host"/"$port") >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done

  return 1
}

open_browser() {
  local url="$1"

  if command -v xdg-open >/dev/null 2>&1; then
    xdg-open "$url" >/dev/null 2>&1 &
    return 0
  fi
  if command -v open >/dev/null 2>&1; then
    open "$url" >/dev/null 2>&1 &
    return 0
  fi
  if command -v wslview >/dev/null 2>&1; then
    wslview "$url" >/dev/null 2>&1 &
    return 0
  fi

  return 1
}

run_sync_then_serve() {
  require_account

  echo "[1/2] Sync operations + forecasts..."
  go_run -mode sync-all -db-path "$DB_PATH" "${COMMON_FLAGS[@]}"

  echo "[2/2] Start web UI..."
  go_run -mode serve -db-path "$DB_PATH" -http-addr "$HTTP_ADDR" -sync-on-start=false "${COMMON_FLAGS[@]}" &
  APP_PID=$!

  cleanup() {
    kill "$APP_PID" >/dev/null 2>&1 || true
  }
  trap cleanup EXIT INT TERM

  local url
  url="$(http_addr_to_url "$HTTP_ADDR")"

  if wait_http_ready "$url" 80; then
    echo "Web UI: $url"
    if ! open_browser "$url"; then
      echo "Cannot auto-open browser. Open manually: $url"
    fi
  else
    echo "Server started but readiness check failed. Open manually: $url"
  fi

  wait "$APP_PID"
}

load_config

case "$ACTION" in
  check-config)
    if [[ -z "${TINVEST_ACCOUNT_ID:-}" ]]; then
      echo "Config loaded. TINVEST_ACCOUNT_ID is empty (required for sync/serve/report)."
      exit 1
    fi
    echo "Config OK: account=${TINVEST_ACCOUNT_ID}, db=${DB_PATH}, addr=${HTTP_ADDR}"
    ;;
  list-accounts)
    go_run -list-accounts "${COMMON_FLAGS[@]}" "${EXTRA_ARGS[@]}"
    ;;
  sync)
    require_account
    go_run -mode sync -db-path "$DB_PATH" "${COMMON_FLAGS[@]}" "${EXTRA_ARGS[@]}"
    ;;
  sync-forecasts)
    require_account
    go_run -mode sync-forecasts -db-path "$DB_PATH" "${COMMON_FLAGS[@]}" "${EXTRA_ARGS[@]}"
    ;;
  eval-forecasts)
    require_account
    go_run -mode evaluate-forecasts -db-path "$DB_PATH" "${COMMON_FLAGS[@]}" "${EXTRA_ARGS[@]}"
    ;;
  sync-all)
    require_account
    go_run -mode sync-all -db-path "$DB_PATH" "${COMMON_FLAGS[@]}" "${EXTRA_ARGS[@]}"
    ;;
  serve)
    require_account
    go_run -mode serve -db-path "$DB_PATH" -http-addr "$HTTP_ADDR" -sync-on-start="${TINVEST_SYNC_ON_START:-false}" "${COMMON_FLAGS[@]}" "${EXTRA_ARGS[@]}"
    ;;
  report)
    require_account
    go_run -mode report "${COMMON_FLAGS[@]}" "${EXTRA_ARGS[@]}"
    ;;
  run)
    run_sync_then_serve
    ;;
  *)
    echo "Unknown action: $ACTION"
    echo "Usage: ./run.sh [check-config|list-accounts|sync|sync-forecasts|eval-forecasts|sync-all|serve|report|run]"
    exit 1
    ;;
esac
