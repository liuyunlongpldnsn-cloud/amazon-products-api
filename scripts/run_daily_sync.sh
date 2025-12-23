#!/usr/bin/env bash
set -euo pipefail

log() { echo "[$(date '+%F %T')] $*"; }
die() { echo "[FATAL] $*" >&2; exit 1; }

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$PROJECT_DIR/.env"
LOG_DIR="$PROJECT_DIR/logs"
ASINS_FILE="$PROJECT_DIR/asins.txt"
PY="$PROJECT_DIR/venv/bin/python"

mkdir -p "$LOG_DIR"

log "Start daily sync"
log "PROJECT_DIR=$PROJECT_DIR"
log "ASINS_FILE=$ASINS_FILE"
log "Python=$PY"

# 1) load .env if exists (do NOT require it)
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  set -a
  source "$ENV_FILE"
  set +a
  log ".env loaded: yes"
else
  log ".env loaded: no"
fi

# 2) env var priority (if user exported outside, keep it)
DATABASE_URL="${DATABASE_URL:-}"
PLATFORM_NAME="${PLATFORM_NAME:-amazon_us}"
KEEPA_API_KEY="${KEEPA_API_KEY:-}"

# basic validation
[[ -x "$PY" ]] || die "Python not found: $PY"
[[ -f "$ASINS_FILE" ]] || die "Missing asins file: $ASINS_FILE"
[[ -n "$DATABASE_URL" ]] || die "DATABASE_URL is empty"
[[ -n "$KEEPA_API_KEY" ]] || die "KEEPA_API_KEY is empty"

# prevent placeholder
if [[ "$KEEPA_API_KEY" == *"keepa_key"* ]]; then
  die "KEEPA_API_KEY looks like placeholder (keepa_key). Please set real key."
fi

log "PLATFORM_NAME=$PLATFORM_NAME"
log "DATABASE_URL set: yes"
log "KEEPA_API_KEY length: ${#KEEPA_API_KEY}"

export DATABASE_URL
export PLATFORM_NAME
export KEEPA_API_KEY

LOG_FILE="$LOG_DIR/sync_keepa.log"

log "Run sync_keepa -> $LOG_FILE"
cd "$PROJECT_DIR"
set +e
"$PY" -m scripts.sync_keepa --asins-file "$ASINS_FILE" >> "$LOG_FILE" 2>&1
rc=$?
set -e

if [[ $rc -ne 0 ]]; then
  log "[ERROR] sync_keepa failed rc=$rc"
  tail -n 80 "$LOG_FILE" || true
  exit $rc
fi

log "Done"
