#!/bin/bash
#
# Deploy Grafana Dashboards
# Uploads all JSON dashboards from deploy/grafana/ to a Grafana instance.
#
# Usage:
#   ./deploy/deploy-grafana.sh [OPTIONS]
#
# Options:
#   --grafana-url=URL       Grafana base URL (required)
#   --user=USER             Grafana admin username (required)
#   --password=PASS         Grafana admin password (required)
#   --folder=NAME           Destination folder name (default: Telemetry)
#   --dashboard-dir=DIR     Dashboard JSON directory (default: deploy/grafana)
#   --dry-run               Show what would be deployed without uploading
#   -h, --help              Show this help message
#
# Examples:
#   ./deploy/deploy-grafana.sh --grafana-url=http://localhost:3000 --user=admin --password=admin
#   ./deploy/deploy-grafana.sh --grafana-url=http://grafana.internal:3000 --user=admin --password=secret --folder=Production
#
set -euo pipefail

# ── Colors ────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }
step()  { echo -e "${BLUE}==>${NC} $*"; }

# ── Defaults ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

GRAFANA_URL=""
GRAFANA_USER=""
GRAFANA_PASSWORD=""
FOLDER_NAME="Telemetry"
# Detect dashboard directory (release vs repo layout)
if [ -d "$PROJECT_ROOT/grafana" ]; then
  DASHBOARD_DIR="$PROJECT_ROOT/grafana"
else
  DASHBOARD_DIR="$PROJECT_ROOT/deploy/grafana"
fi
DRY_RUN=false

# ── Parse Arguments ───────────────────────────────────────────────
parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --grafana-url=*)   GRAFANA_URL="${1#*=}" ;;
      --user=*)          GRAFANA_USER="${1#*=}" ;;
      --password=*)      GRAFANA_PASSWORD="${1#*=}" ;;
      --folder=*)        FOLDER_NAME="${1#*=}" ;;
      --dashboard-dir=*) DASHBOARD_DIR="${1#*=}" ;;
      --dry-run)         DRY_RUN=true ;;
      -h|--help)
        sed -n '2,17p' "$0" | sed 's/^# \?//'
        exit 0
        ;;
      *)
        error "Unknown option: $1"
        exit 1
        ;;
    esac
    shift
  done
}

# ── Validate ──────────────────────────────────────────────────────
validate() {
  local missing=false

  if [ -z "$GRAFANA_URL" ]; then
    error "--grafana-url is required"
    missing=true
  fi
  if [ -z "$GRAFANA_USER" ]; then
    error "--user is required"
    missing=true
  fi
  if [ -z "$GRAFANA_PASSWORD" ]; then
    error "--password is required"
    missing=true
  fi
  if [ ! -d "$DASHBOARD_DIR" ]; then
    error "Dashboard directory not found: $DASHBOARD_DIR"
    missing=true
  fi

  if [ "$missing" = true ]; then
    exit 1
  fi

  # Remove trailing slash
  GRAFANA_URL="${GRAFANA_URL%/}"
}

# ── Grafana API helpers ───────────────────────────────────────────
grafana_api() {
  local method="$1"
  local path="$2"
  local body="${3:-}"

  if [ "$DRY_RUN" = true ]; then
    echo -e "  ${YELLOW}[DRY-RUN]${NC} curl -X $method $GRAFANA_URL/api$path"
    return 0
  fi

  if [ -n "$body" ]; then
    curl -sf -X "$method" \
      -u "$GRAFANA_USER:$GRAFANA_PASSWORD" \
      -H "Content-Type: application/json" \
      -d "$body" \
      "$GRAFANA_URL/api$path"
  else
    curl -sf -X "$method" \
      -u "$GRAFANA_USER:$GRAFANA_PASSWORD" \
      -H "Content-Type: application/json" \
      "$GRAFANA_URL/api$path"
  fi
}

# ── Create or get folder ──────────────────────────────────────────
ensure_folder() {
  step "Ensuring folder '$FOLDER_NAME' exists"

  local existing
  existing=$(grafana_api GET "/folders" 2>/dev/null || echo "[]")

  local folder_uid
  folder_uid=$(echo "$existing" | grep -o "\"uid\":\"[^\"]*\"" | head -1 | cut -d'"' -f4 || true)

  # Search for folder by name
  local folder_id
  folder_id=$(grafana_api GET "/folders" 2>/dev/null \
    | python3 -c "
import sys, json
for f in json.load(sys.stdin):
  if f['title'] == '$FOLDER_NAME':
    print(f['id'])
    break
" 2>/dev/null || true)

  if [ -n "$folder_id" ]; then
    info "Folder '$FOLDER_NAME' already exists (id=$folder_id)"
    echo "$folder_id"
    return
  fi

  # Create folder
  local result
  result=$(grafana_api POST "/folders" "{\"title\":\"$FOLDER_NAME\"}")
  folder_id=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])" 2>/dev/null || true)

  if [ -z "$folder_id" ]; then
    warn "Failed to create folder, using General (id=0)"
    echo "0"
    return
  fi

  info "Created folder '$FOLDER_NAME' (id=$folder_id)"
  echo "$folder_id"
}

# ── Deploy a single dashboard ─────────────────────────────────────
deploy_dashboard() {
  local file="$1"
  local folder_id="$2"
  local basename
  basename=$(basename "$file" .json)

  step "Deploying: $basename"

  # Extract dashboard uid from the JSON
  local uid
  uid=$(python3 -c "
import sys, json
with open('$file') as f:
    d = json.load(f)
print(d.get('uid', '$basename'))
" 2>/dev/null || echo "$basename")

  # Build the payload: wrap dashboard JSON inside the "dashboard" field
  # set overwrite=true so re-runs update in place
  local payload
  payload=$(python3 -c "
import sys, json
with open('$file') as f:
    dashboard = json.load(f)
# Strip server-assigned fields
dashboard.pop('id', None)
dashboard.pop('version', None)
payload = {
    'dashboard': dashboard,
    'folderId': $folder_id,
    'overwrite': True
}
json.dump(payload, sys.stdout)
" 2>/dev/null)

  if [ -z "$payload" ]; then
    error "Failed to build payload for $file"
    return 1
  fi

  local result
  result=$(grafana_api POST "/dashboards/db" "$payload")

  if [ "$DRY_RUN" = true ]; then
    return
  fi

  local status
  status=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || true)

  if [ "$status" = "success" ]; then
    local slug
    slug=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('slug',''))" 2>/dev/null || echo "?")
    info "Deployed: $basename -> $slug (uid=$uid)"
  else
    local msg
    msg=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('message', d))" 2>/dev/null || echo "$result")
    error "Failed to deploy $basename: $msg"
    return 1
  fi
}

# ── Main ──────────────────────────────────────────────────────────
main() {
  parse_args "$@"
  validate

  echo ""
  echo "============================================================"
  echo "  Grafana Dashboard Deployer"
  echo "============================================================"
  echo "  URL:       $GRAFANA_URL"
  echo "  Folder:    $FOLDER_NAME"
  echo "  Dashboards: $DASHBOARD_DIR"
  echo "  Dry run:   $DRY_RUN"
  echo "============================================================"
  echo ""

  # Check connectivity
  if [ "$DRY_RUN" = false ]; then
    step "Testing Grafana connectivity"
    if ! grafana_api GET "/org" > /dev/null 2>&1; then
      error "Cannot connect to Grafana at $GRAFANA_URL or authentication failed"
      exit 1
    fi
    info "Connected to Grafana"
  fi

  # Ensure folder exists
  local folder_id
  folder_id=$(ensure_folder)

  # Count dashboards
  local count
  count=$(find "$DASHBOARD_DIR" -maxdepth 1 -name '*.json' | wc -l)
  if [ "$count" -eq 0 ]; then
    warn "No JSON files found in $DASHBOARD_DIR"
    exit 0
  fi

  echo ""
  info "Found $count dashboards to deploy"
  echo ""

  # Deploy each dashboard
  local success=0
  local failed=0
  for file in "$DASHBOARD_DIR"/*.json; do
    if deploy_dashboard "$file" "$folder_id"; then
      success=$((success + 1))
    else
      failed=$((failed + 1))
    fi
  done

  echo ""
  echo "============================================================"
  if [ "$DRY_RUN" = true ]; then
    echo "  Dry run complete: $count dashboards would be deployed"
  else
    echo "  Done: $success deployed, $failed failed"
  fi
  echo "============================================================"

  if [ "$failed" -gt 0 ]; then
    exit 1
  fi
}

main "$@"
