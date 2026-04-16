#!/bin/bash
# Shared helper functions for benchmark scripts

# ============================================================
# Logging
# ============================================================
log_info()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO]  $*"; }
log_warn()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN]  $*" >&2; }
log_error() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2; }

# ============================================================
# SSH helpers
# ============================================================
# Execute command on remote server via SSH
# Usage: ssh_run "command"
ssh_run() {
  local cmd="$1"
  ssh $SSH_OPTS "$BENCHMARK_SERVER" "export JAVA_HOME=$REMOTE_JAVA_HOME && export PATH=\$JAVA_HOME/bin:\$PATH && $cmd"
}

# Execute command on remote server, capture exit code but don't die
# Usage: ssh_run_safe "command" && echo "ok" || echo "failed"
ssh_run_safe() {
  local cmd="$1"
  ssh $SSH_OPTS "$BENCHMARK_SERVER" "export JAVA_HOME=$REMOTE_JAVA_HOME && export PATH=\$JAVA_HOME/bin:\$PATH && $cmd" 2>&1 || true
}

# Copy file to remote server
# Usage: scp_to_remote local_path remote_path
scp_to_remote() {
  local local_path="$1"
  local remote_path="$2"
  scp $SSH_OPTS "$local_path" "${BENCHMARK_SERVER}:${remote_path}"
}

# Copy file from remote server
# Usage: scp_from_remote remote_path local_path
scp_from_remote() {
  local remote_path="$1"
  local local_path="$2"
  scp $SSH_OPTS "${BENCHMARK_SERVER}:${remote_path}" "$local_path"
}

# ============================================================
# Timing
# ============================================================
# Execute command on remote server with timing
# Prints duration in milliseconds to stdout
# Usage: duration_ms=$(timed_run "label" "command")
timed_run() {
  local label="$1"
  shift
  local cmd="$*"

  local start_ts
  start_ts=$(date +%s%3N)

  log_info "[$label] Starting..."
  ssh_run "$cmd"
  local rc=$?

  local end_ts
  end_ts=$(date +%s%3N)

  if [ $rc -ne 0 ]; then
    log_error "[$label] FAILED (exit code: $rc)"
  else
    log_info "[$label] Completed in $(( end_ts - start_ts )) ms"
  fi

  echo "$(( end_ts - start_ts ))"
  return $rc
}

# ============================================================
# MySQL helpers
# ============================================================
# Execute MySQL query on remote server
# Usage: rows=$(mysql_query "SELECT COUNT(*) FROM task_metrics")
mysql_query() {
  local sql="$1"
  ssh_run "docker exec $MYSQL_CONTAINER mysql -u $MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -N -e \"$sql\"" 2>/dev/null
}

# Get row count from a table
# Usage: count=$(mysql_count "task_metrics")
mysql_count() {
  local table="$1"
  local result
  result=$(mysql_query "SELECT COUNT(*) FROM $table" 2>/dev/null)
  echo "${result:-0}"
}

# Get row count with a WHERE condition
# Usage: count=$(mysql_count_where "task_metrics" "app_id LIKE '%test%'")
mysql_count_where() {
  local table="$1"
  local where="$2"
  local result
  result=$(mysql_query "SELECT COUNT(*) FROM $table WHERE $where" 2>/dev/null)
  echo "${result:-0}"
}

# ============================================================
# Service checks
# ============================================================
# Check if a remote service is available
# Usage: check_service "OTel Collector" "curl -s http://127.0.0.1:4317"
check_service() {
  local name="$1"
  local check_cmd="$2"
  log_info "Checking $name..."
  if ssh_run "$check_cmd" >/dev/null 2>&1; then
    log_info "$name: OK"
    return 0
  else
    log_error "$name: UNAVAILABLE"
    return 1
  fi
}

# Preflight checks - verify all services and paths
preflight_check() {
  local errors=0

  log_info "=== Preflight Checks ==="

  # SSH connectivity
  if ! ssh_run "echo ok" >/dev/null 2>&1; then
    log_error "Cannot SSH to $BENCHMARK_SERVER"
    return 1
  fi
  log_info "SSH connectivity: OK"

  # Java
  if ! ssh_run "test -d $REMOTE_JAVA_HOME" >/dev/null 2>&1; then
    log_error "Java not found at $REMOTE_JAVA_HOME"
    ((errors++))
  else
    log_info "Java: OK"
  fi

  # Hadoop installations
  for combo in "${MR_VERSION_COMBOS[@]}"; do
    IFS=':' read -r _ _ hadoop_dir <<< "$combo"
    if ! ssh_run "test -d /opt/$hadoop_dir" >/dev/null 2>&1; then
      log_warn "Hadoop /opt/$hadoop_dir not found (will skip related tests)"
    else
      log_info "Hadoop $hadoop_dir: OK"
    fi
  done

  # Spark installations
  for combo in "${SPARK_VERSION_COMBOS[@]}"; do
    IFS=':' read -r tag _ spark_dir _ _ <<< "$combo"
    if ! ssh_run "test -d /opt/$spark_dir" >/dev/null 2>&1; then
      log_warn "Spark /opt/$spark_dir not found (will skip $tag tests)"
    else
      log_info "Spark $tag ($spark_dir): OK"
    fi
  done

  # OTel Collector
  if ! check_service "OTel Collector" "curl -sf http://127.0.0.1:4317/ > /dev/null"; then
    log_warn "OTel Collector not reachable - metrics won't be exported"
    ((errors++))
  fi

  # MySQL
  if ! ssh_run "docker exec $MYSQL_CONTAINER mysql -u $MYSQL_USER -p$MYSQL_PASS -e 'SELECT 1' $MYSQL_DB" >/dev/null 2>&1; then
    log_warn "MySQL not reachable - metrics verification will fail"
    ((errors++))
  else
    log_info "MySQL: OK"
  fi

  # Flink Consumer (optional)
  if ssh_run "pgrep -f flink-consumer" >/dev/null 2>&1; then
    log_info "Flink Consumer: running"
  else
    log_warn "Flink Consumer not running - metrics won't reach MySQL"
    ((errors++))
  fi

  log_info "=== Preflight complete: $errors warnings/errors ==="
  return $errors
}

# ============================================================
# File and path helpers
# ============================================================
# Get the directory where this script lives
lib_dir() {
  local this="${BASH_SOURCE[0]}"
  cd -P -- "$(dirname -- "$this")" && pwd
}

# Ensure results directory exists on remote
ensure_results_dir() {
  ssh_run "mkdir -p $RESULTS_DIR"
}

# ============================================================
# CSV result recording
# ============================================================
RESULTS_CSV=""

init_results_csv() {
  RESULTS_CSV="$RESULTS_DIR/timing-${RUN_ID}.csv"
  ssh_run "echo 'combo,workload,mode,duration_ms,exit_code,metrics_found' > $RESULTS_CSV"
}

record_result() {
  local combo="$1"
  local workload="$2"
  local mode="$3"  # baseline or telemetry
  local duration_ms="$4"
  local exit_code="$5"
  local metrics_found="${6:-}"
  ssh_run "echo '$combo,$workload,$mode,$duration_ms,$exit_code,$metrics_found' >> $RESULTS_CSV"
}
