#!/bin/bash
# Metrics verification via MySQL queries

source "$(lib_dir)/helpers.sh"

# ============================================================
# Wait for metrics to appear in MySQL
# Polls every 5 seconds up to timeout
# Usage: wait_for_metrics "task_metrics" "service_name_pattern" 60
# Returns: row count (0 if timeout)
# ============================================================
wait_for_metrics() {
  local table="$1"
  local pattern="$2"
  local timeout="${3:-60}"

  local elapsed=0
  local interval=5

  log_info "Waiting for metrics in $table (pattern: $pattern, timeout: ${timeout}s)"

  while [ $elapsed -lt $timeout ]; do
    local count
    if [ -n "$pattern" ]; then
      count=$(mysql_count_where "$table" "service_name LIKE '%$pattern%'")
    else
      count=$(mysql_count "$table")
    fi

    if [ "$count" -gt 0 ]; then
      log_info "Found $count rows in $table for '$pattern' after ${elapsed}s"
      echo "$count"
      return 0
    fi

    sleep $interval
    elapsed=$((elapsed + interval))
  done

  log_warn "No metrics found in $table for '$pattern' after ${timeout}s"
  echo "0"
  return 1
}

# ============================================================
# Verify Spark metrics arrived in MySQL
# Returns: YES or NO
# ============================================================
verify_spark_metrics() {
  local service_name="$1"

  log_info "Verifying Spark metrics for service: $service_name"
  sleep "$METRICS_DRAIN_WAIT"

  # Check task_metrics table
  local task_count
  task_count=$(mysql_count_where "task_metrics" "service_name LIKE '%${service_name}%'")
  log_info "  task_metrics: $task_count rows"

  # Check stage_metrics table (if stage.detailed enabled)
  local stage_count
  stage_count=$(mysql_count_where "stage_metrics" "service_name LIKE '%${service_name}%'")
  log_info "  stage_metrics: $stage_count rows"

  local total=$((task_count + stage_count))
  if [ "$total" -gt 0 ]; then
    log_info "Spark metrics VERIFIED: $total total rows"
    echo "YES"
  else
    # Fallback: check without service_name filter (column may not exist)
    local fallback_count
    fallback_count=$(mysql_count "task_metrics")
    log_info "  task_metrics (total): $fallback_count rows"
    if [ "$fallback_count" -gt 0 ]; then
      log_info "Spark metrics found (unfiltered): $fallback_count rows"
      echo "YES"
    else
      log_warn "Spark metrics NOT FOUND for $service_name"
      echo "NO"
    fi
  fi
}

# ============================================================
# Verify MR agent metrics arrived in MySQL
# Returns: YES or NO
# ============================================================
verify_mr_metrics() {
  local service_name="$1"

  log_info "Verifying MR metrics for: $service_name"
  sleep "$METRICS_DRAIN_WAIT"

  # Check mr_task_metrics table (from agent)
  local task_count
  task_count=$(mysql_count "mr_task_metrics")
  log_info "  mr_task_metrics: $task_count rows"

  # Check mr_job_metrics table (from collector, if running)
  local job_count
  job_count=$(mysql_count "mr_job_metrics")
  log_info "  mr_job_metrics: $job_count rows"

  local total=$((task_count + job_count))
  if [ "$total" -gt 0 ]; then
    log_info "MR metrics VERIFIED: $total total rows"
    echo "YES"
  else
    log_warn "MR metrics NOT FOUND"
    echo "NO"
  fi
}

# ============================================================
# Verify Hive hook metrics arrived in MySQL
# Returns: YES or NO
# ============================================================
verify_hive_metrics() {
  local service_name="$1"

  log_info "Verifying Hive metrics for: $service_name"
  sleep "$METRICS_DRAIN_WAIT"

  # Check hive_query_metrics table
  local query_count
  query_count=$(mysql_count "hive_query_metrics")
  log_info "  hive_query_metrics: $query_count rows"

  # Check hive_table_io_metrics table
  local table_count
  table_count=$(mysql_count "hive_table_io_metrics")
  log_info "  hive_table_io_metrics: $table_count rows"

  local total=$((query_count + table_count))
  if [ "$total" -gt 0 ]; then
    log_info "Hive metrics VERIFIED: $total total rows"
    echo "YES"
  else
    log_warn "Hive metrics NOT FOUND"
    echo "NO"
  fi
}
