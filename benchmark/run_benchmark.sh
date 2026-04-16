#!/bin/bash
# Main benchmark entry point
# Runs HiBench workloads with and without telemetry plugin/agent/hook
# on server 192.168.10.65

set -uo pipefail

# Resolve script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source configuration and libraries
source "$SCRIPT_DIR/config.sh"
source "$SCRIPT_DIR/lib/helpers.sh"
source "$SCRIPT_DIR/lib/hibench_setup.sh"
source "$SCRIPT_DIR/lib/spark_bench.sh"
source "$SCRIPT_DIR/lib/mr_bench.sh"
source "$SCRIPT_DIR/lib/hive_bench.sh"
source "$SCRIPT_DIR/lib/metrics_verify.sh"
source "$SCRIPT_DIR/lib/report.sh"

# Workload list paths
SPARK_WORKLOADS="$SCRIPT_DIR/workloads/spark_workloads.lst"
MR_WORKLOADS="$SCRIPT_DIR/workloads/mr_workloads.lst"
HIVE_WORKLOADS="$SCRIPT_DIR/workloads/hive_workloads.lst"

# ============================================================
# Usage
# ============================================================
usage() {
  cat << 'EOF'
Usage: run_benchmark.sh [OPTIONS]

Options:
  --combo TAG        Run only a specific Spark version combo (e.g., spark32)
  --workload NAME    Run only a specific workload (e.g., micro/wordcount)
  --type TYPE        Run only: spark, mr, hive, or all (default: all)
  --scale PROFILE    HiBench scale profile (default: small)
  --skip-build       Skip HiBench Maven build
  --skip-prepare     Skip data preparation
  --dry-run          Print what would be run without executing
  --help             Show this help

Examples:
  run_benchmark.sh                                    # Full benchmark
  run_benchmark.sh --combo spark32 --type spark       # Only Spark 3.2
  run_benchmark.sh --workload micro/wordcount         # Only wordcount
  run_benchmark.sh --scale large --type spark         # Large data size
EOF
}

# ============================================================
# Parse arguments
# ============================================================
ARG_COMBO=""
ARG_WORKLOAD=""
ARG_TYPE="all"
ARG_SKIP_BUILD=false
ARG_SKIP_PREPARE=false
ARG_DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --combo)     ARG_COMBO="$2"; shift 2 ;;
    --workload)  ARG_WORKLOAD="$2"; shift 2 ;;
    --type)      ARG_TYPE="$2"; shift 2 ;;
    --scale)     HIBENCH_SCALE="$2"; shift 2 ;;
    --skip-build)   ARG_SKIP_BUILD=true; shift ;;
    --skip-prepare) ARG_SKIP_PREPARE=true; shift ;;
    --dry-run)   ARG_DRY_RUN=true; shift ;;
    --help)      usage; exit 0 ;;
    *)           log_error "Unknown option: $1"; usage; exit 1 ;;
  esac
done

# ============================================================
# Filter version combos based on --combo argument
# ============================================================
filter_combos() {
  local -n arr=$1
  local filter="$2"
  if [ -z "$filter" ]; then
    printf '%s\n' "${arr[@]}"
    return
  fi
  for combo in "${arr[@]}"; do
    local tag="${combo%%:*}"
    if [ "$tag" = "$filter" ]; then
      echo "$combo"
    fi
  done
}

# Create filtered workload list
create_filtered_workload_list() {
  local src_list="$1"
  local filter="$2"
  local tmp_list="/tmp/bench-workloads-$$.lst"

  if [ -n "$filter" ]; then
    grep -v '^#' "$src_list" | grep -F "$filter" > "$tmp_list" || true
  else
    grep -v '^#' "$src_list" > "$tmp_list"
  fi
  echo "$tmp_list"
}

# ============================================================
# Main execution
# ============================================================
main() {
  log_info "========================================"
  log_info "  TELEMETRY BENCHMARK SUITE"
  log_info "  Run ID: $RUN_ID"
  log_info "  Scale: $HIBENCH_SCALE"
  log_info "  Server: $BENCHMARK_SERVER"
  log_info "========================================"

  # --- Phase 1: Preflight ---
  if [ "$ARG_DRY_RUN" = true ]; then
    log_info "[DRY RUN] Would execute the following:"
    log_info "  Scale: $HIBENCH_SCALE"
    log_info "  Spark combos: $(filter_combos SPARK_VERSION_COMBOS "$ARG_COMBO")"
    log_info "  MR combos: $(filter_combos MR_VERSION_COMBOS "$ARG_COMBO")"
    log_info "  Hive combos: $(filter_combos HIVE_VERSION_COMBOS "$ARG_COMBO")"
    log_info "  Spark workloads: $(cat "$SPARK_WORKLOADS" | grep -v '^#' | tr '\n' ' ')"
    log_info "  MR workloads: $(cat "$MR_WORKLOADS" | grep -v '^#' | tr '\n' ' ')"
    log_info "  Hive workloads: $(cat "$HIVE_WORKLOADS" | grep -v '^#' | tr '\n' ' ')"
    exit 0
  fi

  if ! preflight_check; then
    log_error "Preflight checks failed. Fix issues before continuing."
    log_error "Use --dry-run to see what would be executed."
    exit 1
  fi

  # Init results
  ensure_results_dir
  init_results_csv

  # --- Phase 2: Deploy files ---
  log_info "=== Phase 2: Deploying files ==="

  # Sync HiBench to remote
  if [ -d "$SCRIPT_DIR/../hibench" ]; then
    log_info "Syncing HiBench to remote..."
    rsync -az --delete -e "ssh $SSH_OPTS" \
      "$SCRIPT_DIR/../hibench/" "${BENCHMARK_SERVER}:${HIBENCH_HOME}/" || {
      log_warn "rsync failed, trying scp..."
      ssh_run "mkdir -p $HIBENCH_HOME"
    }
  fi

  # Copy plugin JARs to remote
  for jar_src in \
    "spark-telemetry-dist-omni/target/spark-telemetry-dist-omni-*.jar:$OMNI_JAR" \
    "mr-telemetry-agent/target/mr-telemetry-agent-*.jar:$MR_AGENT_JAR" \
    "hive-telemetry-hook/target/hive-telemetry-hook-*.jar:$HIVE_HOOK_JAR"; do
    local src_pattern="${jar_src%%:*}"
    local dst="${jar_src##*:}"
    local src
    src=$(ls "$SCRIPT_DIR/../$src_pattern" 2>/dev/null | head -1)
    if [ -n "$src" ] && [ -f "$src" ]; then
      log_info "Deploying $(basename "$src") -> $dst"
      scp_to_remote "$src" "$dst"
      # Fix module-info for Java 8
      ssh_run "zip -d $dst 'META-INF/versions/9/module-info.class' 2>/dev/null || true"
    else
      log_warn "JAR not found: $SCRIPT_DIR/../$src_pattern"
    fi
  done

  # --- Phase 3: Build HiBench ---
  if [ "$ARG_SKIP_BUILD" = false ]; then
    log_info "=== Phase 3: Building HiBench ==="

    # Build for Scala 2.11 (Spark 2.4)
    local spark24_combos
    spark24_combos=$(filter_combos SPARK_VERSION_COMBOS "spark24")
    if [ -n "$spark24_combos" ]; then
      build_hibench "2.4" "2.11" || log_warn "HiBench build failed for Spark 2.4"
    fi

    # Build for Scala 2.12 (Spark 3.x)
    local spark3_combos
    spark3_combos=$(filter_combos SPARK_VERSION_COMBOS "spark30")
    if [ -n "$spark3_combos" ] || [ -n "$(filter_combos SPARK_VERSION_COMBOS "spark32")" ] || [ -n "$(filter_combos SPARK_VERSION_COMBOS "spark35")" ]; then
      build_hibench "3.0" "2.12" || log_warn "HiBench build failed for Spark 3.x"
    fi
  fi

  # --- Phase 4: Run Hadoop 2.7 benchmarks ---
  local hadoop27_combos
  hadoop27_combos=$(filter_combos SPARK_VERSION_COMBOS "$ARG_COMBO" | grep "hadoop27" || true)

  if [ -n "$hadoop27_combos" ] || ([ "$ARG_TYPE" = "all" ] || [ "$ARG_TYPE" = "mr" ]) && [ -n "$(filter_combos MR_VERSION_COMBOS "$ARG_COMBO" | grep hadoop27 || true)" ]; then
    log_info "=== Phase 4: Hadoop 2.7 benchmarks ==="

    local hadoop27_home="/opt/hadoop-2.7.0"

    if ! check_hadoop_running "$hadoop27_home"; then
      start_hadoop "$hadoop27_home"
    fi

    # Configure HiBench for Hadoop 2.7
    # Use first Spark combo's Spark home for configuration
    local spark24_home="/opt/spark-2.4.4-bin-hadoop2.7"
    configure_hibench "$hadoop27_home" "$spark24_home"

    # Prepare data
    if [ "$ARG_SKIP_PREPARE" = false ]; then
      log_info "Preparing data for Hadoop 2.7..."
      local tmp_workloads
      tmp_workloads=$(create_filtered_workload_list "$SPARK_WORKLOADS" "$ARG_WORKLOAD")
      prepare_all_data "$tmp_workloads" "spark"
      rm -f "$tmp_workloads"

      tmp_workloads=$(create_filtered_workload_list "$MR_WORKLOADS" "$ARG_WORKLOAD")
      prepare_all_data "$tmp_workloads" "hadoop"
      rm -f "$tmp_workloads"
    fi

    # Spark 2.4 benchmarks
    if [ "$ARG_TYPE" = "all" ] || [ "$ARG_TYPE" = "spark" ]; then
      while IFS= read -r combo || [ -n "$combo" ]; do
        [ -z "$combo" ] && continue
        local tag="${combo%%:*}"
        if [ "$tag" = "spark24" ]; then
          local tmp_list
          tmp_list=$(create_filtered_workload_list "$SPARK_WORKLOADS" "$ARG_WORKLOAD")
          run_spark_all "$combo" "$tmp_list" || true
          rm -f "$tmp_list"
        fi
      done <<< "$hadoop27_combos"
    fi

    # MR benchmarks on Hadoop 2.7
    if [ "$ARG_TYPE" = "all" ] || [ "$ARG_TYPE" = "mr" ]; then
      local mr27_combo="mr:hadoop27:hadoop-2.7.0"
      local tmp_list
      tmp_list=$(create_filtered_workload_list "$MR_WORKLOADS" "$ARG_WORKLOAD")
      run_mr_all "$mr27_combo" "$tmp_list" || true
      rm -f "$tmp_list"
    fi

    # Hive benchmarks on Hadoop 2.7
    if [ "$ARG_TYPE" = "all" ] || [ "$ARG_TYPE" = "hive" ]; then
      for hive_combo in "${HIVE_VERSION_COMBOS[@]}"; do
        # Only run hive239 with Hadoop 2.7
        if [[ "$hive_combo" == *"hive239"* ]]; then
          local tmp_list
          tmp_list=$(create_filtered_workload_list "$HIVE_WORKLOADS" "$ARG_WORKLOAD")
          run_hive_all "$hive_combo" "hadoop-2.7.0" "$tmp_list" || true
          rm -f "$tmp_list"
        fi
      done
    fi

    stop_hadoop "$hadoop27_home"
  fi

  # --- Phase 5: Run Hadoop 3.2 benchmarks ---
  local hadoop32_combos
  hadoop32_combos=$(filter_combos SPARK_VERSION_COMBOS "$ARG_COMBO" | grep "hadoop32" || true)

  if [ -n "$hadoop32_combos" ] || ([ "$ARG_TYPE" = "all" ] || [ "$ARG_TYPE" = "mr" ]) || ([ "$ARG_TYPE" = "all" ] || [ "$ARG_TYPE" = "hive" ]); then
    log_info "=== Phase 5: Hadoop 3.2 benchmarks ==="

    local hadoop32_home="/opt/hadoop-3.2.0"

    if ! check_hadoop_running "$hadoop32_home"; then
      start_hadoop "$hadoop32_home"
    fi

    # Configure HiBench for Hadoop 3.2 (use Spark 3.2 for config)
    local spark32_home="/opt/spark-3.2.0-bin-hadoop3.2"
    configure_hibench "$hadoop32_home" "$spark32_home"

    # Prepare data
    if [ "$ARG_SKIP_PREPARE" = false ]; then
      log_info "Preparing data for Hadoop 3.2..."
      local tmp_workloads
      tmp_workloads=$(create_filtered_workload_list "$SPARK_WORKLOADS" "$ARG_WORKLOAD")
      prepare_all_data "$tmp_workloads" "spark"
      rm -f "$tmp_workloads"

      tmp_workloads=$(create_filtered_workload_list "$MR_WORKLOADS" "$ARG_WORKLOAD")
      prepare_all_data "$tmp_workloads" "hadoop"
      rm -f "$tmp_workloads"
    fi

    # Spark 3.x benchmarks (3.0, 3.2, 3.5)
    if [ "$ARG_TYPE" = "all" ] || [ "$ARG_TYPE" = "spark" ]; then
      while IFS= read -r combo || [ -n "$combo" ]; do
        [ -z "$combo" ] && continue
        local tag="${combo%%:*}"
        if [ "$tag" != "spark24" ]; then
          IFS=':' read -r _ _ spark_dir hadoop_dir scala <<< "$combo"
          configure_hibench "/opt/$hadoop_dir" "/opt/$spark_dir"
          local tmp_list
          tmp_list=$(create_filtered_workload_list "$SPARK_WORKLOADS" "$ARG_WORKLOAD")
          run_spark_all "$combo" "$tmp_list" || true
          rm -f "$tmp_list"
        fi
      done <<< "$hadoop32_combos"
    fi

    # MR benchmarks on Hadoop 3.2
    if [ "$ARG_TYPE" = "all" ] || [ "$ARG_TYPE" = "mr" ]; then
      local mr32_combo="mr:hadoop32:hadoop-3.2.0"
      local tmp_list
      tmp_list=$(create_filtered_workload_list "$MR_WORKLOADS" "$ARG_WORKLOAD")
      run_mr_all "$mr32_combo" "$tmp_list" || true
      rm -f "$tmp_list"
    fi

    # Hive benchmarks on Hadoop 3.2
    if [ "$ARG_TYPE" = "all" ] || [ "$ARG_TYPE" = "hive" ]; then
      for hive_combo in "${HIVE_VERSION_COMBOS[@]}"; do
        # Only run hive313 with Hadoop 3.2
        if [[ "$hive_combo" == *"hive313"* ]]; then
          local tmp_list
          tmp_list=$(create_filtered_workload_list "$HIVE_WORKLOADS" "$ARG_WORKLOAD")
          run_hive_all "$hive_combo" "hadoop-3.2.0" "$tmp_list" || true
          rm -f "$tmp_list"
        fi
      done
    fi

    stop_hadoop "$hadoop32_home"
  fi

  # --- Phase 6: Generate report ---
  log_info "=== Phase 6: Generating report ==="
  generate_report "$RESULTS_CSV"

  log_info "========================================"
  log_info "  BENCHMARK COMPLETE"
  log_info "  Run ID: $RUN_ID"
  log_info "========================================"
}

# Run main
main "$@"
