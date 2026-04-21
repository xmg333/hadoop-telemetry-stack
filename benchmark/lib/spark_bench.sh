#!/bin/bash
# Spark workload benchmarking: baseline vs telemetry

source "$(lib_dir)/helpers.sh"
source "$(lib_dir)/metrics_verify.sh"

# ============================================================
# Create spark-submit wrapper that injects telemetry config
# Called on the remote server
# ============================================================
create_spark_submit_wrapper() {
  local spark_home="$1"
  local plugin_key="$2"
  local plugin_class="$3"
  local service_name="$4"

  ssh_run "cat > /tmp/spark-submit-telemetry.sh << 'WRAPPER'
#!/bin/bash
# Spark-submit wrapper that injects telemetry plugin
if [ \"\$BENCHMARK_TELEMETRY\" = \"1\" ]; then
  $spark_home/bin/spark-submit.real \\
    --jars $OMNI_JAR \\
    --conf $plugin_key=$plugin_class \\
    --conf spark.telemetry.otel.exporter.endpoint=$OTEL_ENDPOINT \\
    --conf spark.telemetry.otel.service.name=$service_name \\
    --conf spark.telemetry.otel.export.interval.ms=5000 \\
    --conf spark.telemetry.metrics.task.execution=true \\
    --conf spark.telemetry.metrics.task.shuffle-extended=true \\
    --conf spark.telemetry.metrics.task.info=true \\
    --conf spark.telemetry.metrics.stage.detailed=true \\
    --conf spark.telemetry.metrics.job.lifecycle=true \\
    --conf spark.telemetry.metrics.sql.query-execution=true \\
    \"\$@\"
else
  $spark_home/bin/spark-submit.real \"\$@\"
fi
WRAPPER
chmod +x /tmp/spark-submit-telemetry.sh"
}

# ============================================================
# Inject/Remove spark-submit wrapper
# ============================================================
inject_spark_telemetry() {
  local spark_home="$1"
  local plugin_key="$2"
  local plugin_class="$3"
  local service_name="$4"

  log_info "Injecting Spark telemetry wrapper for $service_name"

  # Backup original spark-submit
  ssh_run "cp $spark_home/bin/spark-submit $spark_home/bin/spark-submit.real"

  # Create wrapper
  create_spark_submit_wrapper "$spark_home" "$plugin_key" "$plugin_class" "$service_name"

  # Replace spark-submit with wrapper
  ssh_run "cp /tmp/spark-submit-telemetry.sh $spark_home/bin/spark-submit && chmod +x $spark_home/bin/spark-submit"
}

remove_spark_telemetry() {
  local spark_home="$1"

  log_info "Removing Spark telemetry wrapper"
  # Restore original spark-submit
  ssh_run "if [ -f $spark_home/bin/spark-submit.real ]; then mv $spark_home/bin/spark-submit.real $spark_home/bin/spark-submit; fi"
  ssh_run "rm -f /tmp/spark-submit-telemetry.sh"
}

# ============================================================
# Clean up old plugin JARs from Spark jars/ directory
# ============================================================
clean_spark_jars() {
  local spark_home="$1"
  ssh_run "rm -f $spark_home/jars/spark-telemetry*.jar $spark_home/jars/omnipackage*.jar"
}

# ============================================================
# Run a single Spark benchmark (baseline + telemetry)
# Usage: run_spark_benchmark "combo_str" "micro/wordcount"
# combo format: "spark32:hadoop32:spark-3.2.0-bin-hadoop3.2:hadoop-3.2.0:2.12"
# ============================================================
run_spark_benchmark() {
  local combo="$1"
  local workload="$2"

  IFS=':' read -r tag hadoop_tag spark_dir hadoop_dir scala <<< "$combo"

  local spark_home="/opt/$spark_dir"
  local hadoop_home="/opt/$hadoop_dir"
  local plugin_key="${SPARK_PLUGIN_KEYS[$tag]}"
  local plugin_class="${SPARK_PLUGIN_CLASSES[$tag]}"
  local service_name="bench-${tag}-${workload//\//-}"

  log_info "=== Spark benchmark: $tag / $workload ==="

  # Clean old JARs
  clean_spark_jars "$spark_home"

  # --- RUN 1: Baseline ---
  log_info "[$tag] Baseline run: $workload"
  local baseline_ms=0
  local baseline_rc=0
  baseline_ms=$(timed_run "baseline-$tag-$workload" \
    "cd $HIBENCH_HOME && SPARK_HOME=$spark_home HADOOP_CONF_DIR=$hadoop_home/etc/hadoop bin/workloads/${workload}/spark/run.sh") || baseline_rc=$?

  if [ $baseline_rc -ne 0 ]; then
    log_warn "[$tag] Baseline FAILED for $workload (rc=$baseline_rc), skipping telemetry run"
    record_result "$combo" "$workload" "baseline" "$baseline_ms" "$baseline_rc" ""
    record_result "$combo" "$workload" "telemetry" "0" "-1" "SKIPPED"
    return 1
  fi

  record_result "$combo" "$workload" "baseline" "$baseline_ms" "0" ""

  # --- RUN 2: With telemetry ---
  log_info "[$tag] Telemetry run: $workload"
  inject_spark_telemetry "$spark_home" "$plugin_key" "$plugin_class" "$service_name"

  local telemetry_ms=0
  local telemetry_rc=0
  telemetry_ms=$(timed_run "telemetry-$tag-$workload" \
    "cd $HIBENCH_HOME && BENCHMARK_TELEMETRY=1 SPARK_HOME=$spark_home HADOOP_CONF_DIR=$hadoop_home/etc/hadoop bin/workloads/${workload}/spark/run.sh") || telemetry_rc=$?

  remove_spark_telemetry "$spark_home"

  # Verify metrics
  local metrics_found="NO"
  if [ $telemetry_rc -eq 0 ]; then
    metrics_found=$(verify_spark_metrics "$service_name")
  fi

  record_result "$combo" "$workload" "telemetry" "$telemetry_ms" "$telemetry_rc" "$metrics_found"

  if [ $telemetry_rc -ne 0 ]; then
    log_error "[$tag] Telemetry run FAILED for $workload - PLUGIN CRASHED"
    return 1
  fi

  local _div=${baseline_ms:-0}; [ "$_div" -eq 0 ] 2>/dev/null && _div=1
  log_info "[$tag] $workload: baseline=${baseline_ms}ms telemetry=${telemetry_ms}ms overhead=$(( (telemetry_ms - baseline_ms) * 100 / _div ))%"
  return 0
}

# ============================================================
# Run all Spark benchmarks for a version combo
# Usage: run_spark_all "combo_str" "workloads.lst"
# ============================================================
run_spark_all() {
  local combo="$1"
  local workload_list="$2"
  local failures=0

  while IFS= read -r workload || [ -n "$workload" ]; do
    [[ -z "$workload" || "$workload" =~ ^# ]] && continue
    if ! run_spark_benchmark "$combo" "$workload"; then
      ((failures++))
    fi
  done < "$workload_list"

  return $failures
}
