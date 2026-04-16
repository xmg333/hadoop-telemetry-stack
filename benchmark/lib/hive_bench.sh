#!/bin/bash
# Hive workload benchmarking: baseline vs hook

source "$(lib_dir)/helpers.sh"
source "$(lib_dir)/metrics_verify.sh"

# ============================================================
# Deploy Hive hook JAR and inject hive-site.xml config
# ============================================================
inject_hive_hook() {
  local hive_home="$1"
  local hive_site="$hive_home/conf/hive-site.xml"

  log_info "Injecting Hive hook into $hive_site"

  # Copy hook JAR to Hive lib
  ssh_run "cp $HIVE_HOOK_JAR $hive_home/lib/ 2>/dev/null || true"

  # Backup hive-site.xml
  ssh_run "cp $hive_site ${hive_site}.bak"

  # Check if hook property already exists
  local has_hook
  has_hook=$(ssh_run "grep -c 'hive.exec.post.hooks' $hive_site || echo 0")

  if [ "$has_hook" -eq 0 ]; then
    # Add hook properties before </configuration>
    ssh_run "sed -i 's|</configuration>|  <property>\\n    <name>hive.exec.post.hooks</name>\\n    <value>x.mg.metrics.hivetelemetry.HiveTelemetryHook</value>\\n  </property>\\n  <property>\\n    <name>hive.telemetry.otel.exporter.endpoint</name>\\n    <value>${OTEL_ENDPOINT}</value>\\n  </property>\\n  <property>\\n    <name>hive.telemetry.otel.service.name</name>\\n    <value>hive-hook-bench</value>\\n  </property>\\n</configuration>|' $hive_site"
  else
    # Update existing hook value
    ssh_run "sed -i 's|<name>hive.exec.post.hooks</name>\\s*<value>[^<]*</value>|<name>hive.exec.post.hooks</name>\\n    <value>x.mg.metrics.hivetelemetry.HiveTelemetryHook</value>|' $hive_site"
  fi

  log_info "Hive hook injected"
}

remove_hive_hook() {
  local hive_home="$1"
  local hive_site="$hive_home/conf/hive-site.xml"

  log_info "Removing Hive hook from $hive_site"
  ssh_run "if [ -f ${hive_site}.bak ]; then mv ${hive_site}.bak $hive_site; fi"
  # Remove hook JAR
  ssh_run "rm -f $hive_home/lib/hive-telemetry-hook*.jar 2>/dev/null || true"
}

# ============================================================
# Run a single Hive benchmark (baseline + hook)
# Usage: run_hive_benchmark "hive_combo" "hadoop_dir" "micro/wordcount"
# hive_combo format: "hive239:apache-hive-2.3.9-bin"
# ============================================================
run_hive_benchmark() {
  local hive_combo="$1"
  local hadoop_dir="$2"
  local workload="$3"

  IFS=':' read -r hive_tag hive_dir <<< "$hive_combo"
  local hive_home="/opt/$hive_dir"
  local hadoop_home="/opt/$hadoop_dir"
  local service_name="bench-${hive_tag}-${workload//\//-}"

  log_info "=== Hive benchmark: $hive_tag / $workload ==="

  # Check Hive installation
  if ! ssh_run "test -d $hive_home" >/dev/null 2>&1; then
    log_warn "Hive $hive_home not found, skipping"
    record_result "$hive_combo:$hadoop_dir" "$workload" "baseline" "0" "-1" "SKIPPED"
    record_result "$hive_combo:$hadoop_dir" "$workload" "telemetry" "0" "-1" "SKIPPED"
    return 1
  fi

  # Set HIVE_HOME in HiBench config
  ssh_run "cd $HIBENCH_HOME && sed -i 's|hibench.hive.release.*|hibench.hive.release\t\t$hive_dir|' conf/hibench.conf"

  # --- RUN 1: Baseline ---
  log_info "[$hive_tag] Baseline run: $workload"
  local baseline_ms=0
  local baseline_rc=0
  baseline_ms=$(timed_run "baseline-$hive_tag-$workload" \
    "cd $HIBENCH_HOME && HIVE_HOME=$hive_home HADOOP_HOME=$hadoop_home HADOOP_CONF_DIR=$hadoop_home/etc/hadoop bin/workloads/${workload}/hadoop/run.sh") || baseline_rc=$?

  if [ $baseline_rc -ne 0 ]; then
    log_warn "[$hive_tag] Baseline FAILED for $workload (rc=$baseline_rc), skipping hook run"
    record_result "$hive_combo:$hadoop_dir" "$workload" "baseline" "$baseline_ms" "$baseline_rc" ""
    record_result "$hive_combo:$hadoop_dir" "$workload" "telemetry" "0" "-1" "SKIPPED"
    return 1
  fi

  record_result "$hive_combo:$hadoop_dir" "$workload" "baseline" "$baseline_ms" "0" ""

  # --- RUN 2: With Hive Hook ---
  log_info "[$hive_tag] Hook run: $workload"
  inject_hive_hook "$hive_home"

  local telemetry_ms=0
  local telemetry_rc=0
  telemetry_ms=$(timed_run "telemetry-$hive_tag-$workload" \
    "cd $HIBENCH_HOME && HIVE_HOME=$hive_home HADOOP_HOME=$hadoop_home HADOOP_CONF_DIR=$hadoop_home/etc/hadoop bin/workloads/${workload}/hadoop/run.sh") || telemetry_rc=$?

  remove_hive_hook "$hive_home"

  # Verify metrics
  local metrics_found="NO"
  if [ $telemetry_rc -eq 0 ]; then
    metrics_found=$(verify_hive_metrics "$service_name")
  fi

  record_result "$hive_combo:$hadoop_dir" "$workload" "telemetry" "$telemetry_ms" "$telemetry_rc" "$metrics_found"

  if [ $telemetry_rc -ne 0 ]; then
    log_error "[$hive_tag] Hook run FAILED for $workload - HOOK CRASHED"
    return 1
  fi

  local _div=${baseline_ms:-0}; [ "$_div" -eq 0 ] 2>/dev/null && _div=1
  log_info "[$hive_tag] $workload: baseline=${baseline_ms}ms hook=${telemetry_ms}ms overhead=$(( (telemetry_ms - baseline_ms) * 100 / _div ))%"
  return 0
}

# ============================================================
# Run all Hive benchmarks for a version combo
# Usage: run_hive_all "hive_combo" "hadoop_dir" "workloads.lst"
# ============================================================
run_hive_all() {
  local hive_combo="$1"
  local hadoop_dir="$2"
  local workload_list="$3"
  local failures=0

  while IFS= read -r workload || [ -n "$workload" ]; do
    [[ -z "$workload" || "$workload" =~ ^# ]] && continue
    if ! run_hive_benchmark "$hive_combo" "$hadoop_dir" "$workload"; then
      ((failures++))
    fi
  done < "$workload_list"

  return $failures
}
