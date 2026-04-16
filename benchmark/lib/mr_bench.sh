#!/bin/bash
# MR (MapReduce) workload benchmarking: baseline vs agent

source "$(lib_dir)/helpers.sh"
source "$(lib_dir)/metrics_verify.sh"

# ============================================================
# Inject MR agent into mapred-site.xml
# ============================================================
inject_mr_agent() {
  local hadoop_home="$1"
  local mapred_site="$hadoop_home/etc/hadoop/mapred-site.xml"

  log_info "Injecting MR agent into $mapred_site"

  # Backup
  ssh_run "cp $mapred_site ${mapred_site}.bak"

  # Add javaagent to map and reduce opts via XSLT-like sed approach
  # We insert properties before </configuration>
  ssh_run "sed -i 's|</configuration>|  <property>\\n    <name>mapreduce.map.java.opts</name>\\n    <value>-javaagent:${MR_AGENT_JAR} -Dmr.telemetry.agent.otel.exporter.endpoint=${OTEL_ENDPOINT} -Dmr.telemetry.agent.otel.service.name=mr-agent-bench</value>\\n  </property>\\n  <property>\\n    <name>mapreduce.reduce.java.opts</name>\\n    <value>-javaagent:${MR_AGENT_JAR} -Dmr.telemetry.agent.otel.exporter.endpoint=${OTEL_ENDPOINT} -Dmr.telemetry.agent.otel.service.name=mr-agent-bench</value>\\n  </property>\\n</configuration>|' $mapred_site"
}

remove_mr_agent() {
  local hadoop_home="$1"
  local mapred_site="$hadoop_home/etc/hadoop/mapred-site.xml"

  log_info "Removing MR agent from $mapred_site"
  # Restore backup
  ssh_run "if [ -f ${mapred_site}.bak ]; then mv ${mapred_site}.bak $mapred_site; fi"
}

# ============================================================
# Run a single MR benchmark (baseline + agent)
# Usage: run_mr_benchmark "combo_str" "micro/wordcount"
# combo format: "mr:hadoop32:hadoop-3.2.0"
# ============================================================
run_mr_benchmark() {
  local combo="$1"
  local workload="$2"

  IFS=':' read -r tag hadoop_tag hadoop_dir <<< "$combo"
  local hadoop_home="/opt/$hadoop_dir"
  local service_name="bench-mr-${hadoop_tag}-${workload//\//-}"

  log_info "=== MR benchmark: $hadoop_tag / $workload ==="

  # --- RUN 1: Baseline ---
  log_info "[MR-$hadoop_tag] Baseline run: $workload"
  local baseline_ms=0
  local baseline_rc=0
  baseline_ms=$(timed_run "baseline-mr-$hadoop_tag-$workload" \
    "cd $HIBENCH_HOME && HADOOP_HOME=$hadoop_home HADOOP_CONF_DIR=$hadoop_home/etc/hadoop bin/workloads/${workload}/hadoop/run.sh") || baseline_rc=$?

  if [ $baseline_rc -ne 0 ]; then
    log_warn "[MR-$hadoop_tag] Baseline FAILED for $workload (rc=$baseline_rc), skipping agent run"
    record_result "$combo" "$workload" "baseline" "$baseline_ms" "$baseline_rc" ""
    record_result "$combo" "$workload" "telemetry" "0" "-1" "SKIPPED"
    return 1
  fi

  record_result "$combo" "$workload" "baseline" "$baseline_ms" "0" ""

  # --- RUN 2: With MR Agent ---
  log_info "[MR-$hadoop_tag] Agent run: $workload"
  inject_mr_agent "$hadoop_home"

  local telemetry_ms=0
  local telemetry_rc=0
  telemetry_ms=$(timed_run "telemetry-mr-$hadoop_tag-$workload" \
    "cd $HIBENCH_HOME && HADOOP_HOME=$hadoop_home HADOOP_CONF_DIR=$hadoop_home/etc/hadoop bin/workloads/${workload}/hadoop/run.sh") || telemetry_rc=$?

  remove_mr_agent "$hadoop_home"

  # Verify metrics
  local metrics_found="NO"
  if [ $telemetry_rc -eq 0 ]; then
    metrics_found=$(verify_mr_metrics "$service_name")
  fi

  record_result "$combo" "$workload" "telemetry" "$telemetry_ms" "$telemetry_rc" "$metrics_found"

  if [ $telemetry_rc -ne 0 ]; then
    log_error "[MR-$hadoop_tag] Agent run FAILED for $workload - AGENT CRASHED"
    return 1
  fi

  local _div=${baseline_ms:-0}; [ "$_div" -eq 0 ] 2>/dev/null && _div=1
  log_info "[MR-$hadoop_tag] $workload: baseline=${baseline_ms}ms agent=${telemetry_ms}ms overhead=$(( (telemetry_ms - baseline_ms) * 100 / _div ))%"
  return 0
}

# ============================================================
# Run all MR benchmarks for a version combo
# Usage: run_mr_all "combo_str" "workloads.lst"
# ============================================================
run_mr_all() {
  local combo="$1"
  local workload_list="$2"
  local failures=0

  while IFS= read -r workload || [ -n "$workload" ]; do
    [[ -z "$workload" || "$workload" =~ ^# ]] && continue
    if ! run_mr_benchmark "$combo" "$workload"; then
      ((failures++))
    fi
  done < "$workload_list"

  return $failures
}
