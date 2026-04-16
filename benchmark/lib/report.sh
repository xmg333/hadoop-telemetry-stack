#!/bin/bash
# Report generation from CSV timing data

source "$(lib_dir)/helpers.sh"

# ============================================================
# Generate text report from CSV results
# Usage: generate_report "$csv_path"
# ============================================================
generate_report() {
  local csv_path="$1"
  local report_path="${csv_path/timing-/report-}"
  report_path="${report_path/.csv/.txt}"

  log_info "Generating report: $report_path"

  # Pull CSV from remote
  local local_csv="/tmp/bench-results-${RUN_ID}.csv"
  scp_from_remote "$csv_path" "$local_csv" 2>/dev/null || true

  if [ ! -f "$local_csv" ] || [ ! -s "$local_csv" ]; then
    log_error "No results CSV found at $local_csv"
    return 1
  fi

  {
    echo "============================================================"
    echo "  TELEMETRY BENCHMARK REPORT"
    echo "  Generated: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "  Run ID: $RUN_ID"
    echo "  HiBench Scale: $HIBENCH_SCALE"
    echo "============================================================"
    echo ""

    # --- Section 1: Compatibility Matrix ---
    echo "============================================================"
    echo "  1. COMPATIBILITY MATRIX (Did the workload complete?)"
    echo "============================================================"
    printf "%-30s %-20s %-10s %-10s\n" "WORKLOAD" "VERSION" "BASELINE" "TELEMETRY"
    echo "------------------------------------------------------------------------"

    # Read CSV, skip header
    tail -n +2 "$local_csv" | while IFS=',' read -r combo workload mode duration_ms exit_code metrics_found; do
      local status
      if [ "$exit_code" = "0" ]; then
        status="PASS"
      elif [ "$exit_code" = "-1" ]; then
        status="SKIP"
      else
        status="FAIL"
      fi

      if [ "$mode" = "baseline" ]; then
        # Print baseline, save for telemetry comparison
        printf "%-30s %-20s %-10s " "$workload" "$combo" "$status"
      elif [ "$mode" = "telemetry" ]; then
        printf "%-10s\n" "$status"
      fi
    done
    echo ""

    # --- Section 2: Performance Comparison ---
    echo "============================================================"
    echo "  2. PERFORMANCE COMPARISON"
    echo "============================================================"
    printf "%-30s %-20s %12s %12s %10s\n" "WORKLOAD" "VERSION" "BASELINE(ms)" "TELEMETRY(ms)" "OVERHEAD%"
    echo "--------------------------------------------------------------------------------"

    # Process pairs
    tail -n +2 "$local_csv" | while IFS=',' read -r combo workload mode duration_ms exit_code metrics_found; do
      if [ "$mode" = "baseline" ] && [ "$exit_code" = "0" ]; then
        echo "${combo}|${workload}|${duration_ms}" > "/tmp/bench-baseline-${workload//\//-}-${combo//\//-}"
      fi
    done

    tail -n +2 "$local_csv" | while IFS=',' read -r combo workload mode duration_ms exit_code metrics_found; do
      if [ "$mode" = "telemetry" ] && [ "$exit_code" = "0" ]; then
        local baseline_file="/tmp/bench-baseline-${workload//\//-}-${combo//\//-}"
        if [ -f "$baseline_file" ]; then
          local baseline_ms
          baseline_ms=$(cut -d'|' -f3 "$baseline_file")
          if [ "$baseline_ms" -gt 0 ] 2>/dev/null; then
            local overhead=$(( (duration_ms - baseline_ms) * 100 / baseline_ms ))
            printf "%-30s %-20s %12s %12s %9d%%\n" "$workload" "$combo" "$baseline_ms" "$duration_ms" "$overhead"
          else
            printf "%-30s %-20s %12s %12s %10s\n" "$workload" "$combo" "$baseline_ms" "$duration_ms" "N/A"
          fi
        fi
      fi
    done

    # Clean up temp files
    rm -f /tmp/bench-baseline-*
    echo ""

    # --- Section 3: Metrics Verification ---
    echo "============================================================"
    echo "  3. METRICS VERIFICATION (Did metrics reach MySQL?)"
    echo "============================================================"
    printf "%-30s %-20s %-10s\n" "WORKLOAD" "VERSION" "METRICS"
    echo "------------------------------------------------------------"

    tail -n +2 "$local_csv" | while IFS=',' read -r combo workload mode duration_ms exit_code metrics_found; do
      if [ "$mode" = "telemetry" ]; then
        local result="${metrics_found:-N/A}"
        if [ "$exit_code" != "0" ]; then
          result="CRASH"
        fi
        printf "%-30s %-20s %-10s\n" "$workload" "$combo" "$result"
      fi
    done
    echo ""

    # --- Section 4: Summary ---
    echo "============================================================"
    echo "  4. SUMMARY"
    echo "============================================================"

    local total_runs
    total_runs=$(tail -n +2 "$local_csv" | wc -l)
    local baseline_pass
    baseline_pass=$(tail -n +2 "$local_csv" | awk -F',' '$3=="baseline" && $5=="0"' | wc -l)
    local telemetry_pass
    telemetry_pass=$(tail -n +2 "$local_csv" | awk -F',' '$3=="telemetry" && $5=="0"' | wc -l)
    local telemetry_fail
    telemetry_fail=$(tail -n +2 "$local_csv" | awk -F',' '$3=="telemetry" && $5!="0" && $5!="-1"' | wc -l)
    local metrics_yes
    metrics_yes=$(tail -n +2 "$local_csv" | awk -F',' '$3=="telemetry" && $6=="YES"' | wc -l)
    local metrics_no
    metrics_no=$(tail -n +2 "$local_csv" | awk -F',' '$3=="telemetry" && $6=="NO"' | wc -l)

    echo "  Total runs:             $total_runs"
    echo "  Baseline passed:        $baseline_pass"
    echo "  Telemetry passed:       $telemetry_pass"
    echo "  Telemetry CRASHED:      $telemetry_fail"
    echo "  Metrics found (YES):    $metrics_yes"
    echo "  Metrics missing (NO):   $metrics_no"
    echo "============================================================"

  } | tee "/tmp/bench-report-${RUN_ID}.txt"

  # Copy report to remote
  scp_to_remote "/tmp/bench-report-${RUN_ID}.txt" "$report_path" 2>/dev/null || true

  log_info "Report saved locally: /tmp/bench-report-${RUN_ID}.txt"
  log_info "Report saved remotely: $report_path"
}
