#!/bin/bash
# Cross-check Grafana dashboard SQL queries against expected DB schema (v8 rename).
# Usage: ./validate.sh [--quiet]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
pass=0; fail=0; warn=0

report_pass() { echo -e "${GREEN}[PASS]${NC} $1"; pass=$((pass+1)); }
report_fail() { echo -e "${RED}[FAIL]${NC} $1"; fail=$((fail+1)); }
report_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; warn=$((warn+1)); }

# v8 expected table names
EXPECTED_TABLES="spark_task_metrics spark_stage_metrics spark_job_metrics spark_jvm_memory spark_jvm_gc spark_task_histogram spark_stage_histogram spark_job_histogram spark_stage_skew spark_sql_metrics spark_sql_table hive_query_metrics hive_query_table mr_job_metrics mr_task_metrics unified_metrics"

# Old table names that must NOT appear
OLD_TABLES="task_metrics stage_metrics job_metrics jvm_memory_metrics jvm_gc_metrics task_histogram_buckets stage_histogram_buckets job_histogram_buckets stage_governance sql_query_metrics sql_query_table_metrics hive_table_io_metrics metric_events"

echo "=== Grafana Dashboard SQL vs DB Schema Cross-Check ==="
echo ""

# Extract all FROM <table> from all grafana JSONs into temp file
extracted_tables=$(mktemp)
trap "rm -f $extracted_tables" EXIT

for json_file in "$SCRIPT_DIR"/*.json; do
  basename=$(basename "$json_file")
  # grep rawSql values, extract FROM clauses
  grep -oP '"rawSql"\s*:\s*"[^"]+"' "$json_file" 2>/dev/null | \
    grep -oP '\bFROM\s+\w+' | \
    awk '{print $2}' | \
    while read -r tbl; do
      echo "$tbl $basename"
    done >> "$extracted_tables"
done

echo "Tables found: $(wc -l < "$extracted_tables") references across $(ls "$SCRIPT_DIR"/*.json | wc -l) dashboards"

# ---- Check 1: No old table names ----
echo ""
echo "--- Check 1: No old table names ---"
for old in $OLD_TABLES; do
  hits=$(grep "^$old " "$extracted_tables" 2>/dev/null | awk '{print $2}' | sort -u | tr '\n' ' ')
  if [ -n "$hits" ]; then
    report_fail "Old table '$old' still in: $hits"
  else
    report_pass "Old table '$old' not found"
  fi
done

# ---- Check 2: All references are expected tables ----
echo ""
echo "--- Check 2: All table references are valid ---"
while read -r tbl json_file; do
  if [ -z "$tbl" ]; then continue; fi
  found=0
  for expected in $EXPECTED_TABLES; do
    if [ "$tbl" = "$expected" ]; then found=1; break; fi
  done
  if [ "$found" = "0" ]; then
    report_warn "Unknown table '$tbl' in $json_file"
  fi
done < "$extracted_tables"

# ---- Check 3: All expected tables are referenced ----
echo ""
echo "--- Check 3: All expected tables referenced ---"
for expected in $EXPECTED_TABLES; do
  if grep -q "^$expected " "$extracted_tables" 2>/dev/null; then
    report_pass "Table '$expected' referenced"
  else
    report_warn "Table '$expected' NOT referenced in any dashboard"
  fi
done

# ---- Check 4: No stray $$ in Grafana macros ----
echo ""
echo "--- Check 4: No stray '\$\$' in Grafana macros ---"
for json_file in "$SCRIPT_DIR"/*.json; do
  basename=$(basename "$json_file")
  # $$ followed by something that isn't __ (e.g. $__interval_ms is OK, $$ is not)
  stray=$(grep -oP '"rawSql"\s*:\s*"[^"]+"' "$json_file" 2>/dev/null | grep -oP '\$\$(?!\w)' || true)
  if [ -n "$stray" ]; then
    report_fail "Stray '\$\$' found in $basename"
  else
    report_pass "No stray '\$\$' in $basename"
  fi
done

# ---- Check 5: unified_metrics uses new column names ----
echo ""
echo "--- Check 5: unified_metrics uses new column names ---"
OLD_UNIFIED_COLS="io_bytes_read io_bytes_written io_records_read io_records_written memory_bytes_spilled time_ms_col"
NEW_UNIFIED_COLS="bytes_read bytes_written records_read records_written bytes_spilled time_ms"

# Convert to arrays
declare -a OLD_ARR=($OLD_UNIFIED_COLS)
declare -a NEW_ARR=($NEW_UNIFIED_COLS)

for json_file in "$SCRIPT_DIR"/*.json; do
  basename=$(basename "$json_file")
  raw_sqls=$(grep -oP '"rawSql"\s*:\s*"[^"]+"' "$json_file" 2>/dev/null || true)
  if echo "$raw_sqls" | grep -q "unified_metrics" 2>/dev/null; then
    for ((i=0; i<${#OLD_ARR[@]}; i++)); do
      old_col="${OLD_ARR[$i]}"
      new_col="${NEW_ARR[$i]}"
      if echo "$raw_sqls" | grep -qP "\b${old_col}\b" 2>/dev/null; then
        report_fail "$basename: unified_metrics has old '$old_col' (use '$new_col')"
      fi
    done
  fi
done

# ---- Summary ----
echo ""
echo "====================================="
echo -e "Results: ${GREEN}$pass passed${NC}, ${YELLOW}$warn warnings${NC}, ${RED}$fail failed${NC}"
echo "====================================="

[ "$fail" -gt 0 ] && exit 1 || exit 0
