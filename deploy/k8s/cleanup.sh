#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Cleaning up..."
kubectl delete -f "$SCRIPT_DIR/grafana/" --ignore-not-found 2>/dev/null || true
kubectl delete -f "$SCRIPT_DIR/hive/" --ignore-not-found 2>/dev/null || true
kubectl delete -f "$SCRIPT_DIR/clickhouse/" --ignore-not-found 2>/dev/null || true
kubectl delete -f "$SCRIPT_DIR/mysql/" --ignore-not-found 2>/dev/null || true
kubectl delete -f "$SCRIPT_DIR/otel-collector/" --ignore-not-found 2>/dev/null || true
kubectl delete -f "$SCRIPT_DIR/kafka/" --ignore-not-found 2>/dev/null || true
kubectl delete -f "$SCRIPT_DIR/spark2/" --ignore-not-found 2>/dev/null || true
kubectl delete -f "$SCRIPT_DIR/spark3/" --ignore-not-found 2>/dev/null || true
kubectl delete -f "$SCRIPT_DIR/hadoop2/" --ignore-not-found 2>/dev/null || true
kubectl delete -f "$SCRIPT_DIR/hadoop3/" --ignore-not-found 2>/dev/null || true
echo "Cleaned up"
