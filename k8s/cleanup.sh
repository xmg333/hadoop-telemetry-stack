#!/bin/bash
set -e
echo "Cleaning up..."
kubectl delete -f hadoop3/ --ignore-not-found 2>/dev/null || true
kubectl delete -f hadoop2/ --ignore-not-found 2>/dev/null || true
kubectl delete -f spark3/ --ignore-not-found 2>/dev/null || true
kubectl delete -f spark2/ --ignore-not-found 2>/dev/null || true
kubectl delete -f kafka/ --ignore-not-found 2>/dev/null || true
kubectl delete -f otel-collector/ --ignore-not-found 2>/dev/null || true
echo "Cleaned up"
