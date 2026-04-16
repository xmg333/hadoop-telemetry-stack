#!/bin/bash
# Benchmark global configuration
# All paths refer to the REMOTE server (192.168.10.65)

set -euo pipefail

# ============================================================
# Remote Server
# ============================================================
export BENCHMARK_SERVER="root@192.168.10.65"
export SSH_KEY="$HOME/.ssh/id_rsa"
export SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10"

# ============================================================
# Java & Service Endpoints (on remote server)
# ============================================================
export REMOTE_JAVA_HOME="/opt/jdk8u482-b08"
export OTEL_ENDPOINT="http://127.0.0.1:4317"
export HIBENCH_HOME="/root/hibench"
export RESULTS_DIR="/root/benchmark-results"

# MySQL (Docker container on remote)
export MYSQL_CONTAINER="mysql"
export MYSQL_USER="root"
export MYSQL_PASS="root123"
export MYSQL_DB="telemetry"

# ============================================================
# Plugin JAR paths (on remote server)
# ============================================================
export OMNI_JAR="/opt/spark-telemetry-omnipackage.jar"
export MR_AGENT_JAR="/opt/mr-telemetry-agent.jar"
export HIVE_HOOK_JAR="/opt/hive-telemetry-hook.jar"

# ============================================================
# HiBench Configuration
# ============================================================
export HIBENCH_SCALE="small"
export SPARK_MASTER="local[2]"   # local mode for single-node testing
export YARN_NUM_EXECUTORS="2"

# ============================================================
# Version Combinations
# Format: tag:hadoop_tag:spark_install_dir:hadoop_install_dir:scala_version
# ============================================================
export SPARK_VERSION_COMBOS=(
  "spark24:hadoop27:spark-2.4.4-bin-hadoop2.7:hadoop-2.7.0:2.11"
  "spark32:hadoop32:spark-3.2.0-bin-hadoop3.2:hadoop-3.2.0:2.12"
)

export MR_VERSION_COMBOS=(
  "mr:hadoop27:hadoop-2.7.0"
  "mr:hadoop32:hadoop-3.2.0"
)

export HIVE_VERSION_COMBOS=(
  "hive239:apache-hive-2.3.9-bin"
  "hive313:apache-hive-3.1.3-bin"
)

# ============================================================
# HiBench Maven build profiles
# HiBench POM has profiles: spark2.4, spark3.0, spark3.1
# Spark 3.2 and 3.5 use spark3.0 profile (Scala 2.12 compatible)
# ============================================================
declare -A HIBENCH_SPARK_PROFILES=(
  ["2.11"]="spark2.4"
  ["2.12"]="spark3.0"
)

# ============================================================
# Spark plugin config key per version
# ============================================================
declare -A SPARK_PLUGIN_KEYS=(
  ["spark24"]="spark.extraListeners"
  ["spark30"]="spark.plugins"
  ["spark32"]="spark.plugins"
  ["spark35"]="spark.plugins"
)

declare -A SPARK_PLUGIN_CLASSES=(
  ["spark24"]="x.mg.metrics.sparktelemetry.adapter.SparkTelemetryListener"
  ["spark30"]="x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin"
  ["spark32"]="x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin"
  ["spark35"]="x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin"
)

# ============================================================
# Pipeline drain wait time (seconds)
# ============================================================
export METRICS_DRAIN_WAIT=30

# ============================================================
# Run ID
# ============================================================
export RUN_ID="bench-$(date +%Y%m%d-%H%M%S)"
