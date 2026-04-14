#!/bin/bash
# Spark 2.4 submit examples — Hadoop 2.7 + Hive 2.3.9
#
# Spark 2.x uses spark.extraListeners (no SparkPlugin API).
# QEL is auto-registered when sql.query-execution=true.

OMNI_JAR=/path/to/spark-telemetry-dist-omni-1.0.0-SNAPSHOT.jar
OTEL_ENDPOINT=http://otel-collector:4317

# ── Basic preset ──────────────────────────────────────────────
spark-submit --master yarn \
  --jars $OMNI_JAR \
  --conf spark.extraListeners=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryListener \
  --conf spark.telemetry.config.path=/path/to/basic/telemetry.conf \
  --conf spark.telemetry.otel.exporter.endpoint=$OTEL_ENDPOINT \
  --conf spark.telemetry.otel.service.name=my-spark-app \
  your-app.jar

# ── Full preset (all metrics) ─────────────────────────────────
spark-submit --master yarn \
  --jars $OMNI_JAR \
  --conf spark.extraListeners=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryListener \
  --conf spark.telemetry.config.path=/path/to/full/telemetry.conf \
  --conf spark.telemetry.otel.exporter.endpoint=$OTEL_ENDPOINT \
  --conf spark.telemetry.otel.service.name=my-spark-app \
  --conf spark.telemetry.otel.export.interval.ms=10000 \
  your-app.jar

# ── Inline config (no file) ───────────────────────────────────
spark-submit --master yarn \
  --jars $OMNI_JAR \
  --conf spark.extraListeners=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryListener \
  --conf spark.telemetry.otel.exporter.endpoint=$OTEL_ENDPOINT \
  --conf spark.telemetry.otel.service.name=my-spark-app \
  --conf spark.telemetry.otel.export.interval.ms=30000 \
  --conf spark.telemetry.metrics.sql.query-execution=true \
  your-app.jar

# ── MR Collector (standalone) ─────────────────────────────────
java -jar $OMNI_JAR --mr-collector /path/to/mr-collector.conf
