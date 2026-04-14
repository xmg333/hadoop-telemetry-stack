#!/bin/bash
# Spark 3.2 submit examples — Hadoop 3 + Hive 2.3.9

OMNI_JAR=/path/to/spark-telemetry-dist-omni-1.0.0-SNAPSHOT.jar
OTEL_ENDPOINT=http://otel-collector:4317

# ── Basic preset ──────────────────────────────────────────────
spark-submit --master yarn \
  --jars $OMNI_JAR \
  --conf spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin \
  --conf spark.telemetry.config.path=/path/to/basic/telemetry.conf \
  --conf spark.telemetry.otel.exporter.endpoint=$OTEL_ENDPOINT \
  --conf spark.telemetry.otel.service.name=my-spark-app \
  your-app.jar

# ── Full preset (all metrics) ─────────────────────────────────
spark-submit --master yarn \
  --jars $OMNI_JAR \
  --conf spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin \
  --conf spark.telemetry.config.path=/path/to/full/telemetry.conf \
  --conf spark.telemetry.otel.exporter.endpoint=$OTEL_ENDPOINT \
  --conf spark.telemetry.otel.service.name=my-spark-app \
  --conf spark.telemetry.otel.export.interval.ms=10000 \
  your-app.jar

# ── MR Collector (standalone) ─────────────────────────────────
java -jar $OMNI_JAR --mr-collector /path/to/mr-collector.conf
