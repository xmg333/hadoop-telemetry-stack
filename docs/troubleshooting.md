# Troubleshooting Guide

This document provides systematic troubleshooting steps and solutions for common issues.

## Problem-Solving Decision Tree

```
Start
  |
  +-- No metrics data?
  |   +-- Check Spark/MR/Hive config -> Not active -> Check JAR path and config keys
  |   +-- OTel Collector no logs -> Connection failure -> Check endpoint address
  |   +-- Kafka no messages -> Export failure -> Check Kafka connection
  |
  +-- Incomplete metrics?
  |   +-- Certain metrics missing -> Config switch not enabled -> Check metrics.* config keys
  |   +-- SQL metrics are NULL -> AQE issue -> Check Spark version
  |
  +-- Performance issues?
      +-- Jobs slower -> Metric overhead too high -> Reduce export.interval
      +-- Out of memory -> OTel buffer too large -> Adjust batch size
```

---

## Common Issues

### Q1: Spark plugin not working, no metrics output

**Symptoms**: After Spark job runs, OTel Collector shows no logs, Kafka has no messages

**Troubleshooting Steps**:

1. Verify the JAR path is correct and accessible
   ```bash
   ls -la /path/to/spark-telemetry-dist-omni-*.jar
   ```

2. Check the `spark.plugins` (Spark 3/4) or `spark.extraListeners` (Spark 2) configuration
   ```bash
   # Spark 3.x
   --conf spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin

   # Spark 2.x
   --conf spark.extraListeners=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryListener
   ```

3. Check Driver/Executor logs for `TelemetryLifecycle initialized`
   ```bash
   yarn logs -applicationId <app_id> -log_files stdout | grep -i telemetry
   ```

4. Verify the OTel Collector address is reachable
   ```bash
   nc -zv otel-collector 4317
   ```

5. Check that config keys include the `.otel.` segment (common mistake)
   ```bash
   # Correct
   --conf spark.telemetry.otel.exporter.endpoint=http://otel-collector:4317

   # Incorrect (missing .otel.)
   --conf spark.telemetry.exporter.endpoint=http://otel-collector:4317
   ```

---

### Q2: Short-running job metrics missing

**Symptoms**: Jobs with runtime shorter than `export.interval` have no exported metrics

**Solutions**:

1. The plugin triggers `flushAsync()` non-blocking flush automatically in `onJobEnd` and calls `forceFlush()` synchronously on shutdown
2. If data is still missing, reduce the export interval:
   ```bash
   --conf spark.telemetry.otel.export.interval.ms=5000
   ```

---

### Q3: MR Collector connection timeout to History Server

**Symptoms**: MR Collector logs show `Connection timed out`

**Troubleshooting Steps**:

1. Verify the URL and port (History Server port is 19888 for both Hadoop 2.x and 3.x)
   ```hocon
   mr-telemetry.history-server.url = "http://hadoop-historyserver:19888"
   ```

2. Increase timeout configuration:
   ```hocon
   mr-telemetry.history-server.connect.timeout.secs = 10
   mr-telemetry.history-server.read.timeout.secs = 30
   ```

3. Check network connectivity:
   ```bash
   curl -v http://hadoop-historyserver:19888/ws/v1/jobs
   ```

---

### Q4: OTel Collector fails to start

**Symptoms**: Docker container exits immediately, logs show errors

**Solutions**:

1. Use `otel/opentelemetry-collector-contrib` (not the core image)
   ```bash
   docker run ... otel/opentelemetry-collector-contrib:0.96.0
   ```

2. The configuration must include the `health_check` extension
   ```yaml
   extensions:
     health_check:
       endpoint: 0.0.0.0:13133
   ```

3. Specify the config file via the `--config` parameter
   ```bash
   docker run ... --config=/etc/otelcol-contrib/config.yaml
   ```

---

### Q5: No metrics data visible in Kafka

**Symptoms**: OTel Collector is running normally, but Kafka has no messages

**Troubleshooting Steps**:

1. Check OTel Collector logs:
   ```bash
   docker logs otel-collector --tail=100 | grep -E "kafka|export"
   ```

2. Verify Kafka exporter configuration (broker addresses, topic)
   ```yaml
   exporters:
     kafka:
       topic: telemetry-metrics
       brokers:
         - kafka:9092
   ```

3. Use `kafka-dump-log.sh` to verify messages exist:
   ```bash
   docker exec kafka /opt/kafka/bin/kafka-dump-log.sh \
     --files /tmp/kafka-logs/telemetry-metrics-0/00000000000000000000.log
   ```

4. Note: `kafka-console-consumer.sh` may time out in single-node KRaft mode

---

### Q6: Omnipackage version detection error

**Symptoms**: Logs show incorrect Spark version, wrong adapter is loaded

**Troubleshooting Steps**:

1. Check the version detected by `OmniContext` in Driver/Executor logs
   ```
   OmniContext detected: Spark 3.5.0, Scala 2.12.15
   ```

2. Verify there are no conflicting `scala-library` JARs on the classpath
   ```bash
   spark-submit --help | grep classpath
   ```

3. If using a custom classpath, ensure `scala-library` matches the Spark version

---

### Q7: SQL shuffle_bytes is NULL

**Symptoms**: `shuffle_bytes` field in `sql_query_metrics` table is NULL

**Cause**: AQE `inputPlan` returns the initial plan

**Solution**: Use the latest version, which has fixed this issue. Ensure you are using the latest Omnipackage JAR.

---

## Log Analysis Guide

### Check Spark Plugin Initialization

```bash
# Driver logs
yarn logs -applicationId <app_id> -log_files stdout | grep -i "telemetry\|plugin"

# Expected output
TelemetryLifecycle initialized
SparkTelemetryPlugin started
```

### Check OTel Collector Status

```bash
# Health check
curl http://otel-collector:13133/health

# Logs
docker logs otel-collector --tail=100 | grep -E "error|warn|kafka"
```

### Check Kafka Messages

```bash
# View Topic message count
docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe \
  --topic telemetry-metrics --bootstrap-server localhost:9092

# Consume test messages
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic telemetry-metrics --bootstrap-server localhost:9092 \
  --from-beginning --max-messages=5
```

### Check MySQL Data

```bash
# Connect to MySQL
docker exec -it mysql mysql -u root -proot123 metrics_db

# Check table data
SELECT COUNT(*) FROM task_metrics;
SELECT COUNT(*) FROM stage_metrics;
SELECT COUNT(*) FROM sql_query_metrics;

# Check latest data
SELECT * FROM task_metrics ORDER BY timestamp DESC LIMIT 5;
```

---

## Metrics Verification Script

### End-to-End Data Flow Verification

```bash
#!/bin/bash
# verify-telemetry.sh

echo "=== Check OTel Collector ==="
curl -s http://localhost:13133/health | jq .

echo "=== Check Kafka Topic ==="
docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe \
  --topic telemetry-metrics --bootstrap-server localhost:9092

echo "=== Check MySQL Data ==="
docker exec mysql mysql -u root -proot123 metrics_db -e \
  "SELECT 'task' as type, COUNT(*) as cnt FROM task_metrics
   UNION ALL SELECT 'stage', COUNT(*) FROM stage_metrics
   UNION ALL SELECT 'sql', COUNT(*) FROM sql_query_metrics;"
```

---

## Performance Tuning Recommendations

### Reducing Metric Overhead

1. Disable unnecessary metric categories:
   ```bash
   --conf spark.telemetry.metrics.stage.detailed=false
   --conf spark.telemetry.metrics.job.lifecycle=false
   ```

2. Increase the export interval (suitable for short-running jobs):
   ```bash
   --conf spark.telemetry.otel.export.interval.ms=30000
   ```

3. Reduce OTel batch size:
   ```yaml
   # OTel Collector config
   exporters:
     kafka:
       producer:
         max_message_bytes: 500000  # Default 1MB
   ```

---

## Contact Support

If you encounter an issue not covered here, please provide the following information:

1. Spark/Hadoop/Hive versions
2. Omnipackage JAR version
3. Relevant log snippets (Driver/Executor/OTel Collector)
4. Configuration file contents (sanitized)
5. Steps to reproduce the issue
