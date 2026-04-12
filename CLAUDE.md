# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

Maven multi-module project with Spark version profiles:

```bash
# Build for Spark 3.x (default profile)
mvn clean package

# Build for Spark 2.x
mvn clean package -Pspark-2

# Build for Spark 4.x
mvn clean package -Pspark-4

# Run unit tests (default profile)
mvn test

# Run a single test class
mvn test -pl spark-telemetry-common -Dtest=TelemetryConfigTest

# Run integration tests (failsafe plugin, Spark 3 profile only)
mvn verify -Pspark-3

# Skip tests during build
mvn clean package -DskipTests
```

## Architecture

This is a **transparent Spark telemetry plugin** that captures Spark task/stage IO metrics and JVM system metrics, exporting them via OpenTelemetry to an OTel Collector. It also includes a standalone **MR (MapReduce) Telemetry Collector** that polls Hadoop History Server for MR job metrics.

### Module Structure

```
spark-telemetry-common/       # Java-only core: config, models, OTel SDK setup, lifecycle
spark-telemetry-adapter-spark2/   # Scala 2.11 adapter for Spark 2.4 (spark.extraListeners)
spark-telemetry-adapter-spark3/   # Scala 2.12 adapter for Spark 3.5 (SparkPlugin API)
spark-telemetry-adapter-spark4/   # Scala 2.13 adapter for Spark 4.0 (SparkPlugin API)
spark-telemetry-dist-spark{2,3,4}/ # Shaded fat JARs for each Spark version
mr-telemetry-collector/        # Standalone MR job metric collector (Java)
mr-telemetry-dist/             # Shaded fat JAR for MR collector
metrics-flink-consumer/        # Flink job consuming OTLP protobuf from Kafka → MySQL/ClickHouse
metrics-flink-consumer-dist/   # Shaded fat JAR for Flink consumer
integration-tests/             # IT/E2E tests (Spark 3 profile only)
```

### Data Flow (Spark Plugin)

1. **SparkTelemetryPlugin** (Scala, SparkPlugin API) is loaded via `spark.plugins` config
2. **TelemetryDriverPlugin** initializes `TelemetryLifecycle` singleton and registers `SparkTelemetryListener`
3. **TelemetryExecutorPlugin** initializes `TelemetryLifecycle` + `SparkTelemetryMetricsSink` for JVM metrics
4. **SparkTelemetryListener** (extends SparkListener) captures `onTaskEnd`/`onStageCompleted` events, converts to `SparkMetricEvent`
5. **TelemetryLifecycle.accept()** routes events to **MetricRecorder**
6. **MetricRecorder** records OTel counters/histograms via OTel SDK Meter API
7. **OtelRegistry** manages the OTel SDK pipeline: PeriodicMetricReader → OTLP gRPC exporter (DELTA temporality) → OTel Collector

### Key Design Decisions

- **DELTA Temporality**: All OTLP exporters use `AggregationTemporalitySelector.deltaPreferred()` to prevent duplicate data on re-export. This applies to all 3 exporter locations: Spark Plugin (`OtelRegistry`), MR Agent (`AgentOtelRegistry`), MR Collector (`Main.java`).
- **Async Flush**: `TelemetryLifecycle.flushAsync()` uses a single-threaded daemon executor to avoid blocking Spark's DAGScheduler thread on `onJobEnd`. Blocking flush would cause 10s job timeouts.
- **appId Fallback**: All SparkTelemetryListener versions fall back from `SparkEnv.get.conf.getAppId` to `spark.app.name` to `"unknown"` to handle local mode and short-lived apps where appId is null.
- **Stage IO Separation**: Stage-level IO counters (`spark.stage.io.bytes_read/written`) are separate instruments from task-level (`spark.task.io.bytes_read/written`) to prevent counter name collision.
- **MR Gauge→Counter**: MR Collector uses `LongCounter.add()` for all metrics (including formerly gauge-like values) to avoid `buildWithCallback` memory leaks.

### Key Differences Between Spark Versions

- **Spark 2.x**: No SparkPlugin API. Uses `spark.extraListeners` registration. Listener lazily initializes via `ensureInit()` on first event. Shuffle write API uses `shuffleBytesWritten`/`shuffleWriteTime`/`shuffleRecordsWritten`.
- **Spark 3.x**: Uses `SparkPlugin`/`DriverPlugin`/`ExecutorPlugin` API. Shuffle write API uses `bytesWritten`/`writeTime`/`recordsWritten`.
- **Spark 4.x**: Same as 3.x API but compiled against Scala 2.13. Uses `scala.jdk.CollectionConverters` instead of `scala.collection.JavaConverters`.

### Configuration (Three-Tier Merge)

Config is loaded by `TelemetryConfig` with priority: Spark conf overrides (`spark.telemetry.*`) > HOCON file (`telemetry.conf`) > built-in defaults. See `telemetry.conf.example` and `mr-collector.conf.example` for all available options.

**Important**: Spark conf keys must include the full internal path, including `.otel.` segment. The mapping converts `spark.telemetry.X` to `spark-telemetry.X`:
- Correct: `spark.telemetry.otel.exporter.endpoint=http://host:4317`
- Wrong: `spark.telemetry.exporter.endpoint=http://host:4317` (maps to `spark-telemetry.exporter.endpoint`, NOT `spark-telemetry.otel.exporter.endpoint`)
- Same pattern for: `spark.telemetry.otel.service.name`, `spark.telemetry.otel.export.interval.ms`, `spark.telemetry.otel.exporter.protocol`

**Metric Category Switches** (all default to `true` except stage detailed and job lifecycle):

| Config Key | Default | Controls |
|------------|---------|----------|
| `spark.telemetry.metrics.task.execution` | `true` | Category 1: executor run time, CPU time, deserialize, GC, scheduler delay, result size, peak memory |
| `spark.telemetry.metrics.task.shuffle-extended` | `true` | Category 2: local blocks fetched, records read, remote bytes to disk, remote reqs duration |
| `spark.telemetry.metrics.task.info` | `true` | Category 3: task host, locality, speculative attributes |
| `spark.telemetry.metrics.stage.detailed` | `false` | Category 4: stage duration, num tasks, executor time, GC, peak memory, stage IO bytes |
| `spark.telemetry.metrics.job.lifecycle` | `false` | Category 5: job start/end events, job duration, num stages |

Custom HOCON config file path can be specified via `spark.telemetry.config.path=/path/to/telemetry.conf`.

### MR Telemetry Collector

Standalone Java app (`Main.java`) that polls Hadoop YARN History Server REST API for completed MR jobs, extracts counters, and exports via OTel. Runs independently from Spark.

### Flink Metrics Consumer

Flink 1.18 job that reads OTLP protobuf metrics from Kafka and writes to MySQL or ClickHouse with 9-table wide-row schema. Includes histogram bucket support and stage governance pre-aggregation.

Data flow: `Kafka (OTLP Protobuf) → Flink Job → MySQL / ClickHouse`

- **Database schema**: 9 tables — `task_metrics`, `stage_metrics`, `job_metrics`, `jvm_memory_metrics`, `jvm_gc_metrics`, `task_histogram_buckets`, `stage_histogram_buckets`, `job_histogram_buckets`, `stage_governance`
- **Configuration**: HOCON file `flink-consumer.conf` (see `flink-consumer.conf.example`)
- **Sink types**: `mysql` (default) or `clickhouse`, switched via `flink-consumer.sink.type`
- **Flink version**: 1.18.0 (last version supporting Java 8)
- **Kafka offset checkpoint**: `flink-consumer.kafka.checkpoint.path` (default: `/tmp/flink-consumer-checkpoint.txt`) for at-least-once delivery guarantee

```bash
# Build Flink consumer
mvn clean package -pl metrics-flink-consumer,metrics-flink-consumer-dist -am -DskipTests

# Run (standalone, needs Kafka + MySQL running)
java -jar metrics-flink-consumer-dist/target/metrics-flink-consumer-dist-1.0.0-SNAPSHOT.jar flink-consumer.conf
```

### Shading

Distribution modules use `maven-shade-plugin` to create self-contained JARs. OTel, gRPC, protobuf, Typesafe Config, and Guava are relocated under `x.mg.metrics.shaded.*`. Spark and Scala are excluded (provided scope).

**Critical shade plugin requirements** (learned the hard way):
1. **OkHttp3 + Kotlin stdlib required**: OTel's gRPC sender uses OkHttp3, which depends on Kotlin stdlib. Without these, you get `No GrpcSenderProvider found`. Must include:
   - `com.squareup.okhttp3:*`, `com.squareup.okhttp:*`, `com.squareup.okio:*`
   - `org.jetbrains.kotlin:*`, `org.jetbrains:annotations:*`
   - `com.google.code.gson:*`, `org.conscrypt:*`
5. **Flink standalone shade**: For `metrics-flink-consumer-dist`, Flink deps are `provided` scope but must be `compile` in dist module for standalone `java -jar` execution. Must also include `flink-connector-base`, `slf4j-api`, `slf4j-jdk14`. Use `<excludes>` (not `<includes>`) in artifactSet to avoid missing transitive deps like SLF4J.
6. **Flink objects must be Serializable**: Any object passed into Flink operators (filters, functions) must implement `Serializable`. Transient fields are null after serialization — initialize in `open()`.
7. **MySQL JSON vs TEXT**: Use `TEXT` column type for labels in MySQL (not `JSON`) to avoid driver-level validation issues with Gson output. Handle `Double.POSITIVE_INFINITY` from OTLP histogram buckets — MySQL doesn't accept it, use `Double.MAX_VALUE` instead.
2. **ServicesResourceTransformer is mandatory**: OTel uses `META-INF/services` for SPI. Without this transformer, service files aren't properly relocated, causing runtime failures.
3. **Only ONE `<transformers>` block**: Maven shade plugin silently uses the last `<transformers>` block if there are duplicates. Merge `ServicesResourceTransformer` and `ManifestResourceTransformer` into one block, and add `<filters>` for signature exclusion separately.
4. **Relocate `okhttp3` and `okio`**: Add relocations for these packages to avoid classpath conflicts.

### Deployment

`k8s/` contains Kubernetes manifests for a test environment: Hadoop cluster (NN, DN, RM, NM, HistoryServer), OTel Collector, and Kafka. `otel-collector-config/` has the OTel Collector pipeline config.

## K8s Integration Test Environment

### Architecture

All services run as bare Ubuntu pods with software installed via `kubectl cp` + `kubectl exec`:
- **hadoop3**: Hadoop 3.4.3 (NN, DN, RM, NM, HistoryServer) — stable for YARN MR jobs
- **hadoop2**: Hadoop 2.7.0 (NN, DN, HistoryServer only) — YARN NodeManager has container isolation bug in single-container K8s pods
- **spark3**: Spark 3.5.8 (Scala 2.12) — for Spark plugin testing
- **spark2**: Spark 2.4.4 (Scala 2.11) — for Spark 2.x plugin testing
- **kafka**: Kafka 3.7.0 (KRaft mode, no ZooKeeper)
- **otel-collector**: OTel Collector Contrib 0.96.0
- **mysql**: MySQL 8.0 (for Flink consumer sink testing)
- **clickhouse**: ClickHouse 23.8 (for Flink consumer sink testing, HTTP port 8123)

### Deploying

```bash
# Deploy all K8s resources
./k8s/deploy.sh

# After pods are running, install software in each pod:
# 1. Copy tarballs: kubectl cp <file> <pod>:/tmp/
# 2. Install Java: kubectl exec <pod> -- apt-get update && apt-get install -y openjdk-8-jdk-headless
# 3. Extract software: kubectl exec <pod> -- tar xzf /tmp/<file> -C /opt/
# 4. Copy config files: kubectl cp configs/ <pod>:/opt/hadoop/etc/hadoop/
# 5. Format NN + start services: kubectl exec <pod> -- bash setup.sh
```

### Key Pitfalls

1. **OTel Collector image**: Must use `otel/opentelemetry-collector-contrib` (NOT `otel/opentelemetry-collector`). The core image doesn't include Kafka exporter. Must pass `--config=/etc/otelcol-contrib/config.yaml` as container args.
2. **OTel Collector health_check**: The deployment has readiness/liveness probes on port 13133. Must add `health_check` extension in the collector config, otherwise the pod gets stuck in CrashLoopBackOff.
3. **Kafka KRaft single-node**: `KAFKA_CONTROLLER_QUORUM_VOTERS` must use `localhost:9093` (NOT `kafka:9093`). Controller binds to localhost when broker and controller are co-located.
4. **Hadoop 2.7.0 + YARN in containers**: NodeManager's `DefaultContainerExecutor` sends SIGTERM to wrong processes when cleaning up child containers in a single K8s pod. Use local mode (`mapreduce.framework.name=local`) instead of YARN for Hadoop 2.x in this environment.
5. **Spark metrics export timing**: Short Spark jobs (< 10 seconds) may complete before OTel SDK's first export interval. The plugin mitigates this with `flushAsync()` on `onJobEnd` and `forceFlush()` on shutdown. For testing, use longer jobs (e.g., SparkPi with 5000+ iterations) or reduce `spark.telemetry.otel.export.interval.ms`.
6. **Spark Scala version**: Spark 3.5.8 ships both Scala 2.12 and 2.13 variants. Match the plugin build profile to the correct Scala version. Use `spark-3.5.8-bin-hadoop3.tgz` (Scala 2.12 default) for the `spark-3` profile.
7. **Hadoop 2.7.0 daemon scripts**: Use legacy `hadoop-daemon.sh`/`yarn-daemon.sh`, NOT `--daemon` flag (Hadoop 3.x only). NN web UI port is 50070 (not 9870).
8. **Kafka console consumer**: In Kafka KRaft single-node, `kafka-console-consumer.sh` may timeout even when data exists (consumer_offsets topic auto-creation loop). Use `kafka-dump-log.sh --files <log-file>` to verify messages exist on disk.

### Running Integration Tests

```bash
# 1. Build all JARs
mvn clean package -Pspark-3 -DskipTests

# 2. Get pod IPs
OTEL_IP=$(kubectl get pods -l app=otel-collector -o jsonpath='{.items[0].status.podIP}')
HADOOP3_IP=$(kubectl get pod hadoop3 -o jsonpath='{.status.podIP}')

# 3. Test Spark plugin (Spark 3)
kubectl cp spark-telemetry-dist-spark3/target/*.jar spark3:/opt/spark-telemetry-plugin.jar
kubectl exec spark3 -- bash -c '
  export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
  export SPARK_HOME=/opt/spark
  $SPARK_HOME/bin/spark-submit --master "local[2]" \
    --class org.apache.spark.examples.SparkPi \
    --jars /opt/spark-telemetry-plugin.jar \
    --conf spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin \
    --conf spark.telemetry.otel.exporter.endpoint=http://'$OTEL_IP':4317 \
    --conf spark.telemetry.otel.service.name=spark3-test \
    --conf spark.telemetry.otel.export.interval.ms=5000 \
    $SPARK_HOME/examples/jars/spark-examples_2.12-*.jar 5000
'

# 4. Test MR Collector (Hadoop 3)
kubectl cp mr-telemetry-dist/target/*.jar hadoop3:/tmp/mr-collector.jar
# Create mr-collector.conf with correct IPs, then:
kubectl exec hadoop3 -- java -jar /tmp/mr-collector.jar /tmp/mr-collector.conf

# 5. Verify metrics in OTel Collector
kubectl logs -l app=otel-collector --tail=100 | grep -E "spark\.|mr\."

# 6. Verify metrics in Kafka (dump-log method)
kubectl exec $(kubectl get pods -l app=kafka -o jsonpath='{.items[0].metadata.name}') \
  -- /opt/kafka/bin/kafka-dump-log.sh --files /tmp/kafka-logs/telemetry-metrics-0/00000000000000000000.log

# 7. Test Flink Consumer (Kafka → MySQL)
kubectl apply -f k8s/mysql/mysql-pod.yaml
kubectl wait --for=condition=ready pod/mysql --timeout=120s
MYSQL_IP=$(kubectl get pod mysql -o jsonpath='{.status.podIP}')
KAFKA_IP=$(kubectl get pods -l app=kafka -o jsonpath='{.items[0].status.podIP}')
# Build + copy JAR
mvn clean package -pl metrics-flink-consumer,metrics-flink-consumer-dist -am -DskipTests
kubectl cp metrics-flink-consumer-dist/target/metrics-flink-consumer-dist-1.0.0-SNAPSHOT.jar spark3:/tmp/flink-consumer.jar
# Create config and run (timeout controls how long it runs)
kubectl exec spark3 -- bash -c '
  export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
  timeout 60 java -jar /tmp/flink-consumer.jar /tmp/flink-consumer.conf
'
# Verify data in MySQL
kubectl exec mysql -- mysql -u metrics -pmetrics metrics_db -e \
  "SELECT metric_name, COUNT(*) FROM metric_samples GROUP BY metric_name;"

# 8. Test Flink Consumer (Kafka → ClickHouse)
kubectl apply -f k8s/clickhouse/clickhouse-pod.yaml
kubectl wait --for=condition=ready pod/clickhouse --timeout=120s
CH_IP=$(kubectl get pod clickhouse -o jsonpath='{.status.podIP}')
kubectl exec clickhouse -- clickhouse-client -q "CREATE DATABASE IF NOT EXISTS metrics_db"
# Create flink-consumer-ch.conf with sink.type = clickhouse, then:
kubectl exec spark3 -- bash -c '
  export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
  timeout 60 java -jar /tmp/flink-consumer.jar /tmp/flink-consumer-ch.conf
'
# Verify data in ClickHouse
kubectl exec clickhouse -- clickhouse-client -d metrics_db -q \
  "SELECT metric_name, COUNT(*) FROM metric_samples GROUP BY metric_name"
```
