# Spark Telemetry Plugin -- Deployment & Metrics Reference

## Deployment

### Spark 3.x / 4.x (SparkPlugin API)

#### Distributing the JAR

```bash
# Option A: HDFS
hdfs dfs -put spark/spark-telemetry-dist-spark3/target/spark-telemetry-dist-spark3-*.jar /spark/libs/

# Option B: Local Path
scp spark/spark-telemetry-dist-spark3/target/spark-telemetry-dist-spark3-*.jar node:/opt/spark/libs/
```

#### Configuration

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --jars /opt/spark/libs/spark-telemetry-plugin.jar \
  --conf spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin \
  --conf spark.telemetry.otel.exporter.endpoint=http://otel-collector:4317 \
  --conf spark.telemetry.otel.service.name=my-spark-app \
  --conf spark.telemetry.otel.export.interval.ms=10000 \
  your-app.jar
```

`spark-defaults.conf` approach:

```properties
spark.plugins              x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin
spark.telemetry.otel.exporter.endpoint    http://otel-collector:4317
spark.telemetry.otel.service.name         my-spark-app
spark.telemetry.otel.export.interval.ms   10000
```

### Spark 2.x (spark.extraListeners)

Spark 2.x does not have the `SparkPlugin` API:

```bash
spark-submit \
  --master yarn \
  --jars /opt/spark/libs/spark-telemetry-plugin.jar \
  --conf spark.extraListeners=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryListener \
  --conf spark.telemetry.otel.exporter.endpoint=http://otel-collector:4317 \
  --conf spark.telemetry.otel.service.name=my-spark2-app \
  your-app.jar
```

**Spark 2.x Notes**:
- Lazy initialization via `ensureInit()` on the first event
- Shuffle Write API uses `shuffleBytesWritten` / `shuffleWriteTime` / `shuffleRecordsWritten` (different from 3.x)

### Omnipackage Unified Deployment

The Omnipackage combines Spark 2/3/4 + MR Collector + MR Agent + Hive Hook into a single JAR, auto-detecting the Spark version at runtime.

Configuration is **identical** to version-specific JARs -- simply replace the JAR:

```bash
# Spark 3/4
spark-submit --jars /opt/omnipackage.jar \
  --conf spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin \
  --conf spark.telemetry.otel.exporter.endpoint=http://otel-collector:4317 \
  --conf spark.telemetry.otel.service.name=my-app \
  your-app.jar

# Spark 2
spark-submit --jars /opt/omnipackage.jar \
  --conf spark.extraListeners=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryListener \
  --conf spark.telemetry.otel.exporter.endpoint=http://otel-collector:4317 \
  --conf spark.telemetry.otel.service.name=my-app \
  your-app.jar

# MR Collector mode
java -jar omnipackage.jar --mr-collector /path/to/mr-collector.conf

# MR Agent mode
-javaagent:/opt/omnipackage.jar -Dmr.telemetry.agent.otel.exporter.endpoint=http://otel-collector:4317
```

#### Omnipackage vs Version-Specific JARs

| Feature | Version-Specific JAR | Omnipackage |
|---------|---------------------|-------------|
| File count | 3 (one each for Spark 2/3/4) | 1 |
| Operational complexity | Distribute by Spark version | Unified distribution |
| Configuration differences | Version-specific entry classes | Same entry class, auto-detection |
| JAR size | ~30MB each | ~50-60MB |
| MR support | Requires separate JARs | Includes MR Collector + Agent |

### HOCON Configuration File (Optional)

In addition to Spark Conf, detailed configuration via a HOCON file is also supported:

```bash
cp conf/examples/telemetry.conf.example telemetry.conf
spark-submit --files telemetry.conf ...
```

Config priority: **Spark Conf override > HOCON file > Built-in defaults**

### Verification

```bash
# Check Driver/Executor logs
# Should see: INFO TelemetryLifecycle: Telemetry initialized, endpoint=http://collector:4317

# Check OTel Collector
kubectl logs -l app=otel-collector --tail=100 | grep "spark\."
```

> **Note**: Short-lived Spark jobs (< 10s) may complete before the OTel SDK's first export. Use longer-running jobs for testing, or reduce `spark.telemetry.otel.export.interval.ms`.

---

## Configuration Parameters

### Minimum Configuration (Required)

| Parameter | Description | Example |
|-----------|-------------|---------|
| `spark.plugins` | Spark plugin class name | `x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin` |
| `spark.telemetry.otel.exporter.endpoint` | OTel Collector gRPC endpoint | `http://collector:4317` |

### Optional Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `spark.telemetry.otel.service.name` | `spark-application` | OTel service name |
| `spark.telemetry.otel.export.interval.ms` | `10000` | Metrics export interval (ms) |
| `spark.telemetry.otel.export.max-data-points-per-batch` | `5000` | Maximum data points per OTLP export batch (used by SplittingMetricExporter for large metric batches) |
| `spark.telemetry.otel.exporter.protocol` | `grpc` | Export protocol (`grpc` / `http`) |
| `spark.telemetry.config.path` | (classpath) | HOCON config file path |
| `spark.telemetry.metrics.task.execution` | `true` | Category 1: task execution metrics |
| `spark.telemetry.metrics.task.shuffle-extended` | `true` | Category 2: extended shuffle metrics |
| `spark.telemetry.metrics.task.info` | `true` | Category 3: task info attributes |
| `spark.telemetry.metrics.stage.detailed` | `true` | Category 4: stage detailed metrics |
| `spark.telemetry.metrics.job.lifecycle` | `true` | Category 5: job lifecycle metrics |
| `spark.telemetry.metrics.sql.query-execution` | `true` | Category 6: SQL query execution metrics |
| `spark.telemetry.metrics.sql.max-length` | `4096` | Maximum SQL text truncation length (characters) |

> **Important**: Spark config keys must include the full internal path, including the `.otel.` segment. The mapping is `spark.telemetry.X` -> `spark-telemetry.X`:
> - Correct: `spark.telemetry.otel.exporter.endpoint=http://host:4317`
> - Incorrect: `spark.telemetry.exporter.endpoint=http://host:4317`

### HOCON Complete Reference (telemetry.conf)

```hocon
spark-telemetry {
  otel {
    exporter.endpoint = "http://localhost:4317"
    exporter.protocol = "grpc"
    service.name = "spark-application"
    export.interval.ms = 10000
    resource.attributes = {
      "deployment.environment" = "production"
    }
  }

  metrics {
    listener {
      enabled = true
      capture.task-end = true
      capture.stage-complete = true
      capture.job-end = true
    }

    system {
      enabled = true
      capture.jvm-memory = true
      capture.jvm-gc = true
      capture.buffer-pools = true
      capture.executor-memory = true
    }

    task.execution = true             # Category 1
    task.shuffle-extended = true      # Category 2
    task.info = true                  # Category 3
    stage.detailed = true             # Category 4
    job.lifecycle = true              # Category 5
    sql.query-execution = true        # Category 6
    sql.max-length = 4096           # SQL text truncation
  }

  filter {
    app.name.include = [".*"]
    app.name.exclude = []
  }
}
```

---

## Metrics Reference

### Core IO Metrics (Always Collected)

| Metric Name | Type | Unit | Description |
|-------------|------|------|-------------|
| `spark.task.io.bytes_read` | Counter | By | Task bytes read |
| `spark.task.io.bytes_written` | Counter | By | Task bytes written |
| `spark.task.io.records_read` | Counter | {records} | Task records read |
| `spark.task.io.records_written` | Counter | {records} | Task records written |
| `spark.task.shuffle.bytes_read` | Counter | By | Shuffle bytes read |
| `spark.task.shuffle.bytes_written` | Counter | By | Shuffle bytes written |
| `spark.task.shuffle.fetch_wait_time_ms` | Counter | ms | Shuffle fetch wait time |
| `spark.task.disk_bytes_spilled` | Counter | By | Disk bytes spilled |
| `spark.task.memory_bytes_spilled` | Counter | By | Memory bytes spilled |
| `spark.task.duration_ms` | Histogram | ms | Task execution duration |

### Task Execution Metrics (Category 1)

| Metric Name | Type | Unit | Description |
|-------------|------|------|-------------|
| `spark.task.executor.run_time_ms` | Histogram | ms | Executor run time |
| `spark.task.executor.cpu_time_ns` | Counter | ns | Executor CPU time |
| `spark.task.deserialize_time_ms` | Histogram | ms | Deserialization time |
| `spark.task.deserialize_cpu_time_ns` | Counter | ns | Deserialization CPU time |
| `spark.task.result_serialization_time_ms` | Histogram | ms | Result serialization time |
| `spark.task.jvm_gc_time_ms` | Histogram | ms | Task JVM GC time |
| `spark.task.scheduler_delay_ms` | Histogram | ms | Scheduler delay |
| `spark.task.result_size_bytes` | Counter | By | Task result size |
| `spark.task.peak_execution_memory_bytes` | Counter | By | Peak execution memory |

### Extended Shuffle Metrics (Category 2)

| Metric Name | Type | Unit | Description |
|-------------|------|------|-------------|
| `spark.task.shuffle.local_blocks_fetched` | Counter | {blocks} | Local shuffle blocks fetched |
| `spark.task.shuffle.records_read` | Counter | {records} | Shuffle records read |
| `spark.task.shuffle.remote_bytes_read_to_disk` | Counter | By | Remote shuffle bytes read to disk |
| `spark.task.shuffle.remote_reqs_duration_ms` | Counter | ms | Remote shuffle request duration |

### Stage Detailed Metrics (Category 4)

| Metric Name | Type | Unit | Description |
|-------------|------|------|-------------|
| `spark.stage.duration_ms` | Histogram | ms | Stage duration |
| `spark.stage.num_tasks` | Counter | {tasks} | Number of tasks in stage |
| `spark.stage.executor.run_time_ms` | Counter | ms | Stage total executor run time |
| `spark.stage.executor.cpu_time_ns` | Counter | ns | Stage total CPU time |
| `spark.stage.jvm_gc_time_ms` | Counter | ms | Stage total GC time |
| `spark.stage.peak_execution_memory_bytes` | Counter | By | Stage peak memory |
| `spark.stage.io.bytes_read` | Counter | By | Stage bytes read (independent of task-level) |
| `spark.stage.io.bytes_written` | Counter | By | Stage bytes written (independent of task-level) |

### Job Lifecycle Metrics (Category 5)

| Metric Name | Type | Unit | Description |
|-------------|------|------|-------------|
| `spark.job.duration_ms` | Histogram | ms | Job duration |
| `spark.job.num_stages` | Counter | {stages} | Number of stages in job |

### JVM System Metrics

| Metric Name | Type | Unit | Description |
|-------------|------|------|-------------|
| `spark.jvm.memory.heap_used` | Gauge | By | JVM heap memory used |
| `spark.jvm.memory.non_heap_used` | Gauge | By | JVM non-heap memory used |
| `spark.jvm.gc.count` | Counter | {count} | GC count (by gc_name) |
| `spark.jvm.gc.time_ms` | Counter | ms | GC time |

### Metric Attributes (Labels)

| Attribute Name | Scope | Description |
|----------------|-------|-------------|
| `spark.app.id` | All | Application ID |
| `spark.app.name` | All | Application name |
| `spark.user` | All | User running the application (from SparkConf `spark.user` or system property) |
| `spark.yarn.queue` | All | YARN queue name |
| `spark.executor.id` | Task / System | Executor ID |
| `spark.stage.id` | Task / Stage | Stage ID |
| `spark.task.id` | Task | Task ID |
| `spark.task.success` | Task | Whether the task succeeded |
| `spark.task.host` | Task | Task execution host (Category 3) |
| `spark.task.locality` | Task | Data locality (Category 3) |
| `spark.task.speculative` | Task | Whether speculative execution (Category 3) |
| `spark.job.id` | Job | Job ID |
| `spark.job.success` | Job | Whether the job succeeded |
| `gc_name` | GC | GC collector name |
| `spark.sql.execution_id` | SQL Query | SQL execution ID (Spark 3.x+) |
| `spark.sql.query_text` | SQL Query | SQL query text (truncated) |
| `spark.sql.table_name` | SQL Table IO | Table name (SQL table IO metrics only) |
| `spark.sql.operation` | SQL Table IO | Operation type: scan/write (SQL table IO metrics only) |

### SQL Query Execution Metrics (Category 6, enabled by default)

Enable with: `spark.telemetry.metrics.sql.query-execution=true`

| Metric Name | Type | Unit | Description |
|-------------|------|------|-------------|
| `spark.sql.query.duration_ms` | Histogram | ms | SQL query execution duration |
| `spark.sql.query.shuffle.bytes_read` | Counter | By | Shuffle bytes read |
| `spark.sql.query.shuffle.bytes_written` | Counter | By | Shuffle bytes written |
| `spark.sql.query.join_count` | Counter | {joins} | Number of joins |
| `spark.sql.table.bytes` | Counter | By | Table-level IO bytes |
| `spark.sql.table.rows` | Counter | {rows} | Table-level IO rows |
| `spark.sql.table.files_read` | Counter | {files} | Number of files scanned |
| `spark.sql.table.time_ms` | Counter | ms | Table IO time (covers both scan and write operations) |

**Spark 2.x Limitation**: In Spark 2.x, `spark.sql.execution_id` and `spark.sql.query_text` are NOT available. Spark 2.x's `QueryExecution` has no `id` field, so `executionId` stays at 0 and SQL text is not captured.
