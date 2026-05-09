# Flink Metrics Consumer -- Deployment & Visualization Reference

## Architecture

The Flink Consumer is built on the Flink DataStream API. It consumes OTLP protobuf metrics from Kafka via Flink KafkaSource, then deserializes, classifies, aggregates into wide rows, and writes to MySQL or ClickHouse.

```
Kafka (OTLP Protobuf) -> Flink KafkaSource -> OtlpDeserializationSchema
  -> MetricRecordSplitFlatMap -> MetricNameFilter -> .keyBy(item -> "all") -> AccumulatingProcessFunction
  -> FlinkCategoryJdbcSink -> MySQL / ClickHouse
```

Core operators:
- **OtlpDeserializationSchema**: Deserializes OTLP protobuf bytes from Kafka into `MetricRecord` (containing `MetricSample` + `HistogramBucket`)
- **MetricRecordSplitFlatMap**: Splits `MetricRecord` into individual `MetricItem` (a single sample or bucket)
- **AccumulatingProcessFunction**: A Flink `ProcessFunction` that uses `ValueState<WideRowAccumulator>` to manage aggregation state, triggering flush based on batch-size thresholds and processing-time timers. Supports Flink checkpoint fault recovery
- **FlinkCategoryJdbcSink**: A Flink `RichSinkFunction` that manages JDBC connection lifecycle (`open()`/`close()`), writing `FlushResult` to 15 category tables + `unified_metrics` unified wide table

## Deployment

### Database Preparation

#### MySQL

```sql
CREATE DATABASE IF NOT EXISTS metrics_db;
CREATE USER 'metrics'@'%' IDENTIFIED BY 'metrics';
GRANT ALL PRIVILEGES ON metrics_db.* TO 'metrics'@'%';
```

Tables (15 category tables + `unified_metrics` unified wide table) are auto-created on startup -- no manual DDL needed.

#### ClickHouse

```sql
CREATE DATABASE IF NOT EXISTS metrics_db;
```

Tables are also auto-created, using ClickHouse-specific types (`DateTime64(3)`, `LowCardinality(String)`, `Nullable(Float64)`, etc.) with monthly partitioning.

### Configuration

Create `flink-consumer.conf`:

```hocon
flink-consumer {
  kafka {
    bootstrap.servers = "kafka:9092"
    topic = "telemetry-metrics"
    group.id = "flink-metrics-consumer"
    startup.mode = "earliest-offset"  # earliest-offset | latest-offset
    checkpoint.path = "/tmp/flink-consumer-checkpoint"  # Flink checkpoint storage path
  }

  sink {
    type = "mysql"  # mysql | clickhouse

    mysql {
      url = "jdbc:mysql://mysql:3306/metrics_db"
      user = "metrics"
      password = "metrics"
      batch.size = 1000
      flush.interval.ms = 5000
    }

    clickhouse {
      url = "jdbc:clickhouse://clickhouse:8123/metrics_db"
      user = "default"
      password = ""
      batch.size = 5000
      flush.interval.ms = 3000
    }
  }

  filter {
    metric.name.include = [".*"]
    metric.name.exclude = []
  }

  processing {
    parallelism = 2
  }
}
```

### Running

```bash
# Standalone (self-contained JAR, uses Flink LocalEnvironment)
java -jar metrics-flink-consumer-dist.jar flink-consumer.conf

# Submit to Flink cluster (recommended for production)
/opt/flink-1.18.0/bin/flink run -c x.mg.metrics.flink.Main \
  -m localhost:8081 \
  metrics-flink-consumer-dist.jar flink-consumer.conf

# Background submit (survives SSH disconnect)
nohup /opt/flink-1.18.0/bin/flink run -c x.mg.metrics.flink.Main \
  -m localhost:8081 \
  metrics-flink-consumer-dist.jar flink-consumer.conf \
  > /tmp/flink-submit.log 2>&1 &

# Check cluster job status
curl -s http://localhost:8081/jobs | python3 -c \
  'import json,sys; [print(j["id"],j["status"]) for j in json.load(sys.stdin)["jobs"]]'

# Cancel a job
/opt/flink-1.18.0/bin/flink cancel <job-id>
```

### Important Notes

- All tables are auto-created on startup -- no manual DDL needed
- All labels are expanded into explicit typed columns, no JSON data type is used
- `Double.POSITIVE_INFINITY` (from OTLP histogram buckets) is automatically converted to `Double.MAX_VALUE`
- Flink 1.18.0 (last version supporting Java 8)
- Kafka offsets are managed by Flink checkpointing, guaranteeing at-least-once semantics. `checkpoint.path` is the Flink checkpoint storage directory (single-file offset persistence is no longer used)
- When `app_id` is empty, it automatically falls back to `"unknown"`
- All objects passed into Flink operators (model classes, config, filter) implement `Serializable`
- Aggregation state is managed via `ValueState`, supporting checkpoint fault recovery

---

## Database Table Structure

| Table Name | Description |
|------------|-------------|
| `spark_task_metrics` | One row per Task completion event, 12 dimension columns + 23 metric columns |
| `spark_stage_metrics` | One row per Stage completion event |
| `spark_job_metrics` | One row per Job event |
| `spark_jvm_memory` | JVM memory time-series data |
| `spark_jvm_gc` | JVM GC time-series data |
| `spark_task_histogram` | Task-level histogram bucket distribution |
| `spark_stage_histogram` | Stage-level histogram bucket distribution |
| `spark_job_histogram` | Job-level histogram bucket distribution |
| `spark_stage_skew` | Stage governance metrics (pre-aggregated) |
| `spark_sql_metrics` | Spark SQL execution metrics (join/shuffle bytes, etc., includes query_text) |
| `spark_sql_table` | Spark SQL table-level IO metrics |
| `hive_query_metrics` | Hive query execution metrics (duration/success/IO, includes query_text) |
| `hive_query_table` | Hive query table-level IO metrics |
| `mr_job_metrics` | MR Collector job-level metrics |
| `mr_task_metrics` | MR Agent task-level metrics |
| `unified_metrics` | Cross-engine unified wide table, partitioned by engine (SPARK/MR/HIVE) and event_type |

### spark_task_metrics

**Dimension columns:** `timestamp_ms`, `app_id`, `app_name`, `user_name`, `queue`, `executor_id`, `stage_id`, `task_id`, `task_success`, `task_host`, `task_locality`, `task_speculative`

**Metric columns:** `duration_ms`, `io_bytes_read`, `io_bytes_written`, `io_records_read`, `io_records_written`, `shuffle_bytes_read`, `shuffle_bytes_written`, `shuffle_fetch_wait_time_ms`, `disk_bytes_spilled`, `memory_bytes_spilled`, `executor_run_time_ms`, `executor_cpu_time_ns`, `deserialize_time_ms`, `deserialize_cpu_time_ns`, `result_serialization_time_ms`, `jvm_gc_time_ms`, `scheduler_delay_ms`, `result_size_bytes`, `peak_execution_memory_bytes`, `shuffle_local_blocks_fetched`, `shuffle_records_read`, `shuffle_remote_bytes_read_to_disk`, `shuffle_remote_reqs_duration_ms`

### spark_stage_metrics

**Dimension columns:** `timestamp_ms`, `app_id`, `app_name`, `user_name`, `queue`, `executor_id`, `stage_id`

**Metric columns:** `duration_ms`, `num_tasks`, `executor_run_time_ms`, `executor_cpu_time_ns`, `jvm_gc_time_ms`, `peak_execution_memory_bytes`, `io_bytes_read`, `io_bytes_written`

### spark_job_metrics

**Dimension columns:** `timestamp_ms`, `app_id`, `app_name`, `user_name`, `queue`, `job_id`, `job_success`

**Metric columns:** `duration_ms`, `num_stages`

### spark_jvm_memory

**Dimension columns:** `timestamp_ms`, `app_id`, `app_name`, `user_name`, `queue`, `executor_id`

**Metric columns:** `heap_used`, `non_heap_used`

### spark_jvm_gc

**Dimension columns:** `timestamp_ms`, `app_id`, `app_name`, `user_name`, `queue`, `executor_id`, `gc_name`

**Metric columns:** `gc_count`, `gc_time_ms`

### spark_sql_metrics

**Dimension columns:** `timestamp_ms`, `app_id`, `execution_id`, `app_name`, `user_name`, `queue`

**Metric columns:** `duration_ms`, `shuffle_bytes_read`, `shuffle_bytes_written`, `join_count`

**Text column:** `query_text` (TEXT, SQL query text, truncated to max 4096 characters)

### hive_query_metrics

**Dimension columns:** `timestamp_ms`, `app_name`, `queue`, `query_id`, `operation`, `user_name`, `success`, `execution_engine`

**Metric columns:** `duration_ms`, `success_count`, `failure_count`, `input_bytes`, `output_bytes`, `input_rows`, `output_rows`

**Text column:** `query_text` (TEXT, Hive SQL query text, truncated to max 4096 characters)

### Histogram Bucket Tables

`spark_task_histogram`, `spark_stage_histogram`, `spark_job_histogram` have dimension columns consistent with their corresponding metrics tables, plus three additional columns: `metric_name`, `bucket_le`, `bucket_count`.

### unified_metrics

**Unified cross-engine wide table**, consolidating data from all 15 category tables. One row per event, distinguished by `engine` (SPARK/MR/HIVE) and `event_type` (TASK/STAGE/JOB/JVM_MEMORY/JVM_GC/SQL_QUERY/SQL_TABLE_IO/HIVE_QUERY/HIVE_TABLE_IO/MR_JOB/MR_TASK).

**Core dimension columns:** `timestamp_ms`, `event_type`, `engine`, `status`, `app_id`, `app_name`, `user_name`, `queue`

**Normalized metric columns:** `duration_ms`, `bytes_read`, `bytes_written`, `shuffle_bytes_read`, `shuffle_bytes_written`, `cpu_time_ms`, `gc_time_ms`, `bytes_spilled`

**Engine-specific dimension/metric columns:** Preserves unique fields per engine (e.g., Spark's `executor_id`/`stage_id`/`task_id`, MR's `job_id`/`launched_maps`/`hdfs_bytes_read`, Hive's `operation`/`execution_engine`), NULL where not applicable.

**Text column:** `query_text` (TEXT, SQL query text)

Full DDL statements can be found in `deploy/sql/v5_migration.sql`. The `query_text` column was added by `deploy/sql/v6_migration.sql`.

---

## Stage Governance Metrics (spark_stage_skew)

The Flink Consumer auto-aggregates one governance row per completed Stage, enabling Grafana to render directly from a single table.

### Governance Dimensions

| Dimension | Calculation Method | Anomaly Threshold |
|-----------|-------------------|-------------------|
| Duration skew | `duration_skew_ratio` = max / avg task duration | > 2 skewed |
| IO skew | `io_read_skew_ratio`, `io_write_skew_ratio`, `shuffle_read_skew_ratio` | > 2 skewed |
| Small file detection | `avg_output_bytes_per_task`, `small_output_task_count` (< 32MB) | task count > 5 |
| CPU efficiency | `cpu_efficiency` = cpu_time / run_time | < 0.5 inefficient |
| GC overhead | `gc_overhead_ratio` = gc_time / run_time | > 0.1 needs tuning |
| Shuffle overhead | `shuffle_wait_ratio` = fetch_wait / run_time | > 0.1 |
| Spill ratio | `spill_ratio` = disk_spilled / bytes_read | > 0.3 |
| Deserialization overhead | `deserialize_overhead` = deserialize_time / run_time | -- |
| Scheduler delay | `scheduler_delay_ratio` = scheduler_delay / total_duration | -- |

### Table Structure

```sql
CREATE TABLE spark_stage_skew (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp_ms BIGINT NOT NULL,
    app_id VARCHAR(255) NOT NULL,
    stage_id INT NOT NULL,
    task_count INT NOT NULL,

    -- Duration
    stage_duration_ms DOUBLE,
    avg_task_duration_ms DOUBLE,
    max_task_duration_ms DOUBLE,
    min_task_duration_ms DOUBLE,
    duration_skew_ratio DOUBLE,

    -- IO
    total_bytes_read DOUBLE,
    total_bytes_written DOUBLE,
    total_shuffle_bytes_read DOUBLE,
    total_shuffle_bytes_written DOUBLE,
    total_records_read DOUBLE,
    total_records_written DOUBLE,

    -- IO skew
    io_read_skew_ratio DOUBLE,
    io_write_skew_ratio DOUBLE,
    shuffle_read_skew_ratio DOUBLE,

    -- Small files
    avg_output_bytes_per_task DOUBLE,
    avg_output_records_per_task DOUBLE,
    small_output_task_count INT,

    -- Resource efficiency
    cpu_efficiency DOUBLE,
    gc_overhead_ratio DOUBLE,
    shuffle_wait_ratio DOUBLE,
    spill_ratio DOUBLE,
    deserialize_overhead DOUBLE,
    scheduler_delay_ratio DOUBLE,

    -- Memory
    max_peak_memory_bytes DOUBLE,
    total_memory_spilled DOUBLE,

    INDEX idx_app_time (app_id, timestamp_ms),
    INDEX idx_stage (app_id, stage_id)
);
```

---

## Grafana Visualization

Pre-built dashboard: `deploy/grafana/spark-mr-telemetry-dashboard.json`

### Import

1. Grafana -> Dashboards -> Import
2. Upload `deploy/grafana/spark-mr-telemetry-dashboard.json`
3. Select MySQL data source
4. Save

### Data Source Configuration

**MySQL** (default): URL `mysql:3306`, Database `metrics_db`, User/Password `metrics` / `metrics`

**ClickHouse**: URL `http://clickhouse:8123`, Database `metrics_db`. Adjust SQL time functions:
- `$__unixEpochFilter(timestamp_ms/1000)` -> `$__timeFilter_ms(timestamp_ms)`
- `$__unixEpochGroup(timestamp_ms/1000, $__interval)` -> `$__timeInterval_ms(timestamp_ms)`
- `FROM_UNIXTIME(timestamp_ms/1000, ...)` -> `DateTime(timestamp_ms)`

### Dashboard Panels

| Panel | Type | Data Source | Description |
|-------|------|-------------|-------------|
| Total Tasks / Stages | Stat | spark_task_metrics/spark_stage_metrics | Total counts |
| Skewed Stages | Stat | spark_stage_skew | skew_ratio > 2 (red/yellow/green) |
| Avg CPU Efficiency | Stat | spark_stage_skew | < 0.5 red, > 0.7 green |
| Task I/O Bytes | Time Series | spark_task_metrics | IO/Shuffle read/write trends |
| Task Duration | Time Series | spark_task_metrics | avg/max/min trends |
| JVM Memory | Time Series | spark_jvm_memory | Heap/non-heap trends |
| JVM GC | Time Series | spark_jvm_gc | GC count and duration |
| Stage Duration & Task Count | Time Series | spark_stage_metrics | Stage duration and task count |
| Job Overview | Table | spark_job_metrics | Job list |
| Data Skew Detection | Table | spark_stage_skew | Skew detection (color thresholds) |
| Resource Efficiency | Table | spark_stage_skew | CPU/GC/Shuffle/Spill efficiency |
| Task Duration Histogram | Bar Chart | spark_task_histogram | Duration distribution |
| Small File Detection | Table | spark_stage_skew | Small file detection |
| Task Detail | Table | spark_task_metrics | Latest 200 task details |

Template variable: `$app_id` (application ID filter, multi-select)

### Alert Thresholds

| Metric | Green | Yellow | Red |
|--------|-------|--------|-----|
| duration_skew_ratio | < 1.5 | 1.5 ~ 2 | > 2 |
| cpu_efficiency | > 0.7 | 0.5 ~ 0.7 | < 0.5 |
| gc_overhead_ratio | < 0.05 | 0.05 ~ 0.1 | > 0.1 |
| shuffle_wait_ratio | < 0.05 | 0.05 ~ 0.1 | > 0.1 |
| spill_ratio | < 0.1 | 0.1 ~ 0.3 | > 0.3 |
| avg_output_bytes_per_task | > 32MB | 1MB ~ 32MB | < 1MB |
| small_output_task_count | 0 | 1 ~ 5 | > 5 |

### Governance Query Examples (MySQL)

```sql
-- Data skew
SELECT app_id, stage_id, task_count, duration_skew_ratio,
       max_task_duration_ms, avg_task_duration_ms
FROM spark_stage_skew WHERE duration_skew_ratio > 2
ORDER BY timestamp_ms DESC LIMIT 20;

-- Small files
SELECT app_id, stage_id, task_count, avg_output_bytes_per_task,
       small_output_task_count
FROM spark_stage_skew WHERE avg_output_bytes_per_task < 33554432
ORDER BY timestamp_ms DESC LIMIT 20;

-- Low CPU efficiency
SELECT app_id, stage_id, task_count, cpu_efficiency,
       avg_task_duration_ms, total_bytes_read
FROM spark_stage_skew WHERE cpu_efficiency < 0.5
ORDER BY timestamp_ms DESC LIMIT 20;

-- High GC overhead
SELECT app_id, stage_id, gc_overhead_ratio, avg_task_duration_ms, task_count
FROM spark_stage_skew WHERE gc_overhead_ratio > 0.1
ORDER BY timestamp_ms DESC LIMIT 20;

-- High shuffle overhead
SELECT app_id, stage_id, shuffle_wait_ratio,
       total_shuffle_bytes_read, total_shuffle_bytes_written
FROM spark_stage_skew WHERE shuffle_wait_ratio > 0.1
ORDER BY timestamp_ms DESC LIMIT 20;

-- Application Stage overview
SELECT app_id, stage_id, task_count, stage_duration_ms, avg_task_duration_ms,
       duration_skew_ratio, cpu_efficiency, gc_overhead_ratio,
       total_bytes_written, avg_output_bytes_per_task
FROM spark_stage_skew WHERE app_id = '$app_id' ORDER BY stage_id;
```
