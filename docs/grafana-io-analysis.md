# Data Throughput and IO Analysis

## Overview
This dashboard approaches analysis from a data IO perspective, helping answer the following questions:
- What are the data read/write throughput trends for each engine?
- Which applications/Stages have the largest Shuffle data volume?
- Are there Spill (memory overflow) issues?
- Which tables have the most frequent IO?

## Prerequisites
- Data sources: `metric_events` wide table, `sql_query_table_metrics` table (written by Flink Consumer)
- Grafana variables: `$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`

## Panel Descriptions

### Cross-Engine Data Throughput Trend (timeseries)

**Purpose**: Shows IO throughput trends over time for each engine (SPARK / MR / HIVE), helping understand cluster data flow patterns.

**SQL Query**:
```sql
SELECT ( Floor(timestamp_ms / $__interval_ms) * $__interval_ms / 1000 ) AS time,
       engine,
       SUM(io_bytes_read) / 1073741824                                    AS read_gb,
       SUM(io_bytes_written) / 1073741824                                 AS write_gb
FROM   metric_events
WHERE  event_type IN ( 'TASK', 'MR_TASK', 'HIVE_QUERY' )
       AND ( io_bytes_read > 0 OR io_bytes_written > 0 )
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY 1,
          engine
ORDER  BY 1,
          2
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `time` | Time bucket | Unix timestamp |
| `engine` | Execution engine (SPARK / MR / HIVE) | - |
| `read_gb` | Total data read during this period | GB |
| `write_gb` | Total data written during this period | GB |

**Usage**: A sudden drop in throughput may indicate that upstream data sources are unavailable or task scheduling is abnormal. Compare the IO patterns of different engines to determine whether some MR jobs could be migrated to Spark for better IO efficiency.

### Shuffle Data Volume Ranking (table)

**Purpose**: Lists the top 15 applications by Shuffle read/write data volume to identify Shuffle bottlenecks.

**SQL Query**:
```sql
SELECT app_id,
       app_name,
       ROUND(SUM(shuffle_bytes_read) / 1073741824, 2)  AS shuffle_read_gb,
       ROUND(SUM(shuffle_bytes_written) / 1073741824, 2) AS shuffle_write_gb,
       ROUND(SUM(shuffle_bytes_read + shuffle_bytes_written) / 1073741824, 2) AS shuffle_total_gb,
       ROUND(SUM(shuffle_fetch_wait_time_ms) / 1000, 1) AS shuffle_wait_sec,
       COUNT(*)                                        AS task_count
FROM   metric_events
WHERE  event_type = 'TASK'
       AND ( shuffle_bytes_read > 0 OR shuffle_bytes_written > 0 )
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY app_id,
          app_name
ORDER  BY shuffle_total_gb DESC
LIMIT  15
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `app_id` | Spark application ID | - |
| `app_name` | Application name | - |
| `shuffle_read_gb` | Total Shuffle read | GB |
| `shuffle_write_gb` | Total Shuffle write | GB |
| `shuffle_total_gb` | Total Shuffle data volume | GB |
| `shuffle_wait_sec` | Total Shuffle wait time | seconds |
| `task_count` | Number of tasks | count |

**Usage**: Applications with excessive Shuffle data should consider optimizing Join strategies (e.g., Broadcast Join), increasing partitions, or using Bucket tables. High `shuffle_wait_sec` indicates that network or disk IO has become a bottleneck.

### Spill Analysis (timeseries)

**Purpose**: Shows the memory spill trend for each application, detecting problems caused by insufficient memory configuration.

**SQL Query**:
```sql
SELECT ( Floor(timestamp_ms / $__interval_ms) * $__interval_ms / 1000 ) AS time,
       app_id,
       ROUND(SUM(memory_bytes_spilled) / 1073741824, 2)                    AS spill_gb,
       ROUND(SUM(disk_bytes_spilled) / 1073741824, 2)                      AS disk_spill_gb
FROM   metric_events
WHERE  event_type = 'TASK'
       AND ( memory_bytes_spilled > 0 OR disk_bytes_spilled > 0 )
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY 1,
          app_id
ORDER  BY 1,
          2
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `time` | Time bucket | Unix timestamp |
| `app_id` | Spark application ID | - |
| `spill_gb` | Total memory Spill (including memory and disk spill) | GB |
| `disk_spill_gb` | Total disk Spill | GB |

**Usage**: Large Spill volumes indicate that Executor memory is insufficient to hold all Shuffle data. Increase `spark.executor.memory`, adjust `spark.memory.fraction`, or increase the number of partitions to reduce per-task data volume. Persistent Spill leads to severe disk IO and performance degradation.

### Hot Table IO Analysis (table)

**Purpose**: Based on SQL Table IO data, identifies the tables with the highest read/write frequency and data volume, pinpointing hot data sources.

**SQL Query**:
```sql
SELECT table_name,
       operation,
       COUNT(*)                                          AS access_count,
       ROUND(SUM(bytes) / 1073741824, 2)                 AS total_gb,
       ROUND(SUM(rows) / 1000000, 2)                     AS total_million_rows,
       ROUND(AVG(time_ms), 0)                            AS avg_scan_ms
FROM   sql_query_table_metrics
WHERE  timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY table_name,
          operation
ORDER  BY total_gb DESC
LIMIT  20
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `table_name` | Table name (full database.table) | - |
| `operation` | Operation type (SCAN / WRITE, etc.) | - |
| `access_count` | Number of times accessed | count |
| `total_gb` | Cumulative IO data volume | GB |
| `total_million_rows` | Cumulative rows processed | million rows |
| `avg_scan_ms` | Average scan duration | milliseconds |

**Usage**: Tables with high access frequency and high `avg_scan_ms` are suitable candidates for partitioning optimization or caching. Tables with abnormally large write volumes should be checked for small file issues. This panel only includes Spark SQL table IO data (collected via `SQL_TABLE_IO` events).

## Notes
- IO fields (`io_bytes_read`, `io_bytes_written`) are standardized fields: for Spark they come from Task metrics, for MR they are aggregated from `hdfs_bytes_read + file_bytes_read`, and for Hive they come from `input_bytes / output_bytes`.
- Shuffle metrics apply only to Spark (MR Shuffle is represented by `reduce_shuffle_bytes` at the MR_JOB level).
- The Hot Table IO panel depends on Spark SQL `SQL_TABLE_IO` events; enable the `spark.telemetry.metrics.sql` related configuration.
- For ClickHouse data sources, the `` `rows` `` column in `sql_query_table_metrics` is a reserved word and must be wrapped in backticks.
