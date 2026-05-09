# Spark Engine Deep Dive

## Overview
This dashboard provides a foundational deep-dive view of the Spark engine, offering the most comprehensive set of Spark metrics. It helps answer the following questions:
- How many Tasks, Stages, and Jobs does the current application have?
- Are there any data-skewed Stages?
- What is the CPU efficiency?
- How are Task IO, Shuffle, and memory usage performing?
- Are JVM memory and GC healthy?
- What are the SQL query and table IO details?

## Prerequisites

Data sources: `task_metrics`, `stage_metrics`, `job_metrics`, `jvm_memory_metrics`, `jvm_gc_metrics`, `sql_query_metrics`, `sql_query_table_metrics`, `stage_governance`, `task_histogram_buckets`

Grafana variables:
:   `$app_id` — Spark application ID (multi-select, includes All)
:   `$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`

Variable query: `SELECT DISTINCT app_id FROM task_metrics ORDER BY app_id`

## Panel Descriptions

### Overview Statistics (4 stat panels, y=0)

#### Total Tasks (stat)

**Purpose**: Displays the total number of Tasks for the selected application.

**SQL Query**:
```sql
SELECT COUNT(DISTINCT task_id) AS value
FROM task_metrics
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
  AND app_id IN ($app_id)
```

#### Total Stages (stat)

**Purpose**: Displays the total number of Stages for the selected application.

**SQL Query**:
```sql
SELECT COUNT(DISTINCT stage_id) AS value
FROM stage_metrics
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
  AND app_id IN ($app_id)
```

#### Skewed Stages (stat)

**Purpose**: Detects the number of data-skewed Stages (`duration_skew_ratio > 2`). Thresholds: 0 green, 1+ yellow, 5+ red.

**SQL Query**:
```sql
SELECT COUNT(DISTINCT stage_id) AS value
FROM stage_governance
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
  AND app_id IN ($app_id)
  AND duration_skew_ratio > 2
```

**Usage**: If the number of skewed Stages > 0, check the Data Skew Detection panel for details and consider repartitioning or increasing the number of partitions.

#### Avg CPU Efficiency (stat)

**Purpose**: Displays the average CPU efficiency across all Stages (CPU time / execution time). Thresholds: <0.3 red, 0.3-0.5 orange, 0.5-0.7 yellow, >0.7 green.

**SQL Query**:
```sql
SELECT AVG(cpu_efficiency) AS value
FROM stage_governance
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
  AND app_id IN ($app_id)
  AND cpu_efficiency IS NOT NULL
```

**Usage**: CPU efficiency below 0.5 indicates tasks spend most of their time waiting for IO or GC. Check the Shuffle and GC panels.

### Task IO and Duration Trends (y=4)

#### Task I/O Bytes (timeseries)

**Purpose**: Displays 4 time-series lines: Bytes Read, Bytes Written, Shuffle Read, Shuffle Written over time.

**SQL Query**:
```sql
-- 4 targets querying io_bytes_read, io_bytes_written, shuffle_bytes_read, shuffle_bytes_written respectively
-- FROM task_metrics WHERE ... AND app_id IN ($app_id) AND duration_ms IS NOT NULL
```

**Usage**: High and continuously growing Shuffle data volume may indicate poor Join strategies or excessive partitioning.

#### Task Duration (timeseries)

**Purpose**: Displays three trend lines for AVG / MAX / MIN Task duration.

**SQL Query**:
```sql
-- 3 targets: AVG(duration_ms), MAX(duration_ms), MIN(duration_ms)
-- FROM task_metrics WHERE ... AND app_id IN ($app_id) AND duration_ms IS NOT NULL
```

**Usage**: A large gap between MAX and AVG indicates the presence of long-tail Tasks, which may be caused by data skew or resource contention.

### JVM Monitoring (y=12)

#### JVM Memory (timeseries)

**Purpose**: Shows the average memory usage trends for Heap Used and Non-Heap Used.

**SQL Query**:
```sql
-- AVG(heap_used), AVG(non_heap_used) FROM jvm_memory_metrics WHERE app_id IN ($app_id)
```

**Usage**: Continuously growing heap that does not decrease may indicate a memory leak. Abnormally growing non-heap memory should be investigated for Metaspace or thread stack issues.

#### JVM GC (timeseries)

**Purpose**: Shows GC Count and GC Time trends. GC Count is on the right axis, GC Time is on the left axis (ms).

**SQL Query**:
```sql
-- SUM(gc_count), SUM(gc_time_ms) FROM jvm_gc_metrics WHERE app_id IN ($app_id)
```

**Usage**: If GC Time accounts for more than 10% of Task Duration, optimize JVM parameters (increase heap or switch GC algorithm).

### Stage and Job (y=20)

#### Stage Duration & Task Count (timeseries)

**Purpose**: Dual-axis chart showing average Stage duration and average Task count.

**SQL Query**:
```sql
-- AVG(duration_ms) AS 'Stage Duration', AVG(num_tasks) AS 'Task Count'
-- FROM stage_metrics WHERE app_id IN ($app_id)
```

#### Job Overview (table)

**Purpose**: Displays a list of Jobs and their completion status.

**SQL Query**:
```sql
SELECT app_id, job_id, job_success, duration_ms, num_stages,
       FROM_UNIXTIME(timestamp_ms/1000) AS completed_at
FROM job_metrics
WHERE ... AND app_id IN ($app_id)
ORDER BY timestamp_ms DESC LIMIT 100
```

**Column Descriptions**:

| Column | Description |
|--------|-------------|
| `job_success` | Job status (SUCCESS green / FAILED red) |
| `duration_ms` | Job duration (ms) |
| `num_stages` | Number of Stages |

### Governance Analysis (y=28)

#### Data Skew Detection (table)

**Purpose**: Displays a list of Stages with data skew, sorted by `duration_skew_ratio` in descending order. The skew ratio is color-coded (>1.5 yellow, >2 red).

**SQL Query**:
```sql
SELECT app_id, stage_id, task_count, stage_duration_ms,
       avg_task_duration_ms, max_task_duration_ms, min_task_duration_ms,
       duration_skew_ratio, io_read_skew_ratio, io_write_skew_ratio,
       shuffle_read_skew_ratio
FROM stage_governance WHERE app_id IN ($app_id)
ORDER BY timestamp_ms DESC LIMIT 50
```

**Usage**: `duration_skew_ratio > 2` indicates that some Tasks have durations far exceeding the average. `io_read_skew_ratio > 2` indicates uneven data reads. Consider using `repartition()` or `coalesce()` to adjust.

#### Resource Efficiency (table)

**Purpose**: Displays resource usage metrics for each Stage, including CPU efficiency, GC overhead, Shuffle wait, Spill ratio, etc. Key metrics are color-coded.

**SQL Query**:
```sql
SELECT app_id, stage_id, task_count, cpu_efficiency, gc_overhead_ratio,
       shuffle_wait_ratio, spill_ratio, deserialize_overhead, scheduler_delay_ratio
FROM stage_governance WHERE app_id IN ($app_id)
ORDER BY timestamp_ms DESC LIMIT 50
```

**Key Metric Thresholds**:
| Metric | Yellow | Red |
|--------|--------|-----|
| `cpu_efficiency` | 0.5 | <0.5 |
| `gc_overhead_ratio` | 0.05 | >0.1 |
| `shuffle_wait_ratio` | 0.05 | >0.1 |
| `spill_ratio` | 0.1 | >0.3 |

### Histograms and Small Files (y=36)

#### Task Duration Histogram (barchart)

**Purpose**: Displays the distribution histogram of Task durations, with the X-axis showing bucket upper bounds (ms) and the Y-axis showing counts.

**SQL Query**:
```sql
SELECT ROUND(bucket_le) AS 'Bucket LE (ms)', SUM(bucket_count) AS 'Count'
FROM task_histogram_buckets
WHERE metric_name = 'spark.task.duration_ms' AND app_id IN ($app_id)
GROUP BY bucket_le ORDER BY bucket_le
```

**Usage**: If a large number of Tasks are concentrated in low-duration ranges but a small number are in high ranges, this indicates a long-tail problem.

#### Small File Detection (table)

**Purpose**: Detects small file issues. When `avg_output_bytes_per_task < 32MB`, there may be a small file problem.

**SQL Query**:
```sql
SELECT app_id, stage_id, task_count, total_bytes_read, total_bytes_written,
       total_shuffle_bytes_read, total_shuffle_bytes_written,
       total_records_read, total_records_written, avg_output_bytes_per_task,
       avg_output_records_per_task, small_output_task_count
FROM stage_governance WHERE app_id IN ($app_id)
ORDER BY timestamp_ms DESC LIMIT 50
```

**Usage**: When `small_output_task_count > 0`, consider reducing the number of partitions or merging output files.

### Task Details (y=44)

#### Task Detail (table, full width)

**Purpose**: Displays complete metric details for the most recent 200 Tasks, including IO, duration, CPU, GC, Spill, etc.

**SQL Query**:
```sql
SELECT FROM_UNIXTIME(timestamp_ms/1000) AS time, app_id, executor_id, stage_id,
       task_id, task_success, duration_ms, io_bytes_read, io_bytes_written,
       shuffle_bytes_read, shuffle_bytes_written, shuffle_fetch_wait_time_ms,
       executor_run_time_ms, executor_cpu_time_ns, jvm_gc_time_ms,
       disk_bytes_spilled, memory_bytes_spilled
FROM task_metrics WHERE app_id IN ($app_id) AND duration_ms IS NOT NULL
ORDER BY timestamp_ms DESC LIMIT 200
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `task_success` | OK (green) / FAIL (red) | - |
| `executor_cpu_time_ns` | Executor CPU time | ns |
| `jvm_gc_time_ms` | JVM GC duration | ms |
| `disk_bytes_spilled` | Disk spill | bytes |
| `memory_bytes_spilled` | Memory spill | bytes |

### SQL Query Analysis (y=52-72)

#### SQL Queries / Avg SQL Query Duration / Total SQL Joins / SQL Table Scans/Writes (4 stats)

**Purpose**: SQL query overview statistics, showing query count, average duration, total Join count, and table scan/write counts respectively.

#### SQL Query Duration (timeseries)

**Purpose**: Average and maximum duration trends for SQL queries.

#### SQL Shuffle Bytes (timeseries)

**Purpose**: Shuffle read and write data volume trends for SQL queries.

#### SQL Table IO Detail (table, full width)

**Purpose**: Displays the table IO details for each SQL query (table name, operation type, bytes, rows, file count).

**SQL Query**:
```sql
SELECT FROM_UNIXTIME(timestamp_ms/1000) AS time, app_id, execution_id,
       table_name, operation, bytes, `rows`, files_read, time_ms
FROM sql_query_table_metrics WHERE app_id IN ($app_id)
ORDER BY timestamp_ms DESC LIMIT 200
```

#### SQL Query Detail (table, full width)

**Purpose**: Displays query-level metrics: duration, Shuffle, Join count.

**SQL Query**:
```sql
SELECT FROM_UNIXTIME(timestamp_ms/1000) AS time, app_id, execution_id,
       duration_ms, shuffle_bytes_read, shuffle_bytes_written, join_count
FROM sql_query_metrics WHERE app_id IN ($app_id)
ORDER BY timestamp_ms DESC LIMIT 100
```

## Navigation
The top navigation bar provides quick access to:
- **Overview** — Platform overview
- **MapReduce** — MR engine detailed view
- **Hive on MR** / **Hive on Spark** — Hive query analysis
- **Spark / MR / Hive** — All-engine consolidated dashboard

## Notes
- The `$app_id` variable is sourced from the `task_metrics` table and only includes applications with Task events
- Data in `stage_governance` and `task_histogram_buckets` depends on Stage completion; short-lived applications may have no data
- JVM panel data comes from the Executor Plugin; ensure `spark.telemetry.metrics.stage.detailed=true` is configured
