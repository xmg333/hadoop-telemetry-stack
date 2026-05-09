# Capacity Planning and Resource Utilization

## Overview
This dashboard approaches analysis from a cluster capacity and resource utilization perspective, helping answer the following questions:
- How does the hourly task concurrency change?
- Is the Executor memory usage trend healthy?
- Is GC activity too frequent?
- Is the daily Job throughput stable?

## Prerequisites
- Data sources: `metric_events` wide table, `jvm_memory_metrics` table, `jvm_gc_metrics` table (written by Flink Consumer)
- Grafana variables: `$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`

## Panel Descriptions

### Hourly Task Concurrency (timeseries)

**Purpose**: Shows the number of tasks executed per hour for each engine, evaluating cluster concurrency load and peak periods.

**SQL Query**:
```sql
SELECT ( Floor(timestamp_ms / $__interval_ms) * $__interval_ms / 1000 ) AS time,
       engine,
       COUNT(*)                                                            AS task_count,
       COUNT(DISTINCT app_id)                                              AS app_count
FROM   metric_events
WHERE  event_type IN ( 'TASK', 'MR_TASK' )
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
| `engine` | Execution engine (SPARK / MR) | - |
| `task_count` | Number of tasks completed in this period | count |
| `app_count` | Number of active applications in this period | count |

**Usage**: The peaks and troughs of the task count curve reflect business cycles. If the task count during peak periods approaches the cluster capacity limit (checkable via YARN ResourceManager), consider scaling up or staggered scheduling. The ratio of `app_count` to `task_count` reflects application concurrency.

### Executor Memory Usage Trend (timeseries)

**Purpose**: Shows the heap and non-heap memory usage trends for each Executor, monitoring memory pressure.

**SQL Query**:
```sql
SELECT ( Floor(timestamp_ms / $__interval_ms) * $__interval_ms / 1000 ) AS time,
       app_id,
       executor_id,
       ROUND(AVG(heap_used) / 1073741824, 2)                              AS heap_gb,
       ROUND(AVG(non_heap_used) / 1073741824, 2)                           AS non_heap_gb
FROM   jvm_memory_metrics
WHERE  timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY 1,
          app_id,
          executor_id
ORDER  BY 1,
          2,
          3
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `time` | Time bucket | Unix timestamp |
| `app_id` | Spark application ID | - |
| `executor_id` | Executor number | - |
| `heap_gb` | Average heap memory usage | GB |
| `non_heap_gb` | Average non-heap memory usage | GB |

**Usage**: Continuously growing heap memory that does not decrease may indicate a memory leak. If heap memory is above 80% of the `spark.executor.memory` configured value, consider increasing memory or optimizing data structures. This panel queries the `jvm_memory_metrics` physical table directly.

### GC Activity Trend (timeseries)

**Purpose**: Shows the GC count and GC time trends for each application, identifying periods of excessive GC pressure.

**SQL Query**:
```sql
SELECT ( Floor(timestamp_ms / $__interval_ms) * $__interval_ms / 1000 ) AS time,
       app_id,
       gc_name,
       SUM(gc_count)                                                       AS total_gc_count,
       ROUND(SUM(gc_time_ms) / 1000, 1)                                    AS total_gc_sec
FROM   jvm_gc_metrics
WHERE  timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY 1,
          app_id,
          gc_name
ORDER  BY 1,
          2,
          3
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `time` | Time bucket | Unix timestamp |
| `app_id` | Spark application ID | - |
| `gc_name` | GC algorithm name (e.g., G1 Old Generation, ParNew, etc.) | - |
| `total_gc_count` | Total GC count during this period | count |
| `total_gc_sec` | Total GC duration during this period | seconds |

**Usage**: Distinguish between Old GC (Full GC) and Young GC (Minor GC). Frequent Full GC (more than 5 times per hour) indicates severe heap memory shortage. High-frequency but short-duration Young GC is normal. This panel queries the `jvm_gc_metrics` physical table directly.

### Daily Job Throughput (timeseries)

**Purpose**: Shows the daily completed job/query count trend for each engine, evaluating overall cluster throughput capacity.

**SQL Query**:
```sql
SELECT ( Floor(timestamp_ms / $__interval_ms) * $__interval_ms / 1000 ) AS time,
       engine,
       COUNT(DISTINCT app_id)                                              AS job_count,
       SUM(CASE
             WHEN status = 'true' THEN 1
             ELSE 0
           END)                                                            AS success_count,
       SUM(CASE
             WHEN status = 'false' THEN 1
             ELSE 0
           END)                                                            AS fail_count
FROM   metric_events
WHERE  event_type IN ( 'JOB', 'MR_JOB', 'HIVE_QUERY' )
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
| `job_count` | Number of distinct jobs/queries | count |
| `success_count` | Number of successes | count |
| `fail_count` | Number of failures | count |

**Usage**: Cyclical fluctuations in job count are normal, but a sustained decline requires checking whether upstream submissions are functioning correctly. Investigate further when `fail_count` is consistently above 0. Cross-engine comparison helps evaluate the effects of migration (e.g., throughput changes after migrating from MR to Spark).

## Notes
- JVM memory and GC metrics apply only to Spark (collected via `ExecutorPlugin`); MR tasks do not include this data.
- The data collection frequency for `jvm_memory_metrics` and `jvm_gc_metrics` depends on the OTel SDK's `export.interval.ms` configuration (default 60 seconds), not one record per task.
- In the memory trend panel, there may be many `executor_id` values; filter by specific applications using Grafana variables.
- When selecting a time range exceeding 7 days, set `$__interval_ms` to hourly (3600000) or daily (86400000) to avoid slow queries.
