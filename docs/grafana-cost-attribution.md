# Cost Attribution and Resource Ranking

## Overview
This dashboard approaches resource analysis from a cost attribution perspective, helping answer the following questions:
- Which users consume the most CPU time?
- Which queues occupy the most computing resources?
- Which applications are the largest resource consumers?
- How does daily resource consumption trend over time?

## Prerequisites
- Data source: `metric_events` wide table (written by Flink Consumer)
- Grafana variables: `$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`
- Time range filter: All queries use `timestamp_ms >= ($__unixEpochFrom() * 1000) AND timestamp_ms <= ($__unixEpochTo() * 1000)`

## Panel Descriptions

### User CPU Time Ranking TOP 20 (table)

**Purpose**: Displays the top 20 users by CPU time consumption, helping identify resource-heavy users and supporting cost allocation and capacity planning.

**SQL Query**:
```sql
SELECT user_name,
       SUM(cpu_time_ms) / 3600000                                AS cpu_hours,
       SUM(gc_time_ms) / 3600000                                 AS gc_hours,
       SUM(duration_ms) / 3600000                                AS wall_clock_hours,
       COUNT(*)                                                  AS event_count
FROM   metric_events
WHERE  event_type IN ( 'TASK', 'MR_TASK' )
       AND cpu_time_ms IS NOT NULL
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY user_name
ORDER  BY cpu_hours DESC
LIMIT  20
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `user_name` | Username who submitted the task | - |
| `cpu_hours` | Cumulative CPU time | hours |
| `gc_hours` | Cumulative GC time | hours |
| `wall_clock_hours` | Cumulative wall clock time (task execution duration) | hours |
| `event_count` | Total number of events (Task level) | count |

**Usage**: Pay attention to the ratio of `cpu_hours` to `wall_clock_hours`. A lower ratio indicates lower CPU utilization, possibly due to IO wait or scheduling delay. If GC time exceeds 10%, check the JVM heap configuration.

### Queue Resource Consumption Comparison (barchart)

**Purpose**: Compares CPU time, IO data volume, and application count across different YARN queues to evaluate whether resource consumption is balanced.

**SQL Query**:
```sql
SELECT queue,
       engine,
       SUM(cpu_time_ms) / 3600000                                          AS cpu_hours,
       SUM(io_bytes_read + io_bytes_written) / 1073741824                  AS io_gb,
       COUNT(DISTINCT app_id)                                              AS app_count
FROM   metric_events
WHERE  event_type IN ( 'TASK', 'MR_JOB', 'MR_TASK' )
       AND queue IS NOT NULL
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY queue,
          engine
ORDER  BY cpu_hours DESC
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `queue` | YARN queue name | - |
| `engine` | Execution engine (SPARK / MR) | - |
| `cpu_hours` | Cumulative CPU time within the queue | hours |
| `io_gb` | Cumulative IO data volume within the queue | GB |
| `app_count` | Number of distinct applications/jobs within the queue | count |

**Usage**: If a queue has a very high CPU share but few applications, it indicates a concentrated resource-heavy application. Adjust queue weights or quotas to achieve more balanced resource allocation.

### Application Resource Consumption Ranking TOP 10 (table)

**Purpose**: Lists the top 10 applications by resource consumption, including CPU, GC, IO, Spill, and other full-dimension metrics, enabling quick identification of high-load applications.

**SQL Query**:
```sql
SELECT app_name,
       engine,
       SUM(cpu_time_ms) / 3600000                                AS cpu_hours,
       SUM(gc_time_ms) / 3600000                                 AS gc_hours,
       SUM(io_bytes_read) / 1073741824                           AS read_gb,
       SUM(io_bytes_written) / 1073741824                        AS write_gb,
       SUM(memory_bytes_spilled) / 1073741824                    AS spill_gb,
       COUNT(*)                                                  AS task_count
FROM   metric_events
WHERE  event_type IN ( 'TASK', 'MR_TASK' )
       AND app_name IS NOT NULL
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY app_name,
          engine
ORDER  BY cpu_hours DESC
LIMIT  10
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `app_name` | Application/job name | - |
| `engine` | Execution engine (SPARK / MR) | - |
| `cpu_hours` | Cumulative CPU time | hours |
| `gc_hours` | Cumulative GC time | hours |
| `read_gb` | Cumulative data read | GB |
| `write_gb` | Cumulative data written | GB |
| `spill_gb` | Cumulative Spill data volume (memory + disk) | GB |
| `task_count` | Total number of tasks | count |

**Usage**: A large `spill_gb` indicates insufficient memory. Increase Executor memory or adjust `spark.sql.shuffle.partitions`. Combining `cpu_hours` with `task_count` helps estimate the average CPU cost per task.

### Daily Resource Consumption Trend (timeseries)

**Purpose**: Shows the daily CPU consumption trend by engine (SPARK / MR / HIVE) over time, used for tracking resource usage changes.

**SQL Query**:
```sql
SELECT ( Floor(timestamp_ms / $__interval_ms) * $__interval_ms / 1000 ) AS time,
       engine,
       SUM(cpu_time_ms) / 3600000                                         AS cpu_hours
FROM   metric_events
WHERE  event_type IN ( 'TASK', 'MR_TASK', 'MR_JOB' )
       AND cpu_time_ms IS NOT NULL
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
| `time` | Time bucket (granularity controlled by `$__interval_ms`) | Unix timestamp |
| `engine` | Execution engine (SPARK / MR) | - |
| `cpu_hours` | CPU time within this time bucket | hours |

**Usage**: When selecting a time range greater than 7 days, set `$__interval_ms` to 86400000 (1 day) in Grafana for daily granularity. If a particular engine's curve suddenly rises, check for newly deployed high-load jobs.

## Notes
- `metric_events` is a unified wide table (physical table, not a view); data is simultaneously written to both the individual category tables and the wide table. To query category tables directly, replace `event_type` with the corresponding table name.
- `cpu_time_ms` for Spark is converted from `executor_cpu_time_ns` (nanoseconds to milliseconds), and for MR it comes from `CPU_MILLISECONDS` in MR Counters.
- The `queue` field depends on the v4+ version of the Flink Consumer. In earlier versions, this field is empty; run `v4_migration.sql` to upgrade.
- For ranking panels, it is recommended to set the time range to 1 to 7 days; larger time ranges will cause slow aggregation queries.
