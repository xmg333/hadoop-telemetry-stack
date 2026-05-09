# Performance Anomaly and Bottleneck Analysis

## Overview
This dashboard approaches analysis from a performance diagnostics perspective, helping answer the following questions:
- Which Stages have the longest execution times?
- Is the GC overhead ratio abnormal?
- Are there data skew issues?
- What is the overall CPU efficiency?

## Prerequisites
- Data sources: `metric_events` wide table, `stage_governance` table (written by Flink Consumer)
- Grafana variables: `$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`
- The `stage_governance` table is automatically computed and written by the Flink Consumer upon Stage completion, containing pre-aggregated metrics such as skew ratios and efficiency scores.

## Panel Descriptions

### Slowest Stage TOP 20 (table)

**Purpose**: Lists the 20 Stages with the longest execution times, helping identify performance bottleneck Stages and applications.

**SQL Query**:
```sql
SELECT app_id,
       stage_id,
       ROUND(duration_ms / 1000, 1)        AS duration_sec,
       ROUND(num_tasks)                     AS num_tasks,
       ROUND(executor_run_time_ms / 1000, 1) AS executor_run_sec,
       ROUND(jvm_gc_time_ms / 1000, 1)     AS gc_sec,
       ROUND(peak_execution_memory_bytes / 1073741824, 2) AS peak_mem_gb,
       ROUND(io_bytes_read / 1073741824, 2) AS read_gb,
       ROUND(io_bytes_written / 1073741824, 2) AS write_gb
FROM   metric_events
WHERE  event_type = 'STAGE'
       AND duration_ms IS NOT NULL
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
ORDER  BY duration_ms DESC
LIMIT  20
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `app_id` | Spark application ID | - |
| `stage_id` | Stage number | - |
| `duration_sec` | Stage execution duration | seconds |
| `num_tasks` | Total number of tasks in the Stage | count |
| `executor_run_sec` | Cumulative Executor runtime | seconds |
| `gc_sec` | Cumulative GC time | seconds |
| `peak_mem_gb` | Peak execution memory | GB |
| `read_gb` | Data read volume | GB |
| `write_gb` | Data written volume | GB |

**Usage**: Prioritize Stages where `duration_sec` is significantly larger than `executor_run_sec / num_tasks`, as this indicates scheduling delay or task skew. When `gc_sec` accounts for a high proportion, consider increasing Executor memory or optimizing data structures.

### GC Overhead Ratio Trend (timeseries)

**Purpose**: Shows the trend of GC time as a proportion of Executor runtime for each application, identifying GC bottlenecks.

**SQL Query**:
```sql
SELECT ( Floor(timestamp_ms / $__interval_ms) * $__interval_ms / 1000 ) AS time,
       app_id,
       ROUND(SUM(jvm_gc_time_ms) * 100.0 / NULLIF(SUM(executor_run_time_ms), 0), 1) AS gc_ratio_pct
FROM   metric_events
WHERE  event_type = 'TASK'
       AND executor_run_time_ms > 0
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY 1,
          app_id
HAVING gc_ratio_pct IS NOT NULL
ORDER  BY 1,
          2
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `time` | Time bucket | Unix timestamp |
| `app_id` | Spark application ID | - |
| `gc_ratio_pct` | GC time as percentage of execution time | % |

**Usage**: A GC ratio exceeding 10% is abnormal; exceeding 20% severely impacts performance. Sudden GC spikes are usually related to increased data volume or improper memory configuration. Cross-reference with the `jvm_gc_metrics` table to distinguish between Full GC and Minor GC.

### Data Skew Detection (table)

**Purpose**: Retrieves Stages with severe data skew from the `stage_governance` table, displaying key metrics such as duration skew ratio and IO skew ratio.

**SQL Query**:
```sql
SELECT app_id,
       stage_id,
       task_count,
       ROUND(stage_duration_ms / 1000, 1)    AS stage_duration_sec,
       ROUND(avg_task_duration_ms / 1000, 1) AS avg_task_sec,
       ROUND(max_task_duration_ms / 1000, 1) AS max_task_sec,
       ROUND(duration_skew_ratio, 2)         AS duration_skew,
       ROUND(io_read_skew_ratio, 2)          AS io_read_skew,
       ROUND(shuffle_read_skew_ratio, 2)     AS shuffle_skew,
       ROUND(gc_overhead_ratio * 100, 1)     AS gc_pct,
       ROUND(cpu_efficiency * 100, 1)        AS cpu_eff_pct
FROM   stage_governance
WHERE  ( duration_skew_ratio > 2.0
          OR io_read_skew_ratio > 3.0
          OR shuffle_read_skew_ratio > 3.0 )
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
ORDER  BY duration_skew_ratio DESC
LIMIT  20
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `app_id` | Spark application ID | - |
| `stage_id` | Stage number | - |
| `task_count` | Number of tasks in the Stage | count |
| `stage_duration_sec` | Stage total duration | seconds |
| `avg_task_sec` | Average task duration | seconds |
| `max_task_sec` | Maximum task duration | seconds |
| `duration_skew` | Duration skew ratio (max/avg), >2 indicates skew | multiplier |
| `io_read_skew` | Read IO skew ratio (max/avg), >3 indicates severe skew | multiplier |
| `shuffle_skew` | Shuffle read skew ratio | multiplier |
| `gc_pct` | GC overhead ratio | % |
| `cpu_eff_pct` | CPU efficiency | % |

**Usage**: A `duration_skew` greater than 2 indicates skew; greater than 5 indicates severe skew. Mitigate by increasing partitions, using `repartition`, or enabling Spark AQE's `skewJoin` optimization. High `shuffle_skew` requires checking the distribution of Join keys.

### Skewed Stage Count (stat)

**Purpose**: Counts the total number of data-skewed Stages detected within the current time range, providing a quick overview.

**SQL Query**:
```sql
SELECT COUNT(*) AS value
FROM   stage_governance
WHERE  ( duration_skew_ratio > 2.0
          OR io_read_skew_ratio > 3.0
          OR shuffle_read_skew_ratio > 3.0 )
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `value` | Number of skewed Stages | count |

**Usage**: Use with Grafana threshold colors: 0 green, 1-3 yellow, >3 red. No further investigation is needed when the value is 0.

### Average CPU Efficiency (stat)

**Purpose**: Displays the average CPU efficiency score across all Stages, measuring overall resource utilization quality.

**SQL Query**:
```sql
SELECT ROUND(AVG(cpu_efficiency) * 100, 1) AS value
FROM   stage_governance
WHERE  cpu_efficiency IS NOT NULL
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `value` | Average CPU efficiency | % |

**Usage**: Pay attention when CPU efficiency is below 60%, as it may indicate IO wait, scheduling delay, or data skew. Above 80% is considered good. This metric comes from `stage_governance.cpu_efficiency`, calculated as `cpu_time_ms / executor_run_time_ms`.

## Notes
- Data in the `stage_governance` table is computed by the Flink Consumer after Stage completion, introducing latency. Stages that are still running will not appear in the skew detection results.
- Skew thresholds (`duration_skew_ratio > 2.0`, `io_read_skew_ratio > 3.0`) are empirical values and can be adjusted based on business characteristics.
- In the GC trend panel, when there are many `app_id` values, the graph lines may become too dense. Filter by specific applications using Grafana variables.
