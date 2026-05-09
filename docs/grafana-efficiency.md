# Comprehensive Efficiency Score

## Overview
This dashboard approaches analysis from an overall efficiency evaluation perspective, helping answer the following questions:
- What are the resource efficiency scores for each application?
- Are there significant efficiency differences between queues?
- How severe is the data skew problem at a global scale?

## Prerequisites
- Data sources: `metric_events` wide table, `stage_governance` table (written by Flink Consumer)
- Grafana variables: `$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`
- Efficiency metrics in the `stage_governance` table (`cpu_efficiency`, `gc_overhead_ratio`, etc.) are automatically computed by the Flink Consumer upon Stage completion.

## Panel Descriptions

### Application Efficiency Score (table)

**Purpose**: Calculates an efficiency score for each application by combining metrics across multiple dimensions, enabling rapid identification of inefficient applications.

**SQL Query**:
```sql
SELECT app_id,
       app_name,
       engine,
       COUNT(DISTINCT CASE
                        WHEN event_type = 'STAGE' THEN stage_id
                        ELSE NULL
                      END)                                                AS stage_count,
       COUNT(*)                                                          AS task_count,
       ROUND(SUM(cpu_time_ms) / 3600000, 2)                              AS cpu_hours,
       ROUND(AVG(CASE
                   WHEN executor_run_time_ms > 0
                        THEN executor_cpu_time_ns / 1000000.0 / executor_run_time_ms
                   ELSE NULL
                 END) * 100, 1)                                           AS avg_cpu_eff_pct,
       ROUND(AVG(CASE
                   WHEN duration_ms > 0 THEN jvm_gc_time_ms / duration_ms
                   ELSE NULL
                 END) * 100, 1)                                           AS avg_gc_pct,
       ROUND(SUM(memory_bytes_spilled) / NULLIF(SUM(io_bytes_read), 0) * 100, 1) AS spill_ratio_pct,
       ROUND(SUM(cpu_time_ms) / NULLIF(SUM(duration_ms), 0) * 100, 1)    AS overall_eff_pct
FROM   metric_events
WHERE  event_type IN ( 'TASK', 'MR_TASK' )
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY app_id,
          app_name,
          engine
ORDER  BY overall_eff_pct ASC
LIMIT  20
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `app_id` | Application/job ID | - |
| `app_name` | Application name | - |
| `engine` | Execution engine (SPARK / MR) | - |
| `stage_count` | Number of Stages (Spark only) | count |
| `task_count` | Total number of tasks | count |
| `cpu_hours` | Cumulative CPU time | hours |
| `avg_cpu_eff_pct` | Average CPU efficiency (CPU time / execution time) | % |
| `avg_gc_pct` | Average GC overhead ratio | % |
| `spill_ratio_pct` | Ratio of Spill data volume to read volume | % |
| `overall_eff_pct` | Comprehensive efficiency score (CPU time / wall clock time) | % |

**Usage**: Sorted by `overall_eff_pct` in ascending order; the top-ranked applications have the lowest efficiency and are optimization priorities. An `avg_cpu_eff_pct` below 50% indicates that CPU spends a significant amount of time waiting for IO or GC. A `spill_ratio_pct` greater than 10% indicates insufficient memory configuration. Since `overall_eff_pct` accounts for parallelism factors, applications below 30% should consider increasing parallelism.

### Queue Efficiency Comparison (table)

**Purpose**: Compares resource usage efficiency across YARN queues, evaluating the fairness of resource allocation between queues.

**SQL Query**:
```sql
SELECT queue,
       engine,
       COUNT(DISTINCT app_id)                                              AS app_count,
       ROUND(SUM(cpu_time_ms) / 3600000, 2)                                AS cpu_hours,
       ROUND(SUM(duration_ms) / 3600000, 2)                                AS wall_clock_hours,
       ROUND(SUM(cpu_time_ms) / NULLIF(SUM(duration_ms), 0) * 100, 1)      AS efficiency_pct,
       ROUND(SUM(gc_time_ms) / NULLIF(SUM(duration_ms), 0) * 100, 1)       AS gc_overhead_pct,
       ROUND(SUM(memory_bytes_spilled) / 1073741824, 2)                    AS total_spill_gb,
       ROUND(AVG(CASE
                   WHEN shuffle_fetch_wait_time_ms > 0
                        THEN shuffle_fetch_wait_time_ms / duration_ms
                   ELSE NULL
                 END) * 100, 1)                                            AS avg_shuffle_wait_pct
FROM   metric_events
WHERE  event_type IN ( 'TASK', 'MR_TASK' )
       AND queue IS NOT NULL
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY queue,
          engine
HAVING cpu_hours > 0
ORDER  BY efficiency_pct ASC
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `queue` | YARN queue name | - |
| `engine` | Execution engine | - |
| `app_count` | Number of applications in the queue | count |
| `cpu_hours` | Cumulative CPU time | hours |
| `wall_clock_hours` | Cumulative wall clock time | hours |
| `efficiency_pct` | Overall efficiency (CPU / wall clock time) | % |
| `gc_overhead_pct` | GC overhead ratio | % |
| `total_spill_gb` | Total Spill data volume | GB |
| `avg_shuffle_wait_pct` | Average Shuffle wait ratio | % |

**Usage**: The least efficient queues should be optimized first. If a high-resource queue also has low efficiency, it indicates that resources are over-allocated but underutilized; consider reducing the queue quota. Queues with high `avg_shuffle_wait_pct` may have network bottlenecks or overloaded Shuffle services.

### Data Skew Issue Summary (table)

**Purpose**: Summarizes all detected data skew issues, sorted by severity, providing a global view of the problem list.

**SQL Query**:
```sql
SELECT app_id,
       stage_id,
       task_count,
       ROUND(stage_duration_ms / 1000, 1)     AS stage_duration_sec,
       ROUND(duration_skew_ratio, 2)          AS duration_skew,
       ROUND(io_read_skew_ratio, 2)           AS io_read_skew,
       ROUND(shuffle_read_skew_ratio, 2)      AS shuffle_skew,
       ROUND(cpu_efficiency * 100, 1)          AS cpu_eff_pct,
       ROUND(gc_overhead_ratio * 100, 1)       AS gc_pct,
       ROUND(spill_ratio * 100, 1)             AS spill_pct,
       ROUND(shuffle_wait_ratio * 100, 1)      AS shuffle_wait_pct,
       ROUND(scheduler_delay_ratio * 100, 1)   AS scheduler_delay_pct,
       CASE
         WHEN duration_skew_ratio > 5.0 THEN 'CRITICAL'
         WHEN duration_skew_ratio > 3.0 THEN 'HIGH'
         WHEN duration_skew_ratio > 2.0 THEN 'MEDIUM'
         ELSE 'LOW'
       END                                     AS severity
FROM   stage_governance
WHERE  ( duration_skew_ratio > 2.0
          OR io_read_skew_ratio > 3.0
          OR shuffle_read_skew_ratio > 3.0 )
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
ORDER  BY duration_skew_ratio DESC
LIMIT  30
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `app_id` | Spark application ID | - |
| `stage_id` | Stage number | - |
| `task_count` | Number of tasks in the Stage | count |
| `stage_duration_sec` | Stage execution duration | seconds |
| `duration_skew` | Duration skew ratio (max/avg) | multiplier |
| `io_read_skew` | Read IO skew ratio | multiplier |
| `shuffle_skew` | Shuffle read skew ratio | multiplier |
| `cpu_eff_pct` | CPU efficiency for this Stage | % |
| `gc_pct` | GC overhead ratio | % |
| `spill_pct` | Spill ratio | % |
| `shuffle_wait_pct` | Shuffle wait ratio | % |
| `scheduler_delay_pct` | Scheduler delay ratio | % |
| `severity` | Severity level (CRITICAL / HIGH / MEDIUM / LOW) | - |

**Usage**: Prioritize CRITICAL and HIGH severity skew issues. The `severity` field is dynamically computed by the query and can be used as a basis for Grafana column styling (red/yellow/green). Combined metrics such as `cpu_eff_pct`, `gc_pct`, and `spill_pct` help determine the cascading impact of skew. High `scheduler_delay_pct` may be caused by task queuing due to insufficient Executor resources.

## Notes
- The comprehensive efficiency score (`overall_eff_pct`) is a simplified metric. Actual efficiency is influenced by multiple factors including parallelism and IO patterns. For high-parallelism applications, `overall_eff_pct` may exceed 100% (because multiple CPU cores work simultaneously).
- The queue efficiency panel depends on the `queue` field, which is written by the v4+ version of the Flink Consumer. The `queue` field for MR tasks comes from the YARN API, while for Spark tasks it comes from the `spark.yarn.queue` configuration.
- Data in the `stage_governance` table is computed by the Flink Consumer's `StageTaskAccumulator` after Stage completion. If a Stage has multiple batch flushes, governance records are only generated on the last batch (when the Stage completes).
- Skew severity thresholds (2.0 / 3.0 / 5.0) are empirical values and may need adjustment for specific business scenarios. Observe historical data over a period before determining appropriate thresholds.
