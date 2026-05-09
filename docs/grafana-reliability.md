# Reliability and Failure Analysis

## Overview
This dashboard approaches analysis from a task reliability perspective, helping answer the following questions:
- What are the task success rate trends for each engine?
- What are the most recent failure events?
- Is GC correlated with failures?
- Is the speculative task ratio abnormal?
- What is the failure distribution for Hive queries?

## Prerequisites
- Data sources: `metric_events` wide table, `hive_query_metrics` table (written by Flink Consumer)
- Grafana variables: `$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`
- Failure events are determined by the `status` field: Spark Task `task_success` = `false`, MR `state` != `SUCCEEDED`, Hive `success` = `false`

## Panel Descriptions

### Cross-Engine Success Rate Trend (timeseries)

**Purpose**: Shows the task/job success rate trend over time for each engine, enabling rapid detection of abnormal declines.

**SQL Query**:
```sql
SELECT ( Floor(timestamp_ms / $__interval_ms) * $__interval_ms / 1000 ) AS time,
       engine,
       ROUND(SUM(CASE
                   WHEN status = 'true' THEN 1
                   ELSE 0
                 END) * 100.0 / NULLIF(COUNT(*), 0), 1)                     AS success_rate_pct
FROM   metric_events
WHERE  event_type IN ( 'TASK', 'MR_TASK', 'HIVE_QUERY' )
       AND status IS NOT NULL
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY 1,
          engine
HAVING success_rate_pct IS NOT NULL
ORDER  BY 1,
          2
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `time` | Time bucket | Unix timestamp |
| `engine` | Execution engine (SPARK / MR / HIVE) | - |
| `success_rate_pct` | Success rate | % |

**Usage**: A success rate below 95% should trigger an alert. Short-term declines may be caused by a single job failure, while long-term declines require investigation of the cluster environment. Review MR and Spark success rates separately; MR is typically higher than Spark (due to different retry mechanisms).

### Recent Failure Events (table)

**Purpose**: Lists details of recent failure events, including application, user, time, and engine type, enabling rapid problem localization.

**SQL Query**:
```sql
SELECT FROM_UNIXTIME(timestamp_ms / 1000) AS event_time,
       engine,
       event_type,
       app_id,
       app_name,
       user_name,
       queue,
       ROUND(duration_ms / 1000, 1)        AS duration_sec
FROM   metric_events
WHERE  status = 'false'
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
ORDER  BY timestamp_ms DESC
LIMIT  50
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `event_time` | Event occurrence time (UTC) | - |
| `engine` | Execution engine | - |
| `event_type` | Event type (TASK / MR_TASK / HIVE_QUERY) | - |
| `app_id` | Application/job/query ID | - |
| `app_name` | Application name | - |
| `user_name` | Username | - |
| `queue` | Queue | - |
| `duration_sec` | Execution duration (before failure) | seconds |

**Usage**: Repeated failures for the same `app_id` indicate a problem with the application itself. A `duration_sec` of 0 may indicate a startup failure, while a larger value suggests a runtime error. Prioritize users and applications with the highest failure frequency.

### GC and Failure Correlation Analysis (barchart)

**Purpose**: Compares GC overhead distribution between successful and failed tasks to verify whether GC is a contributing factor to failures.

**SQL Query**:
```sql
SELECT CASE
         WHEN status = 'true' THEN 'SUCCESS'
         ELSE 'FAILURE'
       END                                        AS outcome,
       CASE
         WHEN gc_time_ms / NULLIF(duration_ms, 0) < 0.05 THEN '< 5%'
         WHEN gc_time_ms / NULLIF(duration_ms, 0) < 0.10 THEN '5-10%'
         WHEN gc_time_ms / NULLIF(duration_ms, 0) < 0.20 THEN '10-20%'
         ELSE '> 20%'
       END                                        AS gc_bucket,
       COUNT(*)                                   AS count
FROM   metric_events
WHERE  event_type IN ( 'TASK', 'MR_TASK' )
       AND gc_time_ms IS NOT NULL
       AND duration_ms > 0
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY 1,
          2
ORDER  BY 1,
          2
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `outcome` | Task outcome (SUCCESS / FAILURE) | - |
| `gc_bucket` | GC time as percentage of execution time range | - |
| `count` | Number of events in this combination | count |

**Usage**: If the proportion of FAILURE events with high GC ratio (>20%) is significantly higher than SUCCESS events, GC is indeed a major factor contributing to failures, and memory configuration should be optimized.

### Speculative Task Rate (stat)

**Purpose**: Calculates the proportion of Spark speculative execution tasks to evaluate whether there is a slow task problem.

**SQL Query**:
```sql
SELECT ROUND(SUM(CASE
                   WHEN task_speculative = 'true' THEN 1
                   ELSE 0
                 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS value
FROM   metric_events
WHERE  event_type = 'TASK'
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `value` | Speculative task ratio | % |

**Usage**: A speculative rate greater than 5% indicates a significant slow task problem, usually caused by data skew, node failures, or IO hotspots. Investigate further using the data skew panel. This metric applies only to Spark (MR has no speculative execution mechanism).

### Hive Operation Failure Rate (barchart)

**Purpose**: Displays the failure rate distribution by Hive operation type (SELECT / INSERT / CREATE, etc.).

**SQL Query**:
```sql
SELECT operation,
       SUM(CASE
             WHEN success = 'false' THEN 1
             ELSE 0
           END)                                   AS fail_count,
       COUNT(*)                                   AS total_count,
       ROUND(SUM(CASE
                   WHEN success = 'false' THEN 1
                   ELSE 0
                 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS fail_rate_pct
FROM   hive_query_metrics
WHERE  timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY operation
ORDER  BY fail_rate_pct DESC
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `operation` | Hive operation type | - |
| `fail_count` | Number of failures | count |
| `total_count` | Total count | count |
| `fail_rate_pct` | Failure rate | % |

**Usage**: A high failure rate for INSERT operations may be due to insufficient target table permissions or space. DDL operation failures are usually related to syntax errors or metadata conflicts. This panel queries the `hive_query_metrics` physical table directly.

## Notes
- The `status` field in `metric_events` is a standardized field: Spark uses `"true"` / `"false"`, MR is mapped from the `state` field (`SUCCEEDED` -> `"true"`), and Hive is mapped from the `success` field.
- MR Job failures are represented by the `mr_job_metrics.state` field, which is not equivalent to Task-level failures.
- The speculative task rate depends on the `task_speculative` field, which requires Flink Consumer v4+ version support.
- The Hive failure analysis panel queries the `hive_query_metrics` physical table and is not affected by the `metric_events` view.
