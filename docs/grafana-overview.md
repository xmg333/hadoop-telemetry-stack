# Platform Overview

## Overview
This dashboard provides a global overview of the entire platform, displaying core metrics from all three engines (Spark / MR / Hive) at a glance. It helps answer the following questions:
- How many Spark applications, MR jobs, and Hive queries are currently running?
- Is the MR job success rate normal?
- How do the success rates compare across engines?
- How does IO throughput change over time?
- What are the average duration trends for Jobs/Queries?
- What are the most recent failed tasks?

## Prerequisites
- Data sources: 15 independent metric tables (written by Flink Consumer)
- Grafana variables: `$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`
- Time range filter: All queries use `timestamp_ms >= ($__unixEpochFrom() * 1000) AND timestamp_ms <= ($__unixEpochTo() * 1000)`

## Panel Descriptions

### Total Spark Apps (stat)

**Purpose**: Displays the number of distinct Spark applications within the time range.

**SQL Query**:
```sql
SELECT COUNT(DISTINCT app_id) AS value
FROM task_metrics
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
```

**Usage**: Under normal operation, the count should match the application submission frequency. If it suddenly drops to 0, check whether the Spark Plugin or OTel Collector is functioning properly.

### Total MR Jobs (stat)

**Purpose**: Displays the number of distinct completed MR jobs within the time range.

**SQL Query**:
```sql
SELECT COUNT(DISTINCT job_id) AS value
FROM mr_job_metrics
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
```

### Total Hive Queries (stat)

**Purpose**: Displays the total number of Hive queries within the time range.

**SQL Query**:
```sql
SELECT COUNT(*) AS value
FROM hive_query_metrics
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
```

### MR Job Success Rate (stat)

**Purpose**: Displays the MR job success rate as a percentage. Thresholds: <80% red, 80-95% yellow, >95% green.

**SQL Query**:
```sql
SELECT ROUND(
  SUM(CASE WHEN state='SUCCEEDED' THEN 1 ELSE 0 END) * 100.0
  / NULLIF(COUNT(DISTINCT job_id), 0), 1
) AS value
FROM mr_job_metrics
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
```

**Usage**: A success rate below 80% requires immediate investigation. Check the logs and resource allocation of failed jobs.

### Success Rates by Engine (piechart)

**Purpose**: Compares the distribution of success and failure counts across the three engines (Spark / MR / Hive) using a donut chart.

**SQL Query**:
```sql
-- Aggregate from job_metrics, mr_job_metrics, and hive_query_metrics
-- Count Spark OK/Fail, MR OK/Fail, Hive OK/Fail respectively
SELECT
  SUM(CASE WHEN job_success='true' THEN 1 ELSE 0 END) AS 'Spark OK',
  SUM(CASE WHEN job_success='false' AND duration_ms IS NULL ... END) AS 'Spark Fail',
  (SELECT COUNT(DISTINCT job_id) FROM mr_job_metrics WHERE state='SUCCEEDED' ...) AS 'MR OK',
  (SELECT COUNT(DISTINCT job_id) FROM mr_job_metrics WHERE state!='SUCCEEDED' ...) AS 'MR Fail',
  (SELECT COUNT(*) FROM hive_query_metrics WHERE success='true' ...) AS 'Hive OK',
  (SELECT COUNT(*) FROM hive_query_metrics WHERE success='false' ...) AS 'Hive Fail'
FROM job_metrics WHERE ...
```

**Usage**: Quickly identify which engine has stability issues. If a particular engine has a high failure ratio, drill down into its dedicated dashboard for further investigation.

### IO Throughput by Engine (timeseries)

**Purpose**: Shows IO throughput trends in bytes over time for each engine, including three lines for Spark, MapReduce, and Hive.

**SQL Query**:
```sql
-- Spark: SUM(io_bytes_read + io_bytes_written) FROM task_metrics
-- MapReduce: SUM(hdfs_bytes_read + hdfs_bytes_written) FROM mr_job_metrics
-- Hive: SUM(input_bytes + output_bytes) FROM hive_query_metrics
```

**Usage**: Observe whether IO peaks align with business cycles. If IO is abnormally high during certain periods, it may be related to data skew or large queries.

### Job Duration Trends (timeseries)

**Purpose**: Shows a trend comparison of average duration for Spark Jobs and MR Jobs over time.

**SQL Query**:
```sql
-- Spark: AVG(duration_ms) FROM job_metrics
-- MR: AVG(elapsed_time_ms) FROM mr_job_metrics
```

**Usage**: Sudden increases in duration may be caused by larger data volumes, insufficient resources, or GC issues. Cross-validate with the GC panel.

### Recent Failed Tasks (table)

**Purpose**: Displays the most recent 50 failed tasks, combining UNION results from Spark and MR.

**SQL Query**:
```sql
SELECT 'Spark' AS engine, app_id AS id, stage_id, task_id, duration_ms,
       FROM_UNIXTIME(timestamp_ms/1000) AS time
FROM task_metrics WHERE task_success='false' ...
UNION ALL
SELECT 'MR' AS engine, job_id AS id, NULL AS stage_id, task_id,
       cpu_time_ms AS duration_ms, FROM_UNIXTIME(timestamp_ms/1000) AS time
FROM mr_task_metrics WHERE state != 'SUCCEEDED' ...
ORDER BY time DESC LIMIT 50
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `engine` | Execution engine (Spark / MR) | - |
| `id` | Application ID or Job ID | - |
| `stage_id` | Stage number (Spark only) | - |
| `task_id` | Task number | - |
| `duration_ms` | Task duration | ms |
| `time` | Event occurrence time | - |

**Usage**: Check this panel regularly. If the same `app_id` appears repeatedly as failed, drill into the Spark/MR dedicated dashboards for investigation.

## Navigation
The top navigation bar of this dashboard provides quick access to:
- **Spark** — Spark engine detailed view
- **MapReduce** — MR engine detailed view
- **Hive on MR** — Hive on MR query analysis
- **Hive on Spark** — Hive on Spark query analysis
- **Spark / MR / Hive** — All-engine consolidated dashboard

## Notes
- This dashboard queries 15 independent tables, not the wide `metric_events` table
- MR task-level failure detection depends on whether MR Agent is deployed
- Hive query statistics do not distinguish execution engines (MR / Spark / Tez); use the Hive dedicated panels for filtering
