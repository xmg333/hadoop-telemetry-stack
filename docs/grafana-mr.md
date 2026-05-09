# MapReduce Engine Deep Dive

## Overview
This dashboard provides a foundational deep-dive view of the MapReduce engine, divided into Job-level (History Server / MR Collector) and Task-level (MR Agent) sections. It helps answer the following questions:
- How many MR Jobs are there? What are the average duration and success rate?
- How many Map / Reduce Tasks are there?
- What is the Job-level IO (HDFS + File) throughput?
- What are the Task-level duration distribution and IO details?

## Prerequisites

Data sources: `mr_job_metrics`, `mr_task_metrics`

Grafana variables:
:   `$mr_job_id` — MR Job ID (multi-select, includes All)
:   `$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`

Variable query: `SELECT DISTINCT job_id FROM (SELECT DISTINCT job_id FROM mr_job_metrics UNION SELECT DISTINCT job_id FROM mr_task_metrics) t ORDER BY job_id`

!!! warning "Note"
    Task-level panels require the MR Agent (ByteBuddy bytecode instrumentation) to be deployed. Relying solely on the MR Collector (History Server polling) will not provide Task-level data.

## Panel Descriptions

### Job Level Metrics Section

#### Total MR Jobs (stat)

**Purpose**: Displays the number of distinct MR jobs within the selected Job ID range.

**SQL Query**:
```sql
SELECT COUNT(DISTINCT job_id) AS value
FROM mr_job_metrics
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
  AND job_id IN ($mr_job_id)
```

#### Avg Job Duration (stat)

**Purpose**: Displays the average Job duration (`elapsed_time_ms`). Thresholds: <5s green, 5-30s yellow, >30s red.

**SQL Query**:
```sql
SELECT ROUND(AVG(elapsed_time_ms)) AS value
FROM mr_job_metrics
WHERE ... AND elapsed_time_ms IS NOT NULL AND job_id IN ($mr_job_id)
```

#### Job Success Rate (stat)

**Purpose**: Displays the Job success rate based on `state='SUCCEEDED'`.

**SQL Query**:
```sql
SELECT ROUND(
  SUM(CASE WHEN state='SUCCEEDED' THEN 1 ELSE 0 END) * 100.0
  / NULLIF(COUNT(DISTINCT job_id), 0), 1
) AS value
FROM mr_job_metrics WHERE job_id IN ($mr_job_id)
```

#### Total Map Tasks (stat)

**Purpose**: Displays the total number of Map Tasks at the Job level (`launched_maps`).

#### Job IO Bytes (timeseries)

**Purpose**: 4 time-series lines showing HDFS Read / HDFS Written / File Read / File Written over time.

**SQL Query**:
```sql
-- 4 targets querying hdfs_bytes_read, hdfs_bytes_written,
-- file_bytes_read, file_bytes_written respectively
-- FROM mr_job_metrics WHERE job_id IN ($mr_job_id)
```

#### Map/Reduce Task Counts (timeseries)

**Purpose**: Displays trends for `launched_maps` and `launched_reduces`.

#### CPU/GC Time (timeseries)

**Purpose**: Displays average CPU Time and average GC Time trends.

**SQL Query**:
```sql
-- AVG(cpu_time_ms), AVG(gc_time_ms) FROM mr_job_metrics WHERE job_id IN ($mr_job_id)
```

#### Job Detail (table)

**Purpose**: Displays a complete list of Job information.

**SQL Query**:
```sql
SELECT FROM_UNIXTIME(timestamp_ms/1000) AS time, job_id, job_name, user_name,
       state, elapsed_time_ms, launched_maps, launched_reduces,
       hdfs_bytes_read, hdfs_bytes_written, cpu_time_ms, gc_time_ms
FROM mr_job_metrics WHERE job_id IN ($mr_job_id)
ORDER BY timestamp_ms DESC LIMIT 200
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `state` | Job status (SUCCEEDED / FAILED / KILLED) | - |
| `elapsed_time_ms` | Total Job duration | ms |
| `launched_maps` / `launched_reduces` | Number of launched Maps / Reduces | count |
| `hdfs_bytes_read` / `hdfs_bytes_written` | HDFS read/write bytes | bytes |
| `cpu_time_ms` / `gc_time_ms` | CPU / GC time | ms |

### Task Level Metrics Section

#### Total Tasks / Reduce Tasks (stat)

**Purpose**: Counts the distinct number of Map and Reduce tasks respectively.

**SQL Query**:
```sql
-- Map: COUNT(DISTINCT task_id) FROM mr_task_metrics WHERE task_type='map' AND job_id IN ($mr_job_id)
-- Reduce: Same as above WHERE task_type='reduce'
```

#### Avg Task Duration (stat)

**Purpose**: Displays the average Task duration.

#### Task Success Rate (stat)

**Purpose**: Calculates the success rate based on `success_count` and `failure_count`.

**SQL Query**:
```sql
SELECT ROUND(
  SUM(COALESCE(success_count, 0)) * 100.0
  / NULLIF(SUM(COALESCE(success_count, 0)) + SUM(COALESCE(failure_count, 0)), 0), 1
) AS value
FROM mr_task_metrics WHERE job_id IN ($mr_job_id)
```

#### Total Map Output Bytes / Total Shuffle Bytes (stat)

**Purpose**: Displays the total Map output bytes and total Reduce Shuffle bytes respectively.

#### Task Duration (timeseries)

**Purpose**: Displays AVG / MAX / MIN trends for Task duration.

#### File IO Throughput (timeseries)

**Purpose**: 4 time-series lines showing Task-level File Read / File Written / HDFS Read / HDFS Written.

#### Task IO Bytes (timeseries)

**Purpose**: Displays `map_output_bytes` and `reduce_shuffle_bytes` trends.

#### Task Record Counts (timeseries)

**Purpose**: 4 time-series lines showing Map Input / Map Output / Reduce Input / Reduce Output record count trends.

#### Task Detail (table, full width)

**Purpose**: Displays complete metric details for the most recent 200 Tasks.

**SQL Query**:
```sql
SELECT FROM_UNIXTIME(timestamp_ms/1000) AS time, task_id, task_type, job_id,
       duration_ms, success_count, map_input_records, map_output_records,
       map_output_bytes, reduce_input_records, reduce_output_records,
       reduce_shuffle_bytes, spilled_records, hdfs_bytes_read, hdfs_bytes_written,
       hdfs_read_ops, hdfs_write_ops, file_read_ops, file_write_ops
FROM mr_task_metrics WHERE job_id IN ($mr_job_id)
ORDER BY timestamp_ms DESC LIMIT 200
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `map_output_bytes` | Map output bytes | bytes |
| `reduce_shuffle_bytes` | Reduce Shuffle bytes | bytes |
| `spilled_records` | Number of spilled records | count |
| `hdfs_read_ops` / `hdfs_write_ops` | HDFS read/write operation count | count |
| `file_read_ops` / `file_write_ops` | Local file read/write operation count | count |

## Navigation
The top navigation bar provides quick access to:
- **Overview** — Platform overview
- **Spark** — Spark engine detailed view
- **Hive on MR** / **Hive on Spark** — Hive query analysis
- **Spark / MR / Hive** — All-engine consolidated dashboard

## Notes
- Task-level data (panels 10-16) depends on MR Agent deployment. If only the MR Collector (History Server polling) is deployed, the Task section will have no data.
- The `$mr_job_id` variable merges job_id from both `mr_job_metrics` and `mr_task_metrics`.
- `elapsed_time_ms` is the Job-level duration, while `duration_ms` is the Task-level duration; they have different semantics.
