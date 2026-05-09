# All-Engine Consolidated Deep Dive

## Overview
This dashboard is the largest foundational deep-dive panel, integrating all metrics from **Spark + SQL + MR + Hive** in a single view with 33 panels. It provides the most detailed metric display and helps answer the following questions:
- What is the overall picture of Spark application Tasks, Stages, and Jobs?
- Are there data skew and resource efficiency issues?
- Are JVM memory and GC healthy?
- What are the SQL query and table IO details?
- What is the operation distribution, duration, and IO for Hive queries?

This dashboard combines all panels from the Spark dashboard and Hive analysis, adding MR-related statistics. It is suitable for scenarios requiring a global perspective.

## Prerequisites

Data sources: All 15 independent metric tables

Grafana variables:
:   `$app_id` — Spark application ID (multi-select, includes All), sourced from `task_metrics`
:   `$hive_operation` — Hive operation type (multi-select, includes All), sourced from `hive_query_metrics`
:   `$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`

## Panel Descriptions

### Spark Overview Statistics (y=0, 4 stats)

#### Total Tasks / Total Stages / Skewed Stages / Avg CPU Efficiency

Identical to [Spark Engine Deep Dive](grafana-spark.md), filtered by the `$app_id` variable.

### Spark Task IO and Duration (y=4)

#### Task I/O Bytes / Task Duration

Same as the Spark dashboard.

### Spark JVM Monitoring (y=12)

#### JVM Memory / JVM GC

Same as the Spark dashboard.

### Spark Stage and Job (y=20)

#### Stage Duration & Task Count / Job Overview

Same as the Spark dashboard.

### Spark Governance Analysis (y=28)

#### Data Skew Detection / Resource Efficiency

Same as the Spark dashboard. See [Spark Engine Deep Dive - Governance Analysis](grafana-spark.md#governance-analysis) for details.

### Spark Histograms and Small Files (y=36)

#### Task Duration Histogram / Small File Detection

Same as the Spark dashboard.

### Spark Task Details (y=44)

#### Task Detail (full-width table)

Same as the Spark dashboard, displaying complete metrics for the 200 most recent Tasks.

### SQL Query Analysis (y=52-72)

#### SQL Queries / Avg SQL Query Duration / Total SQL Joins / SQL Table Scans/Writes (4 stats)

**Purpose**: SQL query overview statistics.

#### SQL Query Duration (timeseries)

**Purpose**: Average and maximum duration trends for SQL queries.

#### SQL Shuffle Bytes (timeseries)

**Purpose**: Shuffle read and write data volume trends for SQL queries. Note the use of `COALESCE` to handle NULL values.

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

### Hive Query Analysis (y=80-100)

#### Total Hive Queries / Avg Hive Duration / Hive Table IO Events / Hive IO Bytes (4 stats)

**Purpose**: Global Hive query statistics. Note that the Hive panels do not use the `$app_id` filter (Hive query_id and Spark app_id are different dimensions).

**SQL Query**:
```sql
-- Total Hive Queries
SELECT COUNT(*) AS value FROM hive_query_metrics WHERE ...

-- Hive IO Bytes (2 targets)
SELECT COALESCE(SUM(input_bytes), 0) AS 'Input Bytes' FROM hive_query_metrics WHERE ...
SELECT COALESCE(SUM(output_bytes), 0) AS 'Output Bytes' FROM hive_query_metrics WHERE ...
```

#### Hive Operations Distribution (piechart)

**Purpose**: Shows the query count distribution by operation type using a donut chart.

**SQL Query**:
```sql
SELECT operation AS metric, COUNT(*) AS value
FROM hive_query_metrics WHERE operation IS NOT NULL
GROUP BY operation ORDER BY value DESC
```

#### Hive Duration by Operation (timeseries)

**Purpose**: Shows average duration trends by operation type, filterable by the `$hive_operation` variable.

#### Hive IO Throughput (timeseries)

**Purpose**: Shows input/output byte trends for Hive queries.

#### Hive Operation Count (barchart)

**Purpose**: Horizontal bar chart showing the query count and average duration for each operation type.

**SQL Query**:
```sql
SELECT operation AS 'Operation', COUNT(*) AS 'Query Count',
       ROUND(AVG(duration_ms), 1) AS 'Avg Duration (ms)'
FROM hive_query_metrics WHERE operation IS NOT NULL
GROUP BY operation ORDER BY 2 DESC
```

#### Hive Query Detail (table)

**Purpose**: Displays details for the most recent 200 Hive queries, filterable by `$hive_operation`.

**SQL Query**:
```sql
SELECT FROM_UNIXTIME(timestamp_ms/1000) AS time, query_id, operation, user_name,
       success, duration_ms, input_bytes, output_bytes, input_rows, output_rows
FROM hive_query_metrics WHERE operation IN ($hive_operation)
ORDER BY timestamp_ms DESC LIMIT 200
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `query_id` | Hive query unique ID | - |
| `success` | OK (green) / FAIL (red) | - |
| `input_bytes` / `output_bytes` | Input/output bytes | bytes |

#### Hive Table IO Detail (table)

**Purpose**: Displays table-level IO lineage, filterable by `$hive_operation`.

**SQL Query**:
```sql
SELECT FROM_UNIXTIME(timestamp_ms/1000) AS time, query_id, table_name,
       table_type, operation, user_name
FROM hive_table_io_metrics WHERE operation IN ($hive_operation)
ORDER BY timestamp_ms DESC LIMIT 200
```

## Navigation
The top navigation bar provides quick access to:
- **Overview** — Platform overview
- **Spark** — Spark engine detailed view
- **MapReduce** — MR engine detailed view
- **Hive on MR** / **Hive on Spark** — Hive query analysis

## Notes
- This dashboard is the most comprehensive foundational view, containing 33 panels; page load may be slower.
- The Spark section uses `$app_id` for filtering, while the Hive section uses `$hive_operation`; they are independent.
- The Hive panels do not distinguish execution engines (showing all Hive queries). To filter by engine, use the Hive on MR / Hive on Spark panels.
- Data in `stage_governance` and `task_histogram_buckets` depends on computation triggered after Stage completion.
