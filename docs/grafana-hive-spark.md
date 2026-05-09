# Hive on Spark Query Deep Dive

## Overview
This dashboard displays Hive query metrics for queries using Spark as the execution engine. It helps answer the following questions:
- How many Hive on Spark queries are there? What are the average duration and IO volume?
- What is the operation type distribution?
- How do the duration trends for each operation change over time?
- What are the details of specific queries and their table IO lineage?

## Prerequisites

Data sources: `hive_query_metrics`, `hive_table_io_metrics` (filtered by `execution_engine='spark'`)

Grafana variables:
:   `$hive_operation` — Hive operation type (multi-select, includes All)
:   `$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`

Variable query: `SELECT DISTINCT operation FROM hive_query_metrics WHERE execution_engine='spark' AND operation IS NOT NULL ORDER BY operation`

## Panel Descriptions

### Overview Statistics (y=0, 4 stats)

#### Total Hive-Spark Queries (stat)

**Purpose**: Displays the total number of Hive on Spark queries within the time range.

**SQL Query**:
```sql
SELECT COUNT(*) AS value
FROM hive_query_metrics
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
  AND execution_engine='spark'
```

#### Avg Hive-Spark Duration (stat)

**Purpose**: Displays the average query duration. Thresholds: <5s green, 5-30s yellow, >30s red.

#### Hive-Spark IO Bytes (stat)

**Purpose**: Displays the total input byte count.

**SQL Query**:
```sql
SELECT SUM(input_bytes) AS value
FROM hive_query_metrics WHERE execution_engine='spark' AND input_bytes IS NOT NULL
```

#### Hive-Spark Table Events (stat)

**Purpose**: Displays the total number of table IO events.

### Operation Distribution and Duration Trends (y=4)

#### Operation Distribution (piechart)

**Purpose**: Shows the query count distribution by operation type using a donut chart.

**SQL Query**:
```sql
SELECT operation AS metric, COUNT(*) AS value
FROM hive_query_metrics
WHERE execution_engine='spark' AND operation IS NOT NULL
GROUP BY operation ORDER BY value DESC
```

**Usage**: Compare with the operation distribution from the Hive on MR dashboard to observe performance differences of the same workload under different execution engines.

#### Duration by Operation (timeseries)

**Purpose**: Shows average duration trends by operation type, filterable by the `$hive_operation` variable.

**SQL Query**:
```sql
SELECT (FLOOR(timestamp_ms / $__interval_ms) * $__interval_ms / 1000) AS time,
       operation AS metric, AVG(duration_ms) AS value
FROM hive_query_metrics
WHERE execution_engine='spark' AND duration_ms IS NOT NULL
  AND operation IN ($hive_operation)
GROUP BY 1, 2 ORDER BY 1, 2
```

### Detail Tables (y=12)

#### Query Detail (table)

**Purpose**: Displays complete details for the most recent 200 Hive on Spark queries.

**SQL Query**:
```sql
SELECT FROM_UNIXTIME(timestamp_ms/1000) AS time, query_id, operation, user_name,
       success, duration_ms, input_bytes, output_bytes, input_rows, output_rows
FROM hive_query_metrics
WHERE execution_engine='spark' AND operation IN ($hive_operation)
ORDER BY timestamp_ms DESC LIMIT 200
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `query_id` | Hive query unique ID | - |
| `success` | OK (green) / FAIL (red) | - |
| `input_bytes` / `output_bytes` | Input/output bytes | bytes |
| `input_rows` / `output_rows` | Input/output row count | count |

#### Table IO Detail (table)

**Purpose**: Displays table-level IO lineage.

**SQL Query**:
```sql
SELECT FROM_UNIXTIME(timestamp_ms/1000) AS time, query_id, table_name,
       table_type, operation, user_name
FROM hive_table_io_metrics
WHERE execution_engine='spark' AND operation IN ($hive_operation)
ORDER BY timestamp_ms DESC LIMIT 200
```

**Column Descriptions**:

| Column | Description |
|--------|-------------|
| `table_type` | INPUT (blue) / OUTPUT (green) |

## Navigation
The top navigation bar provides quick access to:
- **Overview** — Platform overview
- **Spark** — Spark engine detailed view
- **MapReduce** — MR engine detailed view
- **Hive on MR** — Hive on MR query analysis
- **Spark / MR / Hive** — All-engine consolidated dashboard

## Notes
- The `execution_engine='spark'` filter depends on the `hive.execution.engine` configuration reported by the Hive Hook.
- Hive on Spark queries are actually executed by Spark; corresponding Spark metrics can be viewed in the Spark dashboard.
- Comparing the duration of the same operation between Hive on MR and Hive on Spark helps evaluate the impact of switching execution engines.
