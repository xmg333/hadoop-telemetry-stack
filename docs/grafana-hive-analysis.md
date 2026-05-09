# Hive Query and Data Lineage Analysis

## Overview
This dashboard approaches analysis from a Hive query analysis perspective, helping answer the following questions:
- Which Hive queries have the longest execution times?
- What is the distribution of Hive operation types?
- Which tables have the highest read/write frequency?
- How do query performance differences compare across execution engines (MR / Spark / Tez)?

## Prerequisites
- Data sources: `hive_query_metrics` table, `hive_table_io_metrics` table (written by Flink Consumer)
- Grafana variables: `$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`
- Hive metrics are collected by `HiveTelemetryHook` via `ExecuteWithHookContext`. Configure `hive.exec.post.hooks` in HiveServer2's `hive-site.xml`.

## Panel Descriptions

### Hive Slow Query TOP 20 (table)

**Purpose**: Lists the 20 Hive queries with the longest execution times to help optimize slow queries.

**SQL Query**:
```sql
SELECT query_id,
       operation,
       user_name,
       execution_engine,
       success,
       ROUND(duration_ms / 1000, 2)           AS duration_sec,
       ROUND(input_bytes / 1073741824, 2)      AS input_gb,
       ROUND(output_bytes / 1073741824, 2)     AS output_gb,
       ROUND(input_rows / 1000000, 2)          AS input_m_rows,
       ROUND(output_rows / 1000000, 2)         AS output_m_rows,
       FROM_UNIXTIME(timestamp_ms / 1000)      AS event_time
FROM   hive_query_metrics
WHERE  timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
ORDER  BY duration_ms DESC
LIMIT  20
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `query_id` | Hive query unique ID | - |
| `operation` | Operation type (QUERY / DDL / DML, etc.) | - |
| `user_name` | Executing user | - |
| `execution_engine` | Execution engine (mr / spark / tez) | - |
| `success` | Whether successful (true / false) | - |
| `duration_sec` | Query execution duration | seconds |
| `input_gb` | Input data volume | GB |
| `output_gb` | Output data volume | GB |
| `input_m_rows` | Input row count | million rows |
| `output_m_rows` | Output row count | million rows |
| `event_time` | Query occurrence time (UTC) | - |

**Usage**: Pay special attention to queries with high `duration_sec` but low `input_gb`, as this may indicate missing partition filters or indexes. Compare execution times of similar queries under different `execution_engine` values to evaluate the impact of engine switching.

### Operation Type Distribution (piechart)

**Purpose**: Shows the proportion distribution of various Hive query operations (SELECT / INSERT / CREATE, etc.).

**SQL Query**:
```sql
SELECT operation,
       COUNT(*) AS count
FROM   hive_query_metrics
WHERE  timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY operation
ORDER  BY count DESC
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `operation` | Operation type | - |
| `count` | Number of executions for this operation | count |

**Usage**: If SELECT operations account for a very high proportion, it may indicate a workload dominated by ad-hoc queries or reporting systems; consider introducing a caching layer. A high proportion of INSERT operations indicates heavy ETL workloads; focus on write performance.

### Table Read/Write Frequency Ranking (table)

**Purpose**: Identifies the tables with the highest read/write frequency and detects hot data and data lineage relationships.

**SQL Query**:
```sql
SELECT table_name,
       table_type,
       operation,
       execution_engine,
       COUNT(*)                                AS access_count,
       COUNT(DISTINCT query_id)                AS query_count
FROM   hive_table_io_metrics
WHERE  timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY table_name,
          table_type,
          operation,
          execution_engine
ORDER  BY access_count DESC
LIMIT  20
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `table_name` | Table name | - |
| `table_type` | Table role (input / output) | - |
| `operation` | Operation type that triggered the table IO | - |
| `execution_engine` | Execution engine | - |
| `access_count` | Number of times the table was accessed | count |
| `query_count` | Number of distinct queries involving this table | count |

**Usage**: Hot `input` type tables are suitable candidates for partition pruning optimization or materialized views. Hot `output` type tables require attention to concurrent write conflicts and small file issues. Trace back to specific queries via `query_id` to establish data lineage chains.

### Execution Engine Comparison (barchart)

**Purpose**: Compares the query count, average execution time, and success rate across different execution engines (MR / Spark / Tez).

**SQL Query**:
```sql
SELECT execution_engine,
       COUNT(*)                                                          AS query_count,
       ROUND(AVG(duration_ms) / 1000, 2)                                AS avg_duration_sec,
       ROUND(SUM(CASE
                   WHEN success = 'true' THEN 1
                   ELSE 0
                 END) * 100.0 / NULLIF(COUNT(*), 0), 1)                  AS success_rate_pct
FROM   hive_query_metrics
WHERE  timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
       AND execution_engine IS NOT NULL
GROUP  BY execution_engine
ORDER  BY query_count DESC
```

**Column Descriptions**:

| Column | Description | Unit |
|--------|-------------|------|
| `execution_engine` | Execution engine (mr / spark / tez) | - |
| `query_count` | Number of queries executed by this engine | count |
| `avg_duration_sec` | Average query duration | seconds |
| `success_rate_pct` | Success rate | % |

**Usage**: If the Spark engine's average duration is significantly lower than MR, consider migrating more queries to the Spark engine (via `hive.execution.engine=spark`). If there is a large discrepancy in success rates, check engine configuration compatibility.

## Notes
- Hive metrics depend on the `hive.exec.post.hooks` configuration. If `HiveTelemetryHook` is not deployed, these panels will have no data.
- The `hive_table_io_metrics` table records table-level IO relationships without specific byte counts (it only records `input_table_count` and `output_table_count`). For detailed IO byte counts, see `input_bytes` / `output_bytes` in the `hive_query_metrics` table.
- The `execution_engine` field comes from the Hive configuration `hive.execution.engine` and may have values `mr`, `spark`, or `tez`.
- Hive on Spark queries also produce Spark-side Task metrics (collected via the Spark plugin); cross-reference analysis between `query_id` and `app_id` is possible.
- For ClickHouse data sources, time filtering must use the `DateTime64(3)` format, which differs from MySQL's `BIGINT`.
