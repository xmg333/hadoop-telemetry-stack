# 数据吞吐与 IO 分析

## 概述
本仪表盘从数据 IO 视角出发，帮助回答以下问题：
- 各引擎的数据读写吞吐量趋势如何？
- 哪些应用/Stage 的 Shuffle 数据量最大？
- 是否存在 Spill（内存溢写）问题？
- 哪些表的 IO 最频繁？

## 前置条件
- 数据源：`metric_events` 大宽表、`sql_query_table_metrics` 表（由 Flink Consumer 写入）
- Grafana 变量：`$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`

## 面板说明

### 跨引擎数据吞吐趋势（timeseries）

**用途**: 展示各引擎（SPARK / MR / HIVE）随时间变化的 IO 吞吐量趋势，帮助理解集群数据流动情况。

**SQL 查询**:
```sql
SELECT ( Floor(timestamp_ms / $__interval_ms) * $__interval_ms / 1000 ) AS time,
       engine,
       SUM(io_bytes_read) / 1073741824                                    AS read_gb,
       SUM(io_bytes_written) / 1073741824                                 AS write_gb
FROM   metric_events
WHERE  event_type IN ( 'TASK', 'MR_TASK', 'HIVE_QUERY' )
       AND ( io_bytes_read > 0 OR io_bytes_written > 0 )
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY 1,
          engine
ORDER  BY 1,
          2
```

**列说明**:

| 列名 | 含义 | 单位 |
|------|------|------|
| `time` | 时间桶 | Unix 时间戳 |
| `engine` | 执行引擎（SPARK / MR / HIVE） | - |
| `read_gb` | 该时段内读取数据总量 | GB |
| `write_gb` | 该时段内写入数据总量 | GB |

**使用建议**: 吞吐量突然下降可能意味着上游数据源不可用或任务调度异常。可对比不同引擎的 IO 模式，判断是否有必要将部分 MR 作业迁移到 Spark 以提高 IO 效率。

### Shuffle 数据量排行（table）

**用途**: 列出 Shuffle 读写数据量最大的前 15 个应用，定位 Shuffle 瓶颈。

**SQL 查询**:
```sql
SELECT app_id,
       app_name,
       ROUND(SUM(shuffle_bytes_read) / 1073741824, 2)  AS shuffle_read_gb,
       ROUND(SUM(shuffle_bytes_written) / 1073741824, 2) AS shuffle_write_gb,
       ROUND(SUM(shuffle_bytes_read + shuffle_bytes_written) / 1073741824, 2) AS shuffle_total_gb,
       ROUND(SUM(shuffle_fetch_wait_time_ms) / 1000, 1) AS shuffle_wait_sec,
       COUNT(*)                                        AS task_count
FROM   metric_events
WHERE  event_type = 'TASK'
       AND ( shuffle_bytes_read > 0 OR shuffle_bytes_written > 0 )
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY app_id,
          app_name
ORDER  BY shuffle_total_gb DESC
LIMIT  15
```

**列说明**:

| 列名 | 含义 | 单位 |
|------|------|------|
| `app_id` | Spark 应用 ID | - |
| `app_name` | 应用名称 | - |
| `shuffle_read_gb` | Shuffle 读取总量 | GB |
| `shuffle_write_gb` | Shuffle 写入总量 | GB |
| `shuffle_total_gb` | Shuffle 总数据量 | GB |
| `shuffle_wait_sec` | Shuffle 等待时间总计 | 秒 |
| `task_count` | 任务数 | 个 |

**使用建议**: Shuffle 数据量过大的应用应考虑优化 Join 策略（如 Broadcast Join）、增加分区数或使用 Bucket 表。`shuffle_wait_sec` 高说明网络或磁盘 IO 成为瓶颈。

### Spill 分析（timeseries）

**用途**: 展示各应用的内存溢写（Spill）趋势，检测内存配置不足导致的问题。

**SQL 查询**:
```sql
SELECT ( Floor(timestamp_ms / $__interval_ms) * $__interval_ms / 1000 ) AS time,
       app_id,
       ROUND(SUM(memory_bytes_spilled) / 1073741824, 2)                    AS spill_gb,
       ROUND(SUM(disk_bytes_spilled) / 1073741824, 2)                      AS disk_spill_gb
FROM   metric_events
WHERE  event_type = 'TASK'
       AND ( memory_bytes_spilled > 0 OR disk_bytes_spilled > 0 )
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY 1,
          app_id
ORDER  BY 1,
          2
```

**列说明**:

| 列名 | 含义 | 单位 |
|------|------|------|
| `time` | 时间桶 | Unix 时间戳 |
| `app_id` | Spark 应用 ID | - |
| `spill_gb` | 内存 Spill 总量（含内存和磁盘溢写） | GB |
| `disk_spill_gb` | 磁盘 Spill 总量 | GB |

**使用建议**: Spill 数据量大意味着 Executor 内存不足以容纳所有 Shuffle 数据。建议增大 `spark.executor.memory`、调整 `spark.memory.fraction`，或增加分区数以减少单任务数据量。持续的 Spill 会导致严重的磁盘 IO 和性能下降。

### 热点表 IO 分析（table）

**用途**: 基于 SQL Table IO 数据，统计读写频次和数据量最高的表，识别热点数据源。

**SQL 查询**:
```sql
SELECT table_name,
       operation,
       COUNT(*)                                          AS access_count,
       ROUND(SUM(bytes) / 1073741824, 2)                 AS total_gb,
       ROUND(SUM(rows) / 1000000, 2)                     AS total_million_rows,
       ROUND(AVG(time_ms), 0)                            AS avg_scan_ms
FROM   sql_query_table_metrics
WHERE  timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY table_name,
          operation
ORDER  BY total_gb DESC
LIMIT  20
```

**列说明**:

| 列名 | 含义 | 单位 |
|------|------|------|
| `table_name` | 表名（完整库名.表名） | - |
| `operation` | 操作类型（SCAN / WRITE 等） | - |
| `access_count` | 被访问次数 | 次 |
| `total_gb` | 累计 IO 数据量 | GB |
| `total_million_rows` | 累计处理行数 | 百万行 |
| `avg_scan_ms` | 平均扫描耗时 | 毫秒 |

**使用建议**: 访问频次高但 `avg_scan_ms` 也高的表适合做分区优化或缓存。写入量异常大的表需检查是否存在小文件问题。该面板仅包含 Spark SQL 的表 IO 数据（通过 `SQL_TABLE_IO` 事件采集）。

## 注意事项
- IO 字段（`io_bytes_read`、`io_bytes_written`）为标准化字段：Spark 来自 Task 指标，MR 来自 `hdfs_bytes_read + file_bytes_read` 的汇总，Hive 来自 `input_bytes / output_bytes`。
- Shuffle 指标仅适用于 Spark（MR 的 Shuffle 通过 `reduce_shuffle_bytes` 体现，在 MR_JOB 级别）。
- 热点表 IO 面板依赖 Spark SQL 的 `SQL_TABLE_IO` 事件，需开启 `spark.telemetry.metrics.sql` 相关配置。
- 对于 ClickHouse 数据源，`sql_query_table_metrics` 表中的 `` `rows` `` 列为保留字，需用反引号包裹。
