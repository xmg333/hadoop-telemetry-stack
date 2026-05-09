# Hive 查询与数据血缘分析

## 概述
本仪表盘从 Hive 查询分析视角出发，帮助回答以下问题：
- 哪些 Hive 查询执行时间最长？
- Hive 操作类型的分布如何？
- 哪些表的读写频次最高？
- 不同执行引擎（MR / Spark / Tez）的查询性能差异如何？

## 前置条件
- 数据源：`hive_query_metrics` 表、`hive_table_io_metrics` 表（由 Flink Consumer 写入）
- Grafana 变量：`$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`
- Hive 指标由 `HiveTelemetryHook` 通过 `ExecuteWithHookContext` 采集，需在 HiveServer2 的 `hive-site.xml` 中配置 `hive.exec.post.hooks`

## 面板说明

### Hive 慢查询 TOP 20（table）

**用途**: 列出执行时间最长的 20 条 Hive 查询，帮助优化慢查询。

**SQL 查询**:
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

**列说明**:

| 列名 | 含义 | 单位 |
|------|------|------|
| `query_id` | Hive 查询唯一 ID | - |
| `operation` | 操作类型（QUERY / DDL / DML 等） | - |
| `user_name` | 执行用户 | - |
| `execution_engine` | 执行引擎（mr / spark / tez） | - |
| `success` | 是否成功（true / false） | - |
| `duration_sec` | 查询执行时长 | 秒 |
| `input_gb` | 输入数据量 | GB |
| `output_gb` | 输出数据量 | GB |
| `input_m_rows` | 输入行数 | 百万行 |
| `output_m_rows` | 输出行数 | 百万行 |
| `event_time` | 查询发生时间（UTC） | - |

**使用建议**: 重点关注 `duration_sec` 高且 `input_gb` 低的不合理查询，可能是缺少分区过滤或索引。对比不同 `execution_engine` 下同类查询的执行时间，评估引擎切换的效果。

### 操作类型分布（piechart）

**用途**: 展示 Hive 查询中各类操作（SELECT / INSERT / CREATE 等）的占比分布。

**SQL 查询**:
```sql
SELECT operation,
       COUNT(*) AS count
FROM   hive_query_metrics
WHERE  timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY operation
ORDER  BY count DESC
```

**列说明**:

| 列名 | 含义 | 单位 |
|------|------|------|
| `operation` | 操作类型 | - |
| `count` | 该操作的执行次数 | 次 |

**使用建议**: 如果 SELECT 占比过高，说明大量查询可能来自即席查询或报表系统，可考虑引入缓存层。INSERT 占比高说明 ETL 负载较重，需关注写入性能。

### 表读写频次排行（table）

**用途**: 统计被读写频次最高的表，识别热点数据和数据血缘关系。

**SQL 查询**:
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

**列说明**:

| 列名 | 含义 | 单位 |
|------|------|------|
| `table_name` | 表名 | - |
| `table_type` | 表角色（input / output） | - |
| `operation` | 触发该表 IO 的操作类型 | - |
| `execution_engine` | 执行引擎 | - |
| `access_count` | 表被访问的次数 | 次 |
| `query_count` | 涉及该表的独立查询数 | 个 |

**使用建议**: `input` 类型的热点表适合做分区裁剪优化或物化视图。`output` 类型的热点表需关注并发写入冲突和小文件问题。通过 `query_id` 可追溯到具体查询，建立数据血缘链路。

### 执行引擎对比（barchart）

**用途**: 对比不同执行引擎（MR / Spark / Tez）下的查询数量、平均执行时间和成功率。

**SQL 查询**:
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

**列说明**:

| 列名 | 含义 | 单位 |
|------|------|------|
| `execution_engine` | 执行引擎（mr / spark / tez） | - |
| `query_count` | 该引擎执行的查询数 | 个 |
| `avg_duration_sec` | 平均查询时长 | 秒 |
| `success_rate_pct` | 成功率 | % |

**使用建议**: 如果 Spark 引擎的平均时长远低于 MR，建议将更多查询迁移到 Spark 引擎（通过 `hive.execution.engine=spark`）。成功率差异过大时需检查引擎配置兼容性。

## 注意事项
- Hive 指标依赖 `hive.exec.post.hooks` 配置。如果未部署 `HiveTelemetryHook`，这些面板将无数据。
- `hive_table_io_metrics` 表记录的是表级别的 IO 关系，不含具体字节数（仅记录 `input_table_count` 和 `output_table_count`）。详细的 IO 字节数需查看 `hive_query_metrics` 表中的 `input_bytes` / `output_bytes`。
- `execution_engine` 字段来自 Hive 配置的 `hive.execution.engine`，可能值为 `mr`、`spark`、`tez`。
- Hive on Spark 的查询同时会产生 Spark 侧的 Task 指标（通过 Spark 插件采集），可通过 `query_id` 和 `app_id` 进行关联分析。
- ClickHouse 数据源中时间过滤需使用 `DateTime64(3)` 格式，与 MySQL 的 `BIGINT` 有差异。
