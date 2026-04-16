# 全引擎综合透视

## 概述
本仪表盘是最大的基础透视面板，在一个视图中整合了 **Spark + SQL + MR + Hive** 的全部指标，共 33 个面板，提供最详细的指标展示，帮助回答以下问题：
- Spark 应用的 Task、Stage、Job 全景如何？
- 是否存在数据倾斜和资源效率问题？
- JVM 内存和 GC 是否正常？
- SQL 查询和表 IO 详情如何？
- Hive 查询的操作分布、耗时、IO 如何？

本仪表盘整合了 Spark 仪表盘和 Hive 分析的全部面板，并增加 MR 相关统计，适合需要全局视角的场景。

## 前置条件

数据源：全部 11 张独立指标表

Grafana 变量：
:   `$app_id` — Spark 应用 ID（多选，含 All），从 `task_metrics` 获取
:   `$hive_operation` — Hive 操作类型（多选，含 All），从 `hive_query_metrics` 获取
:   `$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`

## 面板说明

### Spark 概览统计（y=0，4 个 stat）

#### Total Tasks / Total Stages / Skewed Stages / Avg CPU Efficiency

与 [Spark 引擎透视](grafana-spark.md) 完全一致，使用 `$app_id` 变量过滤。

### Spark Task IO 与耗时（y=4）

#### Task I/O Bytes / Task Duration

与 Spark 仪表盘一致。

### Spark JVM 监控（y=12）

#### JVM Memory / JVM GC

与 Spark 仪表盘一致。

### Spark Stage 与 Job（y=20）

#### Stage Duration & Task Count / Job Overview

与 Spark 仪表盘一致。

### Spark 治理分析（y=28）

#### Data Skew Detection / Resource Efficiency

与 Spark 仪表盘一致。详情参见 [Spark 引擎透视 - 治理分析](grafana-spark.md#_6)。

### Spark 直方图与小文件（y=36）

#### Task Duration Histogram / Small File Detection

与 Spark 仪表盘一致。

### Spark Task 详情（y=44）

#### Task Detail（全宽表格）

与 Spark 仪表盘一致，展示 200 条最近 Task 的完整指标。

### SQL 查询分析（y=52-72）

#### SQL Queries / Avg SQL Query Duration / Total SQL Joins / SQL Table Scans/Writes（4 个 stat）

**用途**: SQL 查询概览统计。

#### SQL Query Duration（timeseries）

**用途**: SQL 查询的平均和最大耗时趋势。

#### SQL Shuffle Bytes（timeseries）

**用途**: SQL 查询的 Shuffle 读写数据量趋势。注意使用 `COALESCE` 处理 NULL 值。

#### SQL Table IO Detail（table，全宽）

**用途**: 展示每个 SQL 查询的表 IO 详情（表名、操作类型、字节数、行数、文件数）。

**SQL 查询**:
```sql
SELECT FROM_UNIXTIME(timestamp_ms/1000) AS time, app_id, execution_id,
       table_name, operation, bytes, `rows`, files_read, time_ms
FROM sql_query_table_metrics WHERE app_id IN ($app_id)
ORDER BY timestamp_ms DESC LIMIT 200
```

#### SQL Query Detail（table，全宽）

**用途**: 展示查询级别指标：耗时、Shuffle、Join 数。

### Hive 查询分析（y=80-100）

#### Total Hive Queries / Avg Hive Duration / Hive Table IO Events / Hive IO Bytes（4 个 stat）

**用途**: Hive 查询全局统计。注意 Hive 面板不使用 `$app_id` 过滤（Hive query_id 与 Spark app_id 是不同维度）。

**SQL 查询**:
```sql
-- Total Hive Queries
SELECT COUNT(*) AS value FROM hive_query_metrics WHERE ...

-- Hive IO Bytes（2 个 target）
SELECT COALESCE(SUM(input_bytes), 0) AS 'Input Bytes' FROM hive_query_metrics WHERE ...
SELECT COALESCE(SUM(output_bytes), 0) AS 'Output Bytes' FROM hive_query_metrics WHERE ...
```

#### Hive Operations Distribution（piechart）

**用途**: 以环形图展示各操作类型的查询数量分布。

**SQL 查询**:
```sql
SELECT operation AS metric, COUNT(*) AS value
FROM hive_query_metrics WHERE operation IS NOT NULL
GROUP BY operation ORDER BY value DESC
```

#### Hive Duration by Operation（timeseries）

**用途**: 按操作类型展示平均耗时趋势，使用 `$hive_operation` 变量过滤。

#### Hive IO Throughput（timeseries）

**用途**: 展示 Hive 查询的输入/输出字节趋势。

#### Hive Operation Count（barchart）

**用途**: 水平柱状图展示每个操作类型的查询数量和平均耗时。

**SQL 查询**:
```sql
SELECT operation AS 'Operation', COUNT(*) AS 'Query Count',
       ROUND(AVG(duration_ms), 1) AS 'Avg Duration (ms)'
FROM hive_query_metrics WHERE operation IS NOT NULL
GROUP BY operation ORDER BY 2 DESC
```

#### Hive Query Detail（table）

**用途**: 展示最近 200 条 Hive 查询详情，支持 `$hive_operation` 过滤。

**SQL 查询**:
```sql
SELECT FROM_UNIXTIME(timestamp_ms/1000) AS time, query_id, operation, user_name,
       success, duration_ms, input_bytes, output_bytes, input_rows, output_rows
FROM hive_query_metrics WHERE operation IN ($hive_operation)
ORDER BY timestamp_ms DESC LIMIT 200
```

**列说明**:

| 列名 | 含义 | 单位 |
|------|------|------|
| `query_id` | Hive 查询唯一 ID | - |
| `success` | OK（绿色）/ FAIL（红色） | - |
| `input_bytes` / `output_bytes` | 输入/输出字节 | bytes |

#### Hive Table IO Detail（table）

**用途**: 展示表级别 IO 血缘，支持 `$hive_operation` 过滤。

**SQL 查询**:
```sql
SELECT FROM_UNIXTIME(timestamp_ms/1000) AS time, query_id, table_name,
       table_type, operation, user_name
FROM hive_table_io_metrics WHERE operation IN ($hive_operation)
ORDER BY timestamp_ms DESC LIMIT 200
```

## 导航
顶部导航栏可快速切换到：
- **Overview** — 平台总览
- **Spark** — Spark 引擎详细透视
- **MapReduce** — MR 引擎详细透视
- **Hive on MR** / **Hive on Spark** — Hive 查询分析

## 注意事项
- 本仪表盘是最全面的基础透视，包含 33 个面板，页面加载可能较慢
- Spark 部分使用 `$app_id` 过滤，Hive 部分使用 `$hive_operation` 过滤，两者独立
- Hive 面板不区分执行引擎（显示所有 Hive 查询），如需按引擎过滤请使用 Hive on MR / Hive on Spark 面板
- `stage_governance` 和 `task_histogram_buckets` 的数据依赖 Stage 完成后触发计算
