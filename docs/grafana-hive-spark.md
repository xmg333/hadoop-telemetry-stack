# Hive on Spark 查询透视

## 概述
本仪表盘展示以 Spark 为执行引擎的 Hive 查询指标，帮助回答以下问题：
- Hive on Spark 有多少查询？平均耗时和 IO 量如何？
- 操作类型分布如何？
- 各操作的耗时趋势如何变化？
- 具体查询的详情和表 IO 血缘如何？

## 前置条件
- 数据源：`hive_query_metrics`、`hive_table_io_metrics`（过滤 `execution_engine='spark'`）
- Grafana 变量：
  - `$hive_operation` — Hive 操作类型（多选，含 All）
  - `$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`
- 变量查询：`SELECT DISTINCT operation FROM hive_query_metrics WHERE execution_engine='spark' AND operation IS NOT NULL ORDER BY operation`

## 面板说明

### 概览统计（y=0，4 个 stat）

#### Total Hive-Spark Queries（stat）

**用途**: 展示时间范围内 Hive on Spark 查询总数。

**SQL 查询**:
```sql
SELECT COUNT(*) AS value
FROM hive_query_metrics
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
  AND execution_engine='spark'
```

#### Avg Hive-Spark Duration（stat）

**用途**: 展示平均查询耗时。阈值：<5s 绿色，5-30s 黄色，>30s 红色。

#### Hive-Spark IO Bytes（stat）

**用途**: 展示总输入字节量。

**SQL 查询**:
```sql
SELECT SUM(input_bytes) AS value
FROM hive_query_metrics WHERE execution_engine='spark' AND input_bytes IS NOT NULL
```

#### Hive-Spark Table Events（stat）

**用途**: 展示表 IO 事件总数。

### 操作分布与耗时趋势（y=4）

#### Operation Distribution（piechart）

**用途**: 以环形图展示各操作类型的查询数量分布。

**SQL 查询**:
```sql
SELECT operation AS metric, COUNT(*) AS value
FROM hive_query_metrics
WHERE execution_engine='spark' AND operation IS NOT NULL
GROUP BY operation ORDER BY value DESC
```

**使用建议**: 对比 Hive on MR 面板的操作分布，观察同一工作负载在不同执行引擎下的表现差异。

#### Duration by Operation（timeseries）

**用途**: 按操作类型展示平均耗时趋势，支持通过 `$hive_operation` 变量过滤。

**SQL 查询**:
```sql
SELECT (FLOOR(timestamp_ms / $__interval_ms) * $__interval_ms / 1000) AS time,
       operation AS metric, AVG(duration_ms) AS value
FROM hive_query_metrics
WHERE execution_engine='spark' AND duration_ms IS NOT NULL
  AND operation IN ($hive_operation)
GROUP BY 1, 2 ORDER BY 1, 2
```

### 详情表格（y=12）

#### Query Detail（table）

**用途**: 展示最近 200 条 Hive on Spark 查询的完整详情。

**SQL 查询**:
```sql
SELECT FROM_UNIXTIME(timestamp_ms/1000) AS time, query_id, operation, user_name,
       success, duration_ms, input_bytes, output_bytes, input_rows, output_rows
FROM hive_query_metrics
WHERE execution_engine='spark' AND operation IN ($hive_operation)
ORDER BY timestamp_ms DESC LIMIT 200
```

**列说明**:
| 列名 | 含义 | 单位 |
|------|------|------|
| `query_id` | Hive 查询唯一 ID | - |
| `success` | OK（绿色）/ FAIL（红色） | - |
| `input_bytes` / `output_bytes` | 输入/输出字节数 | bytes |
| `input_rows` / `output_rows` | 输入/输出行数 | 条 |

#### Table IO Detail（table）

**用途**: 展示表级别的 IO 血缘关系。

**SQL 查询**:
```sql
SELECT FROM_UNIXTIME(timestamp_ms/1000) AS time, query_id, table_name,
       table_type, operation, user_name
FROM hive_table_io_metrics
WHERE execution_engine='spark' AND operation IN ($hive_operation)
ORDER BY timestamp_ms DESC LIMIT 200
```

**列说明**:
| 列名 | 含义 |
|------|------|
| `table_type` | INPUT（蓝色）/ OUTPUT（绿色） |

## 导航
顶部导航栏可快速切换到：
- **Overview** — 平台总览
- **Spark** — Spark 引擎详细透视
- **MapReduce** — MR 引擎详细透视
- **Hive on MR** — Hive on MR 查询分析
- **Spark / MR / Hive** — 全引擎综合仪表盘

## 注意事项
- `execution_engine='spark'` 过滤条件依赖 Hive Hook 上报时的 `hive.execution.engine` 配置
- Hive on Spark 查询的实际执行由 Spark 完成，对应的 Spark 指标可在 Spark 仪表盘中查看
- 对比 Hive on MR 和 Hive on Spark 的同操作耗时，可评估执行引擎切换的效果
