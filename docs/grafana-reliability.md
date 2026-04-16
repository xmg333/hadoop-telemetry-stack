# 可靠性与失败分析

## 概述
本仪表盘从任务可靠性视角出发，帮助回答以下问题：
- 各引擎的任务成功率趋势如何？
- 最近有哪些失败事件？
- GC 是否与失败存在关联？
- Speculative 任务比例是否异常？
- Hive 查询的失败分布如何？

## 前置条件
- 数据源：`metric_events` 大宽表、`hive_query_metrics` 表（由 Flink Consumer 写入）
- Grafana 变量：`$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`
- 失败事件通过 `status` 字段判断：Spark Task 为 `task_success` = `false`，MR 为 `state` != `SUCCEEDED`，Hive 为 `success` = `false`

## 面板说明

### 跨引擎成功率趋势（timeseries）

**用途**: 展示各引擎随时间变化的任务/作业成功率趋势，快速发现异常下降。

**SQL 查询**:
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

**列说明**:
| 列名 | 含义 | 单位 |
|------|------|------|
| `time` | 时间桶 | Unix 时间戳 |
| `engine` | 执行引擎（SPARK / MR / HIVE） | - |
| `success_rate_pct` | 成功率 | % |

**使用建议**: 成功率低于 95% 应触发告警。短期下降可能是单次作业失败导致，长期下降需排查集群环境问题。MR 和 Spark 的成功率分开看，MR 通常高于 Spark（重试机制不同）。

### 最近失败事件（table）

**用途**: 列出最近的失败事件详情，包括应用、用户、时间和引擎类型，便于快速定位问题。

**SQL 查询**:
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

**列说明**:
| 列名 | 含义 | 单位 |
|------|------|------|
| `event_time` | 事件发生时间（UTC） | - |
| `engine` | 执行引擎 | - |
| `event_type` | 事件类型（TASK / MR_TASK / HIVE_QUERY） | - |
| `app_id` | 应用/作业/查询 ID | - |
| `app_name` | 应用名称 | - |
| `user_name` | 用户名 | - |
| `queue` | 队列 | - |
| `duration_sec` | 执行时长（失败前） | 秒 |

**使用建议**: 同一 `app_id` 多次失败说明应用本身有问题。`duration_sec` 为 0 可能是启动失败，较大值可能是运行时错误。优先关注失败频率最高的用户和应用。

### GC 与失败关联分析（barchart）

**用途**: 对比成功与失败任务的 GC 开销分布，验证 GC 是否为导致失败的因素。

**SQL 查询**:
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

**列说明**:
| 列名 | 含义 | 单位 |
|------|------|------|
| `outcome` | 任务结果（SUCCESS / FAILURE） | - |
| `gc_bucket` | GC 时间占执行时间比例区间 | - |
| `count` | 该组合的事件数量 | 个 |

**使用建议**: 如果 FAILURE 中高 GC 比例（>20%）的占比远高于 SUCCESS，则说明 GC 确实是导致失败的重要因素，需优化内存配置。

### Speculative 任务率（stat）

**用途**: 统计 Spark Speculative（推测执行）任务的比例，评估是否存在慢任务问题。

**SQL 查询**:
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

**列说明**:
| 列名 | 含义 | 单位 |
|------|------|------|
| `value` | Speculative 任务占比 | % |

**使用建议**: Speculative 率大于 5% 说明存在明显的慢任务，通常由数据倾斜、节点故障或 IO 热点导致。需结合数据倾斜面板进一步排查。该指标仅适用于 Spark（MR 无推测执行机制）。

### Hive 操作失败率（barchart）

**用途**: 按 Hive 操作类型（SELECT / INSERT / CREATE 等）展示失败率分布。

**SQL 查询**:
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

**列说明**:
| 列名 | 含义 | 单位 |
|------|------|------|
| `operation` | Hive 操作类型 | - |
| `fail_count` | 失败次数 | 次 |
| `total_count` | 总次数 | 次 |
| `fail_rate_pct` | 失败率 | % |

**使用建议**: INSERT 操作失败率高可能是目标表权限或空间不足。DDL 操作失败通常与语法错误或元数据冲突有关。该面板直接查询 `hive_query_metrics` 物理表。

## 注意事项
- `status` 字段在 `metric_events` 中为标准化字段：Spark 为 `"true"` / `"false"`，MR 从 `state` 字段映射（`SUCCEEDED` -> `"true"`），Hive 从 `success` 字段映射。
- MR Job 的失败由 `mr_job_metrics.state` 字段体现，不等同于 Task 级别的失败。
- Speculative 任务率依赖 `task_speculative` 字段，该字段需要 Flink Consumer v4+ 版本支持。
- Hive 失败分析面板查询的是 `hive_query_metrics` 物理表，不受 `metric_events` 视图影响。
