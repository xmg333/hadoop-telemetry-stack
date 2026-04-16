# 容量规划与资源利用率

## 概述
本仪表盘从集群容量和资源利用视角出发，帮助回答以下问题：
- 每小时的任务并发量如何变化？
- Executor 内存使用趋势是否健康？
- GC 活动是否过于频繁？
- 每日 Job 吞吐量是否稳定？

## 前置条件
- 数据源：`metric_events` 大宽表、`jvm_memory_metrics` 表、`jvm_gc_metrics` 表（由 Flink Consumer 写入）
- Grafana 变量：`$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`

## 面板说明

### 每小时任务并发数（timeseries）

**用途**: 展示各引擎在每小时内执行的任务数，评估集群并发负载和高峰时段。

**SQL 查询**:
```sql
SELECT ( Floor(timestamp_ms / $__interval_ms) * $__interval_ms / 1000 ) AS time,
       engine,
       COUNT(*)                                                            AS task_count,
       COUNT(DISTINCT app_id)                                              AS app_count
FROM   metric_events
WHERE  event_type IN ( 'TASK', 'MR_TASK' )
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
| `engine` | 执行引擎（SPARK / MR） | - |
| `task_count` | 该时段内完成的任务数 | 个 |
| `app_count` | 该时段内活跃的应用数 | 个 |

**使用建议**: 任务数曲线的波峰波谷反映业务周期。如果高峰期任务数接近集群容量上限（通过 YARN ResourceManager 可查看），需考虑扩容或错峰调度。`app_count` 与 `task_count` 的比值反映应用并发度。

### Executor 内存使用趋势（timeseries）

**用途**: 展示各 Executor 的堆内存和非堆内存使用趋势，监控内存压力。

**SQL 查询**:
```sql
SELECT ( Floor(timestamp_ms / $__interval_ms) * $__interval_ms / 1000 ) AS time,
       app_id,
       executor_id,
       ROUND(AVG(heap_used) / 1073741824, 2)                              AS heap_gb,
       ROUND(AVG(non_heap_used) / 1073741824, 2)                           AS non_heap_gb
FROM   jvm_memory_metrics
WHERE  timestamp_ms >= ( $__unixepochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY 1,
          app_id,
          executor_id
ORDER  BY 1,
          2,
          3
```

**列说明**:
| 列名 | 含义 | 单位 |
|------|------|------|
| `time` | 时间桶 | Unix 时间戳 |
| `app_id` | Spark 应用 ID | - |
| `executor_id` | Executor 编号 | - |
| `heap_gb` | 平均堆内存使用量 | GB |
| `non_heap_gb` | 平均非堆内存使用量 | GB |

**使用建议**: 堆内存持续增长且不回落可能存在内存泄漏。如果堆内存接近 `spark.executor.memory` 配置值的 80% 以上，建议增加内存或优化数据结构。该面板直接查询 `jvm_memory_metrics` 物理表。

### GC 活动趋势（timeseries）

**用途**: 展示各应用的 GC 次数和 GC 时间趋势，识别 GC 压力过大的时段。

**SQL 查询**:
```sql
SELECT ( Floor(timestamp_ms / $__interval_ms) * $__interval_ms / 1000 ) AS time,
       app_id,
       gc_name,
       SUM(gc_count)                                                       AS total_gc_count,
       ROUND(SUM(gc_time_ms) / 1000, 1)                                    AS total_gc_sec
FROM   jvm_gc_metrics
WHERE  timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY 1,
          app_id,
          gc_name
ORDER  BY 1,
          2,
          3
```

**列说明**:
| 列名 | 含义 | 单位 |
|------|------|------|
| `time` | 时间桶 | Unix 时间戳 |
| `app_id` | Spark 应用 ID | - |
| `gc_name` | GC 算法名称（如 G1 Old Generation、ParNew 等） | - |
| `total_gc_count` | 该时段内 GC 总次数 | 次 |
| `total_gc_sec` | 该时段内 GC 总耗时 | 秒 |

**使用建议**: 区分 Old GC（Full GC）和 Young GC（Minor GC）。Full GC 频繁出现（每小时超过 5 次）说明堆内存严重不足。Young GC 频率高但耗时短属正常现象。该面板直接查询 `jvm_gc_metrics` 物理表。

### 每日 Job 吞吐量（timeseries）

**用途**: 展示各引擎每日完成的 Job/作业数量趋势，评估整体集群吞吐能力。

**SQL 查询**:
```sql
SELECT ( Floor(timestamp_ms / $__interval_ms) * $__interval_ms / 1000 ) AS time,
       engine,
       COUNT(DISTINCT app_id)                                              AS job_count,
       SUM(CASE
             WHEN status = 'true' THEN 1
             ELSE 0
           END)                                                            AS success_count,
       SUM(CASE
             WHEN status = 'false' THEN 1
             ELSE 0
           END)                                                            AS fail_count
FROM   metric_events
WHERE  event_type IN ( 'JOB', 'MR_JOB', 'HIVE_QUERY' )
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
| `job_count` | 独立 Job/作业/查询数 | 个 |
| `success_count` | 成功数 | 个 |
| `fail_count` | 失败数 | 个 |

**使用建议**: Job 数量的周期性波动正常，但若持续下降则需检查上游提交是否正常。`fail_count` 持续大于 0 时需深入排查失败原因。跨引擎对比可评估迁移效果（如从 MR 迁移到 Spark 后的吞吐变化）。

## 注意事项
- JVM 内存和 GC 指标仅适用于 Spark（通过 `ExecutorPlugin` 采集），MR 任务不包含这些数据。
- `jvm_memory_metrics` 和 `jvm_gc_metrics` 的数据采集频率取决于 OTel SDK 的 `export.interval.ms` 配置（默认 60 秒），非每个任务一条记录。
- 内存趋势面板中 `executor_id` 可能较多，建议通过 Grafana 变量过滤特定应用。
- 时间范围选择超过 7 天时，建议将 `$__interval_ms` 调整为小时级（3600000）或日级（86400000）以避免查询过慢。
- `jvm_memory_metrics` 表的 SQL 查询中使用了 `$__unixepochFrom()`，部分 Grafana 版本可能需要使用 `$__unixEpochFrom()`（注意大小写），请根据实际版本调整。
