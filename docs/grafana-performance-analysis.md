# 性能异常与瓶颈分析

## 概述
本仪表盘从性能诊断视角出发，帮助回答以下问题：
- 哪些 Stage 执行时间最长？
- GC 开销占比是否异常？
- 是否存在数据倾斜问题？
- 整体 CPU 效率如何？

## 前置条件
- 数据源：`metric_events` 大宽表、`stage_governance` 表（由 Flink Consumer 写入）
- Grafana 变量：`$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`
- `stage_governance` 表由 Flink Consumer 在 Stage 完成时自动计算并写入，包含倾斜率、效率评分等预聚合指标

## 面板说明

### 最慢 Stage TOP 20（table）

**用途**: 列出执行时间最长的 20 个 Stage，帮助定位性能瓶颈所在的 Stage 和应用。

**SQL 查询**:
```sql
SELECT app_id,
       stage_id,
       ROUND(duration_ms / 1000, 1)        AS duration_sec,
       ROUND(num_tasks)                     AS num_tasks,
       ROUND(executor_run_time_ms / 1000, 1) AS executor_run_sec,
       ROUND(jvm_gc_time_ms / 1000, 1)     AS gc_sec,
       ROUND(peak_execution_memory_bytes / 1073741824, 2) AS peak_mem_gb,
       ROUND(io_bytes_read / 1073741824, 2) AS read_gb,
       ROUND(io_bytes_written / 1073741824, 2) AS write_gb
FROM   metric_events
WHERE  event_type = 'STAGE'
       AND duration_ms IS NOT NULL
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
ORDER  BY duration_ms DESC
LIMIT  20
```

**列说明**:
| 列名 | 含义 | 单位 |
|------|------|------|
| `app_id` | Spark 应用 ID | - |
| `stage_id` | Stage 编号 | - |
| `duration_sec` | Stage 执行时长 | 秒 |
| `num_tasks` | Stage 内任务总数 | 个 |
| `executor_run_sec` | 累计 Executor 运行时间 | 秒 |
| `gc_sec` | 累计 GC 时间 | 秒 |
| `peak_mem_gb` | 峰值执行内存 | GB |
| `read_gb` | 读取数据量 | GB |
| `write_gb` | 写入数据量 | GB |

**使用建议**: 优先关注 `duration_sec` 远大于 `executor_run_sec / num_tasks` 的 Stage，说明存在调度延迟或任务倾斜。`gc_sec` 占比高时建议增大 Executor 内存或优化数据结构。

### GC 开销占比趋势（timeseries）

**用途**: 展示各应用 GC 时间占 Executor 运行时间的比例趋势，识别 GC 瓶颈。

**SQL 查询**:
```sql
SELECT ( Floor(timestamp_ms / $__interval_ms) * $__interval_ms / 1000 ) AS time,
       app_id,
       ROUND(SUM(jvm_gc_time_ms) * 100.0 / NULLIF(SUM(executor_run_time_ms), 0), 1) AS gc_ratio_pct
FROM   metric_events
WHERE  event_type = 'TASK'
       AND executor_run_time_ms > 0
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY 1,
          app_id
HAVING gc_ratio_pct IS NOT NULL
ORDER  BY 1,
          2
```

**列说明**:
| 列名 | 含义 | 单位 |
|------|------|------|
| `time` | 时间桶 | Unix 时间戳 |
| `app_id` | Spark 应用 ID | - |
| `gc_ratio_pct` | GC 时间占执行时间百分比 | % |

**使用建议**: GC 占比超过 10% 即为异常，超过 20% 会严重影响性能。GC 突增通常与数据量增大或内存配置不当有关。结合 `jvm_gc_metrics` 表可进一步区分 Full GC 和 Minor GC。

### 数据倾斜检测（table）

**用途**: 从 `stage_governance` 表中检索数据倾斜严重的 Stage，展示时长倾斜率、IO 倾斜率等关键指标。

**SQL 查询**:
```sql
SELECT app_id,
       stage_id,
       task_count,
       ROUND(stage_duration_ms / 1000, 1)    AS stage_duration_sec,
       ROUND(avg_task_duration_ms / 1000, 1) AS avg_task_sec,
       ROUND(max_task_duration_ms / 1000, 1) AS max_task_sec,
       ROUND(duration_skew_ratio, 2)         AS duration_skew,
       ROUND(io_read_skew_ratio, 2)          AS io_read_skew,
       ROUND(shuffle_read_skew_ratio, 2)     AS shuffle_skew,
       ROUND(gc_overhead_ratio * 100, 1)     AS gc_pct,
       ROUND(cpu_efficiency * 100, 1)        AS cpu_eff_pct
FROM   stage_governance
WHERE  ( duration_skew_ratio > 2.0
          OR io_read_skew_ratio > 3.0
          OR shuffle_read_skew_ratio > 3.0 )
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
ORDER  BY duration_skew_ratio DESC
LIMIT  20
```

**列说明**:
| 列名 | 含义 | 单位 |
|------|------|------|
| `app_id` | Spark 应用 ID | - |
| `stage_id` | Stage 编号 | - |
| `task_count` | Stage 内任务数 | 个 |
| `stage_duration_sec` | Stage 总时长 | 秒 |
| `avg_task_sec` | 平均任务时长 | 秒 |
| `max_task_sec` | 最大任务时长 | 秒 |
| `duration_skew` | 时长倾斜率（max/avg），>2 为倾斜 | 倍数 |
| `io_read_skew` | 读 IO 倾斜率（max/avg），>3 为严重倾斜 | 倍数 |
| `shuffle_skew` | Shuffle 读倾斜率 | 倍数 |
| `gc_pct` | GC 开销占比 | % |
| `cpu_eff_pct` | CPU 效率 | % |

**使用建议**: `duration_skew` 大于 2 表示存在倾斜，大于 5 为严重倾斜。建议通过增加分区数、使用 `repartition` 或启用 Spark AQE 的 `skewJoin` 优化来解决。`shuffle_skew` 高时需检查 Join 键的分布。

### 倾斜 Stage 数量（stat）

**用途**: 统计当前时间范围内检测到的数据倾斜 Stage 总数，提供快速概览。

**SQL 查询**:
```sql
SELECT COUNT(*) AS value
FROM   stage_governance
WHERE  ( duration_skew_ratio > 2.0
          OR io_read_skew_ratio > 3.0
          OR shuffle_read_skew_ratio > 3.0 )
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
```

**列说明**:
| 列名 | 含义 | 单位 |
|------|------|------|
| `value` | 倾斜 Stage 数量 | 个 |

**使用建议**: 配合 Grafana 阈值颜色：0 为绿色，1-3 为黄色，>3 为红色。该值为 0 时无需进一步排查。

### 平均 CPU 效率（stat）

**用途**: 展示所有 Stage 的平均 CPU 效率评分，衡量整体资源利用质量。

**SQL 查询**:
```sql
SELECT ROUND(AVG(cpu_efficiency) * 100, 1) AS value
FROM   stage_governance
WHERE  cpu_efficiency IS NOT NULL
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
```

**列说明**:
| 列名 | 含义 | 单位 |
|------|------|------|
| `value` | 平均 CPU 效率 | % |

**使用建议**: CPU 效率低于 60% 时需关注，可能存在 IO 等待、调度延迟或数据倾斜。高于 80% 为良好。该指标来自 `stage_governance.cpu_efficiency`，计算方式为 `cpu_time_ms / executor_run_time_ms`。

## 注意事项
- `stage_governance` 表的数据在 Stage 完成后由 Flink Consumer 计算，存在延迟。对于运行中的 Stage 不会出现在倾斜检测结果中。
- 倾斜阈值（`duration_skew_ratio > 2.0`、`io_read_skew_ratio > 3.0`）为经验值，可根据业务特点调整。
- GC 趋势面板中，`app_id` 较多时图线可能过于密集，建议通过 Grafana 变量过滤特定应用。
