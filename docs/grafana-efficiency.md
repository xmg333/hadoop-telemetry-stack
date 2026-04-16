# 综合效率评分

## 概述
本仪表盘从整体效率评估视角出发，帮助回答以下问题：
- 各应用的资源效率评分如何？
- 不同队列的效率是否有显著差异？
- 数据倾斜问题在全局范围内的严重程度如何？

## 前置条件
- 数据源：`metric_events` 大宽表、`stage_governance` 表（由 Flink Consumer 写入）
- Grafana 变量：`$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`
- `stage_governance` 表中的效率指标（`cpu_efficiency`、`gc_overhead_ratio` 等）由 Flink Consumer 在 Stage 完成时自动计算

## 面板说明

### 应用效率评分（table）

**用途**: 综合各维度指标为每个应用计算效率评分，快速定位低效应用。

**SQL 查询**:
```sql
SELECT app_id,
       app_name,
       engine,
       COUNT(DISTINCT CASE
                        WHEN event_type = 'STAGE' THEN stage_id
                        ELSE NULL
                      END)                                                AS stage_count,
       COUNT(*)                                                          AS task_count,
       ROUND(SUM(cpu_time_ms) / 3600000, 2)                              AS cpu_hours,
       ROUND(AVG(CASE
                   WHEN executor_run_time_ms > 0
                        THEN executor_cpu_time_ns / 1000000.0 / executor_run_time_ms
                   ELSE NULL
                 END) * 100, 1)                                           AS avg_cpu_eff_pct,
       ROUND(AVG(CASE
                   WHEN duration_ms > 0 THEN jvm_gc_time_ms / duration_ms
                   ELSE NULL
                 END) * 100, 1)                                           AS avg_gc_pct,
       ROUND(SUM(memory_bytes_spilled) / NULLIF(SUM(io_bytes_read), 0) * 100, 1) AS spill_ratio_pct,
       ROUND(SUM(cpu_time_ms) / NULLIF(SUM(duration_ms), 0) * 100, 1)    AS overall_eff_pct
FROM   metric_events
WHERE  event_type IN ( 'TASK', 'MR_TASK' )
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY app_id,
          app_name,
          engine
ORDER  BY overall_eff_pct ASC
LIMIT  20
```

**列说明**:
| 列名 | 含义 | 单位 |
|------|------|------|
| `app_id` | 应用/作业 ID | - |
| `app_name` | 应用名称 | - |
| `engine` | 执行引擎（SPARK / MR） | - |
| `stage_count` | Stage 数（仅 Spark） | 个 |
| `task_count` | 任务总数 | 个 |
| `cpu_hours` | 累计 CPU 时间 | 小时 |
| `avg_cpu_eff_pct` | 平均 CPU 效率（CPU 时间 / 执行时间） | % |
| `avg_gc_pct` | 平均 GC 开销占比 | % |
| `spill_ratio_pct` | Spill 数据量占读取量的比例 | % |
| `overall_eff_pct` | 综合效率评分（CPU 时间 / 挂钟时间） | % |

**使用建议**: 按 `overall_eff_pct` 升序排列，排名靠前的应用效率最低，是优化重点。`avg_cpu_eff_pct` 低于 50% 说明 CPU 大量时间在等待 IO 或 GC。`spill_ratio_pct` 大于 10% 说明内存配置不足。`overall_eff_pct` 考虑了并行度因素，低于 30% 的应用建议增加并行度。

### 队列效率对比（table）

**用途**: 对比各 YARN 队列的资源使用效率，评估队列间资源分配的合理性。

**SQL 查询**:
```sql
SELECT queue,
       engine,
       COUNT(DISTINCT app_id)                                              AS app_count,
       ROUND(SUM(cpu_time_ms) / 3600000, 2)                                AS cpu_hours,
       ROUND(SUM(duration_ms) / 3600000, 2)                                AS wall_clock_hours,
       ROUND(SUM(cpu_time_ms) / NULLIF(SUM(duration_ms), 0) * 100, 1)      AS efficiency_pct,
       ROUND(SUM(gc_time_ms) / NULLIF(SUM(duration_ms), 0) * 100, 1)       AS gc_overhead_pct,
       ROUND(SUM(memory_bytes_spilled) / 1073741824, 2)                    AS total_spill_gb,
       ROUND(AVG(CASE
                   WHEN shuffle_fetch_wait_time_ms > 0
                        THEN shuffle_fetch_wait_time_ms / duration_ms
                   ELSE NULL
                 END) * 100, 1)                                            AS avg_shuffle_wait_pct
FROM   metric_events
WHERE  event_type IN ( 'TASK', 'MR_TASK' )
       AND queue IS NOT NULL
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY queue,
          engine
HAVING cpu_hours > 0
ORDER  BY efficiency_pct ASC
```

**列说明**:
| 列名 | 含义 | 单位 |
|------|------|------|
| `queue` | YARN 队列名称 | - |
| `engine` | 执行引擎 | - |
| `app_count` | 队列内应用数 | 个 |
| `cpu_hours` | 累计 CPU 时间 | 小时 |
| `wall_clock_hours` | 累计挂钟时间 | 小时 |
| `efficiency_pct` | 综合效率（CPU / 挂钟时间） | % |
| `gc_overhead_pct` | GC 开销占比 | % |
| `total_spill_gb` | 总 Spill 数据量 | GB |
| `avg_shuffle_wait_pct` | 平均 Shuffle 等待占比 | % |

**使用建议**: 效率最低的队列应优先优化。如果高资源队列的效率也很低，说明资源分配过多但未充分利用，可考虑缩减队列配额。`avg_shuffle_wait_pct` 高的队列可能存在网络瓶颈或 Shuffle 服务过载。

### 数据倾斜问题汇总（table）

**用途**: 汇总所有检测到的数据倾斜问题，按严重程度排序，提供全局视角的问题清单。

**SQL 查询**:
```sql
SELECT app_id,
       stage_id,
       task_count,
       ROUND(stage_duration_ms / 1000, 1)     AS stage_duration_sec,
       ROUND(duration_skew_ratio, 2)          AS duration_skew,
       ROUND(io_read_skew_ratio, 2)           AS io_read_skew,
       ROUND(shuffle_read_skew_ratio, 2)      AS shuffle_skew,
       ROUND(cpu_efficiency * 100, 1)          AS cpu_eff_pct,
       ROUND(gc_overhead_ratio * 100, 1)       AS gc_pct,
       ROUND(spill_ratio * 100, 1)             AS spill_pct,
       ROUND(shuffle_wait_ratio * 100, 1)      AS shuffle_wait_pct,
       ROUND(scheduler_delay_ratio * 100, 1)   AS scheduler_delay_pct,
       CASE
         WHEN duration_skew_ratio > 5.0 THEN 'CRITICAL'
         WHEN duration_skew_ratio > 3.0 THEN 'HIGH'
         WHEN duration_skew_ratio > 2.0 THEN 'MEDIUM'
         ELSE 'LOW'
       END                                     AS severity
FROM   stage_governance
WHERE  ( duration_skew_ratio > 2.0
          OR io_read_skew_ratio > 3.0
          OR shuffle_read_skew_ratio > 3.0 )
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
ORDER  BY duration_skew_ratio DESC
LIMIT  30
```

**列说明**:
| 列名 | 含义 | 单位 |
|------|------|------|
| `app_id` | Spark 应用 ID | - |
| `stage_id` | Stage 编号 | - |
| `task_count` | Stage 内任务数 | 个 |
| `stage_duration_sec` | Stage 执行时长 | 秒 |
| `duration_skew` | 时长倾斜率（max/avg） | 倍数 |
| `io_read_skew` | 读 IO 倾斜率 | 倍数 |
| `shuffle_skew` | Shuffle 读倾斜率 | 倍数 |
| `cpu_eff_pct` | 该 Stage 的 CPU 效率 | % |
| `gc_pct` | GC 开销占比 | % |
| `spill_pct` | Spill 占比 | % |
| `shuffle_wait_pct` | Shuffle 等待占比 | % |
| `scheduler_delay_pct` | 调度延迟占比 | % |
| `severity` | 严重程度等级（CRITICAL / HIGH / MEDIUM / LOW） | - |

**使用建议**: 优先处理 CRITICAL 和 HIGH 级别的倾斜。`severity` 字段由查询动态计算，可作为 Grafana 列样式的依据（红色/黄色/绿色）。结合 `cpu_eff_pct`、`gc_pct`、`spill_pct` 等指标可判断倾斜导致的连锁影响。`scheduler_delay_pct` 高可能是由于 Executor 资源不足导致任务排队。

## 注意事项
- 综合效率评分（`overall_eff_pct`）为简化指标，实际效率受并行度、IO 模式等多种因素影响。高并行度应用的 `overall_eff_pct` 可能超过 100%（因为多个 CPU 核心同时工作）。
- 队列效率面板依赖 `queue` 字段，该字段由 v4+ 版本的 Flink Consumer 写入。MR 任务的 `queue` 字段来自 YARN API，Spark 任务的 `queue` 字段来自 `spark.yarn.queue` 配置。
- `stage_governance` 表的数据在 Stage 完成后由 Flink Consumer 的 `StageTaskAccumulator` 计算生成。如果一个 Stage 有多个批次刷新，只在最后一个批次（Stage 完成时）生成治理记录。
- 倾斜严重程度的阈值（2.0 / 3.0 / 5.0）为经验值，对于特定业务场景可能需要调整。建议先观察一段时间的历史数据再确定合适的阈值。
