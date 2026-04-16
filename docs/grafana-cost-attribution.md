# 成本归属与资源排行

## 概述
本仪表盘从资源成本归属视角出发，帮助回答以下问题：
- 哪些用户消耗了最多的 CPU 时间？
- 哪些队列占用了最多的计算资源？
- 哪些应用是资源消耗大户？
- 每日资源消耗趋势如何变化？

## 前置条件
- 数据源：`metric_events` 大宽表（由 Flink Consumer 写入）
- Grafana 变量：`$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`
- 时间范围过滤：所有查询均使用 `timestamp_ms >= ($__unixEpochFrom() * 1000) AND timestamp_ms <= ($__unixEpochTo() * 1000)`

## 面板说明

### 用户 CPU 时间排行 TOP 20（table）

**用途**: 展示 CPU 时间消耗最多的前 20 名用户，帮助定位资源消耗大户，支撑成本分摊与容量规划。

**SQL 查询**:
```sql
SELECT user_name,
       SUM(cpu_time_ms) / 3600000                                AS cpu_hours,
       SUM(gc_time_ms) / 3600000                                 AS gc_hours,
       SUM(duration_ms) / 3600000                                AS wall_clock_hours,
       COUNT(*)                                                  AS event_count
FROM   metric_events
WHERE  event_type IN ( 'TASK', 'MR_TASK' )
       AND cpu_time_ms IS NOT NULL
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY user_name
ORDER  BY cpu_hours DESC
LIMIT  20
```

**列说明**:

| 列名 | 含义 | 单位 |
|------|------|------|
| `user_name` | 提交任务的用户名 | - |
| `cpu_hours` | 累计 CPU 时间 | 小时 |
| `gc_hours` | 累计 GC 时间 | 小时 |
| `wall_clock_hours` | 累计挂钟时间（任务执行时长） | 小时 |
| `event_count` | 事件总数（Task 级别） | 个 |

**使用建议**: 关注 `cpu_hours` 与 `wall_clock_hours` 的比值，比值越低说明 CPU 利用率越低，可能存在 IO 等待或调度延迟。GC 时间占比过高（>10%）需检查 JVM 堆配置。

### 队列资源消耗对比（barchart）

**用途**: 对比不同 YARN 队列的 CPU 时间、IO 数据量和应用数量，评估各队列的资源消耗是否均衡。

**SQL 查询**:
```sql
SELECT queue,
       engine,
       SUM(cpu_time_ms) / 3600000                                          AS cpu_hours,
       SUM(io_bytes_read + io_bytes_written) / 1073741824                  AS io_gb,
       COUNT(DISTINCT app_id)                                              AS app_count
FROM   metric_events
WHERE  event_type IN ( 'TASK', 'MR_JOB', 'MR_TASK' )
       AND queue IS NOT NULL
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY queue,
          engine
ORDER  BY cpu_hours DESC
```

**列说明**:

| 列名 | 含义 | 单位 |
|------|------|------|
| `queue` | YARN 队列名称 | - |
| `engine` | 执行引擎（SPARK / MR） | - |
| `cpu_hours` | 队列内累计 CPU 时间 | 小时 |
| `io_gb` | 队列内累计 IO 数据量 | GB |
| `app_count` | 队列内独立应用/作业数 | 个 |

**使用建议**: 如果某个队列 CPU 占比极高但应用数少，说明存在资源集中的大户应用。可通过调整队列权重或配额实现更公平的资源分配。

### 应用资源消耗排行 TOP 10（table）

**用途**: 列出资源消耗最大的前 10 个应用，包含 CPU、GC、IO、Spill 等全维度指标，便于快速定位高负载应用。

**SQL 查询**:
```sql
SELECT app_name,
       engine,
       SUM(cpu_time_ms) / 3600000                                AS cpu_hours,
       SUM(gc_time_ms) / 3600000                                 AS gc_hours,
       SUM(io_bytes_read) / 1073741824                           AS read_gb,
       SUM(io_bytes_written) / 1073741824                        AS write_gb,
       SUM(memory_bytes_spilled) / 1073741824                    AS spill_gb,
       COUNT(*)                                                  AS task_count
FROM   metric_events
WHERE  event_type IN ( 'TASK', 'MR_TASK' )
       AND app_name IS NOT NULL
       AND timestamp_ms >= ( $__unixEpochFrom() * 1000 )
       AND timestamp_ms <= ( $__unixEpochTo() * 1000 )
GROUP  BY app_name,
          engine
ORDER  BY cpu_hours DESC
LIMIT  10
```

**列说明**:

| 列名 | 含义 | 单位 |
|------|------|------|
| `app_name` | 应用/作业名称 | - |
| `engine` | 执行引擎（SPARK / MR） | - |
| `cpu_hours` | 累计 CPU 时间 | 小时 |
| `gc_hours` | 累计 GC 时间 | 小时 |
| `read_gb` | 累计读取数据量 | GB |
| `write_gb` | 累计写入数据量 | GB |
| `spill_gb` | 累计 Spill 数据量（内存 + 磁盘） | GB |
| `task_count` | 任务总数 | 个 |

**使用建议**: `spill_gb` 较大说明内存不足，需要增加 Executor 内存或调整 `spark.sql.shuffle.partitions`。结合 `cpu_hours` 与 `task_count` 可估算单任务平均 CPU 开销。

### 每日资源消耗趋势（timeseries）

**用途**: 以时间维度展示各引擎（SPARK / MR / HIVE）的每日 CPU 消耗趋势，用于追踪资源使用变化。

**SQL 查询**:
```sql
SELECT ( Floor(timestamp_ms / $__interval_ms) * $__interval_ms / 1000 ) AS time,
       engine,
       SUM(cpu_time_ms) / 3600000                                         AS cpu_hours
FROM   metric_events
WHERE  event_type IN ( 'TASK', 'MR_TASK', 'MR_JOB' )
       AND cpu_time_ms IS NOT NULL
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
| `time` | 时间桶（由 `$__interval_ms` 控制粒度） | Unix 时间戳 |
| `engine` | 执行引擎（SPARK / MR） | - |
| `cpu_hours` | 该时间桶内 CPU 时间 | 小时 |

**使用建议**: 选择时间范围大于 7 天时，建议在 Grafana 中将 `$__interval_ms` 设为 86400000（1 天）以获得日级粒度。若某引擎曲线突然上升，需检查是否有新上线的高负载作业。

## 注意事项
- `metric_events` 为统一大宽表视图，实际数据存储在 `task_metrics`、`mr_task_metrics`、`mr_job_metrics` 等物理表中。如需直接查询物理表，请将 `event_type` 替换为对应表名。
- `cpu_time_ms` 对 Spark 来源于 `executor_cpu_time_ns` 的毫秒转换，对 MR 来源于 MR Counter 中的 `CPU_MILLISECONDS`。
- `queue` 字段依赖 v4+ 版本的 Flink Consumer 写入。早期版本中该字段为空，需先执行 `v4_migration.sql` 升级。
- 排行类面板建议设置时间范围为 1 天至 7 天，过大的时间范围会导致聚合计算缓慢。
