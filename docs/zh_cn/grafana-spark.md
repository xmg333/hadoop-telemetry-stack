# Spark 引擎透视

## 概述
本仪表盘是 Spark 引擎的基础透视面板，提供最全面的 Spark 指标展示，帮助回答以下问题：
- 当前应用有多少 Task、Stage、Job？
- 是否存在数据倾斜的 Stage？
- CPU 效率如何？
- Task IO、Shuffle、内存使用情况如何？
- JVM 内存和 GC 是否正常？
- SQL 查询和表 IO 详情如何？

## 前置条件

数据源：`task_metrics`、`stage_metrics`、`job_metrics`、`jvm_memory_metrics`、`jvm_gc_metrics`、`sql_query_metrics`、`sql_query_table_metrics`、`stage_governance`、`task_histogram_buckets`

Grafana 变量：
:   `$app_id` — Spark 应用 ID（多选，含 All）
:   `$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`

变量查询：`SELECT DISTINCT app_id FROM task_metrics ORDER BY app_id`

## 面板说明

### 概览统计（4 个 stat 面板，y=0）

#### Total Tasks（stat）

**用途**: 展示选中应用的总 Task 数。

**SQL 查询**:
```sql
SELECT COUNT(DISTINCT task_id) AS value
FROM task_metrics
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
  AND app_id IN ($app_id)
```

#### Total Stages（stat）

**用途**: 展示选中应用的总 Stage 数。

**SQL 查询**:
```sql
SELECT COUNT(DISTINCT stage_id) AS value
FROM stage_metrics
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
  AND app_id IN ($app_id)
```

#### Skewed Stages（stat）

**用途**: 检测数据倾斜的 Stage 数量（`duration_skew_ratio > 2`）。阈值：0 绿色，1+ 黄色，5+ 红色。

**SQL 查询**:
```sql
SELECT COUNT(DISTINCT stage_id) AS value
FROM stage_governance
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
  AND app_id IN ($app_id)
  AND duration_skew_ratio > 2
```

**使用建议**: 如果倾斜 Stage 数 > 0，查看 Data Skew Detection 面板详情，考虑 repartition 或增加分区数。

#### Avg CPU Efficiency（stat）

**用途**: 展示所有 Stage 的平均 CPU 效率（CPU 时间 / 执行时间）。阈值：<0.3 红色，0.3-0.5 橙色，0.5-0.7 黄色，>0.7 绿色。

**SQL 查询**:
```sql
SELECT AVG(cpu_efficiency) AS value
FROM stage_governance
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
  AND app_id IN ($app_id)
  AND cpu_efficiency IS NOT NULL
```

**使用建议**: CPU 效率低于 0.5 说明任务大部分时间在等待 IO 或 GC，需检查 Shuffle 和 GC 面板。

### Task IO 与耗时趋势（y=4）

#### Task I/O Bytes（timeseries）

**用途**: 展示 4 条线：Bytes Read、Bytes Written、Shuffle Read、Shuffle Written 随时间的变化。

**SQL 查询**:
```sql
-- 4 个 target 分别查询 io_bytes_read、io_bytes_written、shuffle_bytes_read、shuffle_bytes_written
-- FROM task_metrics WHERE ... AND app_id IN ($app_id) AND duration_ms IS NOT NULL
```

**使用建议**: Shuffle 数据量大且持续增长可能说明 Join 策略不佳或分区过多。

#### Task Duration（timeseries）

**用途**: 展示 Task 耗时的 AVG / MAX / MIN 三条趋势线。

**SQL 查询**:
```sql
-- 3 个 target: AVG(duration_ms), MAX(duration_ms), MIN(duration_ms)
-- FROM task_metrics WHERE ... AND app_id IN ($app_id) AND duration_ms IS NOT NULL
```

**使用建议**: MAX 与 AVG 差距过大说明存在长尾 Task，可能是数据倾斜或资源竞争。

### JVM 监控（y=12）

#### JVM Memory（timeseries）

**用途**: 展示 Heap Used 和 Non-Heap Used 的平均内存使用趋势。

**SQL 查询**:
```sql
-- AVG(heap_used), AVG(non_heap_used) FROM jvm_memory_metrics WHERE app_id IN ($app_id)
```

**使用建议**: Heap 持续增长不回落可能存在内存泄漏。Non-Heap 异常增长检查 Metaspace 或线程栈。

#### JVM GC（timeseries）

**用途**: 展示 GC Count 和 GC Time 趋势。GC Count 在右轴，GC Time 在左轴（ms）。

**SQL 查询**:
```sql
-- SUM(gc_count), SUM(gc_time_ms) FROM jvm_gc_metrics WHERE app_id IN ($app_id)
```

**使用建议**: GC Time 占 Task Duration 比例 > 10% 需优化 JVM 参数（增大堆或切换 GC 算法）。

### Stage 与 Job（y=20）

#### Stage Duration & Task Count（timeseries）

**用途**: 双轴图展示 Stage 平均耗时和平均 Task 数。

**SQL 查询**:
```sql
-- AVG(duration_ms) AS 'Stage Duration', AVG(num_tasks) AS 'Task Count'
-- FROM stage_metrics WHERE app_id IN ($app_id)
```

#### Job Overview（table）

**用途**: 展示 Job 列表及完成状态。

**SQL 查询**:
```sql
SELECT app_id, job_id, job_success, duration_ms, num_stages,
       FROM_UNIXTIME(timestamp_ms/1000) AS completed_at
FROM job_metrics
WHERE ... AND app_id IN ($app_id)
ORDER BY timestamp_ms DESC LIMIT 100
```

**列说明**:

| 列名 | 含义 |
|------|------|
| `job_success` | 任务状态（SUCCESS 绿色 / FAILED 红色） |
| `duration_ms` | Job 耗时（ms） |
| `num_stages` | Stage 数量 |

### 治理分析（y=28）

#### Data Skew Detection（table）

**用途**: 展示存在数据倾斜的 Stage 列表，按 `duration_skew_ratio` 降序排列。倾斜比率用渐变色标注（>1.5 黄色，>2 红色）。

**SQL 查询**:
```sql
SELECT app_id, stage_id, task_count, stage_duration_ms,
       avg_task_duration_ms, max_task_duration_ms, min_task_duration_ms,
       duration_skew_ratio, io_read_skew_ratio, io_write_skew_ratio,
       shuffle_read_skew_ratio
FROM stage_governance WHERE app_id IN ($app_id)
ORDER BY timestamp_ms DESC LIMIT 50
```

**使用建议**: `duration_skew_ratio > 2` 表示某些 Task 耗时远超均值。`io_read_skew_ratio > 2` 表示读取数据不均。可考虑 `repartition()` 或 `coalesce()` 调整。

#### Resource Efficiency（table）

**用途**: 展示每个 Stage 的 CPU 效率、GC 开销、Shuffle 等待、Spill 比率等资源使用指标。关键指标用渐变色标注。

**SQL 查询**:
```sql
SELECT app_id, stage_id, task_count, cpu_efficiency, gc_overhead_ratio,
       shuffle_wait_ratio, spill_ratio, deserialize_overhead, scheduler_delay_ratio
FROM stage_governance WHERE app_id IN ($app_id)
ORDER BY timestamp_ms DESC LIMIT 50
```

**关键指标阈值**:
| 指标 | 黄色 | 红色 |
|------|------|------|
| `cpu_efficiency` | 0.5 | <0.5 |
| `gc_overhead_ratio` | 0.05 | >0.1 |
| `shuffle_wait_ratio` | 0.05 | >0.1 |
| `spill_ratio` | 0.1 | >0.3 |

### 直方图与小文件（y=36）

#### Task Duration Histogram（barchart）

**用途**: 展示 Task 耗时分布直方图，X 轴为 Bucket 上界（ms），Y 轴为计数。

**SQL 查询**:
```sql
SELECT ROUND(bucket_le) AS 'Bucket LE (ms)', SUM(bucket_count) AS 'Count'
FROM task_histogram_buckets
WHERE metric_name = 'spark.task.duration_ms' AND app_id IN ($app_id)
GROUP BY bucket_le ORDER BY bucket_le
```

**使用建议**: 如果大量 Task 集中在低耗时区间但有少量在高区间，说明存在长尾问题。

#### Small File Detection（table）

**用途**: 检测小文件问题。当 `avg_output_bytes_per_task < 32MB` 时可能存在小文件问题。

**SQL 查询**:
```sql
SELECT app_id, stage_id, task_count, total_bytes_read, total_bytes_written,
       total_shuffle_bytes_read, total_shuffle_bytes_written,
       total_records_read, total_records_written, avg_output_bytes_per_task,
       avg_output_records_per_task, small_output_task_count
FROM stage_governance WHERE app_id IN ($app_id)
ORDER BY timestamp_ms DESC LIMIT 50
```

**使用建议**: `small_output_task_count > 0` 时考虑减少分区数或合并输出文件。

### Task 详情（y=44）

#### Task Detail（table，全宽）

**用途**: 展示最近 200 条 Task 的完整指标详情，包括 IO、耗时、CPU、GC、Spill 等。

**SQL 查询**:
```sql
SELECT FROM_UNIXTIME(timestamp_ms/1000) AS time, app_id, executor_id, stage_id,
       task_id, task_success, duration_ms, io_bytes_read, io_bytes_written,
       shuffle_bytes_read, shuffle_bytes_written, shuffle_fetch_wait_time_ms,
       executor_run_time_ms, executor_cpu_time_ns, jvm_gc_time_ms,
       disk_bytes_spilled, memory_bytes_spilled
FROM task_metrics WHERE app_id IN ($app_id) AND duration_ms IS NOT NULL
ORDER BY timestamp_ms DESC LIMIT 200
```

**列说明**:

| 列名 | 含义 | 单位 |
|------|------|------|
| `task_success` | OK（绿色）/ FAIL（红色） | - |
| `executor_cpu_time_ns` | Executor CPU 时间 | ns |
| `jvm_gc_time_ms` | JVM GC 耗时 | ms |
| `disk_bytes_spilled` | 磁盘溢写 | bytes |
| `memory_bytes_spilled` | 内存溢写 | bytes |

### SQL 查询分析（y=52-72）

#### SQL Queries / Avg SQL Query Duration / Total SQL Joins / SQL Table Scans/Writes（4 个 stat）

**用途**: SQL 查询概览统计，分别展示查询数、平均耗时、总 Join 数、表扫描/写入数。

#### SQL Query Duration（timeseries）

**用途**: SQL 查询的平均和最大耗时趋势。

#### SQL Shuffle Bytes（timeseries）

**用途**: SQL 查询的 Shuffle 读写数据量趋势。

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

**SQL 查询**:
```sql
SELECT FROM_UNIXTIME(timestamp_ms/1000) AS time, app_id, execution_id,
       duration_ms, shuffle_bytes_read, shuffle_bytes_written, join_count
FROM sql_query_metrics WHERE app_id IN ($app_id)
ORDER BY timestamp_ms DESC LIMIT 100
```

## 导航
顶部导航栏可快速切换到：
- **Overview** — 平台总览
- **MapReduce** — MR 引擎详细透视
- **Hive on MR** / **Hive on Spark** — Hive 查询分析
- **Spark / MR / Hive** — 全引擎综合仪表盘

## 注意事项
- `$app_id` 变量从 `task_metrics` 表获取，仅包含有 Task 事件的应用
- `stage_governance` 和 `task_histogram_buckets` 的数据依赖 Stage 完成，短生命周期应用可能无数据
- JVM 面板数据来自 Executor Plugin，需确保 `spark.telemetry.metrics.stage.detailed=true` 配置
