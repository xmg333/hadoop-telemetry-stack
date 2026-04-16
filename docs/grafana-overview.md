# 平台总览

## 概述
本仪表盘是整个平台的全局概览，一目了然地展示三大引擎（Spark / MR / Hive）的核心指标，帮助回答以下问题：
- 当前有多少 Spark 应用、MR 作业、Hive 查询在运行？
- MR 作业成功率是否正常？
- 各引擎的成功率对比如何？
- IO 吞吐量随时间如何变化？
- Job/Query 的平均耗时趋势如何？
- 最近有哪些失败的任务？

## 前置条件
- 数据源：11 张独立指标表（由 Flink Consumer 写入）
- Grafana 变量：`$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`
- 时间范围过滤：所有查询均使用 `timestamp_ms >= ($__unixEpochFrom() * 1000) AND timestamp_ms <= ($__unixEpochTo() * 1000)`

## 面板说明

### Total Spark Apps（stat）

**用途**: 展示时间范围内出现的独立 Spark 应用数。

**SQL 查询**:
```sql
SELECT COUNT(DISTINCT app_id) AS value
FROM task_metrics
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
```

**使用建议**: 正常运行时应看到与应用提交频率匹配的数量。如果突降为 0，检查 Spark Plugin 或 OTel Collector 是否正常。

### Total MR Jobs（stat）

**用途**: 展示时间范围内完成的独立 MR 作业数。

**SQL 查询**:
```sql
SELECT COUNT(DISTINCT job_id) AS value
FROM mr_job_metrics
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
```

### Total Hive Queries（stat）

**用途**: 展示时间范围内的 Hive 查询总数。

**SQL 查询**:
```sql
SELECT COUNT(*) AS value
FROM hive_query_metrics
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
```

### MR Job Success Rate（stat）

**用途**: 展示 MR 作业的成功率百分比。阈值：<80% 红色，80-95% 黄色，>95% 绿色。

**SQL 查询**:
```sql
SELECT ROUND(
  SUM(CASE WHEN state='SUCCEEDED' THEN 1 ELSE 0 END) * 100.0
  / NULLIF(COUNT(DISTINCT job_id), 0), 1
) AS value
FROM mr_job_metrics
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
```

**使用建议**: 成功率低于 80% 需要立即排查。检查失败作业的日志和资源分配。

### Success Rates by Engine（piechart）

**用途**: 以环形图形式对比三大引擎（Spark / MR / Hive）的成功与失败数量分布。

**SQL 查询**:
```sql
-- 从 job_metrics、mr_job_metrics、hive_query_metrics 三个表聚合
-- 分别统计 Spark OK/Fail、MR OK/Fail、Hive OK/Fail
SELECT
  SUM(CASE WHEN job_success='true' THEN 1 ELSE 0 END) AS 'Spark OK',
  SUM(CASE WHEN job_success='false' AND duration_ms IS NULL ... END) AS 'Spark Fail',
  (SELECT COUNT(DISTINCT job_id) FROM mr_job_metrics WHERE state='SUCCEEDED' ...) AS 'MR OK',
  (SELECT COUNT(DISTINCT job_id) FROM mr_job_metrics WHERE state!='SUCCEEDED' ...) AS 'MR Fail',
  (SELECT COUNT(*) FROM hive_query_metrics WHERE success='true' ...) AS 'Hive OK',
  (SELECT COUNT(*) FROM hive_query_metrics WHERE success='false' ...) AS 'Hive Fail'
FROM job_metrics WHERE ...
```

**使用建议**: 快速判断哪个引擎存在稳定性问题。如果某个引擎失败占比偏高，深入到对应引擎的专属仪表盘排查。

### IO Throughput by Engine（timeseries）

**用途**: 展示各引擎随时间的 IO 吞吐量趋势（字节），包含 Spark、MapReduce、Hive 三条线。

**SQL 查询**:
```sql
-- Spark: SUM(io_bytes_read + io_bytes_written) FROM task_metrics
-- MapReduce: SUM(hdfs_bytes_read + hdfs_bytes_written) FROM mr_job_metrics
-- Hive: SUM(input_bytes + output_bytes) FROM hive_query_metrics
```

**使用建议**: 观察 IO 峰值是否与业务周期一致。如果某时段 IO 异常高，可能与数据倾斜或大查询有关。

### Job Duration Trends（timeseries）

**用途**: 展示 Spark Job 和 MR Job 的平均耗时随时间的趋势对比。

**SQL 查询**:
```sql
-- Spark: AVG(duration_ms) FROM job_metrics
-- MR: AVG(elapsed_time_ms) FROM mr_job_metrics
```

**使用建议**: 耗时突增可能由数据量增大、资源不足、或 GC 问题导致。与 GC 面板交叉验证。

### Recent Failed Tasks（table）

**用途**: 展示最近 50 条失败任务列表，包含 Spark 和 MR 的 UNION 合并结果。

**SQL 查询**:
```sql
SELECT 'Spark' AS engine, app_id AS id, stage_id, task_id, duration_ms,
       FROM_UNIXTIME(timestamp_ms/1000) AS time
FROM task_metrics WHERE task_success='false' ...
UNION ALL
SELECT 'MR' AS engine, job_id AS id, NULL AS stage_id, task_id,
       cpu_time_ms AS duration_ms, FROM_UNIXTIME(timestamp_ms/1000) AS time
FROM mr_task_metrics WHERE state != 'SUCCEEDED' ...
ORDER BY time DESC LIMIT 50
```

**列说明**:
| 列名 | 含义 | 单位 |
|------|------|------|
| `engine` | 执行引擎（Spark / MR） | - |
| `id` | 应用 ID 或 Job ID | - |
| `stage_id` | Stage 编号（仅 Spark） | - |
| `task_id` | Task 编号 | - |
| `duration_ms` | 任务耗时 | ms |
| `time` | 事件发生时间 | - |

**使用建议**: 定期检查此面板。如果同一 app_id 反复出现失败，需深入 Spark/MR 专属面板排查。

## 导航
本仪表盘顶部导航栏可快速切换到：
- **Spark** — Spark 引擎详细透视
- **MapReduce** — MR 引擎详细透视
- **Hive on MR** — Hive on MR 查询分析
- **Hive on Spark** — Hive on Spark 查询分析
- **Spark / MR / Hive** — 全引擎综合仪表盘

## 注意事项
- 本仪表盘查询 11 张独立表，不使用大宽表 `metric_events`
- MR 任务级别的失败检测依赖 MR Agent 是否部署
- Hive 查询统计不区分执行引擎（MR / Spark / Tez），需在 Hive 专属面板过滤
