# MapReduce 引擎透视

## 概述
本仪表盘是 MapReduce 引擎的基础透视面板，分为 Job 级别（History Server / MR Collector）和 Task 级别（MR Agent）两个区域，帮助回答以下问题：
- 有多少 MR Job？平均耗时和成功率如何？
- Map / Reduce Task 各有多少？
- Job 级别的 IO（HDFS + File）吞吐量如何？
- Task 级别的耗时分布和 IO 详情如何？

## 前置条件
- 数据源：`mr_job_metrics`、`mr_task_metrics`
- Grafana 变量：
  - `$mr_job_id` — MR Job ID（多选，含 All）
  - `$__interval_ms`, `$__unixEpochFrom()`, `$__unixEpochTo()`
- 变量查询：`SELECT DISTINCT job_id FROM (SELECT DISTINCT job_id FROM mr_job_metrics UNION SELECT DISTINCT job_id FROM mr_task_metrics) t ORDER BY job_id`
- **注意**：Task 级别面板需要部署 MR Agent（ByteBuddy 字节码增强），仅靠 MR Collector（History Server 轮询）无法获取 Task 级数据

## 面板说明

### Job Level Metrics 区域

#### Total MR Jobs（stat）

**用途**: 展示选中 Job ID 范围内的独立 MR 作业数。

**SQL 查询**:
```sql
SELECT COUNT(DISTINCT job_id) AS value
FROM mr_job_metrics
WHERE timestamp_ms >= ($__unixEpochFrom() * 1000)
  AND timestamp_ms <= ($__unixEpochTo() * 1000)
  AND job_id IN ($mr_job_id)
```

#### Avg Job Duration（stat）

**用途**: 展示平均 Job 耗时（`elapsed_time_ms`）。阈值：<5s 绿色，5-30s 黄色，>30s 红色。

**SQL 查询**:
```sql
SELECT ROUND(AVG(elapsed_time_ms)) AS value
FROM mr_job_metrics
WHERE ... AND elapsed_time_ms IS NOT NULL AND job_id IN ($mr_job_id)
```

#### Job Success Rate（stat）

**用途**: 展示 Job 成功率。基于 `state='SUCCEEDED'` 判断。

**SQL 查询**:
```sql
SELECT ROUND(
  SUM(CASE WHEN state='SUCCEEDED' THEN 1 ELSE 0 END) * 100.0
  / NULLIF(COUNT(DISTINCT job_id), 0), 1
) AS value
FROM mr_job_metrics WHERE job_id IN ($mr_job_id)
```

#### Total Map Tasks（stat）

**用途**: 展示 Job 级别的 Map Task 总数（`launched_maps`）。

#### Job IO Bytes（timeseries）

**用途**: 4 条线展示 HDFS Read / HDFS Written / File Read / File Written 随时间的变化。

**SQL 查询**:
```sql
-- 4 个 target 分别查询 hdfs_bytes_read, hdfs_bytes_written,
-- file_bytes_read, file_bytes_written
-- FROM mr_job_metrics WHERE job_id IN ($mr_job_id)
```

#### Map/Reduce Task Counts（timeseries）

**用途**: 展示 `launched_maps` 和 `launched_reduces` 的趋势。

#### CPU/GC Time（timeseries）

**用途**: 展示平均 CPU Time 和平均 GC Time 趋势。

**SQL 查询**:
```sql
-- AVG(cpu_time_ms), AVG(gc_time_ms) FROM mr_job_metrics WHERE job_id IN ($mr_job_id)
```

#### Job Detail（table）

**用途**: 展示 Job 完整信息列表。

**SQL 查询**:
```sql
SELECT FROM_UNIXTIME(timestamp_ms/1000) AS time, job_id, job_name, user_name,
       state, elapsed_time_ms, launched_maps, launched_reduces,
       hdfs_bytes_read, hdfs_bytes_written, cpu_time_ms, gc_time_ms
FROM mr_job_metrics WHERE job_id IN ($mr_job_id)
ORDER BY timestamp_ms DESC LIMIT 200
```

**列说明**:
| 列名 | 含义 | 单位 |
|------|------|------|
| `state` | Job 状态（SUCCEEDED / FAILED / KILLED） | - |
| `elapsed_time_ms` | Job 总耗时 | ms |
| `launched_maps` / `launched_reduces` | 启动的 Map / Reduce 数 | 个 |
| `hdfs_bytes_read` / `hdfs_bytes_written` | HDFS 读写字节 | bytes |
| `cpu_time_ms` / `gc_time_ms` | CPU / GC 时间 | ms |

### Task Level Metrics 区域

#### Total Tasks / Reduce Tasks（stat）

**用途**: 分别统计 Map 和 Reduce 类型的独立 Task 数。

**SQL 查询**:
```sql
-- Map: COUNT(DISTINCT task_id) FROM mr_task_metrics WHERE task_type='map' AND job_id IN ($mr_job_id)
-- Reduce: 同上 WHERE task_type='reduce'
```

#### Avg Task Duration（stat）

**用途**: 展示平均 Task 耗时。

#### Task Success Rate（stat）

**用途**: 基于 `success_count` 和 `failure_count` 计算成功率。

**SQL 查询**:
```sql
SELECT ROUND(
  SUM(COALESCE(success_count, 0)) * 100.0
  / NULLIF(SUM(COALESCE(success_count, 0)) + SUM(COALESCE(failure_count, 0)), 0), 1
) AS value
FROM mr_task_metrics WHERE job_id IN ($mr_job_id)
```

#### Total Map Output Bytes / Total Shuffle Bytes（stat）

**用途**: 分别展示 Map 输出字节总量和 Reduce Shuffle 字节总量。

#### Task Duration（timeseries）

**用途**: 展示 Task 耗时的 AVG / MAX / MIN 趋势。

#### File IO Throughput（timeseries）

**用途**: 4 条线展示 Task 级别的 File Read / File Written / HDFS Read / HDFS Written。

#### Task IO Bytes（timeseries）

**用途**: 展示 `map_output_bytes` 和 `reduce_shuffle_bytes` 趋势。

#### Task Record Counts（timeseries）

**用途**: 4 条线展示 Map Input / Map Output / Reduce Input / Reduce Output 记录数趋势。

#### Task Detail（table，全宽）

**用途**: 展示最近 200 条 Task 的完整指标详情。

**SQL 查询**:
```sql
SELECT FROM_UNIXTIME(timestamp_ms/1000) AS time, task_id, task_type, job_id,
       duration_ms, success_count, map_input_records, map_output_records,
       map_output_bytes, reduce_input_records, reduce_output_records,
       reduce_shuffle_bytes, spilled_records, hdfs_bytes_read, hdfs_bytes_written,
       hdfs_read_ops, hdfs_write_ops, file_read_ops, file_write_ops
FROM mr_task_metrics WHERE job_id IN ($mr_job_id)
ORDER BY timestamp_ms DESC LIMIT 200
```

**列说明**:
| 列名 | 含义 | 单位 |
|------|------|------|
| `map_output_bytes` | Map 输出字节数 | bytes |
| `reduce_shuffle_bytes` | Reduce Shuffle 字节数 | bytes |
| `spilled_records` | 溢写记录数 | 条 |
| `hdfs_read_ops` / `hdfs_write_ops` | HDFS 读写操作数 | 次 |
| `file_read_ops` / `file_write_ops` | 本地文件读写操作数 | 次 |

## 导航
顶部导航栏可快速切换到：
- **Overview** — 平台总览
- **Spark** — Spark 引擎详细透视
- **Hive on MR** / **Hive on Spark** — Hive 查询分析
- **Spark / MR / Hive** — 全引擎综合仪表盘

## 注意事项
- Task 级别数据（面板 10-16）依赖 MR Agent 部署。如果只部署了 MR Collector（History Server 轮询），Task 区域将无数据
- `$mr_job_id` 变量合并了 `mr_job_metrics` 和 `mr_task_metrics` 的 job_id
- `elapsed_time_ms` 是 Job 级别耗时，`duration_ms` 是 Task 级别耗时，语义不同
