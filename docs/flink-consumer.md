# Flink Metrics Consumer — 部署与可视化参考

## 架构

Flink Consumer 基于 Flink DataStream API 构建，使用 Flink KafkaSource 消费 Kafka 中的 OTLP protobuf 指标数据，经反序列化、分类、宽行聚合后写入 MySQL 或 ClickHouse。

```
Kafka (OTLP Protobuf) → Flink KafkaSource → OtlpDeserializationSchema
  → MetricRecordSplitFlatMap → MetricNameFilter → AccumulatingProcessFunction
  → FlinkCategoryJdbcSink → MySQL / ClickHouse
```

核心算子：
- **OtlpDeserializationSchema**: 将 Kafka 中的 OTLP protobuf 字节反序列化为 `MetricRecord`（含 `MetricSample` + `HistogramBucket`）
- **MetricRecordSplitFlatMap**: 将 `MetricRecord` 拆分为独立的 `MetricItem`（单个 sample 或 bucket）
- **AccumulatingProcessFunction**: Flink `ProcessFunction`，使用 `ValueState<WideRowAccumulator>` 管理聚合状态，基于 batch-size 阈值和 processing-time timer 触发 flush。支持 Flink checkpoint 故障恢复
- **FlinkCategoryJdbcSink**: Flink `RichSinkFunction`，管理 JDBC 连接生命周期（`open()`/`close()`），将 `FlushResult` 写入 15 张分类表 + `metric_events` 统一宽表

## 部署

### 数据库准备

#### MySQL

```sql
CREATE DATABASE IF NOT EXISTS metrics_db;
CREATE USER 'metrics'@'%' IDENTIFIED BY 'metrics';
GRANT ALL PRIVILEGES ON metrics_db.* TO 'metrics'@'%';
```

启动时自动创建 15 张分类表 + `metric_events` 统一宽表，无需手动建表。

#### ClickHouse

```sql
CREATE DATABASE IF NOT EXISTS metrics_db;
```

同样自动建表，使用 ClickHouse 特有类型（`DateTime64(3)`、`LowCardinality(String)`、`Nullable(Float64)` 等），按月分区。

### 配置

创建 `flink-consumer.conf`：

```hocon
flink-consumer {
  kafka {
    bootstrap.servers = "kafka:9092"
    topic = "telemetry-metrics"
    group.id = "flink-metrics-consumer"
    startup.mode = "earliest-offset"  # earliest-offset | latest-offset
    checkpoint.path = "/tmp/flink-consumer-checkpoint"  # Flink checkpoint 存储路径
  }

  sink {
    type = "mysql"  # mysql | clickhouse

    mysql {
      url = "jdbc:mysql://mysql:3306/metrics_db"
      user = "metrics"
      password = "metrics"
      batch.size = 1000
      flush.interval.ms = 5000
    }

    clickhouse {
      url = "jdbc:clickhouse://clickhouse:8123/metrics_db"
      user = "default"
      password = ""
      batch.size = 5000
      flush.interval.ms = 3000
    }
  }

  filter {
    metric.name.include = [".*"]
    metric.name.exclude = []
  }

  processing {
    parallelism = 2
  }
}
```

### 运行

```bash
# 独立运行（自包含 JAR，使用 Flink LocalEnvironment）
java -jar metrics-flink-consumer-dist.jar flink-consumer.conf

# 提交到 Flink 集群（推荐生产环境）
/opt/flink-1.18.0/bin/flink run -c x.mg.metrics.flink.Main \
  -m localhost:8081 \
  metrics-flink-consumer-dist.jar flink-consumer.conf

# 后台提交（断开 SSH 不中断）
nohup /opt/flink-1.18.0/bin/flink run -c x.mg.metrics.flink.Main \
  -m localhost:8081 \
  metrics-flink-consumer-dist.jar flink-consumer.conf \
  > /tmp/flink-submit.log 2>&1 &

# 查看集群作业状态
curl -s http://localhost:8081/jobs | python3 -c \
  'import json,sys; [print(j["id"],j["status"]) for j in json.load(sys.stdin)["jobs"]]'

# 取消作业
/opt/flink-1.18.0/bin/flink cancel <job-id>
```

### 注意事项

- 启动时自动创建所有表，无需手动建表
- 所有标签已展开为显式类型化列，不使用 JSON 数据类型
- `Double.POSITIVE_INFINITY`（OTLP 直方图桶）自动转换为 `Double.MAX_VALUE`
- Flink 1.18.0（最后支持 Java 8 的版本）
- Kafka offset 由 Flink checkpoint 管理，保证 at-least-once 语义。`checkpoint.path` 为 Flink checkpoint 存储目录（不再使用单文件偏移量持久化）
- `app_id` 为空时自动回退为 `"unknown"`
- 所有传入 Flink 算子的对象（model classes、config、filter）均已实现 `Serializable`
- 聚合状态通过 `ValueState` 管理，支持 checkpoint 故障恢复

---

## 数据库表结构

| 表名 | 说明 |
|------|------|
| `task_metrics` | 每个 Task 完成事件一行，9 维度列 + 23 指标列 |
| `stage_metrics` | 每个 Stage 完成事件一行 |
| `job_metrics` | 每个 Job 事件一行 |
| `jvm_memory_metrics` | JVM 内存时序数据 |
| `jvm_gc_metrics` | JVM GC 时序数据 |
| `task_histogram_buckets` | Task 级直方图桶分布 |
| `stage_histogram_buckets` | Stage 级直方图桶分布 |
| `job_histogram_buckets` | Job 级直方图桶分布 |
| `stage_governance` | Stage 治理指标（预聚合） |
| `sql_query_metrics` | Spark SQL 执行指标（join/shuffle bytes 等，含 query_text） |
| `sql_query_table_metrics` | Spark SQL 表级 IO 指标 |
| `hive_query_metrics` | Hive 查询执行指标（duration/success/IO，含 query_text） |
| `hive_table_io_metrics` | Hive 查询表级 IO 指标 |
| `mr_job_metrics` | MR Collector 作业级指标 |
| `mr_task_metrics` | MR Agent 任务级指标 |
| `metric_events` | 跨引擎统一宽表，按 engine（SPARK/MR/HIVE）和 event_type 分区 |

### task_metrics

**维度列：** `timestamp_ms`, `app_id`, `executor_id`, `stage_id`, `task_id`, `task_success`, `task_host`, `task_locality`, `task_speculative`

**指标列：** `duration_ms`, `io_bytes_read`, `io_bytes_written`, `io_records_read`, `io_records_written`, `shuffle_bytes_read`, `shuffle_bytes_written`, `shuffle_fetch_wait_time_ms`, `disk_bytes_spilled`, `memory_bytes_spilled`, `executor_run_time_ms`, `executor_cpu_time_ns`, `deserialize_time_ms`, `deserialize_cpu_time_ns`, `result_serialization_time_ms`, `jvm_gc_time_ms`, `scheduler_delay_ms`, `result_size_bytes`, `peak_execution_memory_bytes`, `shuffle_local_blocks_fetched`, `shuffle_records_read`, `shuffle_remote_bytes_read_to_disk`, `shuffle_remote_reqs_duration_ms`

### stage_metrics

**维度列：** `timestamp_ms`, `app_id`, `executor_id`, `stage_id`

**指标列：** `duration_ms`, `num_tasks`, `executor_run_time_ms`, `executor_cpu_time_ns`, `jvm_gc_time_ms`, `peak_execution_memory_bytes`, `io_bytes_read`, `io_bytes_written`

### job_metrics

**维度列：** `timestamp_ms`, `app_id`, `job_id`, `job_success`

**指标列：** `duration_ms`, `num_stages`

### jvm_memory_metrics

**维度列：** `timestamp_ms`, `app_id`, `executor_id`

**指标列：** `heap_used`, `non_heap_used`

### jvm_gc_metrics

**维度列：** `timestamp_ms`, `app_id`, `executor_id`, `gc_name`

**指标列：** `gc_count`, `gc_time_ms`

### sql_query_metrics

**维度列：** `timestamp_ms`, `app_id`, `execution_id`, `app_name`, `user_name`, `queue`

**指标列：** `duration_ms`, `shuffle_bytes_read`, `shuffle_bytes_written`, `join_count`

**文本列：** `query_text`（TEXT，SQL 查询文本，截断到最大 4096 字符）

### hive_query_metrics

**维度列：** `timestamp_ms`, `query_id`, `operation`, `user_name`, `success`, `execution_engine`

**指标列：** `duration_ms`, `success_count`, `failure_count`, `input_bytes`, `output_bytes`, `input_rows`, `output_rows`

**文本列：** `query_text`（TEXT，Hive SQL 查询文本，截断到最大 4096 字符）

### 直方图桶表

`task_histogram_buckets`、`stage_histogram_buckets`、`job_histogram_buckets` 维度列与对应 metrics 表一致，额外包含 `metric_name`、`bucket_le`、`bucket_count` 三列。

### metric_events

**统一跨引擎宽表**，整合全部 15 张分类表数据。每个事件一行，通过 `engine`（SPARK/MR/HIVE）和 `event_type`（TASK/STAGE/JOB/JVM_MEMORY/JVM_GC/SQL_QUERY/SQL_TABLE_IO/HIVE_QUERY/HIVE_TABLE_IO/MR_JOB/MR_TASK）区分事件类型。

**核心维度列：** `timestamp_ms`, `event_type`, `engine`, `status`, `app_id`, `app_name`, `user_name`, `queue`

**归一化指标列：** `duration_ms`, `io_bytes_read`, `io_bytes_written`, `shuffle_bytes_read`, `shuffle_bytes_written`, `cpu_time_ms`, `gc_time_ms`, `memory_bytes_spilled`

**引擎特定维度/指标列：** 保留各引擎独有字段（如 Spark 的 `executor_id`/`stage_id`/`task_id`，MR 的 `job_id`/`launched_maps`/`hdfs_bytes_read`，Hive 的 `operation`/`execution_engine`），NULL 表示不适用。

**文本列：** `query_text`（TEXT，SQL 查询文本）

完整建表语句见 `deploy/sql/v5_migration.sql`。`query_text` 列由 `deploy/sql/v6_migration.sql` 添加。

---

## Stage 治理指标（stage_governance）

Flink Consumer 在每个 Stage 完成时自动预聚合一行治理指标，Grafana 可直接查单表出图。

### 治理维度

| 维度 | 计算方式 | 异常阈值 |
|------|---------|---------|
| Duration 倾斜 | `duration_skew_ratio` = max / avg task 时长 | > 2 倾斜 |
| IO 倾斜 | `io_read_skew_ratio`、`io_write_skew_ratio`、`shuffle_read_skew_ratio` | > 2 倾斜 |
| 小文件检测 | `avg_output_bytes_per_task`、`small_output_task_count`（< 32MB） | task 数 > 5 |
| CPU 效率 | `cpu_efficiency` = cpu_time / run_time | < 0.5 低效 |
| GC 开销 | `gc_overhead_ratio` = gc_time / run_time | > 0.1 需调优 |
| Shuffle 开销 | `shuffle_wait_ratio` = fetch_wait / run_time | > 0.1 |
| Spill 比例 | `spill_ratio` = disk_spilled / bytes_read | > 0.3 |
| 反序列化开销 | `deserialize_overhead` = deserialize_time / run_time | — |
| 调度延迟 | `scheduler_delay_ratio` = scheduler_delay / total_duration | — |

### 表结构

```sql
CREATE TABLE stage_governance (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp_ms BIGINT NOT NULL,
    app_id VARCHAR(255) NOT NULL,
    stage_id INT NOT NULL,
    task_count INT NOT NULL,

    -- Duration
    stage_duration_ms DOUBLE,
    avg_task_duration_ms DOUBLE,
    max_task_duration_ms DOUBLE,
    min_task_duration_ms DOUBLE,
    duration_skew_ratio DOUBLE,

    -- IO
    total_bytes_read DOUBLE,
    total_bytes_written DOUBLE,
    total_shuffle_bytes_read DOUBLE,
    total_shuffle_bytes_written DOUBLE,
    total_records_read DOUBLE,
    total_records_written DOUBLE,

    -- IO skew
    io_read_skew_ratio DOUBLE,
    io_write_skew_ratio DOUBLE,
    shuffle_read_skew_ratio DOUBLE,

    -- Small files
    avg_output_bytes_per_task DOUBLE,
    avg_output_records_per_task DOUBLE,
    small_output_task_count INT,

    -- Resource efficiency
    cpu_efficiency DOUBLE,
    gc_overhead_ratio DOUBLE,
    shuffle_wait_ratio DOUBLE,
    spill_ratio DOUBLE,
    deserialize_overhead DOUBLE,
    scheduler_delay_ratio DOUBLE,

    -- Memory
    max_peak_memory_bytes DOUBLE,
    total_memory_spilled DOUBLE,

    INDEX idx_app_time (app_id, timestamp_ms),
    INDEX idx_stage (app_id, stage_id)
);
```

---

## Grafana 可视化

预构建仪表盘：`deploy/grafana/spark-mr-telemetry-dashboard.json`

### 导入

1. Grafana → Dashboards → Import
2. 上传 `deploy/grafana/spark-mr-telemetry-dashboard.json`
3. 选择 MySQL 数据源
4. 保存

### 数据源配置

**MySQL**（默认）：URL `mysql:3306`，Database `metrics_db`，User/Password `metrics` / `metrics`

**ClickHouse**：URL `http://clickhouse:8123`，Database `metrics_db`。需调整 SQL 时间函数：
- `$__unixEpochFilter(timestamp_ms/1000)` → `$__timeFilter_ms(timestamp_ms)`
- `$__unixEpochGroup(timestamp_ms/1000, $__interval)` → `$__timeInterval_ms(timestamp_ms)`
- `FROM_UNIXTIME(timestamp_ms/1000, ...)` → `DateTime(timestamp_ms)`

### 仪表盘面板

| 面板 | 类型 | 数据来源 | 说明 |
|------|------|---------|------|
| Total Tasks / Stages | Stat | task/stage_metrics | 总计数 |
| Skewed Stages | Stat | stage_governance | skew_ratio > 2（红/黄/绿） |
| Avg CPU Efficiency | Stat | stage_governance | < 0.5 红，> 0.7 绿 |
| Task I/O Bytes | Time Series | task_metrics | IO/Shuffle 读写趋势 |
| Task Duration | Time Series | task_metrics | avg/max/min 趋势 |
| JVM Memory | Time Series | jvm_memory_metrics | 堆/非堆趋势 |
| JVM GC | Time Series | jvm_gc_metrics | GC 次数与耗时 |
| Stage Duration & Task Count | Time Series | stage_metrics | Stage 时长与任务数 |
| Job Overview | Table | job_metrics | 作业列表 |
| Data Skew Detection | Table | stage_governance | 倾斜检测（颜色阈值） |
| Resource Efficiency | Table | stage_governance | CPU/GC/Shuffle/Spill 效率 |
| Task Duration Histogram | Bar Chart | task_histogram_buckets | 时长分布 |
| Small File Detection | Table | stage_governance | 小文件检测 |
| Task Detail | Table | task_metrics | 最新 200 条任务明细 |

模板变量：`$app_id`（应用 ID 筛选，多选）

### 告警阈值

| 指标 | 绿色 | 黄色 | 红色 |
|------|------|------|------|
| duration_skew_ratio | < 1.5 | 1.5 ~ 2 | > 2 |
| cpu_efficiency | > 0.7 | 0.5 ~ 0.7 | < 0.5 |
| gc_overhead_ratio | < 0.05 | 0.05 ~ 0.1 | > 0.1 |
| shuffle_wait_ratio | < 0.05 | 0.05 ~ 0.1 | > 0.1 |
| spill_ratio | < 0.1 | 0.1 ~ 0.3 | > 0.3 |
| avg_output_bytes_per_task | > 32MB | 1MB ~ 32MB | < 1MB |
| small_output_task_count | 0 | 1 ~ 5 | > 5 |

### 治理查询示例（MySQL）

```sql
-- 数据倾斜
SELECT app_id, stage_id, task_count, duration_skew_ratio,
       max_task_duration_ms, avg_task_duration_ms
FROM stage_governance WHERE duration_skew_ratio > 2
ORDER BY timestamp_ms DESC LIMIT 20;

-- 小文件
SELECT app_id, stage_id, task_count, avg_output_bytes_per_task,
       small_output_task_count
FROM stage_governance WHERE avg_output_bytes_per_task < 33554432
ORDER BY timestamp_ms DESC LIMIT 20;

-- CPU 效率低
SELECT app_id, stage_id, task_count, cpu_efficiency,
       avg_task_duration_ms, total_bytes_read
FROM stage_governance WHERE cpu_efficiency < 0.5
ORDER BY timestamp_ms DESC LIMIT 20;

-- GC 开销高
SELECT app_id, stage_id, gc_overhead_ratio, avg_task_duration_ms, task_count
FROM stage_governance WHERE gc_overhead_ratio > 0.1
ORDER BY timestamp_ms DESC LIMIT 20;

-- Shuffle 开销大
SELECT app_id, stage_id, shuffle_wait_ratio,
       total_shuffle_bytes_read, total_shuffle_bytes_written
FROM stage_governance WHERE shuffle_wait_ratio > 0.1
ORDER BY timestamp_ms DESC LIMIT 20;

-- 应用 Stage 概览
SELECT app_id, stage_id, task_count, stage_duration_ms, avg_task_duration_ms,
       duration_skew_ratio, cpu_efficiency, gc_overhead_ratio,
       total_bytes_written, avg_output_bytes_per_task
FROM stage_governance WHERE app_id = '$app_id' ORDER BY stage_id;
```
