# Spark Telemetry Listener — 部署与使用手册

## 目录

1. [产品概述](#1-产品概述)
2. [系统架构](#2-系统架构)
3. [编译构建](#3-编译构建)
4. [Spark 插件部署](#4-spark-插件部署)
5. [MR Telemetry Collector 部署](#5-mr-telemetry-collector-部署)
6. [MR Telemetry Agent 部署](#6-mr-telemetry-agent-部署)
7. [Flink Metrics Consumer 部署](#7-flink-metrics-consumer-部署)
8. [OTel Collector 配置](#8-otel-collector-配置)
9. [指标参考](#9-指标参考)
10. [Grafana 可视化](#10-grafana-可视化)
11. [配置参数完整参考](#11-配置参数完整参考)
12. [常见问题与排查](#12-常见问题与排查)

---

## 1. 产品概述

Spark Telemetry Listener 是一套透明的 Spark / MapReduce 可观测性方案，通过 OpenTelemetry 协议将大数据任务的 IO、CPU、GC 等指标导出到 OTel Collector，再经由 Kafka 持久化到 MySQL 或 ClickHouse，最终通过 Grafana 进行可视化。

### 核心组件

| 组件 | 类型 | 说明 |
|------|------|------|
| **Spark Telemetry Plugin** | Spark 插件 | 捕获 Spark 任务/阶段 IO 指标及 JVM 系统指标 |
| **MR Telemetry Collector** | 独立 Java 应用 | 轮询 Hadoop History Server REST API 采集 MR 作业指标 |
| **MR Telemetry Agent** | Java Agent | 字节码增强方式实时采集 MR 任务级指标 |
| **Flink Metrics Consumer** | Flink 作业 | 消费 Kafka 中的 OTLP 指标写入 MySQL / ClickHouse |

### 支持的 Spark 版本

| Spark 版本 | Scala 版本 | Maven Profile | 插件加载方式 |
|------------|-----------|---------------|-------------|
| Spark 2.4.x | 2.11 | `spark-2` | `spark.extraListeners` |
| Spark 3.5.x | 2.12 | `spark-3` (默认) | `SparkPlugin` API |
| Spark 4.0.x | 2.13 | `spark-4` | `SparkPlugin` API |

---

## 2. 系统架构

### 整体数据流

```
┌──────────────────────────────────────────────────────────────────────┐
│                         数据采集层                                    │
│                                                                      │
│  ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐ │
│  │  Spark Plugin    │   │  MR Collector    │   │  MR Agent        │ │
│  │  (Task/Stage/JVM)│   │  (History Server)│   │  (字节码增强)     │ │
│  └────────┬─────────┘   └────────┬─────────┘   └────────┬─────────┘ │
│           │ OTLP gRPC           │ OTLP gRPC           │ OTLP gRPC  │
└───────────┼─────────────────────┼─────────────────────┼────────────┘
            │                     │                     │
            ▼                     ▼                     ▼
┌──────────────────────────────────────────────────────────────────────┐
│                         OTel Collector                               │
│           (接收 OTLP → 调试输出 + Kafka 导出)                         │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ Kafka (OTLP Protobuf)
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    Flink Metrics Consumer                             │
│           (Kafka → 批量写入 MySQL / ClickHouse)                       │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│              MySQL / ClickHouse + Grafana                             │
│           (时序存储 + 可视化仪表盘)                                     │
└──────────────────────────────────────────────────────────────────────┘
```

### Spark 插件内部数据流

```
SparkTelemetryPlugin (SparkPlugin API)
  ├── TelemetryDriverPlugin
  │     └── TelemetryLifecycle (单例)
  │           └── SparkTelemetryListener (SparkListener)
  │                 ├── onTaskEnd → SparkMetricEvent(TASK_END)
  │                 ├── onStageCompleted → SparkMetricEvent(STAGE_COMPLETE)
  │                 └── onJobStart/End → SparkMetricEvent(JOB_*)
  │
  └── TelemetryExecutorPlugin
        └── TelemetryLifecycle + SparkTelemetryMetricsSink
              └── JVM Metrics (Memory, GC, BufferPool)

TelemetryLifecycle.accept()
  └── MetricRecorder (OTel SDK Meter API)
        └── OtelRegistry
              └── PeriodicMetricReader → OTLP gRPC Exporter → OTel Collector
```

---

## 3. 编译构建

### 前置条件

- JDK 8（Spark 2/3）或 JDK 17+（Spark 4）
- Maven 3.6+

### 构建命令

```bash
# 构建 Spark 3.x 版本（默认）
mvn clean package -DskipTests

# 构建 Spark 2.x 版本
mvn clean package -Pspark-2 -DskipTests

# 构建 Spark 4.x 版本（需要 JDK 17+）
mvn clean package -Pspark-4 -DskipTests

# 构建 Flink Consumer
mvn clean package -pl metrics-flink-consumer,metrics-flink-consumer-dist -am -DskipTests

# 运行单元测试
mvn test

# 运行集成测试（仅 Spark 3）
mvn verify -Pspark-3
```

### 产出物

| 构建产物 | 路径 | 说明 |
|---------|------|------|
| Spark 2 Plugin | `spark-telemetry-dist-spark2/target/*.jar` | 自包含 Shaded JAR |
| Spark 3 Plugin | `spark-telemetry-dist-spark3/target/*.jar` | 自包含 Shaded JAR |
| Spark 4 Plugin | `spark-telemetry-dist-spark4/target/*.jar` | 自包含 Shaded JAR |
| MR Collector | `mr-telemetry-dist/target/*.jar` | 自包含 Shaded JAR |
| MR Agent | `mr-telemetry-agent-dist/target/*.jar` | Java Agent JAR |
| Flink Consumer | `metrics-flink-consumer-dist/target/*.jar` | 自包含 Shaded JAR |

所有 Distribution JAR 均通过 `maven-shade-plugin` 打包，OTel、gRPC、Protobuf 等依赖已 relocate 到 `x.mg.metrics.shaded.*` 命名空间下，不会与宿主环境产生冲突。

---

## 4. Spark 插件部署

### 4.1 Spark 3.x / 4.x 部署（SparkPlugin API）

#### 步骤 1：分发 JAR

将构建好的 Shaded JAR 分发到所有 Spark 节点可达路径：

```bash
# 方式 A：放到 HDFS
hdfs dfs -put spark-telemetry-dist-spark3/target/spark-telemetry-plugin.jar /spark/libs/

# 方式 B：放到每个节点的本地路径
scp spark-telemetry-dist-spark3/target/spark-telemetry-plugin.jar node:/opt/spark/libs/
```

#### 步骤 2：配置 Spark

通过 `spark-submit` 参数或 `spark-defaults.conf` 配置：

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --jars /opt/spark/libs/spark-telemetry-plugin.jar \
  --conf spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin \
  --conf spark.telemetry.otel.exporter.endpoint=http://otel-collector:4317 \
  --conf spark.telemetry.otel.service.name=my-spark-app \
  --conf spark.telemetry.otel.export.interval.ms=10000 \
  your-app.jar
```

或者写入 `spark-defaults.conf`：

```properties
spark.plugins              x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin
spark.telemetry.otel.exporter.endpoint    http://otel-collector:4317
spark.telemetry.otel.service.name         my-spark-app
spark.telemetry.otel.export.interval.ms   10000
```

#### 最小配置（必填）

| 参数 | 说明 | 示例 |
|------|------|------|
| `spark.plugins` | Spark 插件类名 | `x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin` |
| `spark.telemetry.otel.exporter.endpoint` | OTel Collector gRPC 地址 | `http://collector:4317` |

#### 可选配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `spark.telemetry.otel.service.name` | `spark-application` | OTel 服务名 |
| `spark.telemetry.otel.export.interval.ms` | `10000` | 指标导出间隔（毫秒） |
| `spark.telemetry.otel.exporter.protocol` | `grpc` | 导出协议 (`grpc` / `http`) |
| `spark.telemetry.config.path` | (classpath) | HOCON 配置文件路径 |
| `spark.telemetry.metrics.task.execution` | `true` | Category 1: 任务执行指标 |
| `spark.telemetry.metrics.task.shuffle-extended` | `true` | Category 2: 扩展 Shuffle 指标 |
| `spark.telemetry.metrics.task.info` | `true` | Category 3: 任务信息属性 |
| `spark.telemetry.metrics.stage.detailed` | `false` | Category 4: 阶段详细指标 |
| `spark.telemetry.metrics.job.lifecycle` | `false` | Category 5: 作业生命周期 |

> **重要提示**：Spark 配置键必须包含完整内部路径，包括 `.otel.` 段。映射规则为 `spark.telemetry.X` → `spark-telemetry.X`：
> - 正确：`spark.telemetry.otel.exporter.endpoint=http://host:4317`
> - 错误：`spark.telemetry.exporter.endpoint=http://host:4317`（映射到 `spark-telemetry.exporter.endpoint`）

### 4.2 Spark 2.x 部署（spark.extraListeners）

Spark 2.x 没有 `SparkPlugin` API，通过 `spark.extraListeners` 注册：

```bash
spark-submit \
  --master yarn \
  --jars /opt/spark/libs/spark-telemetry-plugin.jar \
  --conf spark.extraListeners=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryListener \
  --conf spark.telemetry.otel.exporter.endpoint=http://otel-collector:4317 \
  --conf spark.telemetry.otel.service.name=my-spark2-app \
  your-app.jar
```

#### Spark 2.x 特殊说明

- 插件通过 `spark.extraListeners` 注册，在第一个事件到达时通过 `ensureInit()` 懒初始化
- Shuffle Write API 使用 `shuffleBytesWritten` / `shuffleWriteTime` / `shuffleRecordsWritten`（与 Spark 3.x 的 `bytesWritten` / `writeTime` / `recordsWritten` 不同）
- 不支持 `SparkPlugin` / `DriverPlugin` / `ExecutorPlugin` API

### 4.3 使用 HOCON 配置文件（可选）

除 Spark Conf 外，还可通过 HOCON 配置文件进行详细配置。将 `telemetry.conf.example` 复制为 `telemetry.conf` 并放到 classpath 下：

```bash
cp telemetry.conf.example telemetry.conf
# 编辑后放到 Spark 的 classpath
spark-submit --files telemetry.conf ...
```

配置优先级：**Spark Conf 覆盖 > HOCON 文件 > 内置默认值**

### 4.4 验证插件运行

```bash
# 检查 Spark Driver/Executor 日志
# 应看到类似以下日志：
# INFO TelemetryLifecycle: Telemetry initialized, endpoint=http://collector:4317
# INFO SparkTelemetryListener: Recording task-end event for stage 0 task 0

# 在 OTel Collector 检查接收到的指标
kubectl logs -l app=otel-collector --tail=100 | grep "spark\."
```

> **注意**：短时 Spark 作业（< 10 秒）可能在 OTel SDK 首次导出之前完成。建议使用较长作业（如 SparkPi 5000+ 迭代）测试，或减小 `spark.telemetry.otel.export.interval.ms`。

---

## 5. MR Telemetry Collector 部署

### 5.1 概述

MR Telemetry Collector 是一个独立 Java 应用，定时轮询 Hadoop YARN History Server 的 REST API，获取已完成的 MR 作业计数器，并通过 OTel 导出。它独立于 Spark 运行。

### 5.2 配置

创建 `mr-collector.conf`：

```hocon
mr-telemetry {
  history-server {
    url = "http://history-server:19888"
    poll.interval.secs = 30
    connect.timeout.secs = 10
    read.timeout.secs = 30
  }

  otel {
    exporter.endpoint = "http://otel-collector:4317"
    exporter.protocol = "grpc"
    service.name = "mr-telemetry-collector"
    export.interval.ms = 10000
  }

  state {
    # 持久化文件路径（记录上次轮询时间，重启后不重复采集）
    file = "/var/lib/mr-telemetry/state.json"
  }

  filter {
    # 按用户名过滤（正则表达式）
    user.include = [".*"]
    user.exclude = []
    # 按作业名过滤（正则表达式）
    job.name.include = [".*"]
    job.name.exclude = []
  }

  collection {
    job.counters = true
    task.counters = false    # 任务级粒度（数据量可能较大）
    job.details = true
  }
}
```

### 5.3 运行

```bash
# 前台运行
java -jar mr-telemetry-dist.jar mr-collector.conf

# 后台运行（推荐使用 systemd 或 supervisor）
nohup java -jar mr-telemetry-dist.jar mr-collector.conf > mr-collector.log 2>&1 &

# 使用 systemd
cat > /etc/systemd/system/mr-telemetry-collector.service <<'EOF'
[Unit]
Description=MR Telemetry Collector
After=network.target

[Service]
Type=simple
User=hadoop
ExecStart=/usr/bin/java -jar /opt/mr-telemetry/mr-telemetry-dist.jar /opt/mr-telemetry/mr-collector.conf
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

systemctl enable --now mr-telemetry-collector
```

### 5.4 状态文件

`state.file` 记录上次轮询时间戳，确保重启后只采集新增作业。首次运行会采集 History Server 上所有已完成作业。

---

## 6. MR Telemetry Agent 部署

### 6.1 概述

MR Telemetry Agent 是一个 Java Agent，通过 ByteBuddy 字节码增强拦截 `Mapper.run()` 和 `Reducer.run()` 方法，在任务执行期间实时采样计数器，提供比 Collector 更细粒度的实时指标。

### 6.2 配置

Agent 通过 JVM 系统属性（`-D` 参数）配置，无需配置文件：

| 系统属性 | 默认值 | 说明 |
|---------|--------|------|
| `mr.telemetry.agent.enabled` | `true` | 是否启用 Agent |
| `mr.telemetry.agent.otel.exporter.endpoint` | `http://localhost:4317` | OTel Collector 地址 |
| `mr.telemetry.agent.otel.service.name` | `mr-telemetry-agent` | OTel 服务名 |
| `mr.telemetry.agent.otel.export.interval.ms` | `10000` | 导出间隔（毫秒） |
| `mr.telemetry.agent.sampling.interval.secs` | `5` | 计数器采样间隔（秒） |

### 6.3 部署

在 `mapred-site.xml` 中配置 Java Agent：

```xml
<!-- Map 任务 -->
<property>
  <name>mapreduce.map.java.opts</name>
  <value>-javaagent:/opt/mr-telemetry-agent.jar
    -Dmr.telemetry.agent.otel.exporter.endpoint=http://otel-collector:4317
    -Dmr.telemetry.agent.otel.service.name=my-mr-job</value>
</property>

<!-- Reduce 任务 -->
<property>
  <name>mapreduce.reduce.java.opts</name>
  <value>-javaagent:/opt/mr-telemetry-agent.jar
    -Dmr.telemetry.agent.otel.exporter.endpoint=http://otel-collector:4317
    -Dmr.telemetry.agent.otel.service.name=my-mr-job</value>
</property>
```

或者通过命令行指定：

```bash
hadoop jar my-job.jar \
  -Dmapreduce.map.java.opts="-javaagent:/opt/mr-telemetry-agent.jar -Dmr.telemetry.agent.otel.exporter.endpoint=http://collector:4317" \
  -Dmapreduce.reduce.java.opts="-javaagent:/opt/mr-telemetry-agent.jar -Dmr.telemetry.agent.otel.exporter.endpoint=http://collector:4317"
```

> **注意**：Agent JAR 必须在所有 NodeManager 节点的本地路径可访问。

### 6.4 Collector vs Agent 选择

| 特性 | MR Collector | MR Agent |
|------|-------------|----------|
| 部署方式 | 独立进程 | Java Agent（嵌入 MR 任务） |
| 指标粒度 | Job 级别 + Task 级别（可选） | Task 级别（实时采样） |
| 数据时效 | 作业完成后采集 | 任务执行中实时采集 |
| 运行依赖 | History Server | 无外部依赖 |
| 对任务影响 | 无侵入 | 轻微运行时开销（计数器采样） |

**推荐**：两者可同时使用。Collector 用于作业级汇总，Agent 用于任务级实时监控。

---

## 7. Flink Metrics Consumer 部署

### 7.1 概述

Flink Metrics Consumer 从 Kafka 消费 OTLP Protobuf 格式的指标数据，按指标类别拆分写入 MySQL 或 ClickHouse 的分类宽表。表结构自动创建，无需手动建表。

### 7.2 数据库准备

#### MySQL

```sql
CREATE DATABASE IF NOT EXISTS metrics_db;
CREATE USER 'metrics'@'%' IDENTIFIED BY 'metrics';
GRANT ALL PRIVILEGES ON metrics_db.* TO 'metrics'@'%';
```

Flink Consumer 启动时自动创建以下 9 张表（无需手动建表）：

| 表名 | 说明 |
|------|------|
| `task_metrics` | 每个 Task 完成事件一行，包含 9 个维度列 + 23 个指标列 |
| `stage_metrics` | 每个 Stage 完成事件一行 |
| `job_metrics` | 每个 Job 事件一行 |
| `jvm_memory_metrics` | JVM 内存时序数据 |
| `jvm_gc_metrics` | JVM GC 时序数据 |
| `task_histogram_buckets` | Task 级直方图桶分布 |
| `stage_histogram_buckets` | Stage 级直方图桶分布 |
| `job_histogram_buckets` | Job 级直方图桶分布 |
| `stage_governance` | Stage 治理指标（预聚合，见 7.6 节） |

所有标签（如 `app_id`、`executor_id`、`stage_id`）均作为显式类型化列存储，不使用 JSON 数据类型。

#### ClickHouse

```sql
CREATE DATABASE IF NOT EXISTS metrics_db;
```

同样自动建表，使用 ClickHouse 特有类型（`DateTime64(3)`、`LowCardinality(String)`、`Nullable(Float64)` 等），按月分区。

### 7.3 配置

创建 `flink-consumer.conf`：

```hocon
flink-consumer {
  kafka {
    bootstrap.servers = "kafka:9092"
    topic = "telemetry-metrics"
    group.id = "flink-metrics-consumer"
    startup.mode = "earliest-offset"  # earliest-offset | latest-offset
    checkpoint.path = "/tmp/flink-consumer-checkpoint.txt"  # Kafka offset 持久化文件
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

### 7.4 运行

```bash
# 独立运行（自包含 JAR，内含 Flink 依赖）
java -jar metrics-flink-consumer-dist.jar flink-consumer.conf

# 或提交到 Flink 集群
flink run -c x.mg.metrics.flink.Main metrics-flink-consumer.jar flink-consumer.conf
```

### 7.5 注意事项

- Flink Consumer 启动时自动创建所有表，无需手动建表
- 所有标签已展开为显式类型化列，不使用 JSON 数据类型
- OTLP 直方图桶中的 `Double.POSITIVE_INFINITY` 会自动转换为 `Double.MAX_VALUE`，因为 MySQL 不支持 Infinity
- Flink 版本为 1.18.0（最后一个支持 Java 8 的 Flink 版本）
- Kafka offset 持久化到本地文件（`checkpoint.path`），重启后从上次提交位置继续消费，保证 at-least-once 语义
- 所有模型行中 `app_id` 为空时自动回退为 `"unknown"`，避免数据库非空约束错误

### 7.6 Stage 治理指标（stage_governance）

Flink Consumer 在每个 Stage 完成时，自动从该 Stage 所有 Task 指标中预聚合出一行治理指标，写入 `stage_governance` 表。Grafana 可直接查单表出图，无需运行时聚合。

#### 治理指标维度

| 维度 | 说明 |
|------|------|
| **Duration 倾斜** | `duration_skew_ratio` = max / avg task 时长，>2 表示数据倾斜 |
| **IO 倾斜** | `io_read_skew_ratio`、`io_write_skew_ratio`、`shuffle_read_skew_ratio` |
| **小文件检测** | `avg_output_bytes_per_task`、`small_output_task_count`（输出 < 32MB 的 task 数） |
| **CPU 效率** | `cpu_efficiency` = cpu_time / run_time，<0.5 表示 CPU 利用率低 |
| **GC 开销** | `gc_overhead_ratio` = gc_time / run_time，>0.1 需调优 |
| **Shuffle 开销** | `shuffle_wait_ratio` = fetch_wait / run_time |
| **Spill 比例** | `spill_ratio` = disk_spilled / bytes_read |
| **反序列化开销** | `deserialize_overhead` = deserialize_time / run_time |
| **调度延迟** | `scheduler_delay_ratio` = scheduler_delay / total_duration |

#### `stage_governance` 完整表结构

```sql
CREATE TABLE stage_governance (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp_ms BIGINT NOT NULL,
    app_id VARCHAR(255) NOT NULL,
    stage_id INT NOT NULL,
    task_count INT NOT NULL,

    -- Duration 分析
    stage_duration_ms DOUBLE,          -- Stage 自身持续时间
    avg_task_duration_ms DOUBLE,
    max_task_duration_ms DOUBLE,
    min_task_duration_ms DOUBLE,
    duration_skew_ratio DOUBLE,        -- max / avg（>2 倾斜）

    -- IO 总量
    total_bytes_read DOUBLE,
    total_bytes_written DOUBLE,
    total_shuffle_bytes_read DOUBLE,
    total_shuffle_bytes_written DOUBLE,
    total_records_read DOUBLE,
    total_records_written DOUBLE,

    -- IO 倾斜
    io_read_skew_ratio DOUBLE,
    io_write_skew_ratio DOUBLE,
    shuffle_read_skew_ratio DOUBLE,

    -- 小文件指标
    avg_output_bytes_per_task DOUBLE,  -- total_bytes_written / task_count
    avg_output_records_per_task DOUBLE,
    small_output_task_count INT,       -- bytes_written < 32MB 的 task 数

    -- 资源效率
    cpu_efficiency DOUBLE,             -- cpu_ns / (run_time_ms × 1e6)
    gc_overhead_ratio DOUBLE,
    shuffle_wait_ratio DOUBLE,
    spill_ratio DOUBLE,
    deserialize_overhead DOUBLE,
    scheduler_delay_ratio DOUBLE,

    -- 内存
    max_peak_memory_bytes DOUBLE,
    total_memory_spilled DOUBLE,

    INDEX idx_app_time (app_id, timestamp_ms),
    INDEX idx_stage (app_id, stage_id)
);
```

---

## 8. OTel Collector 配置

### 8.1 最小配置

创建 `config.yaml`：

```yaml
extensions:
  health_check:
    endpoint: 0.0.0.0:13133

receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

exporters:
  debug:
    verbosity: detailed
  kafka:
    topic: telemetry-metrics
    encoding: otlp_proto
    brokers:
      - kafka:9092
    producer:
      compression: snappy
      max_message_bytes: 1000000

service:
  extensions: [health_check]
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [debug, kafka]
```

### 8.2 运行 OTel Collector

```bash
# Docker 方式
docker run -d --name otel-collector \
  -v $(pwd)/config.yaml:/etc/otelcol-contrib/config.yaml \
  -p 4317:4317 -p 4318:4318 -p 13133:13133 \
  otel/opentelemetry-collector-contrib:0.96.0 \
  --config=/etc/otelcol-contrib/config.yaml
```

> **重要**：必须使用 `otel/opentelemetry-collector-contrib` 镜像（不是 `otel/opentelemetry-collector`），因为核心镜像不包含 Kafka exporter。

> **重要**：配置中必须包含 `health_check` 扩展，否则带有 readiness/liveness 探针的 K8s 部署会 CrashLoopBackOff。

---

## 9. 指标参考

### 9.1 Spark 插件指标

#### 核心 IO 指标（始终采集）

| 指标名 | 类型 | 单位 | 说明 |
|--------|------|------|------|
| `spark.task.io.bytes_read` | Counter | By | 任务读取字节数 |
| `spark.task.io.bytes_written` | Counter | By | 任务写入字节数 |
| `spark.task.io.records_read` | Counter | {records} | 任务读取记录数 |
| `spark.task.io.records_written` | Counter | {records} | 任务写入记录数 |
| `spark.task.shuffle.bytes_read` | Counter | By | Shuffle 读取字节数 |
| `spark.task.shuffle.bytes_written` | Counter | By | Shuffle 写入字节数 |
| `spark.task.shuffle.fetch_wait_time_ms` | Counter | ms | Shuffle 等待时间 |
| `spark.task.disk_bytes_spilled` | Counter | By | 磁盘溢写字节数 |
| `spark.task.memory_bytes_spilled` | Counter | By | 内存溢写字节数 |
| `spark.task.duration_ms` | Histogram | ms | 任务执行时长 |

#### 任务执行指标（Category 1）

| 指标名 | 类型 | 单位 | 说明 |
|--------|------|------|------|
| `spark.task.executor.run_time_ms` | Histogram | ms | Executor 运行时间 |
| `spark.task.executor.cpu_time_ns` | Counter | ns | Executor CPU 时间 |
| `spark.task.deserialize_time_ms` | Histogram | ms | 反序列化时间 |
| `spark.task.deserialize_cpu_time_ns` | Counter | ns | 反序列化 CPU 时间 |
| `spark.task.result_serialization_time_ms` | Histogram | ms | 结果序列化时间 |
| `spark.task.jvm_gc_time_ms` | Histogram | ms | 任务 JVM GC 时间 |
| `spark.task.scheduler_delay_ms` | Histogram | ms | 调度延迟 |
| `spark.task.result_size_bytes` | Counter | By | 任务结果大小 |
| `spark.task.peak_execution_memory_bytes` | Counter | By | 峰值执行内存 |

#### 扩展 Shuffle 指标（Category 2）

| 指标名 | 类型 | 单位 | 说明 |
|--------|------|------|------|
| `spark.task.shuffle.local_blocks_fetched` | Counter | {blocks} | 本地 Shuffle 块数 |
| `spark.task.shuffle.records_read` | Counter | {records} | Shuffle 读取记录数 |
| `spark.task.shuffle.remote_bytes_read_to_disk` | Counter | By | 远程 Shuffle 写盘字节数 |
| `spark.task.shuffle.remote_reqs_duration_ms` | Counter | ms | 远程 Shuffle 请求时长 |

#### 阶段详细指标（Category 4）

| 指标名 | 类型 | 单位 | 说明 |
|--------|------|------|------|
| `spark.stage.duration_ms` | Histogram | ms | 阶段时长 |
| `spark.stage.num_tasks` | Counter | {tasks} | 阶段任务数 |
| `spark.stage.executor.run_time_ms` | Counter | ms | 阶段总运行时间 |
| `spark.stage.executor.cpu_time_ns` | Counter | ns | 阶段总 CPU 时间 |
| `spark.stage.jvm_gc_time_ms` | Counter | ms | 阶段总 GC 时间 |
| `spark.stage.peak_execution_memory_bytes` | Counter | By | 阶段峰值内存 |
| `spark.stage.io.bytes_read` | Counter | By | 阶段读取字节数（独立于 task 级计数器） |
| `spark.stage.io.bytes_written` | Counter | By | 阶段写入字节数（独立于 task 级计数器） |

#### 作业生命周期指标（Category 5）

| 指标名 | 类型 | 单位 | 说明 |
|--------|------|------|------|
| `spark.job.duration_ms` | Histogram | ms | 作业时长 |
| `spark.job.num_stages` | Counter | {stages} | 作业阶段数 |

#### JVM 系统指标

| 指标名 | 类型 | 单位 | 说明 |
|--------|------|------|------|
| `spark.jvm.memory.heap_used` | Gauge | By | JVM 堆内存使用量 |
| `spark.jvm.memory.non_heap_used` | Gauge | By | JVM 非堆内存使用量 |
| `spark.jvm.gc.count` | Counter | {count} | GC 次数（按 gc_name 标签区分） |
| `spark.jvm.gc.time_ms` | Counter | ms | GC 时间 |

#### 指标标签（Attributes）

| 标签名 | 适用范围 | 说明 |
|--------|---------|------|
| `spark.app.id` | 所有 | 应用 ID |
| `spark.executor.id` | 任务/系统 | Executor ID |
| `spark.stage.id` | 任务/阶段 | Stage ID |
| `spark.task.id` | 任务 | Task ID |
| `spark.task.success` | 任务 | 任务是否成功 |
| `spark.task.host` | 任务 | 任务执行主机（需开启 Category 3） |
| `spark.task.locality` | 任务 | 数据本地性（需开启 Category 3） |
| `spark.task.speculative` | 任务 | 是否推测执行（需开启 Category 3） |
| `spark.job.id` | 作业 | Job ID |
| `spark.job.success` | 作业 | 作业是否成功 |
| `gc_name` | GC | GC 收集器名称 |

### 9.2 MR Collector 指标

#### 作业级指标

| 指标名 | 类型 | 单位 | 说明 |
|--------|------|------|------|
| `mr.job.io.hdfs_bytes_read` | Counter | By | HDFS 读取字节数 |
| `mr.job.io.hdfs_bytes_written` | Counter | By | HDFS 写入字节数 |
| `mr.job.cpu_time_ms` | Counter | ms | CPU 时间 |
| `mr.job.gc_time_ms` | Counter | ms | GC 时间 |
| `mr.job.spilled_records` | Counter | {records} | 溢出记录数 |
| `mr.job.map_input_records` | Counter | {records} | Map 输入记录数 |
| `mr.job.map_output_records` | Counter | {records} | Map 输出记录数 |
| `mr.job.reduce_input_records` | Counter | {records} | Reduce 输入记录数 |
| `mr.job.reduce_output_records` | Counter | {records} | Reduce 输出记录数 |
| `mr.job.maps_duration_ms` | Counter | ms | Map 总时长 |
| `mr.job.reduces_duration_ms` | Counter | ms | Reduce 总时长 |
| `mr.job.physical_memory_bytes` | Counter | By | 物理内存 |
| `mr.job.virtual_memory_bytes` | Counter | By | 虚拟内存 |
| `mr.job.io.file_bytes_read` | Counter | By | 本地文件读取字节 |
| `mr.job.io.file_bytes_written` | Counter | By | 本地文件写入字节 |
| `mr.job.reduce_shuffle_bytes` | Counter | By | Shuffle 字节 |
| `mr.job.map_output_bytes` | Counter | By | Map 输出字节 |
| `mr.job.launched_maps` | Counter | {tasks} | Map 任务数 |
| `mr.job.launched_reduces` | Counter | {tasks} | Reduce 任务数 |
| `mr.job.elapsed_time_ms` | Counter | ms | 作业运行时长 |

#### 作业级标签

| 标签名 | 说明 |
|--------|------|
| `mr.job.id` | 作业 ID |
| `mr.job.name` | 作业名称 |
| `mr.job.user` | 提交用户 |
| `mr.job.state` | 作业状态 |
| `mr.job.queue` | 队列名称 |

### 9.3 MR Agent 指标（任务级实时）

| 指标名 | 类型 | 单位 | 说明 |
|--------|------|------|------|
| `mr.task.io.map_input_records` | Counter | {records} | Map 输入记录 |
| `mr.task.io.map_output_records` | Counter | {records} | Map 输出记录 |
| `mr.task.io.map_output_bytes` | Counter | By | Map 输出字节 |
| `mr.task.io.reduce_input_records` | Counter | {records} | Reduce 输入记录 |
| `mr.task.io.reduce_output_records` | Counter | {records} | Reduce 输出记录 |
| `mr.task.io.reduce_shuffle_bytes` | Counter | By | Reduce Shuffle 字节 |
| `mr.task.io.spilled_records` | Counter | {records} | 溢出记录 |
| `mr.task.cpu_time_ms` | Counter | ms | CPU 时间 |
| `mr.task.gc_time_ms` | Counter | ms | GC 时间 |
| `mr.task.io.hdfs_bytes_read` | Counter | By | HDFS 读取字节 |
| `mr.task.io.hdfs_bytes_written` | Counter | By | HDFS 写入字节 |
| `mr.task.io.file_bytes_read` | Counter | By | 本地文件读取字节 |
| `mr.task.io.file_bytes_written` | Counter | By | 本地文件写入字节 |

#### 任务级标签

| 标签名 | 说明 |
|--------|------|
| `mr.task.id` | 任务 ID |
| `mr.task.type` | 任务类型（map / reduce） |
| `mr.job.id` | 作业 ID |
| `mr.job.name` | 作业名称 |

### 9.4 Flink ETL 输出表

Flink Consumer 将原始 OTLP 指标按类别写入分类宽表。以下为各表维度列和指标列一览。

#### task_metrics（每行 = 一次 Task 完成事件）

**维度列：** `timestamp_ms`, `app_id`, `executor_id`, `stage_id`, `task_id`, `task_success`, `task_host`, `task_locatity`, `task_speculative`

**指标列：**

| 列名 | 类型 | 来源指标 |
|------|------|---------|
| `duration_ms` | DOUBLE | spark.task.duration_ms |
| `io_bytes_read` | DOUBLE | spark.task.io.bytes_read |
| `io_bytes_written` | DOUBLE | spark.task.io.bytes_written |
| `io_records_read` | DOUBLE | spark.task.io.records_read |
| `io_records_written` | DOUBLE | spark.task.io.records_written |
| `shuffle_bytes_read` | DOUBLE | spark.task.shuffle.bytes_read |
| `shuffle_bytes_written` | DOUBLE | spark.task.shuffle.bytes_written |
| `shuffle_fetch_wait_time_ms` | DOUBLE | spark.task.shuffle.fetch_wait_time_ms |
| `disk_bytes_spilled` | DOUBLE | spark.task.disk_bytes_spilled |
| `memory_bytes_spilled` | DOUBLE | spark.task.memory_bytes_spilled |
| `executor_run_time_ms` | DOUBLE | spark.task.executor.run_time_ms |
| `executor_cpu_time_ns` | DOUBLE | spark.task.executor.cpu_time_ns |
| `deserialize_time_ms` | DOUBLE | spark.task.deserialize_time_ms |
| `deserialize_cpu_time_ns` | DOUBLE | spark.task.deserialize_cpu_time_ns |
| `result_serialization_time_ms` | DOUBLE | spark.task.result_serialization_time_ms |
| `jvm_gc_time_ms` | DOUBLE | spark.task.jvm_gc_time_ms |
| `scheduler_delay_ms` | DOUBLE | spark.task.scheduler_delay_ms |
| `result_size_bytes` | DOUBLE | spark.task.result_size_bytes |
| `peak_execution_memory_bytes` | DOUBLE | spark.task.peak_execution_memory_bytes |
| `shuffle_local_blocks_fetched` | DOUBLE | spark.task.shuffle.local_blocks_fetched |
| `shuffle_records_read` | DOUBLE | spark.task.shuffle.records_read |
| `shuffle_remote_bytes_read_to_disk` | DOUBLE | spark.task.shuffle.remote_bytes_read_to_disk |
| `shuffle_remote_reqs_duration_ms` | DOUBLE | spark.task.shuffle.remote_reqs_duration_ms |

#### stage_metrics（每行 = 一次 Stage 完成事件）

**维度列：** `timestamp_ms`, `app_id`, `executor_id`, `stage_id`

**指标列：**

| 列名 | 类型 | 来源指标 |
|------|------|---------|
| `duration_ms` | DOUBLE | spark.stage.duration_ms |
| `num_tasks` | DOUBLE | spark.stage.num_tasks |
| `executor_run_time_ms` | DOUBLE | spark.stage.executor.run_time_ms |
| `executor_cpu_time_ns` | DOUBLE | spark.stage.executor.cpu_time_ns |
| `jvm_gc_time_ms` | DOUBLE | spark.stage.jvm_gc_time_ms |
| `peak_execution_memory_bytes` | DOUBLE | spark.stage.peak_execution_memory_bytes |
| `io_bytes_read` | DOUBLE | spark.stage.io.bytes_read |
| `io_bytes_written` | DOUBLE | spark.stage.io.bytes_written |

#### job_metrics（每行 = 一次 Job 事件）

**维度列：** `timestamp_ms`, `app_id`, `job_id`, `job_success`

**指标列：** `duration_ms`, `num_stages`

#### jvm_memory_metrics（每行 = 一次 JVM 内存采集）

**维度列：** `timestamp_ms`, `app_id`, `executor_id`

**指标列：** `heap_used`, `non_heap_used`

#### jvm_gc_metrics（每行 = 一次 GC 采集）

**维度列：** `timestamp_ms`, `app_id`, `executor_id`, `gc_name`

**指标列：** `gc_count`, `gc_time_ms`

#### 直方图桶表

`task_histogram_buckets`、`stage_histogram_buckets`、`job_histogram_buckets` 存储直方图分布数据。维度列与对应的 metrics 表一致，额外包含 `metric_name`、`bucket_le`、`bucket_count` 三列。

---

## 10. Grafana 可视化

项目提供了预构建的 Grafana 仪表盘：`grafana/spark-mr-telemetry-dashboard.json`

> **注意**：仪表盘基于新的 9 表宽行模式（task_metrics / stage_metrics / stage_governance 等），使用 MySQL 数据源。如需 ClickHouse，需调整 SQL 中的时间函数。

### 导入步骤

1. 打开 Grafana → Dashboards → Import
2. 上传 `grafana/spark-mr-telemetry-dashboard.json`
3. 选择 MySQL 数据源（导入时自动提示）
4. 保存仪表盘

### 数据源配置

**MySQL 数据源**（仪表盘默认）：
- URL: `mysql:3306`
- Database: `metrics_db`
- User/Password: `metrics` / `metrics`

**ClickHouse 数据源**（需手动调整 SQL）：
- URL: `http://clickhouse:8123`
- Database: `metrics_db`
- SQL 调整：将 `$__unixEpochFilter(timestamp_ms/1000)` 替换为 `$__timeFilter_ms(timestamp_ms)`，将 `$__unixEpochGroup(timestamp_ms/1000, $__interval)` 替换为 `$__timeInterval_ms(timestamp_ms)`，将 `FROM_UNIXTIME(timestamp_ms/1000, ...)` 替换为 `DateTime(timestamp_ms)`

### 仪表盘面板说明

| 面板 | 类型 | 数据来源 | 说明 |
|------|------|---------|------|
| **Total Tasks** | Stat | task_metrics | 时间范围内总任务数 |
| **Total Stages** | Stat | stage_metrics | 时间范围内总 Stage 数 |
| **Skewed Stages** | Stat | stage_governance | duration_skew_ratio > 2 的 Stage 数（红/黄/绿阈值） |
| **Avg CPU Efficiency** | Stat | stage_governance | 平均 CPU 效率（< 0.5 红，> 0.7 绿） |
| **Task I/O Bytes** | Time Series | task_metrics | IO/ Shuffle 读写字节时序趋势 |
| **Task Duration** | Time Series | task_metrics | 任务时长 avg/max/min 时序趋势 |
| **JVM Memory** | Time Series | jvm_memory_metrics | JVM 堆/非堆内存使用趋势 |
| **JVM GC** | Time Series | jvm_gc_metrics | GC 次数与耗时趋势 |
| **Stage Duration & Task Count** | Time Series | stage_metrics | Stage 时长与任务数趋势 |
| **Job Overview** | Table | job_metrics | 作业列表（ID、状态、时长、Stage 数） |
| **Data Skew Detection** | Table | stage_governance | 数据倾斜检测（duration/IO/shuffle skew ratio，颜色阈值告警） |
| **Resource Efficiency** | Table | stage_governance | 资源效率（CPU/GC/Shuffle/Spill/反序列化/调度，颜色阈值告警） |
| **Task Duration Histogram** | Bar Chart | task_histogram_buckets | 任务时长分布直方图 |
| **Small File Detection** | Table | stage_governance | 小文件检测（avg_output_bytes_per_task、small_output_task_count，颜色告警） |
| **Task Detail** | Table | task_metrics | 最新 200 条任务明细（完整 IO、时长、资源指标） |

### 模板变量

| 变量 | 说明 |
|------|------|
| `$app_id` | 应用 ID 筛选（多选，含 All 选项） |

### 治理指标告警阈值

仪表盘内置以下颜色阈值：

| 指标 | 绿色 | 黄色 | 红色 |
|------|------|------|------|
| duration_skew_ratio | < 1.5 | 1.5 ~ 2 | > 2 |
| cpu_efficiency | > 0.7 | 0.5 ~ 0.7 | < 0.5 |
| gc_overhead_ratio | < 0.05 | 0.05 ~ 0.1 | > 0.1 |
| shuffle_wait_ratio | < 0.05 | 0.05 ~ 0.1 | > 0.1 |
| spill_ratio | < 0.1 | 0.1 ~ 0.3 | > 0.3 |
| avg_output_bytes_per_task | > 32MB | 1MB ~ 32MB | < 1MB |
| small_output_task_count | 0 | 1 ~ 5 | > 5 |

### YARN 任务治理查询示例

以下 SQL 可直接用于自定义 Grafana 面板（MySQL 数据源），基于 `stage_governance` 预聚合表。

#### 数据倾斜 Stage

```sql
SELECT app_id, stage_id, task_count, duration_skew_ratio,
       max_task_duration_ms, avg_task_duration_ms
FROM stage_governance
WHERE duration_skew_ratio > 2
ORDER BY timestamp_ms DESC
LIMIT 20;
```

#### 小文件 Stage

```sql
SELECT app_id, stage_id, task_count, avg_output_bytes_per_task,
       small_output_task_count, avg_output_records_per_task
FROM stage_governance
WHERE avg_output_bytes_per_task < 33554432  -- 32MB
ORDER BY timestamp_ms DESC
LIMIT 20;
```

#### CPU 效率低

```sql
SELECT app_id, stage_id, task_count, cpu_efficiency,
       avg_task_duration_ms, total_bytes_read
FROM stage_governance
WHERE cpu_efficiency < 0.5
ORDER BY timestamp_ms DESC
LIMIT 20;
```

#### GC 开销高

```sql
SELECT app_id, stage_id, gc_overhead_ratio,
       avg_task_duration_ms, task_count
FROM stage_governance
WHERE gc_overhead_ratio > 0.1
ORDER BY timestamp_ms DESC
LIMIT 20;
```

#### Shuffle 开销大

```sql
SELECT app_id, stage_id, shuffle_wait_ratio,
       total_shuffle_bytes_read, total_shuffle_bytes_written
FROM stage_governance
WHERE shuffle_wait_ratio > 0.1
ORDER BY timestamp_ms DESC
LIMIT 20;
```

#### 应用 Stage 概览

```sql
SELECT app_id, stage_id, task_count,
       stage_duration_ms, avg_task_duration_ms,
       duration_skew_ratio, cpu_efficiency, gc_overhead_ratio,
       total_bytes_written, avg_output_bytes_per_task
FROM stage_governance
WHERE app_id = '$app_id'
ORDER BY stage_id;
```

---

## 11. 配置参数完整参考

### 11.1 Spark 插件（telemetry.conf）

```hocon
spark-telemetry {
  otel {
    # OTel Collector 端点
    exporter.endpoint = "http://localhost:4317"
    # 导出协议：grpc | http
    exporter.protocol = "grpc"
    # OTel 服务名（标识数据来源）
    service.name = "spark-application"
    # 指标导出间隔（毫秒）
    export.interval.ms = 10000
    # 附加资源属性
    resource.attributes = {
      "deployment.environment" = "production"
    }
  }

  metrics {
    # SparkListener 指标（任务/阶段级 IO）
    listener {
      enabled = true
      capture.task-end = true         # 任务结束事件
      capture.stage-complete = true   # 阶段完成事件
      capture.job-end = false         # 作业结束事件
    }

    # Dropwizard 指标（JVM GC、内存、BufferPool）
    system {
      enabled = true
      capture.jvm-memory = true       # JVM 内存指标
      capture.jvm-gc = true           # GC 指标
      capture.buffer-pools = true     # BufferPool 指标
      capture.executor-memory = true  # Executor 内存指标
    }

    # 指标详细程度（Category 开关）
    task.execution = true             # Category 1: 任务执行指标（run_time, cpu_time, deserialize, gc, scheduler_delay, result_size, peak_memory）
    task.shuffle-extended = true      # Category 2: 扩展 Shuffle 指标（local_blocks, records_read, remote_bytes_to_disk, remote_reqs_duration）
    task.info = true                  # Category 3: 任务信息属性（host, locality, speculative）
    stage.detailed = false            # Category 4: 阶段详细指标（duration, num_tasks, executor_time, gc, peak_memory, stage IO bytes）
    job.lifecycle = false             # Category 5: 作业生命周期（job_start/end, duration, num_stages）
  }

  # 应用名过滤（正则表达式）
  filter {
    app.name.include = [".*"]   # 包含的应用名模式
    app.name.exclude = []       # 排除的应用名模式
  }
}
```

### 11.2 MR Collector（mr-collector.conf）

```hocon
mr-telemetry {
  history-server {
    url = "http://localhost:19888"   # History Server 地址
    poll.interval.secs = 30          # 轮询间隔（秒）
    connect.timeout.secs = 10        # 连接超时（秒）
    read.timeout.secs = 30           # 读取超时（秒）
  }

  otel {
    exporter.endpoint = "http://localhost:4317"
    exporter.protocol = "grpc"
    service.name = "mr-telemetry-collector"
    export.interval.ms = 10000
  }

  state {
    file = "/tmp/mr-telemetry-state.json"  # 状态持久化文件
  }

  filter {
    user.include = [".*"]           # 包含的用户名模式
    user.exclude = []               # 排除的用户名模式
    job.name.include = [".*"]       # 包含的作业名模式
    job.name.exclude = []           # 排除的作业名模式
  }

  collection {
    job.counters = true             # 采集作业级计数器
    task.counters = false           # 采集任务级计数器（数据量大）
    job.details = true              # 采集作业详情
  }
}
```

### 11.3 MR Agent（JVM 系统属性）

| 属性 | 默认值 | 说明 |
|------|--------|------|
| `mr.telemetry.agent.enabled` | `true` | 是否启用 |
| `mr.telemetry.agent.otel.exporter.endpoint` | `http://localhost:4317` | OTel Collector 地址 |
| `mr.telemetry.agent.otel.service.name` | `mr-telemetry-agent` | 服务名 |
| `mr.telemetry.agent.otel.export.interval.ms` | `10000` | 导出间隔 |
| `mr.telemetry.agent.sampling.interval.secs` | `5` | 采样间隔 |

### 11.4 Flink Consumer（flink-consumer.conf）

```hocon
flink-consumer {
  kafka {
    bootstrap.servers = "localhost:9092"
    topic = "telemetry-metrics"
    group.id = "flink-metrics-consumer"
    startup.mode = "earliest-offset"  # earliest-offset | latest-offset
    checkpoint.path = "/tmp/flink-consumer-checkpoint.txt"  # Kafka offset 持久化文件
  }

  sink {
    type = "mysql"  # mysql | clickhouse

    mysql {
      url = "jdbc:mysql://localhost:3306/metrics_db"
      user = "metrics"
      password = "metrics"
      batch.size = 1000           # 批量写入大小
      flush.interval.ms = 5000    # 刷新间隔
    }

    clickhouse {
      url = "jdbc:clickhouse://localhost:8123/metrics_db"
      user = "default"
      password = ""
      batch.size = 5000
      flush.interval.ms = 3000
    }
  }

  filter {
    metric.name.include = [".*"]   # 包含的指标名模式
    metric.name.exclude = []       # 排除的指标名模式
  }

  processing {
    parallelism = 2                # Flink 并行度
  }
}
```

---

## 12. 常见问题与排查

### Q1: Spark 插件不生效，没有指标输出

**排查步骤**：

1. 确认 JAR 文件路径正确且可访问
2. 检查 `spark.plugins` 配置是否正确（Spark 3/4）或 `spark.extraListeners`（Spark 2）
3. 检查 Driver/Executor 日志中是否有 `TelemetryLifecycle initialized` 信息
4. 确认 OTel Collector 地址可达（`spark.telemetry.otel.exporter.endpoint`）
5. 检查配置键是否包含 `.otel.` 段（常见错误）

### Q2: 短时作业指标丢失

**原因**：OTel SDK 使用 `PeriodicMetricReader` 按间隔导出，如果作业在首次导出前结束，指标可能丢失。

**解决方案**：
- 插件在 `onJobEnd` 时自动触发 `flushAsync()` 非阻塞刷新，确保短作业指标也能导出
- 在插件关闭时也会执行同步 `forceFlush()`
- 如仍有丢失，可减小导出间隔：`spark.telemetry.otel.export.interval.ms=5000`
- 使用较长的测试作业

### Q3: MR Collector 连接 History Server 超时

**排查**：
1. 确认 History Server URL 和端口正确（Hadoop 2.x 端口 50070，Hadoop 3.x 端口 9870）
2. 增大超时设置：`connect.timeout.secs` / `read.timeout.secs`
3. 确认网络连通性

### Q4: Flink Consumer 写入 MySQL 失败

**排查**：
1. 确认 MySQL 数据库 `metrics_db` 已创建且用户有权限
2. Flink Consumer 启动时自动建表，检查日志中是否有建表错误
3. 检查 MySQL 用户权限（`GRANT ALL ON metrics_db.*`）
4. 查看 Flink Consumer 日志中的异常信息

### Q5: OTel Collector 启动失败

**常见原因**：
1. 使用了错误的镜像（`otel/opentelemetry-collector` 而非 `otel/opentelemetry-collector-contrib`）
2. 缺少 `health_check` 扩展配置
3. 配置文件路径错误（需通过 `--config` 参数指定）

### Q6: MR Agent 安装后 MR 任务报错

**排查**：
1. 确认 Agent JAR 在所有 NodeManager 节点的路径一致
2. 检查 `-javaagent:` 路径是否正确
3. 查看 MR 任务的 stderr 日志获取异常信息
4. 可通过 `mr.telemetry.agent.enabled=false` 快速禁用

### Q7: Kafka 中看不到指标数据

**排查**：
1. 检查 OTel Collector 日志：`kubectl logs -l app=otel-collector`
2. 确认 Kafka exporter 配置正确（broker 地址、topic 名称）
3. 使用 `kafka-dump-log.sh` 验证消息存在：
   ```bash
   kafka-dump-log.sh --files /tmp/kafka-logs/telemetry-metrics-0/00000000000000000000.log
   ```
4. 注意：`kafka-console-consumer.sh` 在单节点 KRaft 模式下可能超时，即使数据存在

---

## 附录 A：版本兼容性矩阵

| 组件 | 最低版本 | 推荐版本 | 说明 |
|------|---------|---------|------|
| Spark (Plugin) | 2.4.x | 3.5.x | Spark 2 用 listener 方式，3/4 用 Plugin API |
| Hadoop (MR Collector) | 2.7.0 | 3.4.3 | Collector 使用 History Server REST API |
| Hadoop (MR Agent) | 2.x / 3.x | 3.x | Agent 使用 `mapreduce.*.java.opts` |
| Java | 8 | 8 | Spark 4 需要 JDK 17+ |
| OTel Collector | 0.96+ | 0.96.0 (Contrib) | 必须使用 Contrib 版本 |
| Kafka | 3.7+ | 3.7.0 | 支持 KRaft 模式 |
| Flink | 1.18 | 1.18.0 | 最后支持 Java 8 的版本 |
| MySQL | 8.0 | 8.0 | Flink Consumer Sink |
| ClickHouse | 23.8 | 23.8 | Flink Consumer Sink |

## 附录 B：端口参考

| 服务 | 端口 | 协议 | 说明 |
|------|------|------|------|
| OTel Collector (gRPC) | 4317 | gRPC | OTLP 接收 |
| OTel Collector (HTTP) | 4318 | HTTP | OTLP 接收 |
| OTel Collector (Health) | 13133 | HTTP | 健康检查 |
| Kafka Broker | 9092 | TCP | Kafka 客户端 |
| Kafka Controller | 9093 | TCP | KRaft 控制器 |
| History Server (Hadoop 3) | 19888 | HTTP | MR 作业历史 |
| HDFS NN Web (Hadoop 3) | 9870 | HTTP | NameNode Web UI |
| HDFS NN Web (Hadoop 2) | 50070 | HTTP | NameNode Web UI |
| MySQL | 3306 | TCP | MySQL 协议 |
| ClickHouse HTTP | 8123 | HTTP | ClickHouse HTTP 接口 |
| Grafana | 3000 | HTTP | Grafana Web UI |
