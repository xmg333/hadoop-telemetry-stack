# Spark Telemetry Listener — 部署指南

## 产品概述

Spark Telemetry Listener 是一套透明的 Spark / MapReduce 可观测性方案，通过 OpenTelemetry 协议将大数据任务的 IO、CPU、GC 等指标导出到 OTel Collector，再经由 Kafka 持久化到 MySQL 或 ClickHouse，最终通过 Grafana 进行可视化。

### 核心组件

| 组件 | 类型 | 说明 | 部署文档 |
|------|------|------|---------|
| **Spark Telemetry Plugin** | Spark 插件 | 捕获 Spark 任务/阶段 IO 指标及 JVM 系统指标 | [Spark Plugin](spark-plugin.md) |
| **MR Telemetry Collector** | 独立 Java 应用 | 轮询 Hadoop History Server REST API 采集 MR 作业指标 | [MR Telemetry](mr-telemetry.md) |
| **MR Telemetry Agent** | Java Agent | 字节码增强方式实时采集 MR 任务级指标 | [MR Telemetry](mr-telemetry.md) |
| **Flink Metrics Consumer** | Flink 作业 | 消费 Kafka 中的 OTLP 指标写入 MySQL / ClickHouse | [Flink Consumer](flink-consumer.md) |

### 支持的 Spark 版本

| Spark 版本 | Scala 版本 | Maven Profile | 插件加载方式 |
|------------|-----------|---------------|-------------|
| Spark 2.4.x | 2.11 | `spark-2` | `spark.extraListeners` |
| Spark 3.5.x | 2.12 | `spark-3` (默认) | `SparkPlugin` API |
| Spark 4.0.x | 2.13 | `spark-4` | `SparkPlugin` API |

---

## 系统架构

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

---

## 编译构建

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

# 构建 Omnipackage（统一 JAR：Spark 2/3/4 + MR Collector + MR Agent）
chmod +x build-omni.sh && ./build-omni.sh

# 构建 Flink Consumer
mvn clean package -pl flink/metrics-flink-consumer,flink/metrics-flink-consumer-dist -am -DskipTests
```

### 产出物

| 构建产物 | 路径 | 说明 |
|---------|------|------|
| Spark 2 Plugin | `spark/spark-telemetry-dist-spark2/target/*.jar` | 自包含 Shaded JAR |
| Spark 3 Plugin | `spark/spark-telemetry-dist-spark3/target/*.jar` | 自包含 Shaded JAR |
| Spark 4 Plugin | `spark/spark-telemetry-dist-spark4/target/*.jar` | 自包含 Shaded JAR |
| **Omnipackage** | `spark/spark-telemetry-dist-omni/target/*.jar` | **统一 JAR（Spark 2/3/4 + MR Collector + MR Agent）** |
| MR Collector | `mapreduce-collector/mr-telemetry-dist/target/*.jar` | 自包含 Shaded JAR |
| MR Agent | `mapreduce-agent/mr-telemetry-agent-dist/target/*.jar` | Java Agent JAR |
| Flink Consumer | `flink/metrics-flink-consumer-dist/target/*.jar` | 自包含 Shaded JAR |

所有 Distribution JAR 均通过 `maven-shade-plugin` 打包，OTel、gRPC、Protobuf 等依赖已 relocate 到 `x.mg.metrics.shaded.*` 命名空间下，不会与宿主环境产生冲突。

### Omnipackage 构建验证

```bash
# 检查产出
ls -lh spark/spark-telemetry-dist-omni/target/spark-telemetry-dist-omni-*.jar

# 验证重定位后的适配器
jar tf spark/spark-telemetry-dist-omni/target/*.jar | grep "adapter/internal"

# 验证无未 shade 的 OTel 类
jar tf spark/spark-telemetry-dist-omni/target/*.jar | grep "^io/opentelemetry/"
# 应为空
```

---

## OTel Collector 配置

### 最小配置

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

### 运行

```bash
docker run -d --name otel-collector \
  -v $(pwd)/config.yaml:/etc/otelcol-contrib/config.yaml \
  -p 4317:4317 -p 4318:4318 -p 13133:13133 \
  otel/opentelemetry-collector-contrib:0.96.0 \
  --config=/etc/otelcol-contrib/config.yaml
```

> **重要**：必须使用 `otel/opentelemetry-collector-contrib` 镜像（非核心镜像），因核心镜像不含 Kafka exporter。配置中必须包含 `health_check` 扩展，否则带探针的 K8s 部署会 CrashLoopBackOff。

---

## 常见问题与排查

### Q1: Spark 插件不生效，没有指标输出

1. 确认 JAR 路径正确且可访问
2. 检查 `spark.plugins`（Spark 3/4）或 `spark.extraListeners`（Spark 2）配置
3. 检查 Driver/Executor 日志中是否有 `TelemetryLifecycle initialized`
4. 确认 OTel Collector 地址可达
5. 检查配置键是否包含 `.otel.` 段（常见错误）

### Q2: 短时作业指标丢失

插件在 `onJobEnd` 时自动触发 `flushAsync()` 非阻塞刷新，关闭时同步 `forceFlush()`。如仍有丢失，减小导出间隔：`spark.telemetry.otel.export.interval.ms=5000`。

### Q3: MR Collector 连接 History Server 超时

1. 确认 URL 和端口正确（Hadoop 2.x 端口 50070，Hadoop 3.x 端口 9870）
2. 增大超时：`connect.timeout.secs` / `read.timeout.secs`

### Q4: OTel Collector 启动失败

1. 使用 `otel/opentelemetry-collector-contrib`（非核心镜像）
2. 配置中必须包含 `health_check` 扩展
3. 通过 `--config` 参数指定配置文件

### Q5: Kafka 中看不到指标数据

1. 检查 OTel Collector 日志：`kubectl logs -l app=otel-collector`
2. 确认 Kafka exporter 配置（broker 地址、topic）
3. 使用 `kafka-dump-log.sh --files <log-file>` 验证消息存在
4. 注意：`kafka-console-consumer.sh` 在单节点 KRaft 模式下可能超时

### Q6: Omnipackage 版本检测错误

1. 检查 Driver/Executor 日志中 `OmniContext` 检测到的版本号
2. 确认 classpath 上没有冲突的 `scala-library` JAR
3. 如使用自定义 classpath，确保 `scala-library` 与 Spark 版本匹配

### Q7: Omnipackage 构建失败（找不到 adapters-relocated）

`adapters-relocated` 模块仅在 `omni` profile 下激活。使用 `./build-omni.sh` 脚本构建，不要单独构建该模块。

---

## 附录：版本兼容性矩阵

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

## 附录：端口参考

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
