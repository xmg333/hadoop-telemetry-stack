# Spark Telemetry Listener

透明的大数据可观测性方案，通过 OpenTelemetry 协议采集 Spark / MapReduce 任务指标，经 Kafka 持久化到 MySQL / ClickHouse，最终通过 Grafana 可视化。

## 系统架构

```mermaid
graph TD
    subgraph 采集层
        SP["Spark Plugin<br/>(Task/Stage/JVM Metrics)"]
        MR["MR Collector<br/>(History Server)"]
        MA["MR Agent<br/>(字节码增强)"]
    end

    SP -- OTLP gRPC --> OTel
    MR -- OTLP gRPC --> OTel
    MA -- OTLP gRPC --> OTel

    OTel["OTel Collector"]
    Flink["Flink Metrics Consumer"]
    DB[("MySQL / ClickHouse<br/>+ Grafana 可视化")]

    OTel -- "Kafka (OTLP Protobuf)" --> Flink
    Flink --> DB
```

## 核心组件

| 组件 | 说明 |
|------|------|
| **Spark Telemetry Plugin** | 透明 Spark 插件，捕获任务/阶段 IO 指标及 JVM 系统指标 |
| **MR Telemetry Collector** | 独立 Java 应用，轮询 Hadoop History Server 采集 MR 作业指标 |
| **MR Telemetry Agent** | Java Agent，通过字节码增强实时采集 MR 任务级指标 |
| **Hive Telemetry Hook** | Hive 查询 Hook，捕获 HiveServer2 查询指标（支持 MR 和 Spark 引擎） |
| **Flink Metrics Consumer** | Flink 作业，消费 Kafka 中的 OTLP 指标写入 MySQL / ClickHouse |
| **Diagnostic Tool** | 交互式诊断工具，基于状态机检查 OTel Collector、Kafka、MySQL、Grafana 面板等后端组件健康状态及应用配置正确性 |
| **Omnipackage** | 统一 JAR，自动检测 Spark 版本（2/3/4），同时包含 MR 和 Hive 组件 |

## 支持版本

| Spark 版本 | Scala | Maven Profile | 加载方式 |
|------------|-------|---------------|---------|
| Spark 2.4.x | 2.11 | `spark-2` | `spark.extraListeners` |
| Spark 3.5.x | 2.12 | `spark-3`（默认） | `SparkPlugin` API |
| Spark 4.0.x | 2.13 | `spark-4` | `SparkPlugin` API |

## 快速开始

详细的部署指南见 **[快速开始](quickstart.md)**，包含完整的构建、基础设施部署、组件配置和数据验证步骤。

### 构建

```bash
# Omnipackage（推荐，单 JAR 支持 Spark 2/3/4 + MR Agent/Collector + Hive Hook）
chmod +x build-omni.sh && ./build-omni.sh

# 或单独构建各版本
mvn clean package -DskipTests              # Spark 3.x（默认）
mvn clean package -Pspark-2 -DskipTests    # Spark 2.x
mvn clean package -Pspark-4 -DskipTests    # Spark 4.x
```

### 部署

```bash
# 安装 Omnipackage 到 Spark / Hive / MR
./deploy/install-omni.sh \
  --spark-home=/opt/spark --hive-home=/opt/hive --hadoop-home=/opt/hadoop \
  --otel-endpoint=http://otel-collector:4317 -y

# 导入 Grafana 面板
./deploy/deploy-grafana.sh \
  --grafana-url=http://grafana:3000 --user=admin --password=admin
```

## 模块结构

```
spark/spark-telemetry-common/             # 核心库：配置、模型、OTel SDK、生命周期管理
spark/spark-telemetry-adapter-spark2/     # Scala 2.11 适配层，Spark 2.4
spark/spark-telemetry-adapter-spark3/     # Scala 2.12 适配层，Spark 3.5
spark/spark-telemetry-adapter-spark30/    # Scala 2.12 适配层，Spark 3.0
spark/spark-telemetry-adapter-spark32/    # Scala 2.12 适配层，Spark 3.2
spark/spark-telemetry-adapter-spark4/     # Scala 2.13 适配层，Spark 4.0
spark/spark-telemetry-dist-spark{2,3,4}/  # 各版本 Shaded Fat JAR
spark/spark-telemetry-omni-facade/        # Omnipackage Java 门面（自动检测 Spark 版本）
spark/spark-telemetry-adapters-relocated/ # 适配器重定位（v2/v3/v4 包隔离）
spark/spark-telemetry-dist-omni/          # 统一 Shaded Fat JAR（Spark 2/3/4 + MR + Hive）
mapreduce-collector/mr-telemetry-collector/             # MR 作业指标采集器（独立 Java 应用）
mapreduce-agent/mr-telemetry-agent/                 # MR 任务级 Agent（Java Agent）
mapreduce-collector/mr-telemetry-dist/         # Shaded Fat JAR
mapreduce-agent/mr-telemetry-agent-dist/         # Shaded Fat JAR
hive/hive-telemetry-hook/                # Hive 查询 Hook（ExecuteWithHookContext）
hive/hive-telemetry-hook-dist/           # Shaded Fat JAR
flink/metrics-flink-consumer/             # Flink 消费者（Kafka → MySQL / ClickHouse）
flink/metrics-flink-consumer-dist/        # Shaded Fat JAR
diagnostic/diagnostic-core/               # 交互式诊断工具（JLine CLI + 11 状态自动检查）
integration-tests/                  # 集成测试（Spark 3）
```

## 关键特性

- **DELTA Temporality**：所有 OTLP 导出器使用 DELTA 临时性，防止重导出时数据重复
- **异步刷新**：`flushAsync()` 在 `onJobEnd` 时非阻塞刷新，避免阻塞 DAGScheduler
- **appId Fallback**：自动回退 `appId → appName → "unknown"`，兼容 local 模式
- **三层配置合并**：Spark Conf 覆盖 > HOCON 文件 > 内置默认值
- **指标分类开关**：6 个 Category 独立控制采集粒度（含 SQL 查询执行指标和 query_text 追踪）
- **Stage 治理预聚合**：Flink Consumer 自动计算数据倾斜、CPU 效率、GC 开销等治理指标
- **SQL 文本追踪**：Spark 和 Hive 均支持 query_text 捕获，自动截断到配置长度（默认 4096 字符），写入 sql_query_metrics、hive_query_metrics、metric_events 三张表
- **metric_events 统一宽表**：跨引擎分析大宽表，整合 Spark/MR/Hive 全部分类表数据，支持按 engine 和 event_type 维度的跨引擎聚合查询
- **Shaded Fat JAR**：OTel/gRPC/Protobuf 等依赖 relocate 到 `x.mg.metrics.shaded.*`，无依赖冲突

## 配置示例

配置文件示例见 `conf/examples/` 目录：

- `conf/examples/telemetry.conf.example` — Spark 插件配置
- `conf/examples/mr-collector.conf.example` — MR Collector 配置
- `conf/examples/flink-consumer.conf.example` — Flink Consumer 配置
- `conf/examples/hive-telemetry.conf.example` — Hive Hook 配置
- `diagnostic/diagnostic-core/src/main/resources/diagnostic.conf` — 诊断工具配置

## 指标概览

### Spark 指标

| 类别 | 示例指标 |
|------|---------|
| 任务 IO | `spark.task.io.bytes_read/written`, `spark.task.shuffle.bytes_read/written` |
| 任务执行 | `spark.task.executor.run_time_ms`, `spark.task.executor.cpu_time_ns` |
| 任务时长 | `spark.task.duration_ms`（Histogram） |
| Stage 详情 | `spark.stage.duration_ms`, `spark.stage.io.bytes_read/written` |
| 作业生命周期 | `spark.job.duration_ms`, `spark.job.num_stages` |
| JVM | `spark.jvm.memory.heap_used`, `spark.jvm.gc.count/time_ms` |

### MR 指标

| 来源 | 示例指标 |
|------|---------|
| MR Collector（作业级） | `mr.job.io.hdfs_bytes_read/written`, `mr.job.cpu_time_ms` |
| MR Agent（任务级） | `mr.task.io.map_input_records`, `mr.task.cpu_time_ms` |

### Hive 指标

| 类别 | 示例指标 |
|------|---------|
| 查询执行 | `hive.query.duration_ms`, `hive.query.success` / `hive.query.failure` |
| IO 指标 | `hive.query.input_bytes`, `hive.query.output_bytes`, `hive.query.input_rows`, `hive.query.output_rows` |
| 表级统计 | `hive.query.input_tables`, `hive.query.output_tables` |

## Grafana 可视化

`deploy/grafana/` 目录提供预构建仪表盘 JSON 文件，可通过 `deploy/deploy-grafana.sh` 一键导入：

| 文件 | 面板名 | 说明 |
|------|--------|------|
| `overview.json` | Platform Telemetry Overview | 全平台总览 |
| `spark.json` | Spark Telemetry | Task/Stage/SQL 指标 |
| `mr.json` | MapReduce Telemetry | Job Level + Task Level |
| `hive-mr.json` | Hive on MR Telemetry | Hive MR 引擎查询 |
| `hive-spark.json` | Hive on Spark Telemetry | Hive Spark 引擎查询 |
| `spark-mr-telemetry-dashboard.json` | Spark/MR/Hive 合并面板 | 综合视图 |
| `hive-analysis.json` | Hive 查询与数据血缘分析 | 操作分布、表 IO、执行引擎对比 |
| `performance-analysis.json` | 性能异常与瓶颈分析 | Stage 耗时、GC 开销、数据倾斜检测 |
| `efficiency.json` | 综合效率评分 | 资源效率评分、队列效率对比 |
| `reliability.json` | 可靠性与失败分析 | 任务成功率趋势、失败事件 |
| `capacity.json` | 容量规划与资源利用率 | 任务并发量、内存趋势、GC 频率 |
| `cost-attribution.json` | 成本归属与资源排行 | 用户/队列/应用资源排行 |
| `io-analysis.json` | 数据吞吐与 IO 分析 | 各引擎 IO 吞吐量、Shuffle 分析 |

前 6 个面板使用各引擎独立分类表，后 7 个分析面板使用 `metric_events` 统一宽表实现跨引擎聚合查询。覆盖：任务 IO / 时长时序趋势、JVM 内存 / GC 监控、数据倾斜检测、资源效率分析、成本归属、小文件检测、任务时长直方图分布。

## 完整文档

详细的部署指南、配置参数、指标参考、排查手册见 [部署指南](deployment-guide.md)。

打包与发布流程见 [发布指南](release.md)。

## K8s 测试环境

已迁移至裸节点测试，不再依赖 Kubernetes。详见 [集成测试 README](../integration-tests/README.md)。

## 集成测试

所有集成测试在裸节点（bare-metal）上运行，通过 Docker 容器提供后端服务（OTel Collector、Kafka、MySQL），无需 K8s 集群。

### 测试矩阵

| 测试类 | 验证内容 | 依赖 |
|--------|---------|------|
| `InfrastructureIT` | Docker 容器、OTel Collector、History Server、YARN RM 可达性 | Docker |
| `SparkMetricsFieldVerificationIT` | Spark task/stage/job/JVM/SQL 指标字段完整性 | Spark |
| `HiveMetricsFieldVerificationIT` | Hive 查询指标字段完整性 | Hive |
| `MRMetricsFieldVerificationIT` | MR Collector 作业/任务指标字段完整性 | Hadoop YARN |
| `HadoopClusterIT` | Hadoop/YARN 服务可用性 + MR Collector 端到端 | Hadoop YARN |
| `MRAgentIT` | MR Agent 字节码增强 + classpath 安全 + 指标导出 | Hadoop YARN |
| `SparkMultiVersionIT` | Spark 2.4/3.0/3.2/3.5/4.0 跨版本兼容 | 多版本 Spark |
| `ApiCompatibilityIT` | 反射验证 Spark/Hadoop/Hive API 兼容性 | 构建产物 |

### 运行方式

```bash
# 构建并部署
mvn clean package -Pspark-3 -DskipTests
cp spark/spark-telemetry-dist-spark3/target/*.jar $SPARK_HOME/jars/

# 运行全部 IT 测试
mvn verify -Pspark-3 -pl integration-tests -Dtest.skip=true

# 运行单个测试
mvn failsafe:integration-test -Pspark-3 -pl integration-tests -Dit.test=SparkMetricsFieldVerificationIT
```

## 性能基准测试

使用 Intel HiBench 在 4C8G 单节点环境（192.168.10.65）上，对比 telemetry 组件加载前后的业务性能开销。数据规模：HiBench small profile（WordCount 320MB，SQL 100K rows，KMeans 3M samples）。每个工作负载先跑一次 baseline（无 telemetry），再跑一次 with-telemetry，验证指标到达 MySQL。

### Spark 3.2.0 + Hadoop 3.2.0

Omnipackage 通过 `spark.plugins` 加载，`spark.telemetry.otel.export.interval.ms=5000`，开启全部指标类别。

| 工作负载 | Baseline | Telemetry | 开销 | 指标到达 |
|---------|----------|-----------|------|---------|
| micro/wordcount | 14.9s | 18.5s | +24.7% | YES |
| micro/sort | 14.2s | 12.6s | -11.8% | YES |
| micro/terasort | 20.1s | 19.2s | -4.4% | YES |
| micro/repartition | 15.9s | 14.2s | -10.8% | YES |
| sql/aggregation | 22.8s | 20.2s | -11.5% | YES |
| sql/join | 24.3s | 25.2s | +3.7% | YES |
| sql/scan | 22.1s | 23.5s | +6.3% | YES |
| ml/kmeans | 29.4s | 29.4s | -0.1% | YES |
| ml/lr | 72.5s | 75.7s | +4.4% | YES |
| websearch/pagerank | 17.4s | 15.1s | -13.3% | YES |

10 个工作负载全部通过，所有指标均已验证到达 MySQL。平均开销约 -1.3%（在测量噪声范围内）。

### MR Agent + Hadoop 3.2.0

MR Agent 通过 `-javaagent` 注入到 `mapreduce.map/reduce.java.opts`。

| 工作负载 | Baseline | Telemetry | 开销 | 指标到达 |
|---------|----------|-----------|------|---------|
| micro/wordcount | 50.6s | 27.5s | -45.7% | YES |
| micro/sort | 41.3s | 25.7s | -37.8% | YES |
| micro/terasort | 45.6s | 28.2s | -38.1% | YES |

3 个工作负载全部通过。Telemetry 运行更快是因为 MR 任务数和 JVM 预热差异，非 agent 效果。所有 `mr.task.*` 指标已验证到达 MySQL。

### Hive Hook + Hadoop 3.2.0

Hive Hook 通过 `hive.exec.post.hooks` 注入。测试 Hive 3.1.3 和 2.3.9，均使用 MR 引擎。

**Hive 3.1.3**

| 工作负载 | Baseline | Telemetry | 开销 | 指标到达 |
|---------|----------|-----------|------|---------|
| sql/aggregation | 55.8s | 56.8s | +1.7% | YES |
| sql/join | 98.9s | 99.3s | +0.4% | YES |
| sql/scan | 62.4s | 62.9s | +0.8% | YES |

**Hive 2.3.9**

| 工作负载 | Baseline | Telemetry | 开销 | 指标到达 |
|---------|----------|-----------|------|---------|
| sql/aggregation | 55.7s | 52.5s | -5.7% | YES |
| sql/join | 97.2s | 97.9s | +0.7% | YES |
| sql/scan | 61.6s | 61.9s | +0.4% | YES |

12 个 Hive 运行全部成功，所有 `hive.query.*` 指标已验证到达 MySQL。Hook 开销 <2%。

### 兼容性矩阵

| 组件 | Hadoop 2.7.0 | Hadoop 3.2.0 | Spark 2.4.4 | Spark 3.2.0 | Hive 2.3.9 | Hive 3.1.3 |
|------|:---:|:---:|:---:|:---:|:---:|:---:|
| Spark Plugin (Omnipackage) | - | PASS | - | PASS | - | - |
| MR Agent | - | PASS | - | - | - | - |
| Hive Hook | - | PASS | - | - | PASS | PASS |

### 测试环境

- **硬件**: 4C8G 单节点（192.168.10.65），Java 8（`/opt/jdk8u482-b08`）
- **数据流**: Plugin/Agent/Hook → OTLP gRPC → OTel Collector → Kafka → Flink Consumer → MySQL
- **HiBench 版本**: 8.0-SNAPSHOT，`small` profile
- **Benchmark 脚本**: `benchmark/auto_bench.sh`

## 异常隔离测试

Telemetry 组件遵循 **永不阻塞用户任务** 原则：任何 telemetry 初始化失败、OTel 连接断开、SDK 内部异常都不应影响 Spark/Hive/MR 作业的正常执行。

### 防御层次

| 层次 | 位置 | 防御机制 |
|------|------|---------|
| OTel SDK 初始化 | `OtelRegistry.start()` | catch 异常后设置 `OpenTelemetry.noop()`，避免下游 MetricRecorder NPE 级联崩溃 |
| 生命周期初始化 | `TelemetryLifecycle.init()` | catch 构造异常，创建 disabled 实例静默丢弃所有事件 |
| Driver/Executor 插件 | `TelemetryDriverPlugin.init()` / `TelemetryExecutorPlugin.init()` | 整体 try-catch 包裹，失败仅 log warning，SparkContext/Executor 正常启动 |
| Spark Listener | `SparkTelemetryListener` 各事件方法 | try-catch 包裹，异常不影响 Spark DAGScheduler |
| Hive Hook | `HiveTelemetryHook.run()` | 顶层 try-catch，异常不传播到 HiveServer2 |
| MR Agent | ByteBuddy Advice | `@Advice.OnMethodEnter/Exit` 内吞所有异常，不中断 MR Task |
| Omnipackage 门面 | `SparkTelemetryPlugin` / `SparkTelemetryListener` | 反射加载失败时返回 no-op 实现，不抛 RuntimeException |
| OTel SDK 日志 | `OtelRegistry.suppressOtelSdkErrorLogs()` | 将 shaded OTel SDK 的 ERROR 日志降级为 WARNING，避免 collector 不可达时产生虚假告警 |

### 验证场景

| 场景 | 预期行为 | 验证方式 |
|------|---------|---------|
| OTel Collector 不可达 | Spark 任务正常完成，日志仅 WARNING 级别 | 关闭 OTel Collector 后提交 Spark 任务 |
| OTel SDK 初始化失败 | Spark 任务正常完成，telemetry 静默失效 | 配置无效 endpoint 后提交 Spark 任务 |
| Omnipackage 版本检测失败 | 返回 no-op 插件，Spark 正常启动 | 破坏 classpath 后启动 Spark |
| Listener 反射调用失败 | 事件静默丢弃，不影响后续事件 | 模拟 listener 方法异常 |
| Hive Hook 异常 | HiveServer2 查询正常执行 | Hook 内部抛异常后执行 Hive 查询 |
| MR Agent 异常 | MR Task 正常执行完成 | Agent 内部异常后提交 MR 任务 |

### 已知日志行为

当 OTel Collector 不可达时，用户将看到以下日志（WARNING 级别，不阻塞任务）：

```
WARNING x.mg.metrics.sparktelemetry.otel.OtelRegistry: OTel Collector connection failed, metrics will not be exported: ...
```

OTel SDK 内部的 gRPC 重连错误日志已降级为 WARNING，不会以 ERROR 级别输出。

## License

Private
