# Spark Telemetry Listener

透明的大数据可观测性方案，通过 OpenTelemetry 协议采集 Spark / MapReduce 任务指标，经 Kafka 持久化到 MySQL / ClickHouse，最终通过 Grafana 可视化。

## 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│  采集层                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Spark Plugin │  │ MR Collector │  │  MR Agent    │      │
│  │ (Task/Stage/ │  │ (History Svr)│  │ (字节码增强)  │      │
│  │  JVM Metrics)│  │              │  │              │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
│         └──────── OTLP gRPC ────────┬───────┘              │
└─────────────────────────────────────┼───────────────────────┘
                                      ▼
                        ┌─────────────────────────┐
                        │     OTel Collector       │
                        └────────────┬────────────┘
                                     │ Kafka (OTLP Protobuf)
                                     ▼
                        ┌─────────────────────────┐
                        │  Flink Metrics Consumer  │
                        └────────────┬────────────┘
                                     ▼
                        ┌─────────────────────────┐
                        │  MySQL / ClickHouse      │
                        │  + Grafana 可视化         │
                        └─────────────────────────┘
```

## 目录结构

```
docs/                        # 完整文档
  index.md                     全文索引（指标、基准测试、兼容性矩阵）
  quickstart.md                快速开始
  deployment-guide.md          部署指南与排查手册
  spark-plugin.md              Spark Plugin 配置与指标参考
  mr-telemetry.md              MR Collector / Agent 配置与指标参考
  flink-consumer.md            Flink Consumer 配置与数据库表结构
  grafana-problem.md           Grafana 面板问题排查
conf/examples/               # 配置示例
  telemetry.conf.example       Spark 插件
  mr-collector.conf.example    MR Collector
  flink-consumer.conf.example  Flink Consumer
  hive-telemetry.conf.example  Hive Hook
deploy/
  k8s/                        # Kubernetes 测试环境清单
  grafana/                    # Grafana 仪表盘 JSON
  otel-collector/             # OTel Collector 配置
  sql/                        # 数据库建表 SQL
spark/                       # Spark 相关 Maven 模块
mapreduce-agent/             # MR Agent Maven 模块
mapreduce-collector/         # MR Collector Maven 模块
hive/                        # Hive Hook Maven 模块
flink/                       # Flink Consumer Maven 模块
integration-tests/           # 集成测试
```

## 快速构建

```bash
# Omnipackage（推荐，单 JAR 支持 Spark 2/3/4 + MR Agent/Collector + Hive Hook）
chmod +x build-omni.sh && ./build-omni.sh

# 或单独构建各版本
mvn clean package -DskipTests              # Spark 3.x（默认）
mvn clean package -Pspark-2 -DskipTests    # Spark 2.x
mvn clean package -Pspark-4 -DskipTests    # Spark 4.x
```

## 支持版本

| Spark 版本 | Scala | Maven Profile | 加载方式 |
|------------|-------|---------------|---------|
| Spark 2.4.x | 2.11 | `spark-2` | `spark.extraListeners` |
| Spark 3.5.x | 2.12 | `spark-3`（默认） | `SparkPlugin` API |
| Spark 4.0.x | 2.13 | `spark-4` | `SparkPlugin` API |

## 文档导航

| 文档 | 说明 |
|------|------|
| [快速开始](docs/quickstart.md) | 构建到验证的完整部署流程 |
| [部署指南](docs/deployment-guide.md) | 配置参数、指标参考、排查手册 |
| [Spark Plugin](docs/spark-plugin.md) | Spark 插件配置与指标参考 |
| [MR Telemetry](docs/mr-telemetry.md) | MR Collector / Agent 配置与指标参考 |
| [Flink Consumer](docs/flink-consumer.md) | Flink Consumer 配置与数据库表结构 |
| [Grafana 排查](docs/grafana-problem.md) | Grafana 面板问题排查 |

## License

Private
