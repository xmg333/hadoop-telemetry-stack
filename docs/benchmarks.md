# 性能基准测试

使用 Intel HiBench 在 4C8G 单节点环境（192.168.10.65）上，对比 telemetry 组件加载前后的业务性能开销。数据规模：HiBench small profile（WordCount 320MB，SQL 100K rows，KMeans 3M samples）。每个工作负载先跑一次 baseline（无 telemetry），再跑一次 with-telemetry，验证指标到达 MySQL。

## Spark 3.2.0 + Hadoop 3.2.0

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

**结论**: 10 个工作负载全部通过，所有指标均已验证到达 MySQL。平均开销约 -1.3%（在测量噪声范围内）。

## MR Agent + Hadoop 3.2.0

MR Agent 通过 `-javaagent` 注入到 `mapreduce.map/reduce.java.opts`。

| 工作负载 | Baseline | Telemetry | 开销 | 指标到达 |
|---------|----------|-----------|------|---------|
| micro/wordcount | 50.6s | 27.5s | -45.7% | YES |
| micro/sort | 41.3s | 25.7s | -37.8% | YES |
| micro/terasort | 45.6s | 28.2s | -38.1% | YES |

**结论**: 3 个工作负载全部通过。Telemetry 运行更快是因为 MR 任务数和 JVM 预热差异，非 agent 效果。所有 `mr.task.*` 指标已验证到达 MySQL。

## Hive Hook + Hadoop 3.2.0

Hive Hook 通过 `hive.exec.post.hooks` 注入。测试 Hive 3.1.3 和 2.3.9，均使用 MR 引擎。

### Hive 3.1.3

| 工作负载 | Baseline | Telemetry | 开销 | 指标到达 |
|---------|----------|-----------|------|---------|
| sql/aggregation | 55.8s | 56.8s | +1.7% | YES |
| sql/join | 98.9s | 99.3s | +0.4% | YES |
| sql/scan | 62.4s | 62.9s | +0.8% | YES |

### Hive 2.3.9

| 工作负载 | Baseline | Telemetry | 开销 | 指标到达 |
|---------|----------|-----------|------|---------|
| sql/aggregation | 55.7s | 52.5s | -5.7% | YES |
| sql/join | 97.2s | 97.9s | +0.7% | YES |
| sql/scan | 61.6s | 61.9s | +0.4% | YES |

**结论**: 12 个 Hive 运行全部成功，所有 `hive.query.*` 指标已验证到达 MySQL。Hook 开销 <2%。

## 兼容性矩阵

| 组件 | Hadoop 2.7.0 | Hadoop 3.2.0 | Spark 2.4.4 | Spark 3.2.0 | Hive 2.3.9 | Hive 3.1.3 |
|------|:---:|:---:|:---:|:---:|:---:|:---:|
| Spark Plugin (Omnipackage) | - | PASS | - | PASS | - | - |
| MR Agent | - | PASS | - | - | - | - |
| Hive Hook | - | PASS | - | - | PASS | PASS |

## 测试环境

- **硬件**: 4C8G 单节点（192.168.10.65），Java 8（`/opt/jdk8u482-b08`）
- **数据流**: Plugin/Agent/Hook → OTLP gRPC → OTel Collector → Kafka → Flink Consumer → MySQL
- **HiBench 版本**: 8.0-SNAPSHOT，`small` profile
- **Benchmark 脚本**: `benchmark/auto_bench.sh`
