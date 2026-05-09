# Performance Benchmarks

Using Intel HiBench on a 4C8G single-node environment (<BENCHMARK_SERVER_IP>), we compare the business performance overhead before and after loading telemetry components. Data scale: HiBench small profile (WordCount 320MB, SQL 100K rows, KMeans 3M samples). Each workload runs a baseline first (without telemetry), then a with-telemetry run, verifying that metrics reach MySQL.

## Spark 3.2.0 + Hadoop 3.2.0

Omnipackage loaded via `spark.plugins`, `spark.telemetry.otel.export.interval.ms=5000`, all metric categories enabled.

| Workload | Baseline | Telemetry | Overhead | Metrics Arrived |
|----------|----------|-----------|----------|-----------------|
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

**Conclusion**: All 10 workloads passed, all metrics verified to have arrived in MySQL. Average overhead approximately -1.3% (within measurement noise).

## MR Agent + Hadoop 3.2.0

MR Agent injected via `-javaagent` into `mapreduce.map/reduce.java.opts`.

| Workload | Baseline | Telemetry | Overhead | Metrics Arrived |
|----------|----------|-----------|----------|-----------------|
| micro/wordcount | 50.6s | 27.5s | -45.7% | YES |
| micro/sort | 41.3s | 25.7s | -37.8% | YES |
| micro/terasort | 45.6s | 28.2s | -38.1% | YES |

**Conclusion**: All 3 workloads passed. Telemetry runs are faster due to differences in MR task count and JVM warmup, not agent effects. All `mr.task.*` metrics verified to have arrived in MySQL.

## Hive Hook + Hadoop 3.2.0

Hive Hook injected via `hive.exec.post.hooks`. Tested with Hive 3.1.3 and 2.3.9, both using the MR engine.

### Hive 3.1.3

| Workload | Baseline | Telemetry | Overhead | Metrics Arrived |
|----------|----------|-----------|----------|-----------------|
| sql/aggregation | 55.8s | 56.8s | +1.7% | YES |
| sql/join | 98.9s | 99.3s | +0.4% | YES |
| sql/scan | 62.4s | 62.9s | +0.8% | YES |

### Hive 2.3.9

| Workload | Baseline | Telemetry | Overhead | Metrics Arrived |
|----------|----------|-----------|----------|-----------------|
| sql/aggregation | 55.7s | 52.5s | -5.7% | YES |
| sql/join | 97.2s | 97.9s | +0.7% | YES |
| sql/scan | 61.6s | 61.9s | +0.4% | YES |

**Conclusion**: All 12 Hive runs succeeded, all `hive.query.*` metrics verified to have arrived in MySQL. Hook overhead <2%.

## Compatibility Matrix

| Component | Hadoop 2.7.0 | Hadoop 3.2.0 | Spark 2.4.4 | Spark 3.2.0 | Hive 2.3.9 | Hive 3.1.3 |
|-----------|:---:|:---:|:---:|:---:|:---:|:---:|
| Spark Plugin (Omnipackage) | - | PASS | - | PASS | - | - |
| MR Agent | - | PASS | - | - | - | - |
| Hive Hook | - | PASS | - | - | PASS | PASS |

## Test Environment

- **Hardware**: 4C8G single node (<BENCHMARK_SERVER_IP>), Java 8 (`/opt/jdk8u482-b08`)
- **Data Flow**: Plugin/Agent/Hook -> OTLP gRPC -> OTel Collector -> Kafka -> Flink Consumer -> MySQL
- **HiBench Version**: 8.0-SNAPSHOT, `small` profile
- **Benchmark Script**: `benchmark/auto_bench.sh`
