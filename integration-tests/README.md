# Integration Tests

This directory contains integration and end-to-end (E2E) tests for the Spark Telemetry Listener project.

## Overview

The integration tests verify the complete data flow on a bare-metal node:

```
Spark/MR/Hive → Telemetry Plugin/Hook/Agent → OTel Collector → Kafka → Flink → MySQL/ClickHouse
```

All tests run on a single bare-metal node with Docker containers providing backend services (OTel Collector, Kafka, MySQL). No Kubernetes cluster is required.

## Test Structure

| Test Class | Description | Requires |
|------------|-------------|----------|
| `InfrastructureIT` | Smoke tests: Docker containers, OTel Collector, History Server, YARN RM | Docker |
| `HadoopClusterIT` | Hadoop/YARN/HistoryServer REST API checks + MR Collector E2E | Hadoop, YARN |
| `MRAgentIT` | MR Agent instrumentation: classpath safety + metric export | Hadoop, YARN |
| `SparkMetricsFieldVerificationIT` | Spark plugin: task/stage/job/JVM/SQL metrics field verification | Spark |
| `HiveMetricsFieldVerificationIT` | Hive hook: query metrics field verification | Hive |
| `MRMetricsFieldVerificationIT` | MR Collector: job/task metrics field verification | Hadoop, YARN |
| `SparkMultiVersionIT` | Spark plugin across versions (2.4, 3.0, 3.2, 3.5, 4.0) | Multiple Spark installs |
| `HadoopHiveE2EIT` | Hadoop 2.x/3.x and Hive 2.x/3.x compatibility | Hadoop, Hive |
| `ApiCompatibilityIT` | API compatibility verification via reflection | Build artifacts only |

## Prerequisites

### Bare-Metal Node Setup

Required software installations in `/opt/`:

| Software | Example Path | Notes |
|----------|-------------|-------|
| Java 8 | `/opt/jdk8u482-b08` | Required for Spark 3.x, Hadoop, Hive |
| Spark | `/opt/spark-3.2.0-bin-hadoop2.7` | With telemetry plugin JAR in `jars/` |
| Hadoop | `/opt/hadoop-3.2.0` | YARN + History Server running |
| Hive | `/opt/apache-hive-2.3.9-bin` | With hook JAR in `auxlib/` |

Required Docker containers:

| Container | Port | Purpose |
|-----------|------|---------|
| `otel-collector` | 4317 (gRPC) | Receives OTLP metrics |
| `kafka` | 9092 | Buffers metrics as OTLP protobuf |
| `mysql` | 3306 | Stores processed metrics |

### Environment Variables

```bash
export JAVA_HOME=/opt/jdk8u482-b08
export PATH=$JAVA_HOME/bin:$PATH
export SPARK_HOME=/opt/spark-3.2.0-bin-hadoop2.7
export HADOOP_HOME=/opt/hadoop-3.2.0
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HIVE_HOME=/opt/apache-hive-2.3.9-bin
export MYSQL_HOST=localhost
export OTEL_ENDPOINT=http://localhost:4317
```

### Data Reset (before each test run)

```bash
# Clean MySQL
docker exec mysql mysql -u root -proot123 telemetry -e \
  "SET FOREIGN_KEY_CHECKS=0;
   TRUNCATE TABLE task_metrics; TRUNCATE TABLE stage_metrics;
   TRUNCATE TABLE job_metrics; TRUNCATE TABLE jvm_memory_metrics;
   TRUNCATE TABLE jvm_gc_metrics; TRUNCATE TABLE sql_query_metrics;
   TRUNCATE TABLE sql_query_table_metrics; TRUNCATE TABLE hive_query_metrics;
   TRUNCATE TABLE hive_table_io_metrics; TRUNCATE TABLE mr_job_metrics;
   TRUNCATE TABLE mr_task_metrics;
   SET FOREIGN_KEY_CHECKS=1;"

# Clean Kafka
docker exec kafka /opt/kafka/bin/kafka-topics.sh --delete --topic telemetry-metrics --bootstrap-server localhost:9092
sleep 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic telemetry-metrics --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Restart OTel Collector
docker restart otel-collector
```

## Running Tests

### Build and Deploy First

```bash
mvn clean package -Pspark-3 -DskipTests

# Deploy Spark plugin JAR
cp spark/spark-telemetry-dist-spark3/target/spark-telemetry-dist-spark3-*.jar $SPARK_HOME/jars/
```

### Run All IT Tests (failsafe)

```bash
mvn verify -Pspark-3 -pl integration-tests -Dtest.skip=true
```

### Run Specific Test Class

```bash
# Spark metrics verification (most common)
mvn failsafe:integration-test -Pspark-3 -pl integration-tests \
  -Dit.test=SparkMetricsFieldVerificationIT

# Infrastructure smoke tests
mvn failsafe:integration-test -Pspark-3 -pl integration-tests \
  -Dit.test=InfrastructureIT

# MR Agent tests
mvn failsafe:integration-test -Pspark-3 -pl integration-tests \
  -Dit.test=MRAgentIT

# Hadoop + MR Collector
mvn failsafe:integration-test -Pspark-3 -pl integration-tests \
  -Dit.test=HadoopClusterIT
```

### Run Unit Tests Only (no infrastructure needed)

```bash
mvn test -Pspark-3 -pl integration-tests
```

## Troubleshooting

### "No Spark installations found"

Ensure Spark is installed in `/opt/spark-*` with the telemetry plugin JAR in `jars/`.

### "No Hadoop installation found"

Set `HADOOP_HOME` or install Hadoop in `/opt/hadoop-*`.

### "YARN ResourceManager not reachable"

Start YARN: `$HADOOP_HOME/sbin/start-yarn.sh`

### "History Server not reachable"

Start History Server: `$HADOOP_HOME/bin/mapred --daemon start historyserver`

### "Agent JAR not found"

Build the project first: `mvn clean package -Pspark-3 -DskipTests`

## Writing New Tests

1. Create a test class in `src/test/java/x/mg/metrics/integration/`
2. Annotate with `@Tag("integration")`
3. Use `assumeTrue()` for optional prerequisites (auto-skip if not available)
4. Use `MetricsVerificationHelper` for MySQL queries and assertions
5. Follow the bare-metal pattern: detect installation → submit job → wait for metrics → assert values

```java
@Tag("integration")
class MyNewIT {
    @Test
    void testSomething() throws Exception {
        assumeTrue(prerequisite(), "Prerequisite not met");
        // Test code here
    }
}
```
