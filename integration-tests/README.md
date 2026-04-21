# Integration Tests

This directory contains integration and end-to-end (E2E) tests for the Spark Telemetry Listener project.

## Overview

The integration tests verify the complete data flow:

```
Spark/MR/Hive → Telemetry Plugin/Hook/Agent → OTel Collector → Kafka → Flink → MySQL/ClickHouse
```

## Test Structure

| Test Class | Description |
|------------|-------------|
| `K8sTestBase` | Base class providing K8s utilities |
| `E2EMetricsFlowTest` | Verifies all K8s pods are running |
| `MRCollectorIT` | Tests MR Collector polling and metric export |
| `MRAgentIT` | Tests MR Agent instrumentation and real-time export |
| `SparkMultiVersionIT` | Tests Spark plugin across multiple versions (2.4, 3.0, 3.2, 3.5, 4.0) |
| `HadoopHiveE2EIT` | Tests Hadoop 2.x/3.x and Hive 2.x/3.x compatibility |
| `ApiCompatibilityIT` | Verifies API compatibility using reflection |

## Prerequisites

### For K8s-based Tests

1. **kubectl** installed and configured with a running cluster
2. **K8s cluster** with the following deployed:
   - Kafka pod (with port-forward to localhost:9092)
   - MySQL pod (with port-forward to localhost:3306)
   - OTel Collector pod (with port-forward to localhost:4317)
   - Hadoop 2.x and 3.x pods
   - Hive 2.x and 3.x pods

3. **Local installations** for multi-version testing:
   - Spark: `/opt/spark-*` directories (e.g., `/opt/spark-2.4.4`, `/opt/spark-3.2.0`, `/opt/spark-3.5.5`)
   - Hadoop: `/opt/hadoop-*` directories
   - Hive: `/opt/apache-hive-*` directories

### Quick Setup

Use the provided script to set up the K8s environment:

```bash
# Setup only
./src/test/resources/k8s-test-setup.sh setup

# Run tests
./src/test/resources/k8s-test-setup.sh test

# Cleanup
./src/test/resources/k8s-test-setup.sh cleanup

# Full cycle (default)
./src/test/resources/k8s-test-setup.sh all
```

## Running Tests

### Run All Tests

```bash
cd $PROJECT_ROOT
mvn clean verify -Pspark-3
```

### Run Specific Test

```bash
# Run a specific test class
mvn test -Pspark-3 -pl integration-tests -Dtest=ApiCompatibilityIT

# Run with K8s tests (requires cluster)
mvn test -Pspark-3 -pl integration-tests -DskipK8sTests=false
```

### Skip K8s Tests

To skip tests requiring a K8s cluster:

```bash
mvn test -Pspark-3 -pl integration-tests -DskipK8sTests=true
```

## CI/CD

### GitHub Actions

The project includes a CI workflow in `.github/workflows/ci.yml` that:

1. Builds all Spark version profiles (2.4, 3.0, 3.2, 3.5, 4.0)
2. Runs unit tests for each profile
3. Runs integration tests (with K8s tests skipped in CI)
4. Generates code coverage reports

### Test Matrix

| Profile | Spark | Scala | Java | Notes |
|---------|-------|-------|------|-------|
| `spark-2` | 2.4.8 | 2.11 | 8 | Legacy support |
| `spark-30` | 3.0.3 | 2.12 | 8 | Compatibility |
| `spark-32` | 3.2.4 | 2.12 | 8 | Compatibility |
| `spark-3` | 3.5.5 | 2.12 | 8 | Default (all modules) |
| `spark-4` | 4.0.0 | 2.13 | 17 | Preview |
| `omni` | all | mixed | 8 | Unified JAR |

## API Compatibility Testing

The `ApiCompatibilityIT` test uses reflection to verify that all Spark/Hadoop/Hive APIs accessed by the telemetry plugins actually exist. This catches runtime `NoSuchMethodError` issues at build time.

### Known Version-Specific APIs

| API | Added In | Handling |
|-----|----------|----------|
| `ShuffleReadMetrics.remoteBytesReadToDisk()` | Spark 3.3.0 | Try-catch in Spark 3.0/3.2 adapters |
| `ShuffleReadMetrics.remoteReqsDuration()` | Spark 3.3.0 | Try-catch in Spark 3.0/3.2 adapters |
| `ShuffleWriteMetrics.bytesWritten` | Spark 3.0+ | Adapter selects API based on Spark version |
| `QueryExecution.id` | Spark 3.0+ | Spark 2.x uses different mechanism |

## Troubleshooting

### Test Failures

#### "No Spark installations found"

Ensure Spark is installed in `/opt/spark-*` directories with the telemetry plugin JAR in `jars/`.

#### "kubectl not available"

Install kubectl and configure access to a K8s cluster, or skip K8s tests with `-DskipK8sTests=true`.

#### "Port-forward connection refused"

Ensure the K8s services are running and port-forwards are established:

```bash
kubectl port-forward svc/kafka 9092:9092 &
kubectl port-forward svc/mysql 3306:3306 &
kubectl port-forward svc/otel-collector 4317:4317 &
```

#### "NoSuchMethodError during tests"

This indicates an API compatibility issue. Check that the test is running against the correct Spark version and that the adapter handles the missing API gracefully.

## Writing New Tests

### Adding a New Integration Test

1. Create a test class in `src/test/java/x/mg/metrics/integration/`
2. Extend `K8sTestBase` if K8s access is needed
3. Annotate with `@Tag("integration")`
4. Use `assumeTrue()` for optional prerequisites

Example:

```java
@Tag("integration")
class MyNewIT extends K8sTestBase {

    @Test
    void testSomething() throws Exception {
        // Skip if prerequisite not available
        assumeTrue(someCondition(), "Prerequisite not met");

        // Test code here
    }
}
```

## Contributing

When adding new features:

1. Add unit tests in the relevant module
2. Add integration tests if the feature involves external systems
3. Update `ApiCompatibilityIT` if new APIs are accessed
4. Document any version-specific behavior
