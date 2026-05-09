# Configuration Reference

## Three-Tier Configuration Merge

Priority: Spark conf (`spark.telemetry.*`) > HOCON file (`telemetry.conf`) > built-in defaults

Custom HOCON config file path: `spark.telemetry.config.path=/path/to/telemetry.conf`

## Spark Plugin Configuration

### OTel Configuration

| Config Key | Default | Description |
|-----------|---------|-------------|
| `spark.telemetry.otel.exporter.endpoint` | `http://localhost:4317` | OTel Collector gRPC endpoint |
| `spark.telemetry.otel.service.name` | `spark-application` | OTel service name |
| `spark.telemetry.otel.export.interval.ms` | `10000` | Export interval (ms) |
| `spark.telemetry.otel.exporter.protocol` | `grpc` | Export protocol (grpc/http) |

!!! warning "Config keys must include the full path"
    `spark.telemetry.X` maps to `spark-telemetry.X`; the `.otel.` segment is required:
    - Correct: `spark.telemetry.otel.exporter.endpoint=http://host:4317`
    - Incorrect: `spark.telemetry.exporter.endpoint=http://host:4317`

### Metric Category Switches

All categories default to `true`.

| Config Key | Description |
|-----------|-------------|
| `spark.telemetry.metrics.task.execution` | executor run time, CPU time, GC, scheduler delay, result size, peak memory |
| `spark.telemetry.metrics.task.shuffle-extended` | local blocks fetched, records read, remote bytes to disk |
| `spark.telemetry.metrics.task.info` | task host, locality, speculative attributes |
| `spark.telemetry.metrics.stage.detailed` | stage duration, num tasks, executor time, GC, peak memory, stage IO bytes |
| `spark.telemetry.metrics.job.lifecycle` | job start/end events, job duration, num stages |
| `spark.telemetry.metrics.sql.query-execution` | SQL query metrics (duration, join count, shuffle bytes, query text) |

### SQL Text Configuration

| Config Key | Default | Description |
|-----------|---------|-------------|
| `spark.telemetry.sql.max-length` | `4096` | Maximum truncation length for SQL text (characters) |

### Filter Configuration

| Config Key | Default | Description |
|-----------|---------|-------------|
| `spark.telemetry.filter.app.name.include` | (empty) | Application name include regex |
| `spark.telemetry.filter.app.name.exclude` | (empty) | Application name exclude regex |

## Hive Hook Configuration

HiveConf (`hive.telemetry.*`) > HOCON (`hive-telemetry.conf`) > defaults

| Config Key | Default | Description |
|-----------|---------|-------------|
| `hive.telemetry.otel.exporter.endpoint` | `http://localhost:4317` | OTel Collector endpoint |
| `hive.telemetry.otel.service.name` | `hive-server2` | Service name |
| `hive.telemetry.otel.export.interval.ms` | `10000` | Export interval |
| `hive.telemetry.sql.max-length` | `4096` | SQL text truncation length |
| `hive.telemetry.config.path` | -- | HOCON config file path |

### Metric Category Switches

All categories default to `true`.

| Config Key | Default | Description |
|-----------|---------|-------------|
| `hive.telemetry.metrics.enabled` | `true` | Master switch for all Hive metrics |
| `hive.telemetry.metrics.query.duration` | `true` | Query duration metrics |
| `hive.telemetry.metrics.query.io` | `true` | Query IO metrics (bytes/rows) |
| `hive.telemetry.metrics.query.tables` | `true` | Per-table IO metrics |

### Filter Configuration

| Config Key | Default | Description |
|-----------|---------|-------------|
| `hive.telemetry.filter.user.include` | (empty) | User include regex |
| `hive.telemetry.filter.user.exclude` | (empty) | User exclude regex |
| `hive.telemetry.filter.operation.include` | (empty) | Operation include regex |
| `hive.telemetry.filter.operation.exclude` | (empty) | Operation exclude regex |

### Deployment Configuration

```xml
<!-- hive-site.xml -->
<property>
  <name>hive.exec.post.hooks</name>
  <value>x.mg.metrics.hivetelemetry.HiveTelemetryHook</value>
</property>
<property>
  <name>hive.telemetry.config.path</name>
  <value>/opt/apache-hive-2.3.9-bin/conf/hive-telemetry.conf</value>
</property>
```

Place `hive-telemetry-hook-dist-*.jar` in `$HIVE_HOME/auxlib/` for automatic loading.

## MR Collector Configuration

HOCON config file (`mr-collector.conf`):

```hocon
mr-telemetry {
  history-server {
    url = "http://localhost:19888"
    poll.interval.secs = 30
  }
  otel {
    exporter.endpoint = "http://localhost:4317"
    service.name = "mr-telemetry-collector"
    export.interval.ms = 10000
  }
  collection {
    job.counters = true
    task.counters = true
    job.details = true
  }
}
```

## Flink Consumer Configuration

HOCON config file (`flink-consumer.conf`):

```hocon
flink-consumer {
  kafka {
    bootstrap.servers = "localhost:9092"
    group.id = "flink-metrics-consumer"
    topic = "telemetry-metrics"
    checkpoint.path = "/tmp/flink-consumer-checkpoint.txt"
  }
  sink {
    type = "mysql"  # mysql | clickhouse
    jdbc.url = "jdbc:mysql://localhost:3306/telemetry"
    username = "root"
    password = "root123"
    batch-size = 500
    flush-interval-ms = 5000
  }
}
```
