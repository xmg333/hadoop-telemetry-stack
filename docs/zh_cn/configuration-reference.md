# 配置参考

## 三层配置合并

优先级: Spark conf (`spark.telemetry.*`) > HOCON file (`telemetry.conf`) > 内置默认值

自定义 HOCON 配置文件路径: `spark.telemetry.config.path=/path/to/telemetry.conf`

## Spark Plugin 配置

### OTel 配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.telemetry.otel.exporter.endpoint` | `http://localhost:4317` | OTel Collector gRPC 端点 |
| `spark.telemetry.otel.service.name` | `spark-telemetry` | OTel 服务名 |
| `spark.telemetry.otel.export.interval.ms` | `60000` | 导出间隔（毫秒） |
| `spark.telemetry.otel.exporter.protocol` | `grpc` | 导出协议 (grpc/http) |

!!! warning "配置键必须包含完整路径"
    `spark.telemetry.X` 映射到 `spark-telemetry.X`，必须包含 `.otel.` 段：
    - 正确: `spark.telemetry.otel.exporter.endpoint=http://host:4317`
    - 错误: `spark.telemetry.exporter.endpoint=http://host:4317`

### 指标类别开关

所有类别默认 `true`。

| 配置项 | 说明 |
|--------|------|
| `spark.telemetry.metrics.task.execution` | executor run time, CPU time, GC, scheduler delay, result size, peak memory |
| `spark.telemetry.metrics.task.shuffle-extended` | local blocks fetched, records read, remote bytes to disk |
| `spark.telemetry.metrics.task.info` | task host, locality, speculative attributes |
| `spark.telemetry.metrics.stage.detailed` | stage duration, num tasks, executor time, GC, peak memory, stage IO bytes |
| `spark.telemetry.metrics.job.lifecycle` | job start/end events, job duration, num stages |
| `spark.telemetry.metrics.sql.query-execution` | SQL 查询指标 (duration, join count, shuffle bytes, query text) |

### SQL 文本配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.telemetry.sql.max-length` | `4096` | SQL 文本最大截断长度（字符） |

## Hive Hook 配置

HiveConf (`hive.telemetry.*`) > HOCON (`hive-telemetry.conf`) > defaults

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `hive.telemetry.otel.exporter.endpoint` | `http://localhost:4317` | OTel Collector 端点 |
| `hive.telemetry.otel.service.name` | `hive-telemetry` | 服务名 |
| `hive.telemetry.otel.export.interval.ms` | `60000` | 导出间隔 |
| `hive.telemetry.sql.max-length` | `4096` | SQL 文本截断长度 |
| `hive.telemetry.config.path` | — | HOCON 配置文件路径 |

### 部署配置

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

将 `hive-telemetry-hook-dist-*.jar` 放入 `$HIVE_HOME/auxlib/` 即可自动加载。

## MR Collector 配置

HOCON 配置文件 (`mr-collector.conf`):

```hocon
mr-telemetry {
  history-server {
    url = "http://localhost:19888"
    poll.interval.secs = 30
  }
  otel {
    exporter.endpoint = "http://localhost:4317"
    service.name = "mr-telemetry-collector"
    export.interval.ms = 30000
  }
  collection {
    job.counters = true
    task.counters = true
    job.details = true
  }
}
```

## Flink Consumer 配置

HOCON 配置文件 (`flink-consumer.conf`):

```hocon
flink-consumer {
  kafka {
    bootstrap-servers = "localhost:9092"
    group-id = "flink-consumer"
    topic = "telemetry-metrics"
    checkpoint.path = "/tmp/flink-consumer-checkpoint"
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
