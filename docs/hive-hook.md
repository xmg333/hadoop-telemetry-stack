# Hive Telemetry Hook -- Deployment & Metrics Reference

Captures HiveServer2 query metrics via the Hive `ExecuteWithHookContext` mechanism and exports them to an OTel Collector. Supports Hive 2.x and 3.x, compatible with MR / Spark / Tez execution engines.

## Deployment

### Distributing the JAR

```bash
# Option A: Standalone Hook JAR
cp hive/hive-telemetry-hook-dist/target/*.jar $HIVE_HOME/lib/

# Option B: Omnipackage (includes all Spark/MR/Hive components)
cp spark/spark-telemetry-dist-omni/target/*.jar $HIVE_HOME/lib/
```

### Configure hive-site.xml

```xml
<property>
  <name>hive.exec.post.hooks</name>
  <value>x.mg.metrics.hivetelemetry.HiveTelemetryHook</value>
</property>
<property>
  <name>hive.telemetry.otel.exporter.endpoint</name>
  <value>http://otel-collector:4317</value>
</property>
<property>
  <name>hive.telemetry.sql.max-length</name>
  <value>4096</value>
</property>
```

### Configuration Methods (Choose One)

| Method | Example |
|--------|---------|
| HiveConf override | `hive.telemetry.*` properties in `hive-site.xml` |
| HOCON config file | `hive.telemetry.config.path=/etc/hive-telemetry.conf` |
| Hybrid (HiveConf overrides file) | Use both, HiveConf takes highest priority |

### Key Configuration Items

| Config Item | Default | Description |
|-------------|---------|-------------|
| `hive.telemetry.otel.exporter.endpoint` | `http://localhost:4317` | OTel Collector gRPC endpoint |
| `hive.telemetry.otel.service.name` | `hive-server2` | OTel service name |
| `hive.telemetry.otel.export.interval.ms` | `10000` | Export interval (ms) |
| `hive.telemetry.metrics.enabled` | `true` | Master switch |
| `hive.telemetry.metrics.query.duration` | `true` | Query duration metrics |
| `hive.telemetry.metrics.query.io` | `true` | IO bytes/rows metrics |
| `hive.telemetry.metrics.query.tables` | `true` | Table-level IO metrics |
| `hive.telemetry.sql.max-length` | `4096` | Maximum SQL text truncation length (characters) |
| `hive.telemetry.filter.user.include` | `[".*"]` | User allowlist (regex) |
| `hive.telemetry.filter.user.exclude` | `[]` | User denylist (regex) |
| `hive.telemetry.filter.operation.include` | `[".*"]` | Operation allowlist (regex) |
| `hive.telemetry.filter.operation.exclude` | `[]` | Operation denylist (regex) |

> **Note**: HiveConf keys must start with `hive.telemetry.`, internally mapped to `hive-telemetry.*`.

> **Note**: `hive.telemetry.sql.max-length` as a HiveConf override may NOT work because the internal config key is `hive-telemetry.metrics.sql.max-length` (the `.metrics.` segment is automatically added). The HOCON file config path (`hive-telemetry.metrics.sql.max-length`) works correctly. Use HOCON for this key.

### HOCON Config File Example

```hocon
# conf/examples/hive-telemetry.conf.example
hive-telemetry {
  otel {
    exporter.endpoint = "http://localhost:4317"
    service.name = "hive-server2"
    export.interval.ms = 10000
  }
  metrics {
    enabled = true
    query.duration = true
    query.io = true
    query.tables = true
    sql.max-length = 4096
  }
  filter {
    user.include = [".*"]
    user.exclude = []
    operation.include = [".*"]
    operation.exclude = []
  }
}
```

---

## OTel Metrics

### Metric List

| Metric Name | Type | Unit | Description | Condition |
|-------------|------|------|-------------|-----------|
| `hive.query.duration_ms` | Histogram | ms | Query execution duration | `query.duration=true` |
| `hive.query.success` | Counter | - | Successful query count | Always recorded |
| `hive.query.failure` | Counter | - | Failed query count | Always recorded |
| `hive.query.input_bytes` | Counter | By | Input bytes | `query.io=true` |
| `hive.query.output_bytes` | Counter | By | Output bytes | `query.io=true` |
| `hive.query.input_rows` | Counter | {rows} | Input rows | `query.io=true` |
| `hive.query.output_rows` | Counter | {rows} | Output rows | `query.io=true` |
| `hive.query.input_tables` | Counter | - | Input table access count | `query.tables=true` |
| `hive.query.output_tables` | Counter | - | Output table access count | `query.tables=true` |
| `hive.table.io.bytes` | Counter | By | Per-table IO bytes (emitted when query.tables=true) |
| `hive.table.io.rows` | Counter | {rows} | Per-table IO rows (emitted when query.tables=true) |
| `hive.table.io.files_read` | Counter | - | Per-table files read count (emitted when query.tables=true) |
| `hive.table.io.time_ms` | Counter | ms | Per-table IO time (emitted when query.tables=true) |

### Metric Attributes

All metrics share the following base attributes:

| Attribute | Description |
|-----------|-------------|
| `hive.query.id` | Hive query ID |
| `hive.query.operation` | Operation type (QUERY / CREATETABLE / INSERT, etc.) |
| `hive.query.user` | Executing user |
| `hive.query.success` | Whether successful ("true" / "false") |
| `hive.query.execution_engine` | Execution engine (mr / spark / tez) |
| `hive.query.sql_text` | SQL query text (truncated) |

Table-level metric extra attributes:

| Attribute | Description |
|-----------|-------------|
| `hive.query.input_table` | Input table name (for `input_tables`) |
| `hive.query.output_table` | Output table name (for `output_tables`) |

---

## Data Flow

```
Hive Query -> HiveTelemetryHook (POST_EXEC) -> HiveHookContext (singleton)
  -> HiveMetricRecorder -> OTel SDK -> OTLP gRPC -> OTel Collector
  -> Kafka -> Flink Consumer -> MySQL / ClickHouse
```

Database tables written:
- `hive_query_metrics` -- Query-level metrics (duration / success / IO / query_text)
- `hive_table_io_metrics` -- Table-level IO metrics (per-table bytes / rows)
- `metric_events` -- Unified wide table (for cross-engine aggregation)

---

## Design Highlights

### Error Isolation

The Hook's `run()` method is entirely wrapped in try/catch -- **no exception propagates to HiveServer2**, ensuring query execution is never impacted.

### Lazy Initialization

The `HiveHookContext` singleton is initialized on the first query using double-checked locking; the OTel SDK is created only once. A JVM shutdown hook is registered to ensure flushing on exit.

### Short-Lived Processes

Hive CLI is a short-lived JVM process. The Hook proactively calls `flush()` after each query to ensure metrics are exported before the JVM exits.

### IO Metric Precision

IO byte and row counts are based on Hive Metastore table statistics (`totalSize`, `numRows`). These are estimates, not actual scan volume. Run `ANALYZE TABLE` after DML to update statistics.

---

## Compatibility

| Hive Version | Compiled Against | Execution Engine | Status |
|--------------|-----------------|------------------|--------|
| Hive 2.3.x | Hive 2.3.9 | MR / Spark | Verified |
| Hive 3.1.x | Hive 2.3.9 | MR / Spark / Tez | Verified |

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| No metric output | Hook not registered | Check `hive.exec.post.hooks` configuration |
| No metric output | JAR not in classpath | Confirm JAR is in `$HIVE_HOME/lib/` |
| IO bytes are 0 | Table missing statistics | Run `ANALYZE TABLE ... COMPUTE STATISTICS` |
| Metrics filtered | User/operation matches exclude rules | Check `filter.user.*` and `filter.operation.*` |
| Hook initialization failed | OTel Collector unreachable | Check endpoint and network connectivity |
