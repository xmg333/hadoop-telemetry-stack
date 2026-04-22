# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

Maven multi-module project with Spark version profiles:

```bash
# Build for Spark 3.x (default profile)
mvn clean package

# Build for Spark 2.x
mvn clean package -Pspark-2

# Build for Spark 4.x
mvn clean package -Pspark-4

# Build Omnipackage (unified JAR: Spark 2/3/4 + MR Collector + MR Agent)
chmod +x build-omni.sh && ./build-omni.sh

# Run unit tests (default profile)
mvn test

# Run a single test class
mvn test -pl spark/spark-telemetry-common -Dtest=TelemetryConfigTest

# Run integration tests (failsafe plugin, Spark 3 profile only)
mvn verify -Pspark-3

# Skip tests during build
mvn clean package -DskipTests
```

## Architecture

This is a **transparent Spark telemetry plugin** that captures Spark task/stage IO metrics and JVM system metrics, exporting them via OpenTelemetry to an OTel Collector. It also includes a standalone **MR (MapReduce) Telemetry Collector** that polls Hadoop History Server for MR job metrics, and a **Hive Telemetry Hook** that captures Hive query metrics via `ExecuteWithHookContext`.

### Module Structure

```
spark/
├── spark-telemetry-common/           # Java-only core: config, models, OTel SDK setup, lifecycle
├── spark-telemetry-adapter-spark2/   # Scala 2.11 adapter for Spark 2.4 (spark.extraListeners)
├── spark-telemetry-adapter-spark3/   # Scala 2.12 adapter for Spark 3.5 (SparkPlugin API)
├── spark-telemetry-adapter-spark30/  # Scala 2.12 adapter for Spark 3.0 (SparkPlugin API)
├── spark-telemetry-adapter-spark32/  # Scala 2.12 adapter for Spark 3.2 (SparkPlugin API)
├── spark-telemetry-adapter-spark4/   # Scala 2.13 adapter for Spark 4.0 (SparkPlugin API)
├── spark-telemetry-dist-spark{2,3,4}/ # Shaded fat JARs for each Spark version
├── spark-telemetry-omni-facade/      # Pure Java facade for omnipackage (auto-detect + delegate)
├── spark-telemetry-adapters-relocated/ # Intermediate: relocates adapters to v2/v3/v4 packages via shade
└── spark-telemetry-dist-omni/        # Unified distribution: Spark 2/3/4 + MR Collector + MR Agent in one JAR
mapreduce-collector/
├── mr-telemetry-collector/           # Standalone MR job metric collector (Java)
└── mr-telemetry-dist/               # Shaded fat JAR for MR collector
mapreduce-agent/
├── mr-telemetry-agent/              # MR task-level agent via ByteBuddy instrumentation (Java)
└── mr-telemetry-agent-dist/         # Shaded fat JAR for MR agent
hive/
├── hive-telemetry-hook/             # Hive query telemetry hook via ExecuteWithHookContext (Java)
└── hive-telemetry-hook-dist/        # Shaded fat JAR for Hive hook
flink/
├── metrics-flink-consumer/          # Flink job consuming OTLP protobuf from Kafka → MySQL/ClickHouse
└── metrics-flink-consumer-dist/     # Shaded fat JAR for Flink consumer
integration-tests/                   # IT/E2E tests (Spark 3 profile only)
```

### Data Flow (Spark Plugin)

1. **SparkTelemetryPlugin** (Scala, SparkPlugin API) is loaded via `spark.plugins` config
2. **TelemetryDriverPlugin** initializes `TelemetryLifecycle` singleton and registers `SparkTelemetryListener`
3. **TelemetryExecutorPlugin** initializes `TelemetryLifecycle` + `SparkTelemetryMetricsSink` for JVM metrics
4. **SparkTelemetryListener** (extends SparkListener) captures `onTaskEnd`/`onStageCompleted` events, converts to `SparkMetricEvent`
5. **TelemetryLifecycle.accept()** routes events to **MetricRecorder**
6. **MetricRecorder** records OTel counters/histograms via OTel SDK Meter API
7. **OtelRegistry** manages the OTel SDK pipeline: PeriodicMetricReader → OTLP gRPC exporter (DELTA temporality) → OTel Collector

### Key Design Decisions

- **DELTA Temporality**: All OTLP exporters use `AggregationTemporalitySelector.deltaPreferred()` to prevent duplicate data on re-export. This applies to all 3 exporter locations: Spark Plugin (`OtelRegistry`), MR Agent (`AgentOtelRegistry`), MR Collector (`Main.java`).
- **Async Flush**: `TelemetryLifecycle.flushAsync()` uses a single-threaded daemon executor to avoid blocking Spark's DAGScheduler thread on `onJobEnd`. Blocking flush would cause 10s job timeouts.
- **appId Fallback**: All SparkTelemetryListener versions fall back from `SparkEnv.get.conf.getAppId` to `spark.app.name` to `"unknown"` to handle local mode and short-lived apps where appId is null.
- **Stage IO Separation**: Stage-level IO counters (`spark.stage.io.bytes_read/written`) are separate instruments from task-level (`spark.task.io.bytes_read/written`) to prevent counter name collision.
- **MR Gauge→Counter**: MR Collector uses `LongCounter.add()` for all metrics (including formerly gauge-like values) to avoid `buildWithCallback` memory leaks.
- **SQL Text Cache**: Spark 使用 `TelemetryLifecycle` 中的 bounded LRU cache（max 1000 entries）暂存 executionId → sqlText 映射。`SparkTelemetryListener.onOtherEvent` 捕获 `SparkListenerSQLExecutionStart` 写入缓存，`SparkTelemetryQueryExecutionListener.extractMetrics` 读取并清除。`getAndRemoveSqlText` 保证及时清理防止内存泄漏。
- **SQL Text Truncation**: SQL 文本在源头截断（Hive `HiveMetricRecorder` / Spark `QueryExecutionListener`），默认 4096 字符，通过配置 `spark.telemetry.sql.max-length` / `hive.telemetry.sql.max-length` 可调。
- **Spark 2.x Limitation**: Spark 2.x 的 `QueryExecution` 没有 `id` 字段，`executionId` 保持 0 且不捕获 SQL 文本。

### Omnipackage Architecture

The omnipackage (`spark-telemetry-dist-omni`) produces a single JAR supporting Spark 2/3/4 + MR Collector + MR Agent, auto-detecting Spark version at runtime.

**Core challenge**: Three Spark adapters share the same package (`x.mg.metrics.sparktelemetry.adapter`) but are compiled with incompatible Scala versions (2.11/2.12/2.13). They cannot coexist at the same package path.

**Solution**: Package relocation + facade pattern:
1. Each adapter is relocated to `x.mg.metrics.sparktelemetry.adapter.internal.v{2,3,4}` via `maven-shade-plugin` in `spark-telemetry-adapters-relocated`
2. Pure Java facade classes at the original package delegate to the version-specific adapter via reflection
3. Version detection uses `Class.forName` probing — `OmniContext.java` checks for `SparkPlugin` (Spark 3+) and `scala.collection.IterableOnce` (Scala 2.13/Spark 4)

**Key design decision — version detection**:
- Must use `Class.forName("scala.collection.IterableOnce")` to detect Scala 2.13. Do NOT use `scala.jdk.CollectionConverters` — it is backported to Scala 2.12 by `scala-collection-compat` (bundled with Spark 3.2+), causing false Scala 2.13 detection and loading the v4 adapter (compiled with Scala 2.13) on Scala 2.12 runtimes, which crashes with `ClassNotFoundException: scala.$less$colon$less`.
- Must use `Package.getPackage("scala").getImplementationVersion()` is unreliable — returns null during early Spark plugin initialization.

**Key design decision — classloader**: The facade `SparkTelemetryPlugin` must use `SparkTelemetryPlugin.class.getClassLoader()` (child classloader that has `--jars`), NOT `SparkPlugin.class.getClassLoader()` (parent classloader). The parent classloader cannot see the relocated adapter classes (`internal.v32` etc.) that live in the same JAR. Using the wrong classloader causes `ClassNotFoundException` for version-specific adapters.

**Build process**: `build-omni.sh` performs a 7-stage Maven build: common → adapters (3 profiles) → MR modules → relocate → facade + dist. The `omni` profile only includes common, omni-facade, adapters-relocated, and dist-omni modules. Adapter modules are built separately with their own Scala/Spark profiles.

**Backward compatibility**: Per-version dist JARs still build and work unchanged. Existing Spark config keys work with the omnipackage without modification.

### Key Differences Between Spark Versions

- **Spark 2.x**: No SparkPlugin API. Uses `spark.extraListeners` registration. Listener lazily initializes via `ensureInit()` on first event. Shuffle write API uses `shuffleBytesWritten`/`shuffleWriteTime`/`shuffleRecordsWritten`.
- **Spark 3.x**: Uses `SparkPlugin`/`DriverPlugin`/`ExecutorPlugin` API. Shuffle write API uses `bytesWritten`/`writeTime`/`recordsWritten`.
- **Spark 4.x**: Same as 3.x API but compiled against Scala 2.13. Uses `scala.jdk.CollectionConverters` instead of `scala.collection.JavaConverters`.

### Configuration (Three-Tier Merge)

Config is loaded by `TelemetryConfig` with priority: Spark conf overrides (`spark.telemetry.*`) > HOCON file (`telemetry.conf`) > built-in defaults. See `conf/examples/telemetry.conf.example` and `conf/examples/mr-collector.conf.example` for all available options.

**Important**: Spark conf keys must include the full internal path, including `.otel.` segment. The mapping converts `spark.telemetry.X` to `spark-telemetry.X`:
- Correct: `spark.telemetry.otel.exporter.endpoint=http://host:4317`
- Wrong: `spark.telemetry.exporter.endpoint=http://host:4317` (maps to `spark-telemetry.exporter.endpoint`, NOT `spark-telemetry.otel.exporter.endpoint`)
- Same pattern for: `spark.telemetry.otel.service.name`, `spark.telemetry.otel.export.interval.ms`, `spark.telemetry.otel.exporter.protocol`

**Metric Category Switches** (all default to `true`):

| Config Key | Default | Controls |
|------------|---------|----------|
| `spark.telemetry.metrics.task.execution` | `true` | Category 1: executor run time, CPU time, deserialize, GC, scheduler delay, result size, peak memory |
| `spark.telemetry.metrics.task.shuffle-extended` | `true` | Category 2: local blocks fetched, records read, remote bytes to disk, remote reqs duration |
| `spark.telemetry.metrics.task.info` | `true` | Category 3: task host, locality, speculative attributes |
| `spark.telemetry.metrics.stage.detailed` | `true` | Category 4: stage duration, num tasks, executor time, GC, peak memory, stage IO bytes |
| `spark.telemetry.metrics.job.lifecycle` | `true` | Category 5: job start/end events, job duration, num stages |
| `spark.telemetry.metrics.sql.query-execution` | `true` | Category 6: SQL 查询执行指标（duration, join count, shuffle bytes, query text） |
| `spark.telemetry.sql.max-length` | `4096` | SQL 文本最大截断长度（字符） |

Custom HOCON config file path can be specified via `spark.telemetry.config.path=/path/to/telemetry.conf`.

### MR Telemetry Collector

Standalone Java app (`Main.java`) that polls Hadoop YARN History Server REST API for completed MR jobs, extracts counters, and exports via OTel. Runs independently from Spark.

### Hive Telemetry Hook

Hive query telemetry hook that implements `ExecuteWithHookContext`, loaded into HiveServer2 via `hive.exec.post.hooks`. Captures query metrics (duration, IO bytes/rows, input/output tables) and exports via OTel. Compatible with Hive 2.x and 3.x (compiled against Hive 2.3.9).

Data flow: `Hive Query → HiveTelemetryHook (POST_EXEC) → OTel SDK → OTLP gRPC → OTel Collector → Kafka → Flink → MySQL/ClickHouse`

- **Hook class**: `x.mg.metrics.hivetelemetry.HiveTelemetryHook`
- **Lazy init**: `HiveHookContext` singleton initialized on first hook invocation with double-checked locking
- **Error isolation**: Entire `run()` wrapped in try/catch — hook never breaks query execution
- **Configuration**: Three-tier merge: HiveConf (`hive.telemetry.*`) > HOCON (`hive-telemetry.conf`) > defaults

**OTel metric names** (9 instruments):
- `hive.query.duration_ms` (LongHistogram), `hive.query.success` / `hive.query.failure` (LongCounter)
- `hive.query.input_bytes` / `hive.query.output_bytes` / `hive.query.input_rows` / `hive.query.output_rows` (LongCounter)
- `hive.query.input_tables` / `hive.query.output_tables` (LongCounter, per-table data points with `hive.query.input_table` / `hive.query.output_table` attributes)
- `hive.query.sql_text` (String attribute) — SQL 查询文本（截断到 `hive.telemetry.sql.max-length`，默认 4096 字符）

**Flink Consumer tables** (15 tables in MySQL/ClickHouse):
- **Spark**: task_metrics, stage_metrics, job_metrics, jvm_memory_metrics, jvm_gc_metrics
- **Histograms**: task_histogram_buckets, stage_histogram_buckets, job_histogram_buckets
- **Governance**: stage_governance, sql_query_metrics（含 query_text）, sql_query_table_metrics
- **Hive**: hive_query_metrics（含 query_text）, hive_table_io_metrics
- **MR**: mr_job_metrics, mr_task_metrics

```bash
# Build Hive hook
mvn clean package -pl hive/hive-telemetry-hook,hive/hive-telemetry-hook-dist -am -DskipTests

# Deploy: copy shaded JAR to HiveServer2 auxlib, configure hive-site.xml:
# <property>
#   <name>hive.exec.post.hooks</name>
#   <value>x.mg.metrics.hivetelemetry.HiveTelemetryHook</value>
# </property>
# <property>
#   <name>hive.telemetry.otel.exporter.endpoint</name>
#   <value>http://otel-collector:4317</value>
# </property>
```

### Flink Metrics Consumer

Flink 1.18 DataStream job that reads OTLP protobuf metrics from Kafka via Flink KafkaSource and writes to MySQL or ClickHouse with 15-table wide-row schema. Includes histogram bucket support and stage governance pre-aggregation.

Data flow: `Kafka (OTLP Protobuf) → Flink KafkaSource → Deserialization → Filter → ProcessFunction(accumulate) → RichSinkFunction(JDBC)`

**Pipeline operators**:
1. `OtlpDeserializationSchema` — `KafkaRecordDeserializationSchema` wrapping `OtlpMetricsDeserializer`, produces `MetricRecord` per Kafka record
2. `MetricRecordSplitFlatMap` — splits `MetricRecord` into individual `MetricItem` (sample or bucket)
3. `MetricNameFilter` — regex-based include/exclude filter
4. `AccumulatingProcessFunction` — `ProcessFunction` with `ValueState<WideRowAccumulator>`, batch-size and timer-based flush triggers
5. `FlinkCategoryJdbcSink` — `RichSinkFunction` wrapping `CategoryJdbcSink` with Flink lifecycle management

- **Database schema**: 15 tables — `task_metrics`, `stage_metrics`, `job_metrics`, `jvm_memory_metrics`, `jvm_gc_metrics`, `task_histogram_buckets`, `stage_histogram_buckets`, `job_histogram_buckets`, `stage_governance`, `sql_query_metrics`, `sql_query_table_metrics`, `hive_query_metrics`, `hive_table_io_metrics`, `mr_job_metrics`, `mr_task_metrics`
- **Configuration**: HOCON file `flink-consumer.conf` (see `flink-consumer.conf.example`)
- **Sink types**: `mysql` (default) or `clickhouse`, switched via `flink-consumer.sink.type`
- **Flink version**: 1.18.0 (last version supporting Java 8)
- **Checkpointing**: Flink-managed checkpointing replaces manual file-based offset persistence. `flink-consumer.kafka.checkpoint.path` is now the Flink checkpoint storage directory (default: `/tmp/flink-consumer-checkpoint`). Offsets are committed via Flink's checkpoint mechanism
- **Serialization**: All model classes (`MetricSample`, `HistogramBucket`, 13 row types), `WideRowAccumulator`, `FlinkConsumerConfig`, `MetricNameFilter` implement `Serializable`

```bash
# Build Flink consumer
mvn clean package -pl flink/metrics-flink-consumer,flink/metrics-flink-consumer-dist -am -DskipTests

# Run standalone (uses Flink LocalEnvironment, no cluster needed)
java -jar flink/metrics-flink-consumer-dist/target/metrics-flink-consumer-dist-1.0.0-SNAPSHOT.jar flink-consumer.conf

# Submit to Flink cluster
flink run -c x.mg.metrics.flink.Main metrics-flink-consumer-dist-1.0.0-SNAPSHOT.jar flink-consumer.conf
```

### Shading

Distribution modules use `maven-shade-plugin` to create self-contained JARs. OTel, gRPC, protobuf, Typesafe Config, and Guava are relocated under `x.mg.metrics.shaded.*`. Spark and Scala are excluded (provided scope).

**Critical shade plugin requirements** (learned the hard way):
1. **OkHttp3 + Kotlin stdlib required**: OTel's gRPC sender uses OkHttp3, which depends on Kotlin stdlib. Without these, you get `No GrpcSenderProvider found`. Must include:
   - `com.squareup.okhttp3:*`, `com.squareup.okhttp:*`, `com.squareup.okio:*`
   - `org.jetbrains.kotlin:*`, `org.jetbrains:annotations:*`
   - `com.google.code.gson:*`, `org.conscrypt:*`
5. **Flink standalone shade**: For `metrics-flink-consumer-dist`, Flink deps are `provided` scope but must be `compile` in dist module for standalone `java -jar` execution. Must also include `flink-connector-base`, `slf4j-api`, `slf4j-jdk14`. Use `<excludes>` (not `<includes>`) in artifactSet to avoid missing transitive deps like SLF4J.
6. **Flink objects must be Serializable**: All objects passed into Flink operators (model classes, config, filter, accumulators) must implement `Serializable`. `Connection` and `OtlpMetricsDeserializer` are `transient` — initialize in `open()`. `FlinkConsumerConfig` eagerly resolves HOCON values into plain fields since `com.typesafe.config.Config` is not Serializable.
7. **MySQL JSON vs TEXT**: Use `TEXT` column type for labels in MySQL (not `JSON`) to avoid driver-level validation issues with Gson output. Handle `Double.POSITIVE_INFINITY` from OTLP histogram buckets — MySQL doesn't accept it, use `Double.MAX_VALUE` instead.
2. **ServicesResourceTransformer is mandatory**: OTel uses `META-INF/services` for SPI. Without this transformer, service files aren't properly relocated, causing runtime failures.
3. **Only ONE `<transformers>` block**: Maven shade plugin silently uses the last `<transformers>` block if there are duplicates. Merge `ServicesResourceTransformer` and `ManifestResourceTransformer` into one block, and add `<filters>` for signature exclusion separately.
4. **Relocate `okhttp3` and `okio`**: Add relocations for these packages to avoid classpath conflicts.

### Deployment

`deploy/otel-collector/` has the OTel Collector pipeline config. `deploy/grafana/` has Grafana dashboard JSON files. `deploy/sql/` has database migration scripts.
