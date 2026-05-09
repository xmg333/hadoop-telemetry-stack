# Architecture Design

## Overall Architecture

```mermaid
graph LR
    subgraph Data Sources
        SP[Spark Plugin]
        MR[MR Collector]
        HV[Hive Hook]
    end
    subgraph Collection Layer
        OTel[OTel Collector]
    end
    subgraph Transport Layer
        KF[Kafka]
    end
    subgraph Consumption Layer
        FK[Flink Consumer]
    end
    subgraph Storage Layer
        MY[MySQL/ClickHouse]
        GF[Grafana]
    end
    SP -->|gRPC OTLP| OTel
    MR -->|gRPC OTLP| OTel
    HV -->|gRPC OTLP| OTel
    OTel -->|Kafka Producer| KF
    KF -->|Kafka Consumer| FK
    FK -->|JDBC Batch| MY
    MY -->|SQL| GF
```

## Module Structure

```
spark/
├── spark-telemetry-common/           # Java-only core: config, models, OTel SDK setup, lifecycle
├── spark-telemetry-adapter-spark2/   # Scala 2.11 adapter for Spark 2.4
├── spark-telemetry-adapter-spark30/  # Scala 2.12 adapter for Spark 3.0
├── spark-telemetry-adapter-spark32/  # Scala 2.12 adapter for Spark 3.2
├── spark-telemetry-adapter-spark3/   # Scala 2.12 adapter for Spark 3.5
├── spark-telemetry-adapter-spark4/   # Scala 2.13 adapter for Spark 4.0
├── spark-telemetry-dist-spark{2,3,4}/ # Shaded fat JARs for each Spark version
├── spark-telemetry-omni-facade/      # Pure Java facade for omnipackage
├── spark-telemetry-adapters-relocated/ # Relocates adapters to v2/v3/v4 packages
└── spark-telemetry-dist-omni/        # Unified distribution: Spark 2/3/4 + MR in one JAR
mapreduce-collector/
├── mr-telemetry-collector/           # Standalone MR job metric collector
└── mr-telemetry-dist/
mapreduce-agent/
├── mr-telemetry-agent/              # MR task-level agent via ByteBuddy
└── mr-telemetry-agent-dist/
hive/
├── hive-telemetry-hook/             # Hive query telemetry hook
└── hive-telemetry-hook-dist/
flink/
├── metrics-flink-consumer/          # Kafka -> MySQL/ClickHouse
└── metrics-flink-consumer-dist/
diagnostic/
└── diagnostic-core/                  # Diagnostic tool
```

## Data Flow

### Spark Plugin

1. **SparkTelemetryPlugin** loaded via `spark.plugins` config
2. **TelemetryDriverPlugin** initializes `TelemetryLifecycle` singleton and registers `SparkTelemetryListener`
3. **TelemetryExecutorPlugin** initializes `TelemetryLifecycle` + `SparkTelemetryMetricsSink` for JVM metrics
4. **SparkTelemetryListener** captures `onTaskEnd`/`onStageCompleted` events
5. **TelemetryLifecycle.accept()** routes events to **MetricRecorder**
6. **MetricRecorder** records OTel counters/histograms
7. **OtelRegistry** manages: PeriodicMetricReader -> OTLP gRPC exporter (DELTA temporality) -> OTel Collector

### Omnipackage Architecture

The omnipackage supports Spark 2/3/4 in a single JAR, auto-detecting the version at runtime:

1. Each adapter is relocated via shade to `x.mg.metrics.sparktelemetry.adapter.internal.v{2,3,4}`
2. Pure Java facade delegates to the version-specific adapter via reflection
3. Version detection uses `Class.forName` to probe for Spark/Scala classes

## Key Design Decisions

| Decision | Reason |
|----------|--------|
| DELTA Temporality | Prevent duplicate data on re-export |
| Async Flush | Avoid DAGScheduler thread blocking |
| appId Fallback | Handle local mode and short-lived applications |
| MR Gauge -> Counter | Avoid `buildWithCallback` memory leak |
| SQL Text LRU Cache | Prevent memory leak, max 1000 entries |
