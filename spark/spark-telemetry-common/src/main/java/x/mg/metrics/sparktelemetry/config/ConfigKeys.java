package x.mg.metrics.sparktelemetry.config;

/**
 * Configuration key constants for the Spark Telemetry Listener.
 */
public final class ConfigKeys {

    private ConfigKeys() {}

    // Config prefix
    public static final String PREFIX = "spark-telemetry";

    // OTel
    public static final String OTEL_EXPORTER_ENDPOINT = "spark-telemetry.otel.exporter.endpoint";
    public static final String OTEL_EXPORTER_PROTOCOL = "spark-telemetry.otel.exporter.protocol";
    public static final String OTEL_SERVICE_NAME = "spark-telemetry.otel.service.name";
    public static final String OTEL_EXPORT_INTERVAL_MS = "spark-telemetry.otel.export.interval.ms";
    public static final String OTEL_RESOURCE_ATTRIBUTES = "spark-telemetry.otel.resource.attributes";

    // Metrics - Listener
    public static final String METRICS_LISTENER_ENABLED = "spark-telemetry.metrics.listener.enabled";
    public static final String METRICS_LISTENER_TASK_END = "spark-telemetry.metrics.listener.capture.task-end";
    public static final String METRICS_LISTENER_STAGE_COMPLETE = "spark-telemetry.metrics.listener.capture.stage-complete";
    public static final String METRICS_LISTENER_JOB_END = "spark-telemetry.metrics.listener.capture.job-end";

    // Metrics - System
    public static final String METRICS_SYSTEM_ENABLED = "spark-telemetry.metrics.system.enabled";
    public static final String METRICS_SYSTEM_JVM_MEMORY = "spark-telemetry.metrics.system.capture.jvm-memory";
    public static final String METRICS_SYSTEM_JVM_GC = "spark-telemetry.metrics.system.capture.jvm-gc";
    public static final String METRICS_SYSTEM_BUFFER_POOLS = "spark-telemetry.metrics.system.capture.buffer-pools";
    public static final String METRICS_SYSTEM_EXECUTOR_MEMORY = "spark-telemetry.metrics.system.capture.executor-memory";

    // Metrics - Detail
    public static final String METRICS_TASK_EXECUTION = "spark-telemetry.metrics.task.execution";
    public static final String METRICS_TASK_SHUFFLE_EXTENDED = "spark-telemetry.metrics.task.shuffle-extended";
    public static final String METRICS_TASK_INFO = "spark-telemetry.metrics.task.info";
    public static final String METRICS_STAGE_DETAILED = "spark-telemetry.metrics.stage.detailed";
    public static final String METRICS_JOB_LIFECYCLE = "spark-telemetry.metrics.job.lifecycle";
    public static final String METRICS_SQL_QUERY_EXECUTION = "spark-telemetry.metrics.sql.query-execution";
    public static final String METRICS_SQL_MAX_LENGTH = "spark-telemetry.metrics.sql.max-length";

    // Filter
    public static final String FILTER_APP_NAME_INCLUDE = "spark-telemetry.filter.app.name.include";
    public static final String FILTER_APP_NAME_EXCLUDE = "spark-telemetry.filter.app.name.exclude";

    // Spark conf prefix for overrides
    public static final String SPARK_CONF_PREFIX = "spark.telemetry.";

    // Config file path key
    public static final String CONFIG_PATH_KEY = "spark.telemetry.config.path";

    // Defaults
    public static final String DEFAULT_ENDPOINT = "http://localhost:4317";
    public static final String DEFAULT_PROTOCOL = "grpc";
    public static final String DEFAULT_SERVICE_NAME = "spark-application";
    public static final long DEFAULT_EXPORT_INTERVAL_MS = 10000L;
}
