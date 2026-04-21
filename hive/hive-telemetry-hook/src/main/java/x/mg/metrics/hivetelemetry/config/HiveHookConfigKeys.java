package x.mg.metrics.hivetelemetry.config;

public final class HiveHookConfigKeys {
    private HiveHookConfigKeys() {}

    public static final String PREFIX = "hive-telemetry";

    // OTel
    public static final String OTEL_EXPORTER_ENDPOINT = "hive-telemetry.otel.exporter.endpoint";
    public static final String OTEL_SERVICE_NAME = "hive-telemetry.otel.service.name";
    public static final String OTEL_EXPORT_INTERVAL_MS = "hive-telemetry.otel.export.interval.ms";

    // Metrics
    public static final String METRICS_ENABLED = "hive-telemetry.metrics.enabled";
    public static final String METRICS_QUERY_DURATION = "hive-telemetry.metrics.query.duration";
    public static final String METRICS_QUERY_IO = "hive-telemetry.metrics.query.io";
    public static final String METRICS_QUERY_TABLES = "hive-telemetry.metrics.query.tables";
    public static final String SQL_MAX_LENGTH = "hive-telemetry.metrics.sql.max-length";

    // Filter
    public static final String FILTER_USER_INCLUDE = "hive-telemetry.filter.user.include";
    public static final String FILTER_USER_EXCLUDE = "hive-telemetry.filter.user.exclude";
    public static final String FILTER_OPERATION_INCLUDE = "hive-telemetry.filter.operation.include";
    public static final String FILTER_OPERATION_EXCLUDE = "hive-telemetry.filter.operation.exclude";

    // Config file path (set via HiveConf)
    public static final String CONFIG_PATH = "hive.telemetry.config.path";

    // Defaults
    public static final String DEFAULT_ENDPOINT = "http://localhost:4317";
    public static final String DEFAULT_SERVICE_NAME = "hive-server2";
    public static final long DEFAULT_EXPORT_INTERVAL_MS = 10000L;
}
