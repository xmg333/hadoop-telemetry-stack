package x.mg.metrics.mrtelemetry.config;

public final class MRConfigKeys {
    private MRConfigKeys() {}

    public static final String PREFIX = "mr-telemetry";

    // History Server
    public static final String HS_URL = "mr-telemetry.history-server.url";
    public static final String HS_POLL_INTERVAL_SECS = "mr-telemetry.history-server.poll.interval.secs";
    public static final String HS_CONNECT_TIMEOUT_SECS = "mr-telemetry.history-server.connect.timeout.secs";
    public static final String HS_READ_TIMEOUT_SECS = "mr-telemetry.history-server.read.timeout.secs";

    // OTel
    public static final String OTEL_EXPORTER_ENDPOINT = "mr-telemetry.otel.exporter.endpoint";
    public static final String OTEL_EXPORTER_PROTOCOL = "mr-telemetry.otel.exporter.protocol";
    public static final String OTEL_SERVICE_NAME = "mr-telemetry.otel.service.name";
    public static final String OTEL_EXPORT_INTERVAL_MS = "mr-telemetry.otel.export.interval.ms";

    // State
    public static final String STATE_FILE = "mr-telemetry.state.file";

    // Filter
    public static final String FILTER_USER_INCLUDE = "mr-telemetry.filter.user.include";
    public static final String FILTER_USER_EXCLUDE = "mr-telemetry.filter.user.exclude";
    public static final String FILTER_JOB_NAME_INCLUDE = "mr-telemetry.filter.job.name.include";
    public static final String FILTER_JOB_NAME_EXCLUDE = "mr-telemetry.filter.job.name.exclude";

    // Collection
    public static final String COLLECTION_JOB_COUNTERS = "mr-telemetry.collection.job.counters";
    public static final String COLLECTION_TASK_COUNTERS = "mr-telemetry.collection.task.counters";
    public static final String COLLECTION_JOB_DETAILS = "mr-telemetry.collection.job.details";

    // Defaults
    public static final String DEFAULT_HS_URL = "http://localhost:19888";
    public static final int DEFAULT_POLL_INTERVAL_SECS = 30;
    public static final int DEFAULT_CONNECT_TIMEOUT_SECS = 10;
    public static final int DEFAULT_READ_TIMEOUT_SECS = 30;
    public static final String DEFAULT_SERVICE_NAME = "mr-telemetry-collector";
    public static final long DEFAULT_EXPORT_INTERVAL_MS = 10000L;
    public static final String DEFAULT_STATE_FILE = "/tmp/mr-telemetry-state.json";
    public static final String DEFAULT_ENDPOINT = "http://localhost:4317";
}
