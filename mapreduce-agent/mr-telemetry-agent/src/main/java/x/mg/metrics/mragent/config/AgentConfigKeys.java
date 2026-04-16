package x.mg.metrics.mragent.config;

/**
 * Configuration key constants for the MR Telemetry Agent.
 * All config is read from system properties (set via -D flags alongside -javaagent).
 */
public final class AgentConfigKeys {

    private AgentConfigKeys() {}

    // System property keys
    public static final String ENABLED = "mr.telemetry.agent.enabled";
    public static final String OTEL_EXPORTER_ENDPOINT = "mr.telemetry.agent.otel.exporter.endpoint";
    public static final String OTEL_SERVICE_NAME = "mr.telemetry.agent.otel.service.name";
    public static final String OTEL_EXPORT_INTERVAL_MS = "mr.telemetry.agent.otel.export.interval.ms";
    public static final String SAMPLING_INTERVAL_SECS = "mr.telemetry.agent.sampling.interval.secs";

    // Defaults
    public static final boolean DEFAULT_ENABLED = true;
    public static final String DEFAULT_ENDPOINT = "http://localhost:4317";
    public static final String DEFAULT_SERVICE_NAME = "mr-telemetry-agent";
    public static final long DEFAULT_EXPORT_INTERVAL_MS = 10000L;
    public static final int DEFAULT_SAMPLING_INTERVAL_SECS = 5;
}
