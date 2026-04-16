package x.mg.metrics.mragent.config;

/**
 * Agent configuration via system properties.
 * Set alongside -javaagent in mapred-site.xml:
 *   mapreduce.map.java.opts = -javaagent:/path/to/agent.jar
 *     -Dmr.telemetry.agent.otel.exporter.endpoint=http://collector:4317
 */
public class AgentConfig {

    public boolean isEnabled() {
        return Boolean.parseBoolean(
            System.getProperty(AgentConfigKeys.ENABLED,
                String.valueOf(AgentConfigKeys.DEFAULT_ENABLED)));
    }

    public String getOtelEndpoint() {
        return System.getProperty(AgentConfigKeys.OTEL_EXPORTER_ENDPOINT,
            AgentConfigKeys.DEFAULT_ENDPOINT);
    }

    public String getServiceName() {
        return System.getProperty(AgentConfigKeys.OTEL_SERVICE_NAME,
            AgentConfigKeys.DEFAULT_SERVICE_NAME);
    }

    public long getExportIntervalMs() {
        return Long.parseLong(
            System.getProperty(AgentConfigKeys.OTEL_EXPORT_INTERVAL_MS,
                String.valueOf(AgentConfigKeys.DEFAULT_EXPORT_INTERVAL_MS)));
    }

    public int getSamplingIntervalSecs() {
        return Integer.parseInt(
            System.getProperty(AgentConfigKeys.SAMPLING_INTERVAL_SECS,
                String.valueOf(AgentConfigKeys.DEFAULT_SAMPLING_INTERVAL_SECS)));
    }
}
