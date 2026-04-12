package x.mg.metrics.mragent.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AgentConfigTest {

    @BeforeEach
    void clearProperties() {
        System.clearProperty(AgentConfigKeys.ENABLED);
        System.clearProperty(AgentConfigKeys.OTEL_EXPORTER_ENDPOINT);
        System.clearProperty(AgentConfigKeys.OTEL_SERVICE_NAME);
        System.clearProperty(AgentConfigKeys.OTEL_EXPORT_INTERVAL_MS);
        System.clearProperty(AgentConfigKeys.SAMPLING_INTERVAL_SECS);
    }

    @AfterEach
    void restoreProperties() {
        System.clearProperty(AgentConfigKeys.ENABLED);
        System.clearProperty(AgentConfigKeys.OTEL_EXPORTER_ENDPOINT);
        System.clearProperty(AgentConfigKeys.OTEL_SERVICE_NAME);
        System.clearProperty(AgentConfigKeys.OTEL_EXPORT_INTERVAL_MS);
        System.clearProperty(AgentConfigKeys.SAMPLING_INTERVAL_SECS);
    }

    @Test
    void testDefaults() {
        AgentConfig config = new AgentConfig();
        assertTrue(config.isEnabled());
        assertEquals("http://localhost:4317", config.getOtelEndpoint());
        assertEquals("mr-telemetry-agent", config.getServiceName());
        assertEquals(10000L, config.getExportIntervalMs());
        assertEquals(5, config.getSamplingIntervalSecs());
    }

    @Test
    void testSystemPropertyOverrides() {
        System.setProperty(AgentConfigKeys.OTEL_EXPORTER_ENDPOINT, "http://collector:4317");
        System.setProperty(AgentConfigKeys.OTEL_SERVICE_NAME, "my-agent");
        System.setProperty(AgentConfigKeys.OTEL_EXPORT_INTERVAL_MS, "5000");
        System.setProperty(AgentConfigKeys.SAMPLING_INTERVAL_SECS, "10");

        AgentConfig config = new AgentConfig();
        assertEquals("http://collector:4317", config.getOtelEndpoint());
        assertEquals("my-agent", config.getServiceName());
        assertEquals(5000L, config.getExportIntervalMs());
        assertEquals(10, config.getSamplingIntervalSecs());
    }

    @Test
    void testDisabled() {
        System.setProperty(AgentConfigKeys.ENABLED, "false");
        AgentConfig config = new AgentConfig();
        assertFalse(config.isEnabled());
    }
}
