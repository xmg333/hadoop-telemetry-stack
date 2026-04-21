package x.mg.metrics.sparktelemetry.config;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TelemetryConfigTest {

    @Test
    void testDefaultValues() {
        TelemetryConfig config = new TelemetryConfig();
        assertEquals("http://localhost:4317", config.getOtelEndpoint());
        assertEquals("grpc", config.getOtelProtocol());
        assertEquals("spark-application", config.getServiceName());
        assertEquals(10000L, config.getExportIntervalMs());
        assertTrue(config.isListenerEnabled());
        assertTrue(config.isCaptureTaskEnd());
        assertTrue(config.isCaptureStageComplete());
        assertTrue(config.isCaptureJobEnd());  // Changed default to true
        assertTrue(config.isSystemMetricsEnabled());
        assertTrue(config.isCaptureJvmMemory());
        assertTrue(config.isCaptureJvmGc());
        assertTrue(config.isCaptureBufferPools());
        // New detail toggles - all default to true
        assertTrue(config.isCaptureTaskExecution());
        assertTrue(config.isCaptureTaskShuffleExtended());
        assertTrue(config.isCaptureTaskInfo());
        assertTrue(config.isCaptureStageDetailed());  // Changed default to true
        assertTrue(config.isCaptureJobLifecycle());   // Changed default to true
    }

    @Test
    void testSparkConfOverrides() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("spark.telemetry.otel.exporter.endpoint", "http://collector:4317");
        overrides.put("spark.telemetry.otel.service.name", "my-spark-job");
        overrides.put("spark.telemetry.otel.export.interval.ms", "5000");
        overrides.put("spark.telemetry.metrics.listener.enabled", "false");

        TelemetryConfig config = new TelemetryConfig(overrides);
        assertEquals("http://collector:4317", config.getOtelEndpoint());
        assertEquals("my-spark-job", config.getServiceName());
        assertEquals(5000L, config.getExportIntervalMs());
        assertFalse(config.isListenerEnabled());
    }

    @Test
    void testBooleanOverrides() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("spark.telemetry.metrics.system.enabled", "false");
        overrides.put("spark.telemetry.metrics.system.capture.jvm-gc", "false");

        TelemetryConfig config = new TelemetryConfig(overrides);
        assertFalse(config.isSystemMetricsEnabled());
        assertFalse(config.isCaptureJvmGc());
        // Others should remain default
        assertTrue(config.isCaptureJvmMemory());
    }

    @Test
    void testAppFilterAccepts() {
        TelemetryConfig config = new TelemetryConfig();
        assertTrue(config.shouldAcceptApp("my-etl-job"));
        assertTrue(config.shouldAcceptApp("anything"));
    }

    @Test
    void testAppFilterExcludes() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("spark.telemetry.filter.app.name.exclude", "test-.*");
        TelemetryConfig config = new TelemetryConfig(overrides);

        assertTrue(config.shouldAcceptApp("my-etl-job"));
        assertFalse(config.shouldAcceptApp("test-job"));
    }

    @Test
    void testAppFilterNull() {
        TelemetryConfig config = new TelemetryConfig();
        assertTrue(config.shouldAcceptApp(null));
    }

    @Test
    void testConfigPathIgnoredAsOverride() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("spark.telemetry.config.path", "/some/path.conf");
        overrides.put("spark.telemetry.otel.service.name", "test-service");

        TelemetryConfig config = new TelemetryConfig(overrides);
        assertEquals("test-service", config.getServiceName());
        // config.path should not appear as a config key
    }

    @Test
    void testEmptyOverrides() {
        TelemetryConfig config = new TelemetryConfig(Collections.emptyMap());
        assertEquals("http://localhost:4317", config.getOtelEndpoint());
    }

    @Test
    void testNullOverrides() {
        TelemetryConfig config = new TelemetryConfig(null);
        assertEquals("http://localhost:4317", config.getOtelEndpoint());
    }

    @Test
    void testDetailToggles() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("spark.telemetry.metrics.task.execution", "false");
        overrides.put("spark.telemetry.metrics.task.shuffle-extended", "false");
        overrides.put("spark.telemetry.metrics.task.info", "false");
        overrides.put("spark.telemetry.metrics.stage.detailed", "true");
        overrides.put("spark.telemetry.metrics.job.lifecycle", "true");

        TelemetryConfig config = new TelemetryConfig(overrides);
        assertFalse(config.isCaptureTaskExecution());
        assertFalse(config.isCaptureTaskShuffleExtended());
        assertFalse(config.isCaptureTaskInfo());
        assertTrue(config.isCaptureStageDetailed());
        assertTrue(config.isCaptureJobLifecycle());
    }
}
