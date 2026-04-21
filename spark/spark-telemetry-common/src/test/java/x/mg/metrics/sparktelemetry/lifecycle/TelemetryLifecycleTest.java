package x.mg.metrics.sparktelemetry.lifecycle;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import x.mg.metrics.sparktelemetry.config.TelemetryConfig;
import x.mg.metrics.sparktelemetry.model.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TelemetryLifecycleTest {

    @AfterEach
    void tearDown() {
        TelemetryLifecycle.reset();
    }

    @Test
    void testInitWithDefaults() {
        // Use a non-routable endpoint to avoid connection attempts
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.telemetry.otel.exporter.endpoint", "http://localhost:9999");

        TelemetryLifecycle lifecycle = TelemetryLifecycle.init(conf);
        assertNotNull(lifecycle);
        assertTrue(TelemetryLifecycle.isInitialized());
        assertSame(lifecycle, TelemetryLifecycle.getInstance());
    }

    @Test
    void testSingletonBehavior() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.telemetry.otel.exporter.endpoint", "http://localhost:9999");

        TelemetryLifecycle first = TelemetryLifecycle.init(conf);
        TelemetryLifecycle second = TelemetryLifecycle.init(conf);
        assertSame(first, second);
    }

    @Test
    void testReset() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.telemetry.otel.exporter.endpoint", "http://localhost:9999");

        TelemetryLifecycle.init(conf);
        assertTrue(TelemetryLifecycle.isInitialized());

        TelemetryLifecycle.reset();
        assertFalse(TelemetryLifecycle.isInitialized());
    }

    @Test
    void testGetInstanceWithoutInit() {
        TelemetryLifecycle.reset();
        assertThrows(IllegalStateException.class, TelemetryLifecycle::getInstance);
    }

    @Test
    void testAcceptNullEvent() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.telemetry.otel.exporter.endpoint", "http://localhost:9999");

        TelemetryLifecycle lifecycle = TelemetryLifecycle.init(conf);
        // Should not throw
        assertDoesNotThrow(() -> lifecycle.accept(null));
    }

    @Test
    void testAcceptTaskEndEvent() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.telemetry.otel.exporter.endpoint", "http://localhost:9999");

        TelemetryLifecycle lifecycle = TelemetryLifecycle.init(conf);

        SparkMetricEvent event = new SparkMetricEvent();
        event.setEventType(SparkMetricEvent.EventType.TASK_END);
        event.setTimestamp(System.currentTimeMillis());
        event.setApplicationId("test-app");
        event.setExecutorId("0");
        event.setStageId(1);
        event.setTaskId(1);
        event.setTaskSuccessful(true);

        IOMetrics io = new IOMetrics();
        io.setBytesRead(1024);
        io.setBytesWritten(512);
        event.setIoMetrics(io);

        // Should not throw
        assertDoesNotThrow(() -> lifecycle.accept(event));
    }

    @Test
    void testAcceptSystemMetricsEvent() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.telemetry.otel.exporter.endpoint", "http://localhost:9999");

        TelemetryLifecycle lifecycle = TelemetryLifecycle.init(conf);

        SparkMetricEvent event = new SparkMetricEvent();
        event.setEventType(SparkMetricEvent.EventType.PERIODIC_SYSTEM);
        event.setTimestamp(System.currentTimeMillis());
        event.setExecutorId("0");

        MemoryMetrics mem = new MemoryMetrics();
        mem.setHeapUsed(1000000);
        mem.setNonHeapUsed(500000);
        event.setMemoryMetrics(mem);

        GCMetrics gc = new GCMetrics();
        gc.addCollector("G1", 10, 100);
        event.setGcMetrics(gc);

        assertDoesNotThrow(() -> lifecycle.accept(event));
    }

    @Test
    void testStop() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.telemetry.otel.exporter.endpoint", "http://localhost:9999");

        TelemetryLifecycle lifecycle = TelemetryLifecycle.init(conf);
        assertTrue(TelemetryLifecycle.isInitialized());

        lifecycle.stop();
        assertFalse(TelemetryLifecycle.isInitialized());
    }

    @Test
    void testGetConfig() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.telemetry.otel.exporter.endpoint", "http://localhost:9999");
        conf.put("spark.telemetry.otel.service.name", "test-job");

        TelemetryLifecycle lifecycle = TelemetryLifecycle.init(conf);
        TelemetryConfig config = lifecycle.getConfig();
        assertNotNull(config);
        assertEquals("test-job", config.getServiceName());
    }

    @Test
    void testSqlTextCachePutAndGet() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.telemetry.otel.exporter.endpoint", "http://localhost:9999");
        TelemetryLifecycle lifecycle = TelemetryLifecycle.init(conf);

        lifecycle.putSqlText(42L, "SELECT * FROM t");
        assertEquals("SELECT * FROM t", lifecycle.getAndRemoveSqlText(42L));
    }

    @Test
    void testSqlTextCacheGetAndRemove() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.telemetry.otel.exporter.endpoint", "http://localhost:9999");
        TelemetryLifecycle lifecycle = TelemetryLifecycle.init(conf);

        lifecycle.putSqlText(1L, "SQL1");
        lifecycle.getAndRemoveSqlText(1L);
        assertNull(lifecycle.getAndRemoveSqlText(1L));
    }

    @Test
    void testSqlTextCacheLruEviction() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.telemetry.otel.exporter.endpoint", "http://localhost:9999");
        TelemetryLifecycle lifecycle = TelemetryLifecycle.init(conf);

        for (long i = 0; i < 1001; i++) {
            lifecycle.putSqlText(i, "SQL" + i);
        }
        assertNull(lifecycle.getAndRemoveSqlText(0L));
        assertNotNull(lifecycle.getAndRemoveSqlText(1000L));
    }

    @Test
    void testSqlTextCacheNotFound() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.telemetry.otel.exporter.endpoint", "http://localhost:9999");
        TelemetryLifecycle lifecycle = TelemetryLifecycle.init(conf);

        assertNull(lifecycle.getAndRemoveSqlText(999L));
    }
}
