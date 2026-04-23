package x.mg.metrics.sparktelemetry.lifecycle;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import x.mg.metrics.sparktelemetry.config.TelemetryConfig;
import x.mg.metrics.sparktelemetry.model.*;

import java.util.ArrayList;
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

    @Test
    void testUserFallsBackToSystemProperty() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.telemetry.otel.exporter.endpoint", "http://localhost:9999");
        // Intentionally NOT setting "spark.user" — should fall back to user.name system property
        TelemetryLifecycle lifecycle = TelemetryLifecycle.init(conf);
        assertEquals(System.getProperty("user.name"), lifecycle.getUser());
    }

    @Test
    void testUserFromSparkConf() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.telemetry.otel.exporter.endpoint", "http://localhost:9999");
        conf.put("spark.user", "customuser");
        TelemetryLifecycle lifecycle = TelemetryLifecycle.init(conf);
        assertEquals("customuser", lifecycle.getUser());
    }

    @Test
    void testQueueFromSparkConf() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.telemetry.otel.exporter.endpoint", "http://localhost:9999");
        conf.put("spark.yarn.queue", "production");
        TelemetryLifecycle lifecycle = TelemetryLifecycle.init(conf);
        assertEquals("production", lifecycle.getQueue());
    }

    @Test
    void testQueueEmptyWhenNotSet() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.telemetry.otel.exporter.endpoint", "http://localhost:9999");
        TelemetryLifecycle lifecycle = TelemetryLifecycle.init(conf);
        assertEquals("", lifecycle.getQueue());
    }

    @Test
    void testAcceptSqlExecutionEvent() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.telemetry.otel.exporter.endpoint", "http://localhost:9999");

        TelemetryLifecycle lifecycle = TelemetryLifecycle.init(conf);

        SparkMetricEvent event = new SparkMetricEvent();
        event.setEventType(SparkMetricEvent.EventType.SQL_EXECUTION);
        event.setTimestamp(System.currentTimeMillis());
        event.setApplicationId("test-app");

        SqlExecutionMetrics sqlMetrics = new SqlExecutionMetrics();
        sqlMetrics.setExecutionId(1);
        sqlMetrics.setDurationMs(100);
        sqlMetrics.setSuccess(true);
        sqlMetrics.setQueryText("SELECT * FROM t");
        event.setSqlExecutionMetrics(sqlMetrics);

        SqlTableIOMetrics tableIO = new SqlTableIOMetrics();
        tableIO.setExecutionId(1);
        tableIO.setTableName("default.t");
        tableIO.setOperation("scan");
        tableIO.setBytes(1024);
        tableIO.setRows(100);

        ArrayList<SqlTableIOMetrics> tableIOList = new ArrayList<>();
        tableIOList.add(tableIO);
        event.setSqlTableIOMetrics(tableIOList);

        assertDoesNotThrow(() -> lifecycle.accept(event));
    }

    @Test
    void testAcceptJobLifecycleEvents() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.telemetry.otel.exporter.endpoint", "http://localhost:9999");

        TelemetryLifecycle lifecycle = TelemetryLifecycle.init(conf);

        SparkMetricEvent startEvent = new SparkMetricEvent();
        startEvent.setEventType(SparkMetricEvent.EventType.JOB_START);
        startEvent.setTimestamp(System.currentTimeMillis());
        startEvent.setApplicationId("test-app");
        startEvent.setJobId(1);
        startEvent.setJobNumStages(2);
        startEvent.setUser("testuser");
        startEvent.setQueue("default");

        SparkMetricEvent endEvent = new SparkMetricEvent();
        endEvent.setEventType(SparkMetricEvent.EventType.JOB_END);
        endEvent.setTimestamp(System.currentTimeMillis() + 1000);
        endEvent.setApplicationId("test-app");
        endEvent.setJobId(1);
        endEvent.setJobSuccessful(true);
        endEvent.setJobDurationMs(1000);

        assertDoesNotThrow(() -> {
            lifecycle.accept(startEvent);
            lifecycle.accept(endEvent);
        });
    }
}
