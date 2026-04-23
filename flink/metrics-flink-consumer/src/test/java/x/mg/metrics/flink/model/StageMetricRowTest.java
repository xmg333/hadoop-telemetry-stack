package x.mg.metrics.flink.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StageMetricRowTest {

    @Test
    void testFromLabels_AllFieldsPopulated() {
        long timestampMs = 1234567890000L;
        Map<String, String> labels = new HashMap<>();
        labels.put("spark.app.id", "app-123");
        labels.put("spark.executor.id", "executor-1");
        labels.put("spark.stage.id", "5");
        labels.put("spark.app.name", "TestApp");
        labels.put("spark.user", "testuser");
        labels.put("spark.yarn.queue", "production");

        StageMetricRow row = StageMetricRow.fromLabels(timestampMs, labels);

        assertEquals(timestampMs, row.getTimestampMs());
        assertEquals("app-123", row.getAppId());
        assertEquals("executor-1", row.getExecutorId());
        assertEquals(5, row.getStageId());
        assertEquals("TestApp", row.getAppName());
        assertEquals("testuser", row.getUserName());
        assertEquals("production", row.getQueue());
    }

    @Test
    void testFromLabels_DefaultValues() {
        long timestampMs = 1234567890000L;
        Map<String, String> labels = new HashMap<>();

        StageMetricRow row = StageMetricRow.fromLabels(timestampMs, labels);

        assertEquals(timestampMs, row.getTimestampMs());
        assertEquals("unknown", row.getAppId());
        assertEquals("unknown", row.getExecutorId());
        assertEquals(0, row.getStageId());
        assertEquals("", row.getAppName());
        assertEquals("", row.getUserName());
        assertEquals("", row.getQueue());
    }

    @Test
    void testSetMetricColumn_AllColumns() {
        StageMetricRow row = new StageMetricRow();

        row.setMetricColumn("duration_ms", 5000.0);
        row.setMetricColumn("num_tasks", 100.0);
        row.setMetricColumn("executor_run_time_ms", 4500.0);
        row.setMetricColumn("executor_cpu_time_ns", 4000000000.0);
        row.setMetricColumn("jvm_gc_time_ms", 100.0);
        row.setMetricColumn("peak_execution_memory_bytes", 512000000.0);
        row.setMetricColumn("io_bytes_read", 1024000.0);
        row.setMetricColumn("io_bytes_written", 512000.0);

        assertEquals(5000.0, row.getDurationMs());
        assertEquals(100.0, row.getNumTasks());
        assertEquals(4500.0, row.getExecutorRunTimeMs());
        assertEquals(4000000000.0, row.getExecutorCpuTimeNs());
        assertEquals(100.0, row.getJvmGcTimeMs());
        assertEquals(512000000.0, row.getPeakExecutionMemoryBytes());
        assertEquals(1024000.0, row.getIoBytesRead());
        assertEquals(512000.0, row.getIoBytesWritten());
    }

    @Test
    void testSetMetricColumn_NullByDefault() {
        StageMetricRow row = new StageMetricRow();

        assertNull(row.getDurationMs());
        assertNull(row.getNumTasks());
        assertNull(row.getExecutorRunTimeMs());
        assertNull(row.getJvmGcTimeMs());
        assertNull(row.getIoBytesRead());
        assertNull(row.getIoBytesWritten());
    }

    @Test
    void testSerializable() {
        StageMetricRow row = new StageMetricRow();
        row.setMetricColumn("duration_ms", 5000.0);

        assertDoesNotThrow(() -> {
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos);
            oos.writeObject(row);
            oos.close();
        });
    }
}
