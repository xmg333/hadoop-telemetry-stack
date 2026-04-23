package x.mg.metrics.flink.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JvmMemoryMetricRowTest {

    @Test
    void testFromLabels_AllFieldsPopulated() {
        long timestampMs = 1234567890000L;
        Map<String, String> labels = new HashMap<>();
        labels.put("spark.app.id", "app-123");
        labels.put("spark.executor.id", "executor-1");
        labels.put("spark.app.name", "TestApp");
        labels.put("spark.user", "testuser");
        labels.put("spark.yarn.queue", "production");

        JvmMemoryMetricRow row = JvmMemoryMetricRow.fromLabels(timestampMs, labels);

        assertEquals(timestampMs, row.getTimestampMs());
        assertEquals("app-123", row.getAppId());
        assertEquals("executor-1", row.getExecutorId());
        assertEquals("TestApp", row.getAppName());
        assertEquals("testuser", row.getUserName());
        assertEquals("production", row.getQueue());
    }

    @Test
    void testFromLabels_DefaultValues() {
        long timestampMs = 1234567890000L;
        Map<String, String> labels = new HashMap<>();

        JvmMemoryMetricRow row = JvmMemoryMetricRow.fromLabels(timestampMs, labels);

        assertEquals(timestampMs, row.getTimestampMs());
        assertEquals("unknown", row.getAppId());
        assertEquals("unknown", row.getExecutorId());
        assertEquals("", row.getAppName());
        assertEquals("", row.getUserName());
        assertEquals("", row.getQueue());
    }

    @Test
    void testSetMetricColumn_AllColumns() {
        JvmMemoryMetricRow row = new JvmMemoryMetricRow();

        row.setMetricColumn("heap_used", 512000000.0);
        row.setMetricColumn("non_heap_used", 128000000.0);

        assertEquals(512000000.0, row.getHeapUsed());
        assertEquals(128000000.0, row.getNonHeapUsed());
    }

    @Test
    void testSetMetricColumn_NullByDefault() {
        JvmMemoryMetricRow row = new JvmMemoryMetricRow();

        assertNull(row.getHeapUsed());
        assertNull(row.getNonHeapUsed());
    }

    @Test
    void testSerializable() {
        JvmMemoryMetricRow row = new JvmMemoryMetricRow();
        row.setMetricColumn("heap_used", 512000000.0);

        assertDoesNotThrow(() -> {
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos);
            oos.writeObject(row);
            oos.close();
        });
    }
}
