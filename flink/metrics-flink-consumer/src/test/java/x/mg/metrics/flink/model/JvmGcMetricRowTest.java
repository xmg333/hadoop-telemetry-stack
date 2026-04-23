package x.mg.metrics.flink.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JvmGcMetricRowTest {

    @Test
    void testFromLabels_AllFieldsPopulated() {
        long timestampMs = 1234567890000L;
        Map<String, String> labels = new HashMap<>();
        labels.put("spark.app.id", "app-123");
        labels.put("spark.executor.id", "executor-1");
        labels.put("gc_name", "G1 Young Generation");
        labels.put("spark.app.name", "TestApp");
        labels.put("spark.user", "testuser");
        labels.put("spark.yarn.queue", "production");

        JvmGcMetricRow row = JvmGcMetricRow.fromLabels(timestampMs, labels);

        assertEquals(timestampMs, row.getTimestampMs());
        assertEquals("app-123", row.getAppId());
        assertEquals("executor-1", row.getExecutorId());
        assertEquals("G1 Young Generation", row.getGcName());
        assertEquals("TestApp", row.getAppName());
        assertEquals("testuser", row.getUserName());
        assertEquals("production", row.getQueue());
    }

    @Test
    void testFromLabels_DefaultValues() {
        long timestampMs = 1234567890000L;
        Map<String, String> labels = new HashMap<>();

        JvmGcMetricRow row = JvmGcMetricRow.fromLabels(timestampMs, labels);

        assertEquals(timestampMs, row.getTimestampMs());
        assertEquals("unknown", row.getAppId());
        assertEquals("unknown", row.getExecutorId());
        assertEquals("unknown", row.getGcName());
        assertEquals("", row.getAppName());
        assertEquals("", row.getUserName());
        assertEquals("", row.getQueue());
    }

    @Test
    void testSetMetricColumn_AllColumns() {
        JvmGcMetricRow row = new JvmGcMetricRow();

        row.setMetricColumn("gc_count", 100.0);
        row.setMetricColumn("gc_time_ms", 5000.0);

        assertEquals(100.0, row.getGcCount());
        assertEquals(5000.0, row.getGcTimeMs());
    }

    @Test
    void testSetMetricColumn_NullByDefault() {
        JvmGcMetricRow row = new JvmGcMetricRow();

        assertNull(row.getGcCount());
        assertNull(row.getGcTimeMs());
    }

    @Test
    void testSerializable() {
        JvmGcMetricRow row = new JvmGcMetricRow();
        row.setMetricColumn("gc_count", 100.0);

        assertDoesNotThrow(() -> {
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos);
            oos.writeObject(row);
            oos.close();
        });
    }
}
