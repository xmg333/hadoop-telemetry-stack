package x.mg.metrics.flink.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JobMetricRowTest {

    @Test
    void testFromLabels_AllFieldsPopulated() {
        long timestampMs = 1234567890000L;
        Map<String, String> labels = new HashMap<>();
        labels.put("spark.app.id", "app-123");
        labels.put("spark.job.id", "10");
        labels.put("spark.job.success", "true");
        labels.put("spark.app.name", "TestApp");
        labels.put("spark.user", "testuser");
        labels.put("spark.yarn.queue", "production");

        JobMetricRow row = JobMetricRow.fromLabels(timestampMs, labels);

        assertEquals(timestampMs, row.getTimestampMs());
        assertEquals("app-123", row.getAppId());
        assertEquals(10, row.getJobId());
        assertEquals("true", row.getJobSuccess());
        assertEquals("TestApp", row.getAppName());
        assertEquals("testuser", row.getUserName());
        assertEquals("production", row.getQueue());
    }

    @Test
    void testFromLabels_DefaultValues() {
        long timestampMs = 1234567890000L;
        Map<String, String> labels = new HashMap<>();

        JobMetricRow row = JobMetricRow.fromLabels(timestampMs, labels);

        assertEquals(timestampMs, row.getTimestampMs());
        assertEquals("unknown", row.getAppId());
        assertEquals(0, row.getJobId());
        assertNull(row.getJobSuccess());
        assertEquals("", row.getAppName());
        assertEquals("", row.getUserName());
        assertEquals("", row.getQueue());
    }

    @Test
    void testSetMetricColumn_AllColumns() {
        JobMetricRow row = new JobMetricRow();

        row.setMetricColumn("duration_ms", 10000.0);
        row.setMetricColumn("num_stages", 5.0);

        assertEquals(10000.0, row.getDurationMs());
        assertEquals(5.0, row.getNumStages());
    }

    @Test
    void testSetMetricColumn_NullByDefault() {
        JobMetricRow row = new JobMetricRow();

        assertNull(row.getDurationMs());
        assertNull(row.getNumStages());
    }

    @Test
    void testSerializable() {
        JobMetricRow row = new JobMetricRow();
        row.setMetricColumn("duration_ms", 10000.0);

        assertDoesNotThrow(() -> {
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos);
            oos.writeObject(row);
            oos.close();
        });
    }
}
