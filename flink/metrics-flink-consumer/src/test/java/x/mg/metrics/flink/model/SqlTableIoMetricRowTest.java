package x.mg.metrics.flink.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SqlTableIoMetricRowTest {

    @Test
    void testFromLabels_AllFieldsPopulated() {
        long timestampMs = 1234567890000L;
        Map<String, String> labels = new HashMap<>();
        labels.put("spark.sql.execution_id", "100");
        labels.put("spark.sql.table_name", "default.users");
        labels.put("spark.sql.operation", "scan");
        labels.put("spark.app.id", "app-123");
        labels.put("spark.app.name", "TestApp");
        labels.put("spark.user", "testuser");
        labels.put("spark.yarn.queue", "production");

        SqlTableIoMetricRow row = SqlTableIoMetricRow.fromLabels(timestampMs, labels);

        assertEquals(timestampMs, row.getTimestampMs());
        assertEquals("app-123", row.getAppId());
        assertEquals("100", row.getExecutionId());
        assertEquals("default.users", row.getTableName());
        assertEquals("scan", row.getOperation());
        assertEquals("TestApp", row.getAppName());
        assertEquals("testuser", row.getUserName());
        assertEquals("production", row.getQueue());
    }

    @Test
    void testFromLabels_DefaultValues() {
        long timestampMs = 1234567890000L;
        Map<String, String> labels = new HashMap<>();

        SqlTableIoMetricRow row = SqlTableIoMetricRow.fromLabels(timestampMs, labels);

        assertEquals(timestampMs, row.getTimestampMs());
        assertEquals("unknown", row.getAppId());
        assertEquals("0", row.getExecutionId());
        assertEquals("unknown", row.getTableName());
        assertEquals("unknown", row.getOperation());
        assertEquals("", row.getAppName());
        assertEquals("", row.getUserName());
        assertEquals("", row.getQueue());
    }

    @Test
    void testSetMetricColumn_AllColumns() {
        SqlTableIoMetricRow row = new SqlTableIoMetricRow();

        row.setMetricColumn("bytes", 1024000.0);
        row.setMetricColumn("rows", 10000.0);
        row.setMetricColumn("files_read", 10.0);
        row.setMetricColumn("time_ms", 5000.0);

        assertEquals(1024000.0, row.getBytes());
        assertEquals(10000.0, row.getRows());
        assertEquals(10.0, row.getFilesRead());
        assertEquals(5000.0, row.getTimeMs());
    }

    @Test
    void testSetMetricColumn_NullByDefault() {
        SqlTableIoMetricRow row = new SqlTableIoMetricRow();

        assertNull(row.getBytes());
        assertNull(row.getRows());
        assertNull(row.getFilesRead());
        assertNull(row.getTimeMs());
    }

    @Test
    void testSerializable() {
        SqlTableIoMetricRow row = new SqlTableIoMetricRow();
        row.setMetricColumn("bytes", 1024000.0);

        assertDoesNotThrow(() -> {
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos);
            oos.writeObject(row);
            oos.close();
        });
    }
}
