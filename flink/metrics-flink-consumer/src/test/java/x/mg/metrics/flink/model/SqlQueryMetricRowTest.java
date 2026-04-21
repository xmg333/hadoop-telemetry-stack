package x.mg.metrics.flink.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SqlQueryMetricRowTest {

    @Test
    void testQueryTextFromLabels() {
        Map<String, String> labels = new HashMap<>();
        labels.put("spark.app.id", "app-1");
        labels.put("spark.sql.execution_id", "42");
        labels.put("spark.sql.query_text", "SELECT * FROM users WHERE id = 1");
        SqlQueryMetricRow row = SqlQueryMetricRow.fromLabels(System.currentTimeMillis(), labels);
        assertEquals("SELECT * FROM users WHERE id = 1", row.getQueryText());
    }

    @Test
    void testQueryTextNullWhenAbsent() {
        Map<String, String> labels = new HashMap<>();
        labels.put("spark.app.id", "app-1");
        labels.put("spark.sql.execution_id", "42");
        SqlQueryMetricRow row = SqlQueryMetricRow.fromLabels(System.currentTimeMillis(), labels);
        assertNull(row.getQueryText());
    }

    @Test
    void testBasicFieldsFromLabels() {
        Map<String, String> labels = new HashMap<>();
        labels.put("spark.app.id", "app-1");
        labels.put("spark.sql.execution_id", "42");
        labels.put("spark.app.name", "test-app");
        labels.put("spark.user", "testuser");
        labels.put("spark.yarn.queue", "default");
        SqlQueryMetricRow row = SqlQueryMetricRow.fromLabels(12345L, labels);
        assertEquals(12345L, row.getTimestampMs());
        assertEquals("app-1", row.getAppId());
        assertEquals("42", row.getExecutionId());
        assertEquals("test-app", row.getAppName());
        assertEquals("testuser", row.getUserName());
        assertEquals("default", row.getQueue());
    }

    @Test
    void testSetMetricColumn() {
        SqlQueryMetricRow row = new SqlQueryMetricRow();
        row.setMetricColumn("duration_ms", 100.0);
        row.setMetricColumn("shuffle_bytes_read", 1024.0);
        row.setMetricColumn("shuffle_bytes_written", 2048.0);
        row.setMetricColumn("join_count", 3.0);

        assertEquals(100.0, row.getDurationMs());
        assertEquals(1024.0, row.getShuffleBytesRead());
        assertEquals(2048.0, row.getShuffleBytesWritten());
        assertEquals(3.0, row.getJoinCount());
    }
}
