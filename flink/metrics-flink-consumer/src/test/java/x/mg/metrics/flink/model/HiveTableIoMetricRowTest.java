package x.mg.metrics.flink.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HiveTableIoMetricRowTest {

    @Test
    void testFromLabels_InputTable() {
        long timestampMs = 1234567890000L;
        Map<String, String> labels = new HashMap<>();
        labels.put("hive.query.id", "query-123");
        labels.put("hive.query.operation", "QUERY");
        labels.put("hive.query.user", "hiveuser");
        labels.put("hive.query.execution_engine", "tez");
        labels.put("hive.query.input_table", "default.users");
        labels.put("hive.query.queue", "production");

        HiveTableIoMetricRow row = HiveTableIoMetricRow.fromLabels(timestampMs, labels);

        assertEquals(timestampMs, row.getTimestampMs());
        assertEquals("query-123", row.getQueryId());
        assertEquals("default.users", row.getTableName());
        assertEquals("input", row.getTableType());
        assertEquals("QUERY", row.getOperation());
        assertEquals("hiveuser", row.getUserName());
        assertEquals("tez", row.getExecutionEngine());
        assertEquals("production", row.getQueue());
    }

    @Test
    void testFromLabels_OutputTable() {
        long timestampMs = 1234567890000L;
        Map<String, String> labels = new HashMap<>();
        labels.put("hive.query.id", "query-456");
        labels.put("hive.query.operation", "QUERY");
        labels.put("hive.query.user", "hiveuser");
        labels.put("hive.query.execution_engine", "spark");
        labels.put("hive.query.output_table", "default.results");

        HiveTableIoMetricRow row = HiveTableIoMetricRow.fromLabels(timestampMs, labels);

        assertEquals("query-456", row.getQueryId());
        assertEquals("default.results", row.getTableName());
        assertEquals("output", row.getTableType());
        assertEquals("spark", row.getExecutionEngine());
    }

    @Test
    void testFromLabels_DefaultValues() {
        long timestampMs = 1234567890000L;
        Map<String, String> labels = new HashMap<>();

        HiveTableIoMetricRow row = HiveTableIoMetricRow.fromLabels(timestampMs, labels);

        assertEquals(timestampMs, row.getTimestampMs());
        assertEquals("unknown", row.getQueryId());
        assertEquals("unknown", row.getTableName());
        assertEquals("unknown", row.getTableType());
        assertEquals("unknown", row.getOperation());
        assertEquals("unknown", row.getUserName());
        assertEquals("unknown", row.getExecutionEngine());
        assertNull(row.getQueue());
    }

    @Test
    void testSetMetricColumn_AllColumns() {
        HiveTableIoMetricRow row = new HiveTableIoMetricRow();

        row.setMetricColumn("input_table_count", 2.0);
        row.setMetricColumn("output_table_count", 1.0);
        row.setMetricColumn("bytes", 2048000.0);
        row.setMetricColumn("rows", 100000.0);
        row.setMetricColumn("files_read", 20.0);
        row.setMetricColumn("time_ms", 10000.0);

        assertEquals(2.0, row.getInputTableCount());
        assertEquals(1.0, row.getOutputTableCount());
        assertEquals(2048000.0, row.getBytes());
        assertEquals(100000.0, row.getRows());
        assertEquals(20.0, row.getFilesRead());
        assertEquals(10000.0, row.getTimeMs());
    }

    @Test
    void testSetMetricColumn_NullByDefault() {
        HiveTableIoMetricRow row = new HiveTableIoMetricRow();

        assertNull(row.getInputTableCount());
        assertNull(row.getOutputTableCount());
        assertNull(row.getBytes());
        assertNull(row.getRows());
        assertNull(row.getFilesRead());
        assertNull(row.getTimeMs());
    }

    @Test
    void testSerializable() {
        HiveTableIoMetricRow row = new HiveTableIoMetricRow();
        row.setMetricColumn("bytes", 2048000.0);

        assertDoesNotThrow(() -> {
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos);
            oos.writeObject(row);
            oos.close();
        });
    }
}
