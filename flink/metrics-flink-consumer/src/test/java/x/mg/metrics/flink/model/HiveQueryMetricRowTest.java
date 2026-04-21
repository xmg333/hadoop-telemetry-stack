package x.mg.metrics.flink.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HiveQueryMetricRowTest {

    @Test
    void testQueryTextFromLabels() {
        Map<String, String> labels = new HashMap<>();
        labels.put("hive.query.id", "q-1");
        labels.put("hive.query.sql_text", "INSERT OVERWRITE TABLE t SELECT * FROM src");
        HiveQueryMetricRow row = HiveQueryMetricRow.fromLabels(System.currentTimeMillis(), labels);
        assertEquals("INSERT OVERWRITE TABLE t SELECT * FROM src", row.getQueryText());
    }

    @Test
    void testQueryTextNullWhenAbsent() {
        Map<String, String> labels = new HashMap<>();
        labels.put("hive.query.id", "q-1");
        HiveQueryMetricRow row = HiveQueryMetricRow.fromLabels(System.currentTimeMillis(), labels);
        assertNull(row.getQueryText());
    }

    @Test
    void testBasicFieldsFromLabels() {
        Map<String, String> labels = new HashMap<>();
        labels.put("hive.query.id", "q-1");
        labels.put("hive.query.operation", "QUERY");
        labels.put("hive.query.user", "hiveuser");
        labels.put("hive.query.success", "true");
        labels.put("hive.query.execution_engine", "mr");
        HiveQueryMetricRow row = HiveQueryMetricRow.fromLabels(12345L, labels);
        assertEquals("q-1", row.getQueryId());
        assertEquals("QUERY", row.getOperation());
        assertEquals("hiveuser", row.getUserName());
        assertEquals("true", row.getSuccess());
        assertEquals("mr", row.getExecutionEngine());
    }

    @Test
    void testSetMetricColumn() {
        HiveQueryMetricRow row = new HiveQueryMetricRow();
        row.setMetricColumn("duration_ms", 5000.0);
        row.setMetricColumn("input_bytes", 1024.0);
        row.setMetricColumn("output_bytes", 2048.0);
        row.setMetricColumn("input_rows", 100.0);
        row.setMetricColumn("output_rows", 50.0);

        assertEquals(5000.0, row.getDurationMs());
        assertEquals(1024.0, row.getInputBytes());
        assertEquals(2048.0, row.getOutputBytes());
        assertEquals(100.0, row.getInputRows());
        assertEquals(50.0, row.getOutputRows());
    }
}
