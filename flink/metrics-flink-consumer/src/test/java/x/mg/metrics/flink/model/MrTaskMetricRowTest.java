package x.mg.metrics.flink.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MrTaskMetricRowTest {

    @Test
    void testFromLabels_AllFieldsPopulated() {
        long timestampMs = 1234567890000L;
        Map<String, String> labels = new HashMap<>();
        labels.put("mr.task.id", "task_123456789_0001_m_000000");
        labels.put("mr.task.type", "MAP");
        labels.put("mr.job.id", "job_123456789_0001");
        labels.put("mr.job.name", "test_mr_job");
        labels.put("mr.job.user", "testuser");
        labels.put("mr.task.state", "SUCCEEDED");
        labels.put("mr.job.queue", "production");
        labels.put("mr.job.finish_time_ms", "1234567890000");

        MrTaskMetricRow row = MrTaskMetricRow.fromLabels(timestampMs, labels);

        assertEquals(1234567890000L, row.getTimestampMs());
        assertEquals("task_123456789_0001_m_000000", row.getTaskId());
        assertEquals("MAP", row.getTaskType());
        assertEquals("job_123456789_0001", row.getJobId());
        assertEquals("test_mr_job", row.getJobName());
        assertEquals("testuser", row.getUserName());
        assertEquals("SUCCEEDED", row.getState());
        assertEquals("production", row.getQueue());
    }

    @Test
    void testFromLabels_UseJobFinishTime() {
        long timestampMs = 9999999999999L;
        Map<String, String> labels = new HashMap<>();
        labels.put("mr.job.finish_time_ms", "1234567890000");
        labels.put("mr.task.id", "task_123");

        MrTaskMetricRow row = MrTaskMetricRow.fromLabels(timestampMs, labels);

        assertEquals(1234567890000L, row.getTimestampMs());
        assertEquals("task_123", row.getTaskId());
    }

    @Test
    void testFromLabels_DefaultValues() {
        long timestampMs = 1234567890000L;
        Map<String, String> labels = new HashMap<>();

        MrTaskMetricRow row = MrTaskMetricRow.fromLabels(timestampMs, labels);

        assertEquals(timestampMs, row.getTimestampMs());
        assertEquals("unknown", row.getTaskId());
        assertEquals("unknown", row.getTaskType());
        assertEquals("unknown", row.getJobId());
        assertEquals("unknown", row.getJobName());
        assertEquals("unknown", row.getUserName());
        assertEquals("unknown", row.getState());
        assertEquals("", row.getQueue());
    }

    @Test
    void testSetMetricColumn_AllColumns() {
        MrTaskMetricRow row = new MrTaskMetricRow();

        row.setMetricColumn("hdfs_bytes_read", 256000.0);
        row.setMetricColumn("hdfs_bytes_written", 128000.0);
        row.setMetricColumn("file_bytes_read", 64000.0);
        row.setMetricColumn("file_bytes_written", 32000.0);
        row.setMetricColumn("map_input_records", 1000.0);
        row.setMetricColumn("map_output_records", 2000.0);
        row.setMetricColumn("map_output_bytes", 512000.0);
        row.setMetricColumn("reduce_input_records", 0.0);
        row.setMetricColumn("reduce_output_records", 0.0);
        row.setMetricColumn("reduce_shuffle_bytes", 0.0);
        row.setMetricColumn("spilled_records", 100.0);
        row.setMetricColumn("cpu_time_ms", 2000.0);
        row.setMetricColumn("gc_time_ms", 500.0);
        row.setMetricColumn("duration_ms", 5000.0);
        row.setMetricColumn("success_count", 1.0);
        row.setMetricColumn("failure_count", 0.0);
        row.setMetricColumn("hdfs_read_ops", 10.0);
        row.setMetricColumn("hdfs_write_ops", 5.0);
        row.setMetricColumn("hdfs_large_read_ops", 2.0);

        assertEquals(256000.0, row.getHdfsBytesRead());
        assertEquals(128000.0, row.getHdfsBytesWritten());
        assertEquals(64000.0, row.getFileBytesRead());
        assertEquals(32000.0, row.getFileBytesWritten());
        assertEquals(1000.0, row.getMapInputRecords());
        assertEquals(2000.0, row.getMapOutputRecords());
        assertEquals(512000.0, row.getMapOutputBytes());
        assertEquals(0.0, row.getReduceInputRecords());
        assertEquals(0.0, row.getReduceOutputRecords());
        assertEquals(0.0, row.getReduceShuffleBytes());
        assertEquals(100.0, row.getSpilledRecords());
        assertEquals(2000.0, row.getCpuTimeMs());
        assertEquals(500.0, row.getGcTimeMs());
        assertEquals(5000.0, row.getDurationMs());
        assertEquals(1.0, row.getSuccessCount());
        assertEquals(0.0, row.getFailureCount());
        assertEquals(10.0, row.getHdfsReadOps());
        assertEquals(5.0, row.getHdfsWriteOps());
        assertEquals(2.0, row.getHdfsLargeReadOps());
    }

    @Test
    void testSetMetricColumn_NullByDefault() {
        MrTaskMetricRow row = new MrTaskMetricRow();

        assertNull(row.getHdfsBytesRead());
        assertNull(row.getCpuTimeMs());
        assertNull(row.getDurationMs());
    }

    @Test
    void testSerializable() {
        MrTaskMetricRow row = new MrTaskMetricRow();
        row.setMetricColumn("hdfs_bytes_read", 256000.0);

        assertDoesNotThrow(() -> {
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos);
            oos.writeObject(row);
            oos.close();
        });
    }
}
