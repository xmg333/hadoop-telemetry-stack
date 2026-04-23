package x.mg.metrics.flink.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MrJobMetricRowTest {

    @Test
    void testFromLabels_AllFieldsPopulated() {
        long timestampMs = 1234567890000L;
        Map<String, String> labels = new HashMap<>();
        labels.put("mr.job.id", "job_123456789_0001");
        labels.put("mr.job.name", "test_mr_job");
        labels.put("mr.job.user", "testuser");
        labels.put("mr.job.state", "SUCCEEDED");
        labels.put("mr.job.queue", "production");
        labels.put("mr.job.finish_time_ms", "1234567890000");
        labels.put("mr.job.start_time_ms", "1234567000000");

        MrJobMetricRow row = MrJobMetricRow.fromLabels(timestampMs, labels);

        assertEquals(1234567890000L, row.getTimestampMs());
        assertEquals("job_123456789_0001", row.getJobId());
        assertEquals("test_mr_job", row.getJobName());
        assertEquals("testuser", row.getUserName());
        assertEquals("SUCCEEDED", row.getState());
        assertEquals("production", row.getQueue());
        assertEquals(1234567000000L, row.getStartTimeMs());
        assertEquals(1234567890000L, row.getFinishTimeMs());
    }

    @Test
    void testFromLabels_DefaultValues() {
        long timestampMs = 1234567890000L;
        Map<String, String> labels = new HashMap<>();

        MrJobMetricRow row = MrJobMetricRow.fromLabels(timestampMs, labels);

        assertEquals(timestampMs, row.getTimestampMs());
        assertEquals("unknown", row.getJobId());
        assertEquals("unknown", row.getJobName());
        assertEquals("unknown", row.getUserName());
        assertEquals("unknown", row.getState());
        assertEquals("unknown", row.getQueue());
        assertEquals(0L, row.getStartTimeMs());
        assertEquals(timestampMs, row.getFinishTimeMs());
    }

    @Test
    void testSetMetricColumn_AllColumns() {
        MrJobMetricRow row = new MrJobMetricRow();

        row.setMetricColumn("hdfs_bytes_read", 1024000.0);
        row.setMetricColumn("hdfs_bytes_written", 512000.0);
        row.setMetricColumn("file_bytes_read", 256000.0);
        row.setMetricColumn("file_bytes_written", 128000.0);
        row.setMetricColumn("map_input_records", 5000.0);
        row.setMetricColumn("map_output_records", 10000.0);
        row.setMetricColumn("map_output_bytes", 2048000.0);
        row.setMetricColumn("reduce_input_records", 10000.0);
        row.setMetricColumn("reduce_output_records", 2000.0);
        row.setMetricColumn("reduce_shuffle_bytes", 2048000.0);
        row.setMetricColumn("spilled_records", 1000.0);
        row.setMetricColumn("cpu_time_ms", 5000.0);
        row.setMetricColumn("gc_time_ms", 1000.0);
        row.setMetricColumn("physical_memory_bytes", 512000000.0);
        row.setMetricColumn("virtual_memory_bytes", 2048000000.0);
        row.setMetricColumn("committed_heap_bytes", 256000000.0);
        row.setMetricColumn("maps_duration_ms", 4000.0);
        row.setMetricColumn("reduces_duration_ms", 3000.0);
        row.setMetricColumn("elapsed_time_ms", 9000.0);
        row.setMetricColumn("launched_maps", 4.0);
        row.setMetricColumn("launched_reduces", 2.0);

        assertEquals(1024000.0, row.getHdfsBytesRead());
        assertEquals(512000.0, row.getHdfsBytesWritten());
        assertEquals(256000.0, row.getFileBytesRead());
        assertEquals(128000.0, row.getFileBytesWritten());
        assertEquals(5000.0, row.getMapInputRecords());
        assertEquals(10000.0, row.getMapOutputRecords());
        assertEquals(2048000.0, row.getMapOutputBytes());
        assertEquals(10000.0, row.getReduceInputRecords());
        assertEquals(2000.0, row.getReduceOutputRecords());
        assertEquals(2048000.0, row.getReduceShuffleBytes());
        assertEquals(1000.0, row.getSpilledRecords());
        assertEquals(5000.0, row.getCpuTimeMs());
        assertEquals(1000.0, row.getGcTimeMs());
        assertEquals(512000000.0, row.getPhysicalMemoryBytes());
        assertEquals(2048000000.0, row.getVirtualMemoryBytes());
        assertEquals(256000000.0, row.getCommittedHeapBytes());
        assertEquals(4000.0, row.getMapsDurationMs());
        assertEquals(3000.0, row.getReducesDurationMs());
        assertEquals(9000.0, row.getElapsedTimeMs());
        assertEquals(4.0, row.getLaunchedMaps());
        assertEquals(2.0, row.getLaunchedReduces());
    }

    @Test
    void testSetMetricColumn_NullByDefault() {
        MrJobMetricRow row = new MrJobMetricRow();

        assertNull(row.getHdfsBytesRead());
        assertNull(row.getCpuTimeMs());
        assertNull(row.getElapsedTimeMs());
    }

    @Test
    void testSerializable() {
        MrJobMetricRow row = new MrJobMetricRow();
        row.setMetricColumn("hdfs_bytes_read", 1024000.0);

        assertDoesNotThrow(() -> {
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos);
            oos.writeObject(row);
            oos.close();
        });
    }
}
