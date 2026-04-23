package x.mg.metrics.flink.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TaskMetricRowTest {

    @Test
    void testFromLabels_AllFieldsPopulated() {
        long timestampMs = 1234567890000L;
        Map<String, String> labels = new HashMap<>();
        labels.put("spark.app.id", "app-123");
        labels.put("spark.executor.id", "executor-1");
        labels.put("spark.stage.id", "5");
        labels.put("spark.task.id", "100");
        labels.put("spark.task.success", "true");
        labels.put("spark.task.host", "host1.example.com");
        labels.put("spark.task.locality", "NODE_LOCAL");
        labels.put("spark.task.speculative", "false");
        labels.put("spark.app.name", "TestApp");
        labels.put("spark.user", "testuser");
        labels.put("spark.yarn.queue", "production");

        TaskMetricRow row = TaskMetricRow.fromLabels(timestampMs, labels);

        assertEquals(timestampMs, row.getTimestampMs());
        assertEquals("app-123", row.getAppId());
        assertEquals("executor-1", row.getExecutorId());
        assertEquals(5, row.getStageId());
        assertEquals(100L, row.getTaskId());
        assertEquals("true", row.getTaskSuccess());
        assertEquals("host1.example.com", row.getTaskHost());
        assertEquals("NODE_LOCAL", row.getTaskLocality());
        assertEquals("false", row.getTaskSpeculative());
        assertEquals("TestApp", row.getAppName());
        assertEquals("testuser", row.getUserName());
        assertEquals("production", row.getQueue());
    }

    @Test
    void testFromLabels_DefaultValues() {
        long timestampMs = 1234567890000L;
        Map<String, String> labels = new HashMap<>();

        TaskMetricRow row = TaskMetricRow.fromLabels(timestampMs, labels);

        assertEquals(timestampMs, row.getTimestampMs());
        assertEquals("unknown", row.getAppId());
        assertEquals("unknown", row.getExecutorId());
        assertEquals(0, row.getStageId());
        assertEquals(0L, row.getTaskId());
        assertNull(row.getTaskSuccess());
        assertNull(row.getTaskHost());
        assertNull(row.getTaskLocality());
        assertNull(row.getTaskSpeculative());
        assertEquals("", row.getAppName());
        assertEquals("", row.getUserName());
        assertEquals("", row.getQueue());
    }

    @Test
    void testSetMetricColumn_AllColumns() {
        TaskMetricRow row = new TaskMetricRow();

        row.setMetricColumn("duration_ms", 1000.0);
        row.setMetricColumn("io_bytes_read", 512000.0);
        row.setMetricColumn("io_bytes_written", 256000.0);
        row.setMetricColumn("io_records_read", 1000.0);
        row.setMetricColumn("io_records_written", 500.0);
        row.setMetricColumn("shuffle_bytes_read", 2048000.0);
        row.setMetricColumn("shuffle_bytes_written", 1024000.0);
        row.setMetricColumn("shuffle_fetch_wait_time_ms", 100.0);
        row.setMetricColumn("disk_bytes_spilled", 50000.0);
        row.setMetricColumn("memory_bytes_spilled", 100000.0);
        row.setMetricColumn("executor_run_time_ms", 900.0);
        row.setMetricColumn("executor_cpu_time_ns", 800000000.0);
        row.setMetricColumn("deserialize_time_ms", 50.0);
        row.setMetricColumn("deserialize_cpu_time_ns", 40000000.0);
        row.setMetricColumn("result_serialization_time_ms", 30.0);
        row.setMetricColumn("jvm_gc_time_ms", 20.0);
        row.setMetricColumn("scheduler_delay_ms", 10.0);
        row.setMetricColumn("result_size_bytes", 1000.0);
        row.setMetricColumn("peak_execution_memory_bytes", 512000000.0);
        row.setMetricColumn("shuffle_local_blocks_fetched", 100.0);
        row.setMetricColumn("shuffle_records_read", 2000.0);
        row.setMetricColumn("shuffle_remote_bytes_read_to_disk", 256000.0);
        row.setMetricColumn("shuffle_remote_reqs_duration_ms", 200.0);

        assertEquals(1000.0, row.getDurationMs());
        assertEquals(512000.0, row.getIoBytesRead());
        assertEquals(256000.0, row.getIoBytesWritten());
        assertEquals(1000.0, row.getIoRecordsRead());
        assertEquals(500.0, row.getIoRecordsWritten());
        assertEquals(2048000.0, row.getShuffleBytesRead());
        assertEquals(1024000.0, row.getShuffleBytesWritten());
        assertEquals(100.0, row.getShuffleFetchWaitTimeMs());
        assertEquals(50000.0, row.getDiskBytesSpilled());
        assertEquals(100000.0, row.getMemoryBytesSpilled());
        assertEquals(900.0, row.getExecutorRunTimeMs());
        assertEquals(800000000.0, row.getExecutorCpuTimeNs());
        assertEquals(50.0, row.getDeserializeTimeMs());
        assertEquals(40000000.0, row.getDeserializeCpuTimeNs());
        assertEquals(30.0, row.getResultSerializationTimeMs());
        assertEquals(20.0, row.getJvmGcTimeMs());
        assertEquals(10.0, row.getSchedulerDelayMs());
        assertEquals(1000.0, row.getResultSizeBytes());
        assertEquals(512000000.0, row.getPeakExecutionMemoryBytes());
        assertEquals(100.0, row.getShuffleLocalBlocksFetched());
        assertEquals(2000.0, row.getShuffleRecordsRead());
        assertEquals(256000.0, row.getShuffleRemoteBytesReadToDisk());
        assertEquals(200.0, row.getShuffleRemoteReqsDurationMs());
    }

    @Test
    void testSetMetricColumn_NullByDefault() {
        TaskMetricRow row = new TaskMetricRow();

        assertNull(row.getDurationMs());
        assertNull(row.getIoBytesRead());
        assertNull(row.getIoBytesWritten());
        assertNull(row.getExecutorRunTimeMs());
        assertNull(row.getJvmGcTimeMs());
    }

    @Test
    void testSerializable() {
        TaskMetricRow row = new TaskMetricRow();
        row.setMetricColumn("duration_ms", 1000.0);

        assertDoesNotThrow(() -> {
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos);
            oos.writeObject(row);
            oos.close();
        });
    }
}
