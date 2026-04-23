package x.mg.metrics.mrtelemetry.otel;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import x.mg.metrics.mrtelemetry.model.MRJobMetrics;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MRMetricRecorderTest {

    private MRMetricRecorder recorder;

    @BeforeEach
    void setUp() {
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .build();
        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
            .setMeterProvider(meterProvider)
            .build();
        recorder = new MRMetricRecorder(openTelemetry);
    }

    @Test
    void testRecordJob_AllFieldsPopulated() {
        MRJobMetrics metrics = new MRJobMetrics();
        metrics.setJobId("job_123456789_0001");
        metrics.setJobName("test_mr_job");
        metrics.setUser("testuser");
        metrics.setQueue("default");
        metrics.setState("SUCCEEDED");
        metrics.setSubmitTime(1000000L);
        metrics.setStartTime(1001000L);
        metrics.setFinishTime(1010000L);
        metrics.setElapsedTime(9000L);
        metrics.setTotalMaps(4);
        metrics.setTotalReduces(2);
        metrics.setFailedMaps(0);
        metrics.setFailedReduces(0);
        metrics.setHdfsBytesRead(1024000L);
        metrics.setHdfsBytesWritten(512000L);
        metrics.setFileBytesRead(256000L);
        metrics.setFileBytesWritten(128000L);
        metrics.setMapInputRecords(5000L);
        metrics.setMapOutputRecords(10000L);
        metrics.setMapOutputBytes(2048000L);
        metrics.setReduceInputRecords(10000L);
        metrics.setReduceOutputRecords(2000L);
        metrics.setReduceShuffleBytes(2048000L);
        metrics.setSpilledRecords(1000L);
        metrics.setCpuMilliseconds(5000L);
        metrics.setGcTimeMillis(1000L);
        metrics.setPhysicalMemoryBytes(512000000L);
        metrics.setVirtualMemoryBytes(2048000000L);
        metrics.setCommittedHeapBytes(256000000L);
        metrics.setMillisMaps(4000L);
        metrics.setMillisReduces(3000L);

        assertDoesNotThrow(() -> recorder.record(metrics));
    }

    @Test
    void testRecordTask_AllFieldsPopulated() {
        String taskId = "task_123456789_0001_m_000000";
        String taskType = "MAP";
        String jobId = "job_123456789_0001";
        String jobName = "test_mr_job";
        String user = "testuser";
        String queue = "default";
        String taskState = "SUCCEEDED";
        long taskElapsedTime = 5000L;
        long jobFinishTime = 1010000L;

        Map<String, Long> counters = new HashMap<>();
        counters.put("mr.task.io.map_input_records", 1000L);
        counters.put("mr.task.io.map_output_records", 2000L);
        counters.put("mr.task.io.map_output_bytes", 512000L);
        counters.put("mr.task.io.reduce_input_records", 0L);
        counters.put("mr.task.io.reduce_output_records", 0L);
        counters.put("mr.task.io.reduce_shuffle_bytes", 0L);
        counters.put("mr.task.io.spilled_records", 100L);
        counters.put("mr.task.cpu_time_ms", 2000L);
        counters.put("mr.task.gc_time_ms", 500L);
        counters.put("mr.task.io.hdfs_bytes_read", 256000L);
        counters.put("mr.task.io.hdfs_bytes_written", 128000L);
        counters.put("mr.task.io.file_bytes_read", 64000L);
        counters.put("mr.task.io.file_bytes_written", 32000L);

        assertDoesNotThrow(() -> recorder.recordTask(taskId, taskType, jobId, jobName, user, queue,
            taskState, taskElapsedTime, jobFinishTime, counters));
    }

    @Test
    void testRecordTask_FailedTask() {
        String taskId = "task_123456789_0001_m_000001";
        String taskType = "MAP";
        String jobId = "job_123456789_0001";
        String jobName = "test_mr_job";
        String user = "testuser";
        String queue = "default";
        String taskState = "FAILED";
        long taskElapsedTime = 1000L;
        long jobFinishTime = 1010000L;

        Map<String, Long> counters = new HashMap<>();

        assertDoesNotThrow(() -> recorder.recordTask(taskId, taskType, jobId, jobName, user, queue,
            taskState, taskElapsedTime, jobFinishTime, counters));
    }

    @Test
    void testRecordNullJob() {
        assertDoesNotThrow(() -> recorder.record(null));
    }

    @Test
    void testRecordTaskWithNullCounters() {
        assertDoesNotThrow(() -> recorder.recordTask("task_id", "MAP", "job_id", "job_name",
            "user", "queue", "SUCCEEDED", 1000L, 2000L, null));
    }

    @Test
    void testRecordTaskWithEmptyCounters() {
        Map<String, Long> counters = new HashMap<>();
        assertDoesNotThrow(() -> recorder.recordTask("task_id", "REDUCE", "job_id", "job_name",
            "user", "queue", "SUCCEEDED", 1000L, 2000L, counters));
    }
}
