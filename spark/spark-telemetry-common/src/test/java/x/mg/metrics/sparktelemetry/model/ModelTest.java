package x.mg.metrics.sparktelemetry.model;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ModelTest {

    @Test
    void testIOMetrics() {
        IOMetrics io = new IOMetrics();
        io.setBytesRead(1024);
        io.setBytesWritten(2048);
        io.setRecordsRead(100);
        io.setRecordsWritten(50);
        io.setShuffleRemoteBytesRead(512);
        io.setShuffleLocalBytesRead(256);
        io.setShuffleRemoteBlocksFetched(10);
        io.setShuffleFetchWaitTime(100);
        io.setShuffleBytesWritten(768);
        io.setShuffleWriteTime(200);
        io.setShuffleRecordsWritten(30);
        io.setDiskBytesSpilled(128);
        io.setMemoryBytesSpilled(64);

        assertEquals(1024, io.getBytesRead());
        assertEquals(2048, io.getBytesWritten());
        assertEquals(100, io.getRecordsRead());
        assertEquals(50, io.getRecordsWritten());
        assertEquals(768, io.getShuffleTotalBytesRead()); // 512 + 256
        assertEquals(768, io.getShuffleBytesWritten());
        assertEquals(128, io.getDiskBytesSpilled());
        assertEquals(64, io.getMemoryBytesSpilled());
    }

    @Test
    void testIOMetricsDefaults() {
        IOMetrics io = new IOMetrics();
        assertEquals(0, io.getBytesRead());
        assertEquals(0, io.getShuffleTotalBytesRead());
    }

    @Test
    void testMemoryMetrics() {
        MemoryMetrics mem = new MemoryMetrics();
        mem.setHeapUsed(1000000);
        mem.setHeapCommitted(2000000);
        mem.setHeapMax(4000000);
        mem.setNonHeapUsed(500000);
        mem.setNonHeapCommitted(600000);
        mem.setDirectBufferCount(10);
        mem.setDirectBufferUsed(100000);
        mem.setDirectBufferCapacity(200000);

        assertEquals(1000000, mem.getHeapUsed());
        assertEquals(2000000, mem.getHeapCommitted());
        assertEquals(4000000, mem.getHeapMax());
        assertEquals(500000, mem.getNonHeapUsed());
        assertEquals(10, mem.getDirectBufferCount());
    }

    @Test
    void testGCMetrics() {
        GCMetrics gc = new GCMetrics();
        gc.addCollector("G1 Old Generation", 5, 500);
        gc.addCollector("G1 Young Generation", 100, 200);

        Map<String, GCMetrics.GCCollectorStats> collectors = gc.getCollectors();
        assertEquals(2, collectors.size());

        GCMetrics.GCCollectorStats old = collectors.get("G1 Old Generation");
        assertEquals(5, old.getCount());
        assertEquals(500, old.getTimeMs());

        assertEquals(105, gc.getTotalGcCount());
        assertEquals(700, gc.getTotalGcTimeMs());
    }

    @Test
    void testGCMetricsEmpty() {
        GCMetrics gc = new GCMetrics();
        assertEquals(0, gc.getTotalGcCount());
        assertEquals(0, gc.getTotalGcTimeMs());
        assertTrue(gc.getCollectors().isEmpty());
    }

    @Test
    void testSparkMetricEvent() {
        SparkMetricEvent event = new SparkMetricEvent();
        event.setEventType(SparkMetricEvent.EventType.TASK_END);
        event.setTimestamp(System.currentTimeMillis());
        event.setApplicationId("app-20260411-1234");
        event.setApplicationName("test-job");
        event.setExecutorId("3");
        event.setStageId(5);
        event.setTaskId(42);
        event.setTaskSuccessful(true);

        IOMetrics io = new IOMetrics();
        io.setBytesRead(1024);
        event.setIoMetrics(io);

        assertEquals(SparkMetricEvent.EventType.TASK_END, event.getEventType());
        assertEquals("app-20260411-1234", event.getApplicationId());
        assertEquals("test-job", event.getApplicationName());
        assertEquals("3", event.getExecutorId());
        assertEquals(5, event.getStageId());
        assertEquals(42, event.getTaskId());
        assertTrue(event.isTaskSuccessful());
        assertNotNull(event.getIoMetrics());
        assertEquals(1024, event.getIoMetrics().getBytesRead());
    }

    @Test
    void testSparkMetricEventSystemMetrics() {
        SparkMetricEvent event = new SparkMetricEvent();
        event.setEventType(SparkMetricEvent.EventType.PERIODIC_SYSTEM);

        MemoryMetrics mem = new MemoryMetrics();
        mem.setHeapUsed(500000);
        event.setMemoryMetrics(mem);

        GCMetrics gc = new GCMetrics();
        gc.addCollector("G1", 10, 100);
        event.setGcMetrics(gc);

        assertEquals(SparkMetricEvent.EventType.PERIODIC_SYSTEM, event.getEventType());
        assertNotNull(event.getMemoryMetrics());
        assertEquals(500000, event.getMemoryMetrics().getHeapUsed());
        assertNotNull(event.getGcMetrics());
        assertEquals(1, event.getGcMetrics().getCollectors().size());
    }

    @Test
    void testAllEventTypes() {
        // Verify all event types including new JOB_START
        assertEquals(6, SparkMetricEvent.EventType.values().length);
        assertNotNull(SparkMetricEvent.EventType.valueOf("JOB_START"));
        for (SparkMetricEvent.EventType type : SparkMetricEvent.EventType.values()) {
            SparkMetricEvent event = new SparkMetricEvent();
            event.setEventType(type);
            assertEquals(type, event.getEventType());
        }
    }

    @Test
    void testTaskExecutionMetrics() {
        TaskExecutionMetrics exec = new TaskExecutionMetrics();
        exec.setExecutorRunTime(1000);
        exec.setExecutorCpuTime(500_000_000L);
        exec.setExecutorDeserializeTime(50);
        exec.setExecutorDeserializeCpuTime(10_000_000L);
        exec.setResultSerializationTime(5);
        exec.setJvmGcTime(200);
        exec.setSchedulerDelay(30);
        exec.setResultSize(65536);
        exec.setPeakExecutionMemory(1_048_576);

        assertEquals(1000, exec.getExecutorRunTime());
        assertEquals(500_000_000L, exec.getExecutorCpuTime());
        assertEquals(50, exec.getExecutorDeserializeTime());
        assertEquals(10_000_000L, exec.getExecutorDeserializeCpuTime());
        assertEquals(5, exec.getResultSerializationTime());
        assertEquals(200, exec.getJvmGcTime());
        assertEquals(30, exec.getSchedulerDelay());
        assertEquals(65536, exec.getResultSize());
        assertEquals(1_048_576, exec.getPeakExecutionMemory());
    }

    @Test
    void testIOMetricsExtendedShuffle() {
        IOMetrics io = new IOMetrics();
        io.setShuffleLocalBlocksFetched(42);
        io.setShuffleRecordsRead(1000);
        io.setShuffleRemoteBytesReadToDisk(2048);
        io.setShuffleRemoteReqsDuration(150);

        assertEquals(42, io.getShuffleLocalBlocksFetched());
        assertEquals(1000, io.getShuffleRecordsRead());
        assertEquals(2048, io.getShuffleRemoteBytesReadToDisk());
        assertEquals(150, io.getShuffleRemoteReqsDuration());
    }

    @Test
    void testSparkMetricEventNewFields() {
        SparkMetricEvent event = new SparkMetricEvent();

        // Task info (Category 3)
        event.setTaskHost("executor-1.host");
        event.setTaskLocality("NODE_LOCAL");
        event.setTaskSpeculative(true);
        assertEquals("executor-1.host", event.getTaskHost());
        assertEquals("NODE_LOCAL", event.getTaskLocality());
        assertTrue(event.isTaskSpeculative());

        // Stage detailed (Category 4)
        event.setStageNumTasks(100);
        event.setStageDurationMs(5000);
        assertEquals(100, event.getStageNumTasks());
        assertEquals(5000, event.getStageDurationMs());

        // Job lifecycle (Category 5)
        event.setJobId(7);
        event.setJobNumStages(3);
        event.setJobDurationMs(30000);
        event.setJobSuccessful(true);
        assertEquals(7, event.getJobId());
        assertEquals(3, event.getJobNumStages());
        assertEquals(30000, event.getJobDurationMs());
        assertTrue(event.isJobSuccessful());

        // Task execution (Category 1)
        TaskExecutionMetrics exec = new TaskExecutionMetrics();
        exec.setExecutorRunTime(500);
        event.setTaskExecutionMetrics(exec);
        assertNotNull(event.getTaskExecutionMetrics());
        assertEquals(500, event.getTaskExecutionMetrics().getExecutorRunTime());
    }

    @Test
    void testSqlExecutionMetricsQueryText() {
        SqlExecutionMetrics m = new SqlExecutionMetrics();
        assertNull(m.getQueryText());
        m.setQueryText("SELECT 1");
        assertEquals("SELECT 1", m.getQueryText());
    }

    @Test
    void testSqlExecutionMetricsExecutionId() {
        SqlExecutionMetrics m = new SqlExecutionMetrics();
        assertEquals(0L, m.getExecutionId());
        m.setExecutionId(42L);
        assertEquals(42L, m.getExecutionId());
    }
}
