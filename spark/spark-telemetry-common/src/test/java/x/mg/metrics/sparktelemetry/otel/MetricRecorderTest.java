package x.mg.metrics.sparktelemetry.otel;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import x.mg.metrics.sparktelemetry.config.TelemetryConfig;
import x.mg.metrics.sparktelemetry.model.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MetricRecorderTest {

    private InMemoryMetricReader metricReader;
    private MetricRecorder recorder;
    private TelemetryConfig config;

    @BeforeEach
    void setUp() {
        metricReader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .registerMetricReader(metricReader)
                .build();
        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setMeterProvider(meterProvider)
                .build();
        config = new TelemetryConfig();
        recorder = new DefaultMetricRecorder(openTelemetry, config);
    }

    private MetricData findMetric(String name) {
        return metricReader.collectAllMetrics().stream()
                .filter(m -> m.getName().equals(name))
                .findFirst()
                .orElse(null);
    }

    private void assertHasAttribute(MetricData metric, String key, String expectedValue) {
        boolean found = false;
        for (LongPointData point : metric.getLongSumData().getPoints()) {
            Attributes attrs = point.getAttributes();
            String val = attrs.get(AttributeKey.stringKey(key));
            if (expectedValue.equals(val)) {
                found = true;
                break;
            }
        }
        assertTrue(found, "Attribute " + key + "=" + expectedValue + " not found in metric " + metric.getName());
    }

    private void assertHasAttribute(MetricData metric, String key, long expectedValue) {
        boolean found = false;
        for (LongPointData point : metric.getLongSumData().getPoints()) {
            Attributes attrs = point.getAttributes();
            Long val = attrs.get(AttributeKey.longKey(key));
            if (val != null && val == expectedValue) {
                found = true;
                break;
            }
        }
        assertTrue(found, "Attribute " + key + "=" + expectedValue + " not found in metric " + metric.getName());
    }

    private void assertHasAttribute(MetricData metric, String key, boolean expectedValue) {
        boolean found = false;
        for (LongPointData point : metric.getLongSumData().getPoints()) {
            Attributes attrs = point.getAttributes();
            Boolean val = attrs.get(AttributeKey.booleanKey(key));
            if (val != null && val == expectedValue) {
                found = true;
                break;
            }
        }
        assertTrue(found, "Attribute " + key + "=" + expectedValue + " not found in metric " + metric.getName());
    }

    private long getCounterValue(MetricData metric) {
        return metric.getLongSumData().getPoints().stream()
                .mapToLong(LongPointData::getValue)
                .sum();
    }

    private long getHistogramValue(MetricData metric) {
        return (long) metric.getHistogramData().getPoints().stream()
                .mapToDouble(HistogramPointData::getSum)
                .sum();
    }

    @Test
    void testRecordTaskEnd_AllFieldsPopulated() {
        SparkMetricEvent event = new SparkMetricEvent();
        event.setEventType(SparkMetricEvent.EventType.TASK_END);
        event.setApplicationId("app-001");
        event.setApplicationName("test-app");
        event.setUser("testuser");
        event.setQueue("production");
        event.setExecutorId("3");
        event.setStageId(5);
        event.setTaskId(42);
        event.setTaskSuccessful(true);
        event.setTaskDurationMs(1500);
        event.setTaskHost("host3");
        event.setTaskLocality("NODE_LOCAL");
        event.setTaskSpeculative(false);

        IOMetrics io = new IOMetrics();
        io.setBytesRead(1000);
        io.setRecordsRead(100);
        io.setBytesWritten(2000);
        io.setRecordsWritten(200);
        io.setShuffleRemoteBytesRead(3000);
        io.setShuffleLocalBytesRead(4000);
        io.setShuffleRemoteBlocksFetched(5);
        io.setShuffleFetchWaitTime(100);
        io.setShuffleBytesWritten(5000);
        io.setShuffleWriteTime(200);
        io.setShuffleRecordsWritten(300);
        io.setDiskBytesSpilled(600);
        io.setMemoryBytesSpilled(700);
        io.setShuffleLocalBlocksFetched(8);
        io.setShuffleRecordsRead(400);
        io.setShuffleRemoteBytesReadToDisk(800);
        io.setShuffleRemoteReqsDuration(150);
        event.setIoMetrics(io);

        TaskExecutionMetrics exec = new TaskExecutionMetrics();
        exec.setExecutorRunTime(10000);
        exec.setExecutorCpuTime(9000000000L);
        exec.setExecutorDeserializeTime(50);
        exec.setExecutorDeserializeCpuTime(4000000000L);
        exec.setResultSerializationTime(30);
        exec.setJvmGcTime(200);
        exec.setSchedulerDelay(500);
        exec.setResultSize(1024);
        exec.setPeakExecutionMemory(512 * 1024 * 1024);
        event.setTaskExecutionMetrics(exec);

        recorder.record(event);

        MetricData bytesRead = findMetric("spark.task.io.bytes_read");
        assertNotNull(bytesRead);
        assertEquals(1000, getCounterValue(bytesRead));
        assertHasAttribute(bytesRead, "spark.app.id", "app-001");
        assertHasAttribute(bytesRead, "spark.app.name", "test-app");
        assertHasAttribute(bytesRead, "spark.user", "testuser");
        assertHasAttribute(bytesRead, "spark.yarn.queue", "production");
        assertHasAttribute(bytesRead, "spark.executor.id", "3");
        assertHasAttribute(bytesRead, "spark.stage.id", 5);
        assertHasAttribute(bytesRead, "spark.task.id", 42);
        assertHasAttribute(bytesRead, "spark.task.success", true);
        assertHasAttribute(bytesRead, "spark.task.host", "host3");
        assertHasAttribute(bytesRead, "spark.task.locality", "NODE_LOCAL");
        assertHasAttribute(bytesRead, "spark.task.speculative", false);

        MetricData recordsRead = findMetric("spark.task.io.records_read");
        assertNotNull(recordsRead);
        assertEquals(100, getCounterValue(recordsRead));

        MetricData bytesWritten = findMetric("spark.task.io.bytes_written");
        assertNotNull(bytesWritten);
        assertEquals(2000, getCounterValue(bytesWritten));

        MetricData recordsWritten = findMetric("spark.task.io.records_written");
        assertNotNull(recordsWritten);
        assertEquals(200, getCounterValue(recordsWritten));

        MetricData shuffleBytesRead = findMetric("spark.task.shuffle.bytes_read");
        assertNotNull(shuffleBytesRead);
        assertEquals(7000, getCounterValue(shuffleBytesRead));

        MetricData shuffleBytesWritten = findMetric("spark.task.shuffle.bytes_written");
        assertNotNull(shuffleBytesWritten);
        assertEquals(5000, getCounterValue(shuffleBytesWritten));

        MetricData shuffleFetchWaitTime = findMetric("spark.task.shuffle.fetch_wait_time_ms");
        assertNotNull(shuffleFetchWaitTime);
        assertEquals(100, getCounterValue(shuffleFetchWaitTime));

        MetricData diskBytesSpilled = findMetric("spark.task.disk_bytes_spilled");
        assertNotNull(diskBytesSpilled);
        assertEquals(600, getCounterValue(diskBytesSpilled));

        MetricData memoryBytesSpilled = findMetric("spark.task.memory_bytes_spilled");
        assertNotNull(memoryBytesSpilled);
        assertEquals(700, getCounterValue(memoryBytesSpilled));

        MetricData taskDuration = findMetric("spark.task.duration_ms");
        assertNotNull(taskDuration);
        assertEquals(1500, getHistogramValue(taskDuration));

        MetricData executorRunTime = findMetric("spark.task.executor.run_time_ms");
        assertNotNull(executorRunTime);
        assertEquals(10000, getHistogramValue(executorRunTime));

        MetricData executorCpuTime = findMetric("spark.task.executor.cpu_time_ns");
        assertNotNull(executorCpuTime);
        assertEquals(9000000000L, getCounterValue(executorCpuTime));

        MetricData deserializeTime = findMetric("spark.task.deserialize_time_ms");
        assertNotNull(deserializeTime);
        assertEquals(50, getHistogramValue(deserializeTime));

        MetricData deserializeCpuTime = findMetric("spark.task.deserialize_cpu_time_ns");
        assertNotNull(deserializeCpuTime);
        assertEquals(4000000000L, getCounterValue(deserializeCpuTime));

        MetricData resultSerializationTime = findMetric("spark.task.result_serialization_time_ms");
        assertNotNull(resultSerializationTime);
        assertEquals(30, getHistogramValue(resultSerializationTime));

        MetricData taskJvmGcTime = findMetric("spark.task.jvm_gc_time_ms");
        assertNotNull(taskJvmGcTime);
        assertEquals(200, getHistogramValue(taskJvmGcTime));

        MetricData schedulerDelay = findMetric("spark.task.scheduler_delay_ms");
        assertNotNull(schedulerDelay);
        assertEquals(500, getHistogramValue(schedulerDelay));

        MetricData resultSize = findMetric("spark.task.result_size_bytes");
        assertNotNull(resultSize);
        assertEquals(1024, getCounterValue(resultSize));

        MetricData peakExecutionMemory = findMetric("spark.task.peak_execution_memory_bytes");
        assertNotNull(peakExecutionMemory);
        assertEquals(512 * 1024 * 1024, getCounterValue(peakExecutionMemory));

        MetricData shuffleLocalBlocksFetched = findMetric("spark.task.shuffle.local_blocks_fetched");
        assertNotNull(shuffleLocalBlocksFetched);
        assertEquals(8, getCounterValue(shuffleLocalBlocksFetched));

        MetricData shuffleRecordsRead = findMetric("spark.task.shuffle.records_read");
        assertNotNull(shuffleRecordsRead);
        assertEquals(400, getCounterValue(shuffleRecordsRead));

        MetricData shuffleRemoteBytesReadToDisk = findMetric("spark.task.shuffle.remote_bytes_read_to_disk");
        assertNotNull(shuffleRemoteBytesReadToDisk);
        assertEquals(800, getCounterValue(shuffleRemoteBytesReadToDisk));

        MetricData shuffleRemoteReqsDuration = findMetric("spark.task.shuffle.remote_reqs_duration_ms");
        assertNotNull(shuffleRemoteReqsDuration);
        assertEquals(150, getCounterValue(shuffleRemoteReqsDuration));
    }

    @Test
    void testRecordStageComplete_AllFieldsPopulated() {
        SparkMetricEvent event = new SparkMetricEvent();
        event.setEventType(SparkMetricEvent.EventType.STAGE_COMPLETE);
        event.setApplicationId("app-002");
        event.setApplicationName("stage-test");
        event.setExecutorId("executor-1");
        event.setStageId(10);
        event.setStageNumTasks(100);
        event.setStageDurationMs(5000);

        IOMetrics io = new IOMetrics();
        io.setBytesRead(10000);
        io.setBytesWritten(20000);
        event.setIoMetrics(io);

        TaskExecutionMetrics exec = new TaskExecutionMetrics();
        exec.setExecutorRunTime(50000);
        exec.setExecutorCpuTime(45000000000L);
        exec.setJvmGcTime(1000);
        exec.setPeakExecutionMemory(1024 * 1024 * 1024);
        event.setTaskExecutionMetrics(exec);

        recorder.record(event);

        MetricData stageBytesRead = findMetric("spark.stage.io.bytes_read");
        assertNotNull(stageBytesRead);
        assertEquals(10000, getCounterValue(stageBytesRead));

        MetricData stageBytesWritten = findMetric("spark.stage.io.bytes_written");
        assertNotNull(stageBytesWritten);
        assertEquals(20000, getCounterValue(stageBytesWritten));

        MetricData stageDuration = findMetric("spark.stage.duration_ms");
        assertNotNull(stageDuration);
        assertEquals(5000, getHistogramValue(stageDuration));

        MetricData stageNumTasks = findMetric("spark.stage.num_tasks");
        assertNotNull(stageNumTasks);
        assertEquals(100, getCounterValue(stageNumTasks));

        MetricData stageExecutorRunTime = findMetric("spark.stage.executor.run_time_ms");
        assertNotNull(stageExecutorRunTime);
        assertEquals(50000, getCounterValue(stageExecutorRunTime));

        MetricData stageExecutorCpuTime = findMetric("spark.stage.executor.cpu_time_ns");
        assertNotNull(stageExecutorCpuTime);
        assertEquals(45000000000L, getCounterValue(stageExecutorCpuTime));

        MetricData stageJvmGcTime = findMetric("spark.stage.jvm_gc_time_ms");
        assertNotNull(stageJvmGcTime);
        assertEquals(1000, getCounterValue(stageJvmGcTime));

        MetricData stagePeakExecutionMemory = findMetric("spark.stage.peak_execution_memory_bytes");
        assertNotNull(stagePeakExecutionMemory);
        assertEquals(1024 * 1024 * 1024, getCounterValue(stagePeakExecutionMemory));
    }

    @Test
    void testRecordJobStart_AllFieldsPopulated() {
        SparkMetricEvent event = new SparkMetricEvent();
        event.setEventType(SparkMetricEvent.EventType.JOB_START);
        event.setJobId(5);
        event.setJobNumStages(10);

        recorder.record(event);

        MetricData jobNumStages = findMetric("spark.job.num_stages");
        assertNotNull(jobNumStages);
        assertEquals(10, getCounterValue(jobNumStages));
    }

    @Test
    void testRecordJobEnd_AllFieldsPopulated() {
        SparkMetricEvent startEvent = new SparkMetricEvent();
        startEvent.setEventType(SparkMetricEvent.EventType.JOB_START);
        startEvent.setTimestamp(1000);
        startEvent.setJobId(7);

        recorder.record(startEvent);

        SparkMetricEvent endEvent = new SparkMetricEvent();
        endEvent.setEventType(SparkMetricEvent.EventType.JOB_END);
        endEvent.setTimestamp(4000);
        endEvent.setJobId(7);
        endEvent.setJobSuccessful(true);

        recorder.record(endEvent);

        MetricData jobDuration = findMetric("spark.job.duration_ms");
        assertNotNull(jobDuration);
        assertEquals(3000, getHistogramValue(jobDuration));
    }

    @Test
    void testRecordSqlExecution_AllFieldsPopulated() {
        SparkMetricEvent event = new SparkMetricEvent();
        event.setEventType(SparkMetricEvent.EventType.SQL_EXECUTION);
        event.setApplicationId("app-sql-001");
        event.setApplicationName("sql-test");
        event.setUser("sqluser");
        event.setQueue("default");

        SqlExecutionMetrics sql = new SqlExecutionMetrics();
        sql.setExecutionId(42);
        sql.setQueryText("SELECT * FROM users WHERE active = true");
        sql.setDurationMs(2000);
        sql.setJoinCount(3);
        sql.setShuffleBytesRead(1024);
        sql.setShuffleBytesWritten(2048);
        event.setSqlExecutionMetrics(sql);

        recorder.record(event);

        MetricData sqlQueryDuration = findMetric("spark.sql.query.duration_ms");
        assertNotNull(sqlQueryDuration);
        assertEquals(2000, getHistogramValue(sqlQueryDuration));

        MetricData sqlQueryShuffleBytesRead = findMetric("spark.sql.query.shuffle.bytes_read");
        assertNotNull(sqlQueryShuffleBytesRead);
        assertEquals(1024, getCounterValue(sqlQueryShuffleBytesRead));

        MetricData sqlQueryShuffleBytesWritten = findMetric("spark.sql.query.shuffle.bytes_written");
        assertNotNull(sqlQueryShuffleBytesWritten);
        assertEquals(2048, getCounterValue(sqlQueryShuffleBytesWritten));

        MetricData sqlQueryJoinCount = findMetric("spark.sql.query.join_count");
        assertNotNull(sqlQueryJoinCount);
        assertEquals(3, getCounterValue(sqlQueryJoinCount));
    }

    @Test
    void testRecordSqlTableIO_AllFieldsPopulated() {
        SparkMetricEvent event = new SparkMetricEvent();
        event.setEventType(SparkMetricEvent.EventType.SQL_EXECUTION);
        event.setApplicationId("app-table-io");
        event.setApplicationName("table-io-test");
        event.setUser("tableuser");
        event.setQueue("production");

        SqlExecutionMetrics sql = new SqlExecutionMetrics();
        sql.setExecutionId(42);
        sql.setQueryText("INSERT INTO results SELECT * FROM users");
        event.setSqlExecutionMetrics(sql);

        List<SqlTableIOMetrics> tableMetrics = new ArrayList<>();

        SqlTableIOMetrics scanMetric = new SqlTableIOMetrics();
        scanMetric.setExecutionId(42);
        scanMetric.setTableName("db.users");
        scanMetric.setOperation("scan");
        scanMetric.setBytes(10240);
        scanMetric.setRows(500);
        scanMetric.setFilesRead(3);
        scanMetric.setTimeMs(100);
        tableMetrics.add(scanMetric);

        SqlTableIOMetrics writeMetric = new SqlTableIOMetrics();
        writeMetric.setExecutionId(42);
        writeMetric.setTableName("db.results");
        writeMetric.setOperation("write");
        writeMetric.setBytes(20480);
        writeMetric.setRows(250);
        writeMetric.setFilesRead(0);
        writeMetric.setTimeMs(0);
        tableMetrics.add(writeMetric);

        event.setSqlTableIOMetrics(tableMetrics);

        recorder.record(event);

        MetricData sqlTableBytes = findMetric("spark.sql.table.bytes");
        assertNotNull(sqlTableBytes);
        assertEquals(30720, getCounterValue(sqlTableBytes));

        MetricData sqlTableRows = findMetric("spark.sql.table.rows");
        assertNotNull(sqlTableRows);
        assertEquals(750, getCounterValue(sqlTableRows));

        MetricData sqlTableFilesRead = findMetric("spark.sql.table.files_read");
        assertNotNull(sqlTableFilesRead);
        assertEquals(3, getCounterValue(sqlTableFilesRead));

        MetricData sqlTableTimeMs = findMetric("spark.sql.table.time_ms");
        assertNotNull(sqlTableTimeMs);
        assertEquals(100, getCounterValue(sqlTableTimeMs));

        for (MetricData metric : Arrays.asList(sqlTableBytes, sqlTableRows)) {
            boolean hasScan = false;
            boolean hasWrite = false;
            for (LongPointData point : metric.getLongSumData().getPoints()) {
                Attributes attrs = point.getAttributes();
                String operation = attrs.get(AttributeKey.stringKey("spark.sql.operation"));
                String tableName = attrs.get(AttributeKey.stringKey("spark.sql.table_name"));
                if ("scan".equals(operation) && "db.users".equals(tableName)) {
                    hasScan = true;
                }
                if ("write".equals(operation) && "db.results".equals(tableName)) {
                    hasWrite = true;
                }
            }
            assertTrue(hasScan, "Metric " + metric.getName() + " should have scan entry");
            assertTrue(hasWrite, "Metric " + metric.getName() + " should have write entry");
        }

        // files_read only has scan entry (write metric has filesRead=0, not recorded)
        boolean hasScanFiles = false;
        for (LongPointData point : sqlTableFilesRead.getLongSumData().getPoints()) {
            String operation = point.getAttributes().get(AttributeKey.stringKey("spark.sql.operation"));
            String tableName = point.getAttributes().get(AttributeKey.stringKey("spark.sql.table_name"));
            if ("scan".equals(operation) && "db.users".equals(tableName)) hasScanFiles = true;
        }
        assertTrue(hasScanFiles, "spark.sql.table.files_read should have scan entry");
    }

    @Test
    void testRecordSystemMetrics_AllFieldsPopulated() {
        SparkMetricEvent event = new SparkMetricEvent();
        event.setEventType(SparkMetricEvent.EventType.PERIODIC_SYSTEM);
        event.setApplicationId("app-system-001");
        event.setApplicationName("system-test");
        event.setExecutorId("executor-1");

        MemoryMetrics mem = new MemoryMetrics();
        mem.setHeapUsed(512 * 1024 * 1024);
        mem.setNonHeapUsed(64 * 1024 * 1024);
        event.setMemoryMetrics(mem);

        GCMetrics gc = new GCMetrics();
        gc.addCollector("G1 Young Generation", 10, 100);
        gc.addCollector("G1 Old Generation", 2, 200);
        event.setGcMetrics(gc);

        recorder.record(event);

        gc.addCollector("G1 Young Generation", 15, 150);
        gc.addCollector("G1 Old Generation", 3, 250);

        SparkMetricEvent event2 = new SparkMetricEvent();
        event2.setEventType(SparkMetricEvent.EventType.PERIODIC_SYSTEM);
        event2.setApplicationId("app-system-001");
        event2.setApplicationName("system-test");
        event2.setExecutorId("executor-1");
        event2.setGcMetrics(gc);

        recorder.record(event2);

        MetricData gcCount = findMetric("spark.jvm.gc.count");
        assertNotNull(gcCount);
        assertEquals(18, getCounterValue(gcCount));

        MetricData gcTime = findMetric("spark.jvm.gc.time_ms");
        assertNotNull(gcTime);
        assertEquals(400, getCounterValue(gcTime));

        for (MetricData metric : Arrays.asList(gcCount, gcTime)) {
            boolean hasG1Young = false;
            boolean hasG1Old = false;
            for (LongPointData point : metric.getLongSumData().getPoints()) {
                Attributes attrs = point.getAttributes();
                String gcName = attrs.get(AttributeKey.stringKey("gc_name"));
                if ("G1 Young Generation".equals(gcName)) {
                    hasG1Young = true;
                }
                if ("G1 Old Generation".equals(gcName)) {
                    hasG1Old = true;
                }
            }
            assertTrue(hasG1Young, "Metric " + metric.getName() + " should have G1 Young Generation");
            assertTrue(hasG1Old, "Metric " + metric.getName() + " should have G1 Old Generation");
        }
    }

    @Test
    void testUserAndQueuePopulated_AllEventTypes() {
        String user = "testuser";
        String queue = "production";

        SparkMetricEvent taskEvent = new SparkMetricEvent();
        taskEvent.setEventType(SparkMetricEvent.EventType.TASK_END);
        taskEvent.setUser(user);
        taskEvent.setQueue(queue);
        taskEvent.setTaskId(1);
        taskEvent.setStageId(1);
        recorder.record(taskEvent);

        SparkMetricEvent stageEvent = new SparkMetricEvent();
        stageEvent.setEventType(SparkMetricEvent.EventType.STAGE_COMPLETE);
        stageEvent.setUser(user);
        stageEvent.setQueue(queue);
        stageEvent.setStageId(1);
        recorder.record(stageEvent);

        SparkMetricEvent jobStartEvent = new SparkMetricEvent();
        jobStartEvent.setEventType(SparkMetricEvent.EventType.JOB_START);
        jobStartEvent.setUser(user);
        jobStartEvent.setQueue(queue);
        jobStartEvent.setJobId(1);
        jobStartEvent.setJobNumStages(1);
        recorder.record(jobStartEvent);

        SparkMetricEvent jobEndEvent = new SparkMetricEvent();
        jobEndEvent.setEventType(SparkMetricEvent.EventType.JOB_END);
        jobEndEvent.setUser(user);
        jobEndEvent.setQueue(queue);
        jobEndEvent.setJobId(2);
        jobEndEvent.setJobSuccessful(true);
        recorder.record(jobEndEvent);

        SparkMetricEvent sqlEvent = new SparkMetricEvent();
        sqlEvent.setEventType(SparkMetricEvent.EventType.SQL_EXECUTION);
        sqlEvent.setUser(user);
        sqlEvent.setQueue(queue);
        SqlExecutionMetrics sql = new SqlExecutionMetrics();
        sql.setExecutionId(1);
        sql.setDurationMs(100);
        sqlEvent.setSqlExecutionMetrics(sql);
        recorder.record(sqlEvent);

        SparkMetricEvent systemEvent = new SparkMetricEvent();
        systemEvent.setEventType(SparkMetricEvent.EventType.PERIODIC_SYSTEM);
        systemEvent.setUser(user);
        systemEvent.setQueue(queue);
        systemEvent.setExecutorId("1");
        GCMetrics gc = new GCMetrics();
        gc.addCollector("test", 1, 10);
        systemEvent.setGcMetrics(gc);
        recorder.record(systemEvent);

        java.util.Collection<MetricData> metrics = metricReader.collectAllMetrics();
        assertFalse(metrics.isEmpty());

        for (MetricData metric : metrics) {
            if (metric.getData() != null && !metric.getData().getPoints().isEmpty()) {
                PointData firstPoint = metric.getData().getPoints().iterator().next();
                Attributes attrs = firstPoint.getAttributes();
                String userAttr = attrs.get(AttributeKey.stringKey("spark.user"));
                String queueAttr = attrs.get(AttributeKey.stringKey("spark.yarn.queue"));
                assertEquals(user, userAttr, "Metric " + metric.getName() + " should have user attribute");
                assertEquals(queue, queueAttr, "Metric " + metric.getName() + " should have queue attribute");
            }
        }
    }
}
