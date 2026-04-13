package x.mg.metrics.sparktelemetry.otel;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.DoubleHistogram;
import x.mg.metrics.sparktelemetry.config.TelemetryConfig;
import x.mg.metrics.sparktelemetry.model.*;
import java.util.List;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Records SparkMetricEvents as OTel metrics via the Meter API.
 * Supports configurable metric categories via TelemetryConfig.
 */
public class MetricRecorder {

    private static final Logger LOG = Logger.getLogger(MetricRecorder.class.getName());
    private static final String METER_NAME = "spark-telemetry";

    private final Meter meter;
    private final TelemetryConfig config;

    // Core task metrics (always recorded when task-end enabled)
    private final LongCounter bytesReadCounter;
    private final LongCounter bytesWrittenCounter;
    private final LongCounter recordsReadCounter;
    private final LongCounter recordsWrittenCounter;
    private final LongCounter shuffleBytesReadCounter;
    private final LongCounter shuffleBytesWrittenCounter;
    private final LongCounter shuffleFetchWaitTimeCounter;
    private final LongCounter diskBytesSpilledCounter;
    private final LongCounter memoryBytesSpilledCounter;
    private final LongHistogram taskDurationHistogram;

    // Category 1: Task execution (config: metrics.task.execution)
    private final LongHistogram executorRunTimeHistogram;
    private final LongCounter executorCpuTimeCounter;
    private final LongHistogram deserializeTimeHistogram;
    private final LongCounter deserializeCpuTimeCounter;
    private final LongHistogram resultSerializationTimeHistogram;
    private final LongHistogram taskJvmGcTimeHistogram;
    private final LongHistogram schedulerDelayHistogram;
    private final LongCounter resultSizeCounter;
    private final LongCounter peakExecutionMemoryCounter;

    // Category 2: Extended shuffle (config: metrics.task.shuffle-extended)
    private final LongCounter shuffleLocalBlocksFetchedCounter;
    private final LongCounter shuffleRecordsReadCounter;
    private final LongCounter shuffleRemoteBytesReadToDiskCounter;
    private final LongCounter shuffleRemoteReqsDurationCounter;

    // Category 4: Stage detailed (config: metrics.stage.detailed)
    private final LongHistogram stageDurationHistogram;
    private final LongCounter stageNumTasksCounter;
    private final LongCounter stageExecutorRunTimeCounter;
    private final LongCounter stageExecutorCpuTimeCounter;
    private final LongCounter stageJvmGcTimeCounter;
    private final LongCounter stagePeakExecutionMemoryCounter;
    private final LongCounter stageBytesReadCounter;
    private final LongCounter stageBytesWrittenCounter;

    // Category 5: Job lifecycle (config: metrics.job.lifecycle)
    private final LongHistogram jobDurationHistogram;
    private final LongCounter jobNumStagesCounter;
    private final Map<Integer, Long> jobStartTimes = new ConcurrentHashMap<>();

    // Category 6: SQL query execution (config: metrics.sql.query-execution)
    private final LongHistogram sqlQueryDurationHistogram;
    private final LongCounter sqlQueryShuffleBytesReadCounter;
    private final LongCounter sqlQueryShuffleBytesWrittenCounter;
    private final LongCounter sqlQueryJoinCountCounter;
    // Table-level instruments
    private final LongCounter sqlTableBytesCounter;
    private final LongCounter sqlTableRowsCounter;
    private final LongCounter sqlTableFilesReadCounter;
    private final LongCounter sqlTableTimeMsCounter;

    // GC metric state (system metrics)
    private final LongCounter gcCountCounter;
    private final LongCounter gcTimeCounter;
    private final Map<String, Long> prevGcCount = new ConcurrentHashMap<>();
    private final Map<String, Long> prevGcTime = new ConcurrentHashMap<>();

    // Memory metric state
    private volatile MemoryMetrics latestMemory;
    private volatile Attributes latestSystemAttrs;

    public MetricRecorder(OpenTelemetry openTelemetry, TelemetryConfig config) {
        this.meter = openTelemetry.getMeter(METER_NAME);
        this.config = config;

        // Core task metrics
        this.bytesReadCounter = meter.counterBuilder("spark.task.io.bytes_read")
                .setDescription("Total bytes read by tasks").setUnit("By").build();
        this.bytesWrittenCounter = meter.counterBuilder("spark.task.io.bytes_written")
                .setDescription("Total bytes written by tasks").setUnit("By").build();
        this.recordsReadCounter = meter.counterBuilder("spark.task.io.records_read")
                .setDescription("Total records read by tasks").setUnit("{records}").build();
        this.recordsWrittenCounter = meter.counterBuilder("spark.task.io.records_written")
                .setDescription("Total records written by tasks").setUnit("{records}").build();
        this.shuffleBytesReadCounter = meter.counterBuilder("spark.task.shuffle.bytes_read")
                .setDescription("Total shuffle bytes read").setUnit("By").build();
        this.shuffleBytesWrittenCounter = meter.counterBuilder("spark.task.shuffle.bytes_written")
                .setDescription("Total shuffle bytes written").setUnit("By").build();
        this.shuffleFetchWaitTimeCounter = meter.counterBuilder("spark.task.shuffle.fetch_wait_time_ms")
                .setDescription("Shuffle fetch wait time").setUnit("ms").build();
        this.diskBytesSpilledCounter = meter.counterBuilder("spark.task.disk_bytes_spilled")
                .setDescription("Disk bytes spilled").setUnit("By").build();
        this.memoryBytesSpilledCounter = meter.counterBuilder("spark.task.memory_bytes_spilled")
                .setDescription("Memory bytes spilled").setUnit("By").build();
        this.taskDurationHistogram = meter.histogramBuilder("spark.task.duration_ms")
                .setDescription("Task duration").setUnit("ms").ofLongs().build();

        // Category 1: Task execution
        if (config.isCaptureTaskExecution()) {
            this.executorRunTimeHistogram = meter.histogramBuilder("spark.task.executor.run_time_ms")
                    .setDescription("Executor run time per task").setUnit("ms").ofLongs().build();
            this.executorCpuTimeCounter = meter.counterBuilder("spark.task.executor.cpu_time_ns")
                    .setDescription("Executor CPU time per task").setUnit("ns").build();
            this.deserializeTimeHistogram = meter.histogramBuilder("spark.task.deserialize_time_ms")
                    .setDescription("Task deserialization time").setUnit("ms").ofLongs().build();
            this.deserializeCpuTimeCounter = meter.counterBuilder("spark.task.deserialize_cpu_time_ns")
                    .setDescription("Task deserialization CPU time").setUnit("ns").build();
            this.resultSerializationTimeHistogram = meter.histogramBuilder("spark.task.result_serialization_time_ms")
                    .setDescription("Result serialization time").setUnit("ms").ofLongs().build();
            this.taskJvmGcTimeHistogram = meter.histogramBuilder("spark.task.jvm_gc_time_ms")
                    .setDescription("Task JVM GC time").setUnit("ms").ofLongs().build();
            this.schedulerDelayHistogram = meter.histogramBuilder("spark.task.scheduler_delay_ms")
                    .setDescription("Scheduler delay").setUnit("ms").ofLongs().build();
            this.resultSizeCounter = meter.counterBuilder("spark.task.result_size_bytes")
                    .setDescription("Task result size").setUnit("By").build();
            this.peakExecutionMemoryCounter = meter.counterBuilder("spark.task.peak_execution_memory_bytes")
                    .setDescription("Peak execution memory").setUnit("By").build();
        } else {
            this.executorRunTimeHistogram = null;
            this.executorCpuTimeCounter = null;
            this.deserializeTimeHistogram = null;
            this.deserializeCpuTimeCounter = null;
            this.resultSerializationTimeHistogram = null;
            this.taskJvmGcTimeHistogram = null;
            this.schedulerDelayHistogram = null;
            this.resultSizeCounter = null;
            this.peakExecutionMemoryCounter = null;
        }

        // Category 2: Extended shuffle
        if (config.isCaptureTaskShuffleExtended()) {
            this.shuffleLocalBlocksFetchedCounter = meter.counterBuilder("spark.task.shuffle.local_blocks_fetched")
                    .setDescription("Local shuffle blocks fetched").setUnit("{blocks}").build();
            this.shuffleRecordsReadCounter = meter.counterBuilder("spark.task.shuffle.records_read")
                    .setDescription("Shuffle records read").setUnit("{records}").build();
            this.shuffleRemoteBytesReadToDiskCounter = meter.counterBuilder("spark.task.shuffle.remote_bytes_read_to_disk")
                    .setDescription("Remote shuffle bytes read to disk").setUnit("By").build();
            this.shuffleRemoteReqsDurationCounter = meter.counterBuilder("spark.task.shuffle.remote_reqs_duration_ms")
                    .setDescription("Remote shuffle request duration").setUnit("ms").build();
        } else {
            this.shuffleLocalBlocksFetchedCounter = null;
            this.shuffleRecordsReadCounter = null;
            this.shuffleRemoteBytesReadToDiskCounter = null;
            this.shuffleRemoteReqsDurationCounter = null;
        }

        // Category 4: Stage detailed
        if (config.isCaptureStageDetailed()) {
            this.stageDurationHistogram = meter.histogramBuilder("spark.stage.duration_ms")
                    .setDescription("Stage duration").setUnit("ms").ofLongs().build();
            this.stageNumTasksCounter = meter.counterBuilder("spark.stage.num_tasks")
                    .setDescription("Number of tasks in stage").setUnit("{tasks}").build();
            this.stageExecutorRunTimeCounter = meter.counterBuilder("spark.stage.executor.run_time_ms")
                    .setDescription("Stage total executor run time").setUnit("ms").build();
            this.stageExecutorCpuTimeCounter = meter.counterBuilder("spark.stage.executor.cpu_time_ns")
                    .setDescription("Stage total executor CPU time").setUnit("ns").build();
            this.stageJvmGcTimeCounter = meter.counterBuilder("spark.stage.jvm_gc_time_ms")
                    .setDescription("Stage total JVM GC time").setUnit("ms").build();
            this.stagePeakExecutionMemoryCounter = meter.counterBuilder("spark.stage.peak_execution_memory_bytes")
                    .setDescription("Stage peak execution memory").setUnit("By").build();
            this.stageBytesReadCounter = meter.counterBuilder("spark.stage.io.bytes_read")
                    .setDescription("Total bytes read by stage").setUnit("By").build();
            this.stageBytesWrittenCounter = meter.counterBuilder("spark.stage.io.bytes_written")
                    .setDescription("Total bytes written by stage").setUnit("By").build();
        } else {
            this.stageDurationHistogram = null;
            this.stageNumTasksCounter = null;
            this.stageExecutorRunTimeCounter = null;
            this.stageExecutorCpuTimeCounter = null;
            this.stageJvmGcTimeCounter = null;
            this.stagePeakExecutionMemoryCounter = null;
            this.stageBytesReadCounter = null;
            this.stageBytesWrittenCounter = null;
        }

        // Category 5: Job lifecycle
        if (config.isCaptureJobLifecycle()) {
            this.jobDurationHistogram = meter.histogramBuilder("spark.job.duration_ms")
                    .setDescription("Job duration").setUnit("ms").ofLongs().build();
            this.jobNumStagesCounter = meter.counterBuilder("spark.job.num_stages")
                    .setDescription("Number of stages in job").setUnit("{stages}").build();
        } else {
            this.jobDurationHistogram = null;
            this.jobNumStagesCounter = null;
        }

        // Category 6: SQL query execution
        if (config.isCaptureSqlQueryExecution()) {
            this.sqlQueryDurationHistogram = meter.histogramBuilder("spark.sql.query.duration_ms")
                    .setDescription("SQL query duration").setUnit("ms").ofLongs().build();
            this.sqlQueryShuffleBytesReadCounter = meter.counterBuilder("spark.sql.query.shuffle.bytes_read")
                    .setDescription("SQL query shuffle bytes read").setUnit("By").build();
            this.sqlQueryShuffleBytesWrittenCounter = meter.counterBuilder("spark.sql.query.shuffle.bytes_written")
                    .setDescription("SQL query shuffle bytes written").setUnit("By").build();
            this.sqlQueryJoinCountCounter = meter.counterBuilder("spark.sql.query.join_count")
                    .setDescription("SQL query join count").setUnit("{joins}").build();
            // Table-level instruments
            this.sqlTableBytesCounter = meter.counterBuilder("spark.sql.table.bytes")
                    .setDescription("SQL table IO bytes").setUnit("By").build();
            this.sqlTableRowsCounter = meter.counterBuilder("spark.sql.table.rows")
                    .setDescription("SQL table IO rows").setUnit("{rows}").build();
            this.sqlTableFilesReadCounter = meter.counterBuilder("spark.sql.table.files_read")
                    .setDescription("SQL table files read").setUnit("{files}").build();
            this.sqlTableTimeMsCounter = meter.counterBuilder("spark.sql.table.time_ms")
                    .setDescription("SQL table IO time").setUnit("ms").build();
        } else {
            this.sqlQueryDurationHistogram = null;
            this.sqlQueryShuffleBytesReadCounter = null;
            this.sqlQueryShuffleBytesWrittenCounter = null;
            this.sqlQueryJoinCountCounter = null;
            this.sqlTableBytesCounter = null;
            this.sqlTableRowsCounter = null;
            this.sqlTableFilesReadCounter = null;
            this.sqlTableTimeMsCounter = null;
        }

        // GC counters (system metrics)
        this.gcCountCounter = meter.counterBuilder("spark.jvm.gc.count")
                .setDescription("JVM GC count").setUnit("{count}").build();
        this.gcTimeCounter = meter.counterBuilder("spark.jvm.gc.time_ms")
                .setDescription("JVM GC time").setUnit("ms").build();

        // Memory gauges (system metrics)
        meter.gaugeBuilder("spark.jvm.memory.heap_used")
                .setDescription("JVM heap memory used").setUnit("By")
                .buildWithCallback(measurement -> {
                    MemoryMetrics m = latestMemory;
                    if (m != null) measurement.record(m.getHeapUsed(), latestSystemAttrs);
                });
        meter.gaugeBuilder("spark.jvm.memory.non_heap_used")
                .setDescription("JVM non-heap memory used").setUnit("By")
                .buildWithCallback(measurement -> {
                    MemoryMetrics m = latestMemory;
                    if (m != null) measurement.record(m.getNonHeapUsed(), latestSystemAttrs);
                });
    }

    public void record(SparkMetricEvent event) {
        if (event == null) return;
        try {
            switch (event.getEventType()) {
                case TASK_END:
                    recordTaskEnd(event);
                    break;
                case STAGE_COMPLETE:
                    recordStageComplete(event);
                    if (config.isCaptureStageDetailed()) recordStageDetailed(event);
                    break;
                case JOB_START:
                    if (config.isCaptureJobLifecycle()) recordJobStart(event);
                    break;
                case JOB_END:
                    if (config.isCaptureJobLifecycle()) recordJobEnd(event);
                    break;
                case SQL_EXECUTION:
                    if (config.isCaptureSqlQueryExecution()) {
                        recordSqlExecution(event);
                        recordSqlTableIO(event);
                    }
                    break;
                case PERIODIC_SYSTEM:
                    recordSystemMetrics(event);
                    break;
            }
        } catch (Exception e) {
            LOG.warning("Failed to record metric event: " + e.getMessage());
        }
    }

    private void recordTaskEnd(SparkMetricEvent event) {
        Attributes attrs = buildTaskAttributes(event);

        if (event.getTaskDurationMs() > 0) {
            taskDurationHistogram.record(event.getTaskDurationMs(), attrs);
        }

        IOMetrics io = event.getIoMetrics();
        if (io != null) {
            bytesReadCounter.add(io.getBytesRead(), attrs);
            bytesWrittenCounter.add(io.getBytesWritten(), attrs);
            recordsReadCounter.add(io.getRecordsRead(), attrs);
            recordsWrittenCounter.add(io.getRecordsWritten(), attrs);
            shuffleBytesReadCounter.add(io.getShuffleTotalBytesRead(), attrs);
            shuffleBytesWrittenCounter.add(io.getShuffleBytesWritten(), attrs);
            shuffleFetchWaitTimeCounter.add(io.getShuffleFetchWaitTime(), attrs);
            diskBytesSpilledCounter.add(io.getDiskBytesSpilled(), attrs);
            memoryBytesSpilledCounter.add(io.getMemoryBytesSpilled(), attrs);

            // Category 2: Extended shuffle
            if (config.isCaptureTaskShuffleExtended()) {
                shuffleLocalBlocksFetchedCounter.add(io.getShuffleLocalBlocksFetched(), attrs);
                shuffleRecordsReadCounter.add(io.getShuffleRecordsRead(), attrs);
                shuffleRemoteBytesReadToDiskCounter.add(io.getShuffleRemoteBytesReadToDisk(), attrs);
                shuffleRemoteReqsDurationCounter.add(io.getShuffleRemoteReqsDuration(), attrs);
            }
        }

        // Category 1: Task execution
        if (config.isCaptureTaskExecution()) {
            TaskExecutionMetrics exec = event.getTaskExecutionMetrics();
            if (exec != null) {
                if (exec.getExecutorRunTime() > 0)
                    executorRunTimeHistogram.record(exec.getExecutorRunTime(), attrs);
                if (exec.getExecutorCpuTime() > 0)
                    executorCpuTimeCounter.add(exec.getExecutorCpuTime(), attrs);
                if (exec.getExecutorDeserializeTime() > 0)
                    deserializeTimeHistogram.record(exec.getExecutorDeserializeTime(), attrs);
                if (exec.getExecutorDeserializeCpuTime() > 0)
                    deserializeCpuTimeCounter.add(exec.getExecutorDeserializeCpuTime(), attrs);
                if (exec.getResultSerializationTime() > 0)
                    resultSerializationTimeHistogram.record(exec.getResultSerializationTime(), attrs);
                if (exec.getJvmGcTime() > 0)
                    taskJvmGcTimeHistogram.record(exec.getJvmGcTime(), attrs);
                if (exec.getSchedulerDelay() > 0)
                    schedulerDelayHistogram.record(exec.getSchedulerDelay(), attrs);
                if (exec.getResultSize() > 0)
                    resultSizeCounter.add(exec.getResultSize(), attrs);
                if (exec.getPeakExecutionMemory() > 0)
                    peakExecutionMemoryCounter.add(exec.getPeakExecutionMemory(), attrs);
            }
        }
    }

    private void recordStageComplete(SparkMetricEvent event) {
        if (stageBytesReadCounter == null) return;
        IOMetrics io = event.getIoMetrics();
        if (io != null) {
            Attributes attrs = buildStageAttributes(event);
            stageBytesReadCounter.add(io.getBytesRead(), attrs);
            stageBytesWrittenCounter.add(io.getBytesWritten(), attrs);
        }
    }

    private void recordStageDetailed(SparkMetricEvent event) {
        Attributes attrs = buildStageAttributes(event);

        if (event.getStageDurationMs() > 0) {
            stageDurationHistogram.record(event.getStageDurationMs(), attrs);
        }
        if (event.getStageNumTasks() > 0) {
            stageNumTasksCounter.add(event.getStageNumTasks(), attrs);
        }

        TaskExecutionMetrics exec = event.getTaskExecutionMetrics();
        if (exec != null) {
            if (exec.getExecutorRunTime() > 0)
                stageExecutorRunTimeCounter.add(exec.getExecutorRunTime(), attrs);
            if (exec.getExecutorCpuTime() > 0)
                stageExecutorCpuTimeCounter.add(exec.getExecutorCpuTime(), attrs);
            if (exec.getJvmGcTime() > 0)
                stageJvmGcTimeCounter.add(exec.getJvmGcTime(), attrs);
            if (exec.getPeakExecutionMemory() > 0)
                stagePeakExecutionMemoryCounter.add(exec.getPeakExecutionMemory(), attrs);
        }
    }

    private void recordJobStart(SparkMetricEvent event) {
        jobStartTimes.put(event.getJobId(), event.getTimestamp());
        if (event.getJobNumStages() > 0) {
            Attributes attrs = buildJobAttributes(event);
            jobNumStagesCounter.add(event.getJobNumStages(), attrs);
        }
    }

    private void recordJobEnd(SparkMetricEvent event) {
        Long startTime = jobStartTimes.remove(event.getJobId());
        if (startTime != null) {
            long duration = event.getTimestamp() - startTime;
            if (duration > 0) {
                Attributes attrs = buildJobAttributes(event);
                jobDurationHistogram.record(duration, attrs);
            }
        }
    }

    private void recordSystemMetrics(SparkMetricEvent event) {
        Attributes attrs = buildSystemAttributes(event);

        MemoryMetrics mem = event.getMemoryMetrics();
        if (mem != null) {
            latestMemory = mem;
            latestSystemAttrs = attrs;
        }

        GCMetrics gc = event.getGcMetrics();
        if (gc != null) {
            for (Map.Entry<String, GCMetrics.GCCollectorStats> entry : gc.getCollectors().entrySet()) {
                String gcName = entry.getKey();
                long currentCount = entry.getValue().getCount();
                long currentTime = entry.getValue().getTimeMs();

                long deltaCount = currentCount - prevGcCount.getOrDefault(gcName, 0L);
                long deltaTime = currentTime - prevGcTime.getOrDefault(gcName, 0L);

                prevGcCount.put(gcName, currentCount);
                prevGcTime.put(gcName, currentTime);

                if (deltaCount > 0 || deltaTime > 0) {
                    Attributes gcAttrs = attrs.toBuilder()
                            .put("gc_name", gcName)
                            .build();
                    if (deltaCount > 0) gcCountCounter.add(deltaCount, gcAttrs);
                    if (deltaTime > 0) gcTimeCounter.add(deltaTime, gcAttrs);
                }
            }
        }
    }

    private Attributes buildTaskAttributes(SparkMetricEvent event) {
        AttributesBuilder builder = Attributes.builder();
        if (event.getApplicationId() != null) builder.put("spark.app.id", event.getApplicationId());
        if (event.getExecutorId() != null) builder.put("spark.executor.id", event.getExecutorId());
        builder.put("spark.stage.id", event.getStageId());
        builder.put("spark.task.id", event.getTaskId());
        builder.put("spark.task.success", event.isTaskSuccessful());
        // Category 3: Task info attributes
        if (config.isCaptureTaskInfo()) {
            if (event.getTaskHost() != null) builder.put("spark.task.host", event.getTaskHost());
            if (event.getTaskLocality() != null) builder.put("spark.task.locality", event.getTaskLocality());
            builder.put("spark.task.speculative", event.isTaskSpeculative());
        }
        return builder.build();
    }

    private Attributes buildStageAttributes(SparkMetricEvent event) {
        AttributesBuilder builder = Attributes.builder();
        if (event.getApplicationId() != null) builder.put("spark.app.id", event.getApplicationId());
        if (event.getExecutorId() != null) builder.put("spark.executor.id", event.getExecutorId());
        builder.put("spark.stage.id", event.getStageId());
        return builder.build();
    }

    private Attributes buildJobAttributes(SparkMetricEvent event) {
        AttributesBuilder builder = Attributes.builder();
        if (event.getApplicationId() != null) builder.put("spark.app.id", event.getApplicationId());
        builder.put("spark.job.id", event.getJobId());
        builder.put("spark.job.success", event.isJobSuccessful());
        return builder.build();
    }

    private void recordSqlExecution(SparkMetricEvent event) {
        SqlExecutionMetrics sql = event.getSqlExecutionMetrics();
        if (sql == null) return;

        Attributes attrs = Attributes.builder()
                .put("spark.app.id", event.getApplicationId() != null ? event.getApplicationId() : "unknown")
                .put("spark.sql.execution_id", String.valueOf(sql.getExecutionId()))
                .build();

        if (sql.getDurationMs() > 0) {
            sqlQueryDurationHistogram.record(sql.getDurationMs(), attrs);
        }
        if (sql.getShuffleBytesRead() > 0) {
            sqlQueryShuffleBytesReadCounter.add(sql.getShuffleBytesRead(), attrs);
        }
        if (sql.getShuffleBytesWritten() > 0) {
            sqlQueryShuffleBytesWrittenCounter.add(sql.getShuffleBytesWritten(), attrs);
        }
        if (sql.getJoinCount() > 0) {
            sqlQueryJoinCountCounter.add(sql.getJoinCount(), attrs);
        }
    }

    private void recordSqlTableIO(SparkMetricEvent event) {
        List<SqlTableIOMetrics> tableMetrics = event.getSqlTableIOMetrics();
        if (tableMetrics == null || tableMetrics.isEmpty()) return;

        String appId = event.getApplicationId() != null ? event.getApplicationId() : "unknown";

        for (SqlTableIOMetrics tm : tableMetrics) {
            Attributes attrs = Attributes.builder()
                    .put("spark.app.id", appId)
                    .put("spark.sql.execution_id", String.valueOf(tm.getExecutionId()))
                    .put("spark.sql.table_name", tm.getTableName() != null ? tm.getTableName() : "unknown")
                    .put("spark.sql.operation", tm.getOperation() != null ? tm.getOperation() : "unknown")
                    .build();

            if (tm.getBytes() > 0) sqlTableBytesCounter.add(tm.getBytes(), attrs);
            if (tm.getRows() > 0) sqlTableRowsCounter.add(tm.getRows(), attrs);
            if (tm.getFilesRead() > 0) sqlTableFilesReadCounter.add(tm.getFilesRead(), attrs);
            if (tm.getTimeMs() > 0) sqlTableTimeMsCounter.add(tm.getTimeMs(), attrs);
        }
    }

    private Attributes buildSystemAttributes(SparkMetricEvent event) {
        AttributesBuilder builder = Attributes.builder();
        if (event.getApplicationId() != null) builder.put("spark.app.id", event.getApplicationId());
        if (event.getExecutorId() != null) builder.put("spark.executor.id", event.getExecutorId());
        return builder.build();
    }
}
