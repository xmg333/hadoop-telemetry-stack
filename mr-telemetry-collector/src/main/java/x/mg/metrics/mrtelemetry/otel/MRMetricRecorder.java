package x.mg.metrics.mrtelemetry.otel;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import x.mg.metrics.mrtelemetry.model.MRJobMetrics;

import java.util.Map;

/**
 * Records MR job metrics as OTel metrics via the Meter API.
 */
public class MRMetricRecorder {
    private static final String METER_NAME = "mr-telemetry";

    private final Meter meter;
    private final LongCounter hdfsBytesReadCounter;
    private final LongCounter hdfsBytesWrittenCounter;
    private final LongCounter cpuMsCounter;
    private final LongCounter gcMsCounter;
    private final LongCounter spilledRecordsCounter;
    private final LongCounter mapInputRecordsCounter;
    private final LongCounter mapOutputRecordsCounter;
    private final LongCounter reduceInputRecordsCounter;
    private final LongCounter reduceOutputRecordsCounter;
    private final LongCounter millisMapsCounter;
    private final LongCounter millisReducesCounter;

    // Job-level counters (replacing gauges to avoid callback leak)
    private final LongCounter physicalMemoryBytesCounter;
    private final LongCounter virtualMemoryBytesCounter;
    private final LongCounter fileBytesReadCounter;
    private final LongCounter fileBytesWrittenCounter;
    private final LongCounter reduceShuffleBytesCounter;
    private final LongCounter mapOutputBytesCounter;
    private final LongCounter launchedMapsCounter;
    private final LongCounter launchedReducesCounter;
    private final LongCounter elapsedTimeCounter;

    // Task-level counters (same OTel names as Agent's CounterMapping.ALL)
    private final LongCounter taskHdfsBytesReadCounter;
    private final LongCounter taskHdfsBytesWrittenCounter;
    private final LongCounter taskCpuMsCounter;
    private final LongCounter taskGcMsCounter;
    private final LongCounter taskSpilledRecordsCounter;
    private final LongCounter taskMapInputRecordsCounter;
    private final LongCounter taskMapOutputRecordsCounter;
    private final LongCounter taskMapOutputBytesCounter;
    private final LongCounter taskReduceInputRecordsCounter;
    private final LongCounter taskReduceOutputRecordsCounter;
    private final LongCounter taskReduceShuffleBytesCounter;
    private final LongCounter taskFileBytesReadCounter;
    private final LongCounter taskFileBytesWrittenCounter;

    public MRMetricRecorder(OpenTelemetry openTelemetry) {
        this.meter = openTelemetry.getMeter(METER_NAME);

        this.hdfsBytesReadCounter = meter.counterBuilder("mr.job.io.hdfs_bytes_read")
                .setDescription("HDFS bytes read").setUnit("By").build();
        this.hdfsBytesWrittenCounter = meter.counterBuilder("mr.job.io.hdfs_bytes_written")
                .setDescription("HDFS bytes written").setUnit("By").build();
        this.cpuMsCounter = meter.counterBuilder("mr.job.cpu_time_ms")
                .setDescription("CPU time").setUnit("ms").build();
        this.gcMsCounter = meter.counterBuilder("mr.job.gc_time_ms")
                .setDescription("GC time").setUnit("ms").build();
        this.spilledRecordsCounter = meter.counterBuilder("mr.job.spilled_records")
                .setDescription("Spilled records").setUnit("{records}").build();
        this.mapInputRecordsCounter = meter.counterBuilder("mr.job.map_input_records")
                .setDescription("Map input records").setUnit("{records}").build();
        this.mapOutputRecordsCounter = meter.counterBuilder("mr.job.map_output_records")
                .setDescription("Map output records").setUnit("{records}").build();
        this.reduceInputRecordsCounter = meter.counterBuilder("mr.job.reduce_input_records")
                .setDescription("Reduce input records").setUnit("{records}").build();
        this.reduceOutputRecordsCounter = meter.counterBuilder("mr.job.reduce_output_records")
                .setDescription("Reduce output records").setUnit("{records}").build();
        this.millisMapsCounter = meter.counterBuilder("mr.job.maps_duration_ms")
                .setDescription("Total maps duration").setUnit("ms").build();
        this.millisReducesCounter = meter.counterBuilder("mr.job.reduces_duration_ms")
                .setDescription("Total reduces duration").setUnit("ms").build();

        // Job-level counters (replacing gauges)
        this.physicalMemoryBytesCounter = meter.counterBuilder("mr.job.physical_memory_bytes")
                .setDescription("Physical memory").setUnit("By").build();
        this.virtualMemoryBytesCounter = meter.counterBuilder("mr.job.virtual_memory_bytes")
                .setDescription("Virtual memory").setUnit("By").build();
        this.fileBytesReadCounter = meter.counterBuilder("mr.job.io.file_bytes_read")
                .setDescription("Local file bytes read").setUnit("By").build();
        this.fileBytesWrittenCounter = meter.counterBuilder("mr.job.io.file_bytes_written")
                .setDescription("Local file bytes written").setUnit("By").build();
        this.reduceShuffleBytesCounter = meter.counterBuilder("mr.job.reduce_shuffle_bytes")
                .setDescription("Shuffle bytes").setUnit("By").build();
        this.mapOutputBytesCounter = meter.counterBuilder("mr.job.map_output_bytes")
                .setDescription("Map output bytes").setUnit("By").build();
        this.launchedMapsCounter = meter.counterBuilder("mr.job.launched_maps")
                .setDescription("Launched maps").setUnit("{tasks}").build();
        this.launchedReducesCounter = meter.counterBuilder("mr.job.launched_reduces")
                .setDescription("Launched reduces").setUnit("{tasks}").build();
        this.elapsedTimeCounter = meter.counterBuilder("mr.job.elapsed_time_ms")
                .setDescription("Job elapsed time").setUnit("ms").build();

        // Task-level counters
        this.taskHdfsBytesReadCounter = meter.counterBuilder("mr.task.io.hdfs_bytes_read")
                .setDescription("Task HDFS bytes read").setUnit("By").build();
        this.taskHdfsBytesWrittenCounter = meter.counterBuilder("mr.task.io.hdfs_bytes_written")
                .setDescription("Task HDFS bytes written").setUnit("By").build();
        this.taskCpuMsCounter = meter.counterBuilder("mr.task.cpu_time_ms")
                .setDescription("Task CPU time").setUnit("ms").build();
        this.taskGcMsCounter = meter.counterBuilder("mr.task.gc_time_ms")
                .setDescription("Task GC time").setUnit("ms").build();
        this.taskSpilledRecordsCounter = meter.counterBuilder("mr.task.io.spilled_records")
                .setDescription("Task spilled records").setUnit("{records}").build();
        this.taskMapInputRecordsCounter = meter.counterBuilder("mr.task.io.map_input_records")
                .setDescription("Task map input records").setUnit("{records}").build();
        this.taskMapOutputRecordsCounter = meter.counterBuilder("mr.task.io.map_output_records")
                .setDescription("Task map output records").setUnit("{records}").build();
        this.taskMapOutputBytesCounter = meter.counterBuilder("mr.task.io.map_output_bytes")
                .setDescription("Task map output bytes").setUnit("By").build();
        this.taskReduceInputRecordsCounter = meter.counterBuilder("mr.task.io.reduce_input_records")
                .setDescription("Task reduce input records").setUnit("{records}").build();
        this.taskReduceOutputRecordsCounter = meter.counterBuilder("mr.task.io.reduce_output_records")
                .setDescription("Task reduce output records").setUnit("{records}").build();
        this.taskReduceShuffleBytesCounter = meter.counterBuilder("mr.task.io.reduce_shuffle_bytes")
                .setDescription("Task reduce shuffle bytes").setUnit("By").build();
        this.taskFileBytesReadCounter = meter.counterBuilder("mr.task.io.file_bytes_read")
                .setDescription("Task local file bytes read").setUnit("By").build();
        this.taskFileBytesWrittenCounter = meter.counterBuilder("mr.task.io.file_bytes_written")
                .setDescription("Task local file bytes written").setUnit("By").build();
    }

    public void record(MRJobMetrics m) {
        if (m == null) return;
        try {
            Attributes attrs = buildAttributes(m);

            hdfsBytesReadCounter.add(m.getHdfsBytesRead(), attrs);
            hdfsBytesWrittenCounter.add(m.getHdfsBytesWritten(), attrs);
            cpuMsCounter.add(m.getCpuMilliseconds(), attrs);
            gcMsCounter.add(m.getGcTimeMillis(), attrs);
            spilledRecordsCounter.add(m.getSpilledRecords(), attrs);
            mapInputRecordsCounter.add(m.getMapInputRecords(), attrs);
            mapOutputRecordsCounter.add(m.getMapOutputRecords(), attrs);
            reduceInputRecordsCounter.add(m.getReduceInputRecords(), attrs);
            reduceOutputRecordsCounter.add(m.getReduceOutputRecords(), attrs);
            millisMapsCounter.add(m.getMillisMaps(), attrs);
            millisReducesCounter.add(m.getMillisReduces(), attrs);

            // Job-level counters (formerly gauges, converted to avoid callback leak)
            physicalMemoryBytesCounter.add(m.getPhysicalMemoryBytes(), attrs);
            virtualMemoryBytesCounter.add(m.getVirtualMemoryBytes(), attrs);
            fileBytesReadCounter.add(m.getFileBytesRead(), attrs);
            fileBytesWrittenCounter.add(m.getFileBytesWritten(), attrs);
            reduceShuffleBytesCounter.add(m.getReduceShuffleBytes(), attrs);
            mapOutputBytesCounter.add(m.getMapOutputBytes(), attrs);
            launchedMapsCounter.add(m.getTotalMaps(), attrs);
            launchedReducesCounter.add(m.getTotalReduces(), attrs);
            elapsedTimeCounter.add(m.getElapsedTime(), attrs);
        } catch (Exception e) {
            // Don't let recording failures propagate
        }
    }

    /**
     * Record task-level metrics from History Server task counters.
     * Uses the same mr.task.* metric names as the Agent's CounterMapping.ALL.
     */
    public void recordTask(String taskId, String taskType, String jobId,
                           String jobName, String user, String state,
                           long finishTime,
                           Map<String, Long> counters) {
        if (counters == null || counters.isEmpty()) return;
        try {
            String type = taskType != null ? taskType.toLowerCase() : "unknown";
            Attributes attrs = Attributes.builder()
                    .put(AttributeKey.stringKey("mr.task.id"), taskId != null ? taskId : "")
                    .put(AttributeKey.stringKey("mr.task.type"), type)
                    .put(AttributeKey.stringKey("mr.job.id"), jobId != null ? jobId : "")
                    .put(AttributeKey.stringKey("mr.job.name"), jobName != null ? jobName : "")
                    .put(AttributeKey.stringKey("mr.job.user"), user != null ? user : "")
                    .put(AttributeKey.stringKey("mr.job.state"), state != null ? state : "")
                    .put(AttributeKey.longKey("mr.job.finish_time_ms"), finishTime)
                    .put(AttributeKey.longKey("mr.job.start_time_ms"), 0L) // task-level: no individual task start time available
                    .build();

            safeAdd(taskMapInputRecordsCounter, counters.get("mr.task.io.map_input_records"), attrs);
            safeAdd(taskMapOutputRecordsCounter, counters.get("mr.task.io.map_output_records"), attrs);
            safeAdd(taskMapOutputBytesCounter, counters.get("mr.task.io.map_output_bytes"), attrs);
            safeAdd(taskReduceInputRecordsCounter, counters.get("mr.task.io.reduce_input_records"), attrs);
            safeAdd(taskReduceOutputRecordsCounter, counters.get("mr.task.io.reduce_output_records"), attrs);
            safeAdd(taskReduceShuffleBytesCounter, counters.get("mr.task.io.reduce_shuffle_bytes"), attrs);
            safeAdd(taskSpilledRecordsCounter, counters.get("mr.task.io.spilled_records"), attrs);
            safeAdd(taskCpuMsCounter, counters.get("mr.task.cpu_time_ms"), attrs);
            safeAdd(taskGcMsCounter, counters.get("mr.task.gc_time_ms"), attrs);
            safeAdd(taskHdfsBytesReadCounter, counters.get("mr.task.io.hdfs_bytes_read"), attrs);
            safeAdd(taskHdfsBytesWrittenCounter, counters.get("mr.task.io.hdfs_bytes_written"), attrs);
            safeAdd(taskFileBytesReadCounter, counters.get("mr.task.io.file_bytes_read"), attrs);
            safeAdd(taskFileBytesWrittenCounter, counters.get("mr.task.io.file_bytes_written"), attrs);
        } catch (Exception e) {
            // Don't let recording failures propagate
        }
    }

    private void safeAdd(LongCounter counter, Long value, Attributes attrs) {
        if (value != null && value > 0) {
            counter.add(value, attrs);
        }
    }

    private Attributes buildAttributes(MRJobMetrics m) {
        AttributesBuilder b = Attributes.builder();
        if (m.getJobId() != null) b.put(AttributeKey.stringKey("mr.job.id"), m.getJobId());
        if (m.getJobName() != null) b.put(AttributeKey.stringKey("mr.job.name"), m.getJobName());
        if (m.getUser() != null) b.put(AttributeKey.stringKey("mr.job.user"), m.getUser());
        if (m.getState() != null) b.put(AttributeKey.stringKey("mr.job.state"), m.getState());
        if (m.getQueue() != null) b.put(AttributeKey.stringKey("mr.job.queue"), m.getQueue());
        // Carry actual job start/finish time so Flink consumer can store it instead of OTel export time
        b.put(AttributeKey.longKey("mr.job.finish_time_ms"), m.getFinishTime());
        b.put(AttributeKey.longKey("mr.job.start_time_ms"), m.getStartTime());
        return b.build();
    }
}
