package x.mg.metrics.flink.model;

import java.util.Map;

public class TaskMetricRow {
    // Dimensions
    private long timestampMs;
    private String appId;
    private String executorId;
    private int stageId;
    private long taskId;
    private String taskSuccess;
    private String taskHost;
    private String taskLocality;
    private String taskSpeculative;
    private String appName;
    private String userName;
    private String queue;

    // Core IO metrics
    private Double durationMs;
    private Double ioBytesRead;
    private Double ioBytesWritten;
    private Double ioRecordsRead;
    private Double ioRecordsWritten;
    private Double shuffleBytesRead;
    private Double shuffleBytesWritten;
    private Double shuffleFetchWaitTimeMs;
    private Double diskBytesSpilled;
    private Double memoryBytesSpilled;

    // Task execution metrics
    private Double executorRunTimeMs;
    private Double executorCpuTimeNs;
    private Double deserializeTimeMs;
    private Double deserializeCpuTimeNs;
    private Double resultSerializationTimeMs;
    private Double jvmGcTimeMs;
    private Double schedulerDelayMs;
    private Double resultSizeBytes;
    private Double peakExecutionMemoryBytes;

    // Extended shuffle metrics
    private Double shuffleLocalBlocksFetched;
    private Double shuffleRecordsRead;
    private Double shuffleRemoteBytesReadToDisk;
    private Double shuffleRemoteReqsDurationMs;

    public TaskMetricRow() {}

    public static TaskMetricRow fromLabels(long timestampMs, Map<String, String> labels) {
        TaskMetricRow row = new TaskMetricRow();
        row.timestampMs = timestampMs;
        row.appId = labels.getOrDefault("spark.app.id", "unknown");
        row.executorId = labels.getOrDefault("spark.executor.id", "unknown");
        row.stageId = parseInt(labels.get("spark.stage.id"), 0);
        row.taskId = parseLong(labels.get("spark.task.id"), 0);
        row.taskSuccess = labels.get("spark.task.success");
        row.taskHost = labels.get("spark.task.host");
        row.taskLocality = labels.get("spark.task.locality");
        row.taskSpeculative = labels.get("spark.task.speculative");
        row.appName = labels.getOrDefault("spark.app.name", "");
        row.userName = labels.getOrDefault("spark.user", "");
        row.queue = labels.getOrDefault("spark.yarn.queue", "");
        return row;
    }

    private static int parseInt(String s, int def) {
        try { return s != null ? Integer.parseInt(s) : def; }
        catch (NumberFormatException e) { return def; }
    }

    private static long parseLong(String s, long def) {
        try { return s != null ? Long.parseLong(s) : def; }
        catch (NumberFormatException e) { return def; }
    }

    // Setters for metric columns
    public void setMetricColumn(String columnName, double value) {
        switch (columnName) {
            case "duration_ms": durationMs = value; break;
            case "io_bytes_read": ioBytesRead = value; break;
            case "io_bytes_written": ioBytesWritten = value; break;
            case "io_records_read": ioRecordsRead = value; break;
            case "io_records_written": ioRecordsWritten = value; break;
            case "shuffle_bytes_read": shuffleBytesRead = value; break;
            case "shuffle_bytes_written": shuffleBytesWritten = value; break;
            case "shuffle_fetch_wait_time_ms": shuffleFetchWaitTimeMs = value; break;
            case "disk_bytes_spilled": diskBytesSpilled = value; break;
            case "memory_bytes_spilled": memoryBytesSpilled = value; break;
            case "executor_run_time_ms": executorRunTimeMs = value; break;
            case "executor_cpu_time_ns": executorCpuTimeNs = value; break;
            case "deserialize_time_ms": deserializeTimeMs = value; break;
            case "deserialize_cpu_time_ns": deserializeCpuTimeNs = value; break;
            case "result_serialization_time_ms": resultSerializationTimeMs = value; break;
            case "jvm_gc_time_ms": jvmGcTimeMs = value; break;
            case "scheduler_delay_ms": schedulerDelayMs = value; break;
            case "result_size_bytes": resultSizeBytes = value; break;
            case "peak_execution_memory_bytes": peakExecutionMemoryBytes = value; break;
            case "shuffle_local_blocks_fetched": shuffleLocalBlocksFetched = value; break;
            case "shuffle_records_read": shuffleRecordsRead = value; break;
            case "shuffle_remote_bytes_read_to_disk": shuffleRemoteBytesReadToDisk = value; break;
            case "shuffle_remote_reqs_duration_ms": shuffleRemoteReqsDurationMs = value; break;
        }
    }

    // Getters
    public long getTimestampMs() { return timestampMs; }
    public String getAppId() { return appId; }
    public String getExecutorId() { return executorId; }
    public int getStageId() { return stageId; }
    public long getTaskId() { return taskId; }
    public String getTaskSuccess() { return taskSuccess; }
    public String getTaskHost() { return taskHost; }
    public String getTaskLocality() { return taskLocality; }
    public String getTaskSpeculative() { return taskSpeculative; }
    public String getAppName() { return appName; }
    public String getUserName() { return userName; }
    public String getQueue() { return queue; }
    public Double getDurationMs() { return durationMs; }
    public Double getIoBytesRead() { return ioBytesRead; }
    public Double getIoBytesWritten() { return ioBytesWritten; }
    public Double getIoRecordsRead() { return ioRecordsRead; }
    public Double getIoRecordsWritten() { return ioRecordsWritten; }
    public Double getShuffleBytesRead() { return shuffleBytesRead; }
    public Double getShuffleBytesWritten() { return shuffleBytesWritten; }
    public Double getShuffleFetchWaitTimeMs() { return shuffleFetchWaitTimeMs; }
    public Double getDiskBytesSpilled() { return diskBytesSpilled; }
    public Double getMemoryBytesSpilled() { return memoryBytesSpilled; }
    public Double getExecutorRunTimeMs() { return executorRunTimeMs; }
    public Double getExecutorCpuTimeNs() { return executorCpuTimeNs; }
    public Double getDeserializeTimeMs() { return deserializeTimeMs; }
    public Double getDeserializeCpuTimeNs() { return deserializeCpuTimeNs; }
    public Double getResultSerializationTimeMs() { return resultSerializationTimeMs; }
    public Double getJvmGcTimeMs() { return jvmGcTimeMs; }
    public Double getSchedulerDelayMs() { return schedulerDelayMs; }
    public Double getResultSizeBytes() { return resultSizeBytes; }
    public Double getPeakExecutionMemoryBytes() { return peakExecutionMemoryBytes; }
    public Double getShuffleLocalBlocksFetched() { return shuffleLocalBlocksFetched; }
    public Double getShuffleRecordsRead() { return shuffleRecordsRead; }
    public Double getShuffleRemoteBytesReadToDisk() { return shuffleRemoteBytesReadToDisk; }
    public Double getShuffleRemoteReqsDurationMs() { return shuffleRemoteReqsDurationMs; }
}
