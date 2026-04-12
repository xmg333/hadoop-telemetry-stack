package x.mg.metrics.flink.model;

import java.util.Map;

public class StageMetricRow {
    // Dimensions
    private long timestampMs;
    private String appId;
    private String executorId;
    private int stageId;

    // Metrics
    private Double durationMs;
    private Double numTasks;
    private Double executorRunTimeMs;
    private Double executorCpuTimeNs;
    private Double jvmGcTimeMs;
    private Double peakExecutionMemoryBytes;
    private Double ioBytesRead;
    private Double ioBytesWritten;

    public StageMetricRow() {}

    public static StageMetricRow fromLabels(long timestampMs, Map<String, String> labels) {
        StageMetricRow row = new StageMetricRow();
        row.timestampMs = timestampMs;
        row.appId = labels.getOrDefault("spark.app.id", "unknown");
        row.executorId = labels.get("spark.executor.id");
        row.stageId = parseInt(labels.get("spark.stage.id"), 0);
        return row;
    }

    private static int parseInt(String s, int def) {
        try { return s != null ? Integer.parseInt(s) : def; }
        catch (NumberFormatException e) { return def; }
    }

    public void setMetricColumn(String columnName, double value) {
        switch (columnName) {
            case "duration_ms": durationMs = value; break;
            case "num_tasks": numTasks = value; break;
            case "executor_run_time_ms": executorRunTimeMs = value; break;
            case "executor_cpu_time_ns": executorCpuTimeNs = value; break;
            case "jvm_gc_time_ms": jvmGcTimeMs = value; break;
            case "peak_execution_memory_bytes": peakExecutionMemoryBytes = value; break;
            case "io_bytes_read": ioBytesRead = value; break;
            case "io_bytes_written": ioBytesWritten = value; break;
        }
    }

    public long getTimestampMs() { return timestampMs; }
    public String getAppId() { return appId; }
    public String getExecutorId() { return executorId; }
    public int getStageId() { return stageId; }
    public Double getDurationMs() { return durationMs; }
    public Double getNumTasks() { return numTasks; }
    public Double getExecutorRunTimeMs() { return executorRunTimeMs; }
    public Double getExecutorCpuTimeNs() { return executorCpuTimeNs; }
    public Double getJvmGcTimeMs() { return jvmGcTimeMs; }
    public Double getPeakExecutionMemoryBytes() { return peakExecutionMemoryBytes; }
    public Double getIoBytesRead() { return ioBytesRead; }
    public Double getIoBytesWritten() { return ioBytesWritten; }
}
