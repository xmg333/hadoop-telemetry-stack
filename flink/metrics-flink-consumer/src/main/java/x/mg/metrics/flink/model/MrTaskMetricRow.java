package x.mg.metrics.flink.model;

import java.io.Serializable;
import java.util.Map;

public class MrTaskMetricRow implements Serializable {
    private static final long serialVersionUID = 1L;
    private long timestampMs;
    private String taskId;
    private String taskType;
    private String jobId;
    private String jobName;
    private String userName;
    private String state;
    private String queue;

    // IO metrics
    private Double hdfsBytesRead;
    private Double hdfsBytesWritten;
    private Double fileBytesRead;
    private Double fileBytesWritten;
    private Double mapInputRecords;
    private Double mapOutputRecords;
    private Double mapOutputBytes;
    private Double reduceInputRecords;
    private Double reduceOutputRecords;
    private Double reduceShuffleBytes;
    private Double spilledRecords;

    // Resource metrics
    private Double cpuTimeMs;
    private Double gcTimeMs;

    // Duration and result metrics
    private Double durationMs;
    private Double successCount;
    private Double failureCount;

    // File operation metrics
    private Double hdfsReadOps;
    private Double hdfsWriteOps;
    private Double hdfsLargeReadOps;
    private Double fileReadOps;
    private Double fileWriteOps;
    private Double fileLargeReadOps;

    public static MrTaskMetricRow fromLabels(long timestampMs, Map<String, String> labels) {
        MrTaskMetricRow row = new MrTaskMetricRow();
        // Use actual job finish time if available, otherwise fall back to OTel export time
        String finishTimeStr = labels.get("mr.job.finish_time_ms");
        if (finishTimeStr != null && !finishTimeStr.isEmpty()) {
            try { row.timestampMs = Long.parseLong(finishTimeStr); }
            catch (NumberFormatException e) { row.timestampMs = timestampMs; }
        } else {
            row.timestampMs = timestampMs;
        }
        row.taskId = labels.getOrDefault("mr.task.id", "unknown");
        row.taskType = labels.getOrDefault("mr.task.type", "unknown");
        row.jobId = labels.getOrDefault("mr.job.id", "unknown");
        row.jobName = labels.getOrDefault("mr.job.name", "unknown");
        row.userName = labels.getOrDefault("mr.job.user", "unknown");
        row.state = labels.getOrDefault("mr.task.state", "unknown");
        row.queue = labels.getOrDefault("mr.job.queue", "");
        return row;
    }

    public void setMetricColumn(String columnName, double value) {
        switch (columnName) {
            case "hdfs_bytes_read": hdfsBytesRead = value; break;
            case "hdfs_bytes_written": hdfsBytesWritten = value; break;
            case "file_bytes_read": fileBytesRead = value; break;
            case "file_bytes_written": fileBytesWritten = value; break;
            case "map_input_records": mapInputRecords = value; break;
            case "map_output_records": mapOutputRecords = value; break;
            case "map_output_bytes": mapOutputBytes = value; break;
            case "reduce_input_records": reduceInputRecords = value; break;
            case "reduce_output_records": reduceOutputRecords = value; break;
            case "reduce_shuffle_bytes": reduceShuffleBytes = value; break;
            case "spilled_records": spilledRecords = value; break;
            case "cpu_time_ms": cpuTimeMs = value; break;
            case "gc_time_ms": gcTimeMs = value; break;
            case "duration_ms": durationMs = value; break;
            case "success_count": successCount = value; break;
            case "failure_count": failureCount = value; break;
            case "hdfs_read_ops": hdfsReadOps = value; break;
            case "hdfs_write_ops": hdfsWriteOps = value; break;
            case "hdfs_large_read_ops": hdfsLargeReadOps = value; break;
            case "file_read_ops": fileReadOps = value; break;
            case "file_write_ops": fileWriteOps = value; break;
            case "file_large_read_ops": fileLargeReadOps = value; break;
        }
    }

    public long getTimestampMs() { return timestampMs; }
    public String getTaskId() { return taskId; }
    public String getTaskType() { return taskType; }
    public String getJobId() { return jobId; }
    public String getJobName() { return jobName; }
    public String getUserName() { return userName; }
    public String getState() { return state; }
    public String getQueue() { return queue; }
    public Double getHdfsBytesRead() { return hdfsBytesRead; }
    public Double getHdfsBytesWritten() { return hdfsBytesWritten; }
    public Double getFileBytesRead() { return fileBytesRead; }
    public Double getFileBytesWritten() { return fileBytesWritten; }
    public Double getMapInputRecords() { return mapInputRecords; }
    public Double getMapOutputRecords() { return mapOutputRecords; }
    public Double getMapOutputBytes() { return mapOutputBytes; }
    public Double getReduceInputRecords() { return reduceInputRecords; }
    public Double getReduceOutputRecords() { return reduceOutputRecords; }
    public Double getReduceShuffleBytes() { return reduceShuffleBytes; }
    public Double getSpilledRecords() { return spilledRecords; }
    public Double getCpuTimeMs() { return cpuTimeMs; }
    public Double getGcTimeMs() { return gcTimeMs; }
    public Double getDurationMs() { return durationMs; }
    public Double getSuccessCount() { return successCount; }
    public Double getFailureCount() { return failureCount; }
    public Double getHdfsReadOps() { return hdfsReadOps; }
    public Double getHdfsWriteOps() { return hdfsWriteOps; }
    public Double getHdfsLargeReadOps() { return hdfsLargeReadOps; }
    public Double getFileReadOps() { return fileReadOps; }
    public Double getFileWriteOps() { return fileWriteOps; }
    public Double getFileLargeReadOps() { return fileLargeReadOps; }
}
