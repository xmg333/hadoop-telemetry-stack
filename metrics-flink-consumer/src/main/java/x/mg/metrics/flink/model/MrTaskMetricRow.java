package x.mg.metrics.flink.model;

import java.util.Map;

public class MrTaskMetricRow {
    private long timestampMs;
    private String taskId;
    private String taskType;
    private String jobId;
    private String jobName;
    private String userName;
    private String state;

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

    public static MrTaskMetricRow fromLabels(long timestampMs, Map<String, String> labels) {
        MrTaskMetricRow row = new MrTaskMetricRow();
        row.timestampMs = timestampMs;
        row.taskId = labels.getOrDefault("mr.task.id", "unknown");
        row.taskType = labels.getOrDefault("mr.task.type", "unknown");
        row.jobId = labels.getOrDefault("mr.job.id", "unknown");
        row.jobName = labels.getOrDefault("mr.job.name", "unknown");
        row.userName = labels.getOrDefault("mr.job.user", "unknown");
        row.state = labels.getOrDefault("mr.job.state", "unknown");
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
        }
    }

    public long getTimestampMs() { return timestampMs; }
    public String getTaskId() { return taskId; }
    public String getTaskType() { return taskType; }
    public String getJobId() { return jobId; }
    public String getJobName() { return jobName; }
    public String getUserName() { return userName; }
    public String getState() { return state; }
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
}
