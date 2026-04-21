package x.mg.metrics.flink.model;

import java.io.Serializable;
import java.util.Map;

public class MrJobMetricRow implements Serializable {
    private static final long serialVersionUID = 1L;
    private long timestampMs;
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

    // Processing metrics
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
    private Double physicalMemoryBytes;
    private Double virtualMemoryBytes;
    private Double committedHeapBytes;

    // Duration & count metrics
    private Double mapsDurationMs;
    private Double reducesDurationMs;
    private Double elapsedTimeMs;
    private Double launchedMaps;
    private Double launchedReduces;

    // Actual job timing from History Server
    private long startTimeMs;
    private long finishTimeMs;

    public static MrJobMetricRow fromLabels(long timestampMs, Map<String, String> labels) {
        MrJobMetricRow row = new MrJobMetricRow();
        // Use actual job finish time if available, otherwise fall back to OTel export time
        String finishTimeStr = labels.get("mr.job.finish_time_ms");
        if (finishTimeStr != null && !finishTimeStr.isEmpty()) {
            try { row.timestampMs = Long.parseLong(finishTimeStr); }
            catch (NumberFormatException e) { row.timestampMs = timestampMs; }
        } else {
            row.timestampMs = timestampMs;
        }
        // Extract actual start/finish time from labels
        String startTimeStr = labels.get("mr.job.start_time_ms");
        if (startTimeStr != null && !startTimeStr.isEmpty()) {
            try { row.startTimeMs = Long.parseLong(startTimeStr); }
            catch (NumberFormatException e) { row.startTimeMs = 0; }
        }
        row.finishTimeMs = row.timestampMs; // timestampMs is already set to finish time above

        row.jobId = labels.getOrDefault("mr.job.id", "unknown");
        row.jobName = labels.getOrDefault("mr.job.name", "unknown");
        row.userName = labels.getOrDefault("mr.job.user", "unknown");
        row.state = labels.getOrDefault("mr.job.state", "unknown");
        row.queue = labels.getOrDefault("mr.job.queue", "unknown");
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
            case "physical_memory_bytes": physicalMemoryBytes = value; break;
            case "virtual_memory_bytes": virtualMemoryBytes = value; break;
            case "committed_heap_bytes": committedHeapBytes = value; break;
            case "maps_duration_ms": mapsDurationMs = value; break;
            case "reduces_duration_ms": reducesDurationMs = value; break;
            case "elapsed_time_ms": elapsedTimeMs = value; break;
            case "launched_maps": launchedMaps = value; break;
            case "launched_reduces": launchedReduces = value; break;
        }
    }

    public long getTimestampMs() { return timestampMs; }
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
    public Double getPhysicalMemoryBytes() { return physicalMemoryBytes; }
    public Double getVirtualMemoryBytes() { return virtualMemoryBytes; }
    public Double getCommittedHeapBytes() { return committedHeapBytes; }
    public Double getMapsDurationMs() { return mapsDurationMs; }
    public Double getReducesDurationMs() { return reducesDurationMs; }
    public Double getElapsedTimeMs() { return elapsedTimeMs; }
    public Double getLaunchedMaps() { return launchedMaps; }
    public Double getLaunchedReduces() { return launchedReduces; }
    public long getStartTimeMs() { return startTimeMs; }
    public long getFinishTimeMs() { return finishTimeMs; }
}
