package x.mg.metrics.mrtelemetry.model;

import java.util.HashMap;
import java.util.Map;

/**
 * MapReduce job-level metrics extracted from History Server counters.
 */
public class MRJobMetrics {
    private String jobId;
    private String jobName;
    private String user;
    private String queue;
    private String state;
    private long submitTime;
    private long startTime;
    private long finishTime;
    private long elapsedTime;
    private int totalMaps;
    private int totalReduces;
    private int failedMaps;
    private int failedReduces;

    // IO counters
    private long hdfsBytesRead;
    private long hdfsBytesWritten;
    private long fileBytesRead;
    private long fileBytesWritten;

    // Task counters
    private long mapInputRecords;
    private long mapOutputRecords;
    private long mapOutputBytes;
    private long reduceInputRecords;
    private long reduceOutputRecords;
    private long reduceShuffleBytes;
    private long spilledRecords;

    // Resource counters
    private long cpuMilliseconds;
    private long gcTimeMillis;
    private long physicalMemoryBytes;
    private long virtualMemoryBytes;
    private long committedHeapBytes;

    // Duration counters
    private long millisMaps;
    private long millisReduces;

    // Raw counters (for flexibility)
    private final Map<String, Map<String, Long>> counterGroups = new HashMap<>();

    // Getters and setters
    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }
    public String getJobName() { return jobName; }
    public void setJobName(String jobName) { this.jobName = jobName; }
    public String getUser() { return user; }
    public void setUser(String user) { this.user = user; }
    public String getQueue() { return queue; }
    public void setQueue(String queue) { this.queue = queue; }
    public String getState() { return state; }
    public void setState(String state) { this.state = state; }
    public long getSubmitTime() { return submitTime; }
    public void setSubmitTime(long v) { this.submitTime = v; }
    public long getStartTime() { return startTime; }
    public void setStartTime(long v) { this.startTime = v; }
    public long getFinishTime() { return finishTime; }
    public void setFinishTime(long v) { this.finishTime = v; }
    public long getElapsedTime() { return elapsedTime; }
    public void setElapsedTime(long v) { this.elapsedTime = v; }
    public int getTotalMaps() { return totalMaps; }
    public void setTotalMaps(int v) { this.totalMaps = v; }
    public int getTotalReduces() { return totalReduces; }
    public void setTotalReduces(int v) { this.totalReduces = v; }
    public int getFailedMaps() { return failedMaps; }
    public void setFailedMaps(int v) { this.failedMaps = v; }
    public int getFailedReduces() { return failedReduces; }
    public void setFailedReduces(int v) { this.failedReduces = v; }
    public long getHdfsBytesRead() { return hdfsBytesRead; }
    public void setHdfsBytesRead(long v) { this.hdfsBytesRead = v; }
    public long getHdfsBytesWritten() { return hdfsBytesWritten; }
    public void setHdfsBytesWritten(long v) { this.hdfsBytesWritten = v; }
    public long getFileBytesRead() { return fileBytesRead; }
    public void setFileBytesRead(long v) { this.fileBytesRead = v; }
    public long getFileBytesWritten() { return fileBytesWritten; }
    public void setFileBytesWritten(long v) { this.fileBytesWritten = v; }
    public long getMapInputRecords() { return mapInputRecords; }
    public void setMapInputRecords(long v) { this.mapInputRecords = v; }
    public long getMapOutputRecords() { return mapOutputRecords; }
    public void setMapOutputRecords(long v) { this.mapOutputRecords = v; }
    public long getMapOutputBytes() { return mapOutputBytes; }
    public void setMapOutputBytes(long v) { this.mapOutputBytes = v; }
    public long getReduceInputRecords() { return reduceInputRecords; }
    public void setReduceInputRecords(long v) { this.reduceInputRecords = v; }
    public long getReduceOutputRecords() { return reduceOutputRecords; }
    public void setReduceOutputRecords(long v) { this.reduceOutputRecords = v; }
    public long getReduceShuffleBytes() { return reduceShuffleBytes; }
    public void setReduceShuffleBytes(long v) { this.reduceShuffleBytes = v; }
    public long getSpilledRecords() { return spilledRecords; }
    public void setSpilledRecords(long v) { this.spilledRecords = v; }
    public long getCpuMilliseconds() { return cpuMilliseconds; }
    public void setCpuMilliseconds(long v) { this.cpuMilliseconds = v; }
    public long getGcTimeMillis() { return gcTimeMillis; }
    public void setGcTimeMillis(long v) { this.gcTimeMillis = v; }
    public long getPhysicalMemoryBytes() { return physicalMemoryBytes; }
    public void setPhysicalMemoryBytes(long v) { this.physicalMemoryBytes = v; }
    public long getVirtualMemoryBytes() { return virtualMemoryBytes; }
    public void setVirtualMemoryBytes(long v) { this.virtualMemoryBytes = v; }
    public long getCommittedHeapBytes() { return committedHeapBytes; }
    public void setCommittedHeapBytes(long v) { this.committedHeapBytes = v; }
    public long getMillisMaps() { return millisMaps; }
    public void setMillisMaps(long v) { this.millisMaps = v; }
    public long getMillisReduces() { return millisReduces; }
    public void setMillisReduces(long v) { this.millisReduces = v; }
    public Map<String, Map<String, Long>> getCounterGroups() { return counterGroups; }

    public void addCounter(String group, String name, long value) {
        counterGroups.computeIfAbsent(group, k -> new HashMap<>()).put(name, value);
    }

    public Long getCounter(String group, String name) {
        Map<String, Long> g = counterGroups.get(group);
        return g != null ? g.get(name) : null;
    }
}
