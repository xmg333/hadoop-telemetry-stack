package x.mg.metrics.flink.model;

public class StageGovernanceRow {
    // Dimensions
    private long timestampMs;
    private String appId;
    private int stageId;
    private int taskCount;

    // Duration analysis
    private Double stageDurationMs;
    private Double avgTaskDurationMs;
    private Double maxTaskDurationMs;
    private Double minTaskDurationMs;
    private Double durationSkewRatio;

    // IO totals
    private Double totalBytesRead;
    private Double totalBytesWritten;
    private Double totalShuffleBytesRead;
    private Double totalShuffleBytesWritten;
    private Double totalRecordsRead;
    private Double totalRecordsWritten;

    // IO skew
    private Double ioReadSkewRatio;
    private Double ioWriteSkewRatio;
    private Double shuffleReadSkewRatio;

    // Small file indicators
    private Double avgOutputBytesPerTask;
    private Double avgOutputRecordsPerTask;
    private int smallOutputTaskCount;

    // Resource efficiency
    private Double cpuEfficiency;
    private Double gcOverheadRatio;
    private Double shuffleWaitRatio;
    private Double spillRatio;
    private Double deserializeOverhead;
    private Double schedulerDelayRatio;

    // Memory
    private Double maxPeakMemoryBytes;
    private Double totalMemorySpilled;

    public long getTimestampMs() { return timestampMs; }
    public void setTimestampMs(long timestampMs) { this.timestampMs = timestampMs; }
    public String getAppId() { return appId; }
    public void setAppId(String appId) { this.appId = appId; }
    public int getStageId() { return stageId; }
    public void setStageId(int stageId) { this.stageId = stageId; }
    public int getTaskCount() { return taskCount; }
    public void setTaskCount(int taskCount) { this.taskCount = taskCount; }
    public Double getStageDurationMs() { return stageDurationMs; }
    public void setStageDurationMs(Double stageDurationMs) { this.stageDurationMs = stageDurationMs; }
    public Double getAvgTaskDurationMs() { return avgTaskDurationMs; }
    public void setAvgTaskDurationMs(Double avgTaskDurationMs) { this.avgTaskDurationMs = avgTaskDurationMs; }
    public Double getMaxTaskDurationMs() { return maxTaskDurationMs; }
    public void setMaxTaskDurationMs(Double maxTaskDurationMs) { this.maxTaskDurationMs = maxTaskDurationMs; }
    public Double getMinTaskDurationMs() { return minTaskDurationMs; }
    public void setMinTaskDurationMs(Double minTaskDurationMs) { this.minTaskDurationMs = minTaskDurationMs; }
    public Double getDurationSkewRatio() { return durationSkewRatio; }
    public void setDurationSkewRatio(Double durationSkewRatio) { this.durationSkewRatio = durationSkewRatio; }
    public Double getTotalBytesRead() { return totalBytesRead; }
    public void setTotalBytesRead(Double totalBytesRead) { this.totalBytesRead = totalBytesRead; }
    public Double getTotalBytesWritten() { return totalBytesWritten; }
    public void setTotalBytesWritten(Double totalBytesWritten) { this.totalBytesWritten = totalBytesWritten; }
    public Double getTotalShuffleBytesRead() { return totalShuffleBytesRead; }
    public void setTotalShuffleBytesRead(Double totalShuffleBytesRead) { this.totalShuffleBytesRead = totalShuffleBytesRead; }
    public Double getTotalShuffleBytesWritten() { return totalShuffleBytesWritten; }
    public void setTotalShuffleBytesWritten(Double totalShuffleBytesWritten) { this.totalShuffleBytesWritten = totalShuffleBytesWritten; }
    public Double getTotalRecordsRead() { return totalRecordsRead; }
    public void setTotalRecordsRead(Double totalRecordsRead) { this.totalRecordsRead = totalRecordsRead; }
    public Double getTotalRecordsWritten() { return totalRecordsWritten; }
    public void setTotalRecordsWritten(Double totalRecordsWritten) { this.totalRecordsWritten = totalRecordsWritten; }
    public Double getIoReadSkewRatio() { return ioReadSkewRatio; }
    public void setIoReadSkewRatio(Double ioReadSkewRatio) { this.ioReadSkewRatio = ioReadSkewRatio; }
    public Double getIoWriteSkewRatio() { return ioWriteSkewRatio; }
    public void setIoWriteSkewRatio(Double ioWriteSkewRatio) { this.ioWriteSkewRatio = ioWriteSkewRatio; }
    public Double getShuffleReadSkewRatio() { return shuffleReadSkewRatio; }
    public void setShuffleReadSkewRatio(Double shuffleReadSkewRatio) { this.shuffleReadSkewRatio = shuffleReadSkewRatio; }
    public Double getAvgOutputBytesPerTask() { return avgOutputBytesPerTask; }
    public void setAvgOutputBytesPerTask(Double avgOutputBytesPerTask) { this.avgOutputBytesPerTask = avgOutputBytesPerTask; }
    public Double getAvgOutputRecordsPerTask() { return avgOutputRecordsPerTask; }
    public void setAvgOutputRecordsPerTask(Double avgOutputRecordsPerTask) { this.avgOutputRecordsPerTask = avgOutputRecordsPerTask; }
    public int getSmallOutputTaskCount() { return smallOutputTaskCount; }
    public void setSmallOutputTaskCount(int smallOutputTaskCount) { this.smallOutputTaskCount = smallOutputTaskCount; }
    public Double getCpuEfficiency() { return cpuEfficiency; }
    public void setCpuEfficiency(Double cpuEfficiency) { this.cpuEfficiency = cpuEfficiency; }
    public Double getGcOverheadRatio() { return gcOverheadRatio; }
    public void setGcOverheadRatio(Double gcOverheadRatio) { this.gcOverheadRatio = gcOverheadRatio; }
    public Double getShuffleWaitRatio() { return shuffleWaitRatio; }
    public void setShuffleWaitRatio(Double shuffleWaitRatio) { this.shuffleWaitRatio = shuffleWaitRatio; }
    public Double getSpillRatio() { return spillRatio; }
    public void setSpillRatio(Double spillRatio) { this.spillRatio = spillRatio; }
    public Double getDeserializeOverhead() { return deserializeOverhead; }
    public void setDeserializeOverhead(Double deserializeOverhead) { this.deserializeOverhead = deserializeOverhead; }
    public Double getSchedulerDelayRatio() { return schedulerDelayRatio; }
    public void setSchedulerDelayRatio(Double schedulerDelayRatio) { this.schedulerDelayRatio = schedulerDelayRatio; }
    public Double getMaxPeakMemoryBytes() { return maxPeakMemoryBytes; }
    public void setMaxPeakMemoryBytes(Double maxPeakMemoryBytes) { this.maxPeakMemoryBytes = maxPeakMemoryBytes; }
    public Double getTotalMemorySpilled() { return totalMemorySpilled; }
    public void setTotalMemorySpilled(Double totalMemorySpilled) { this.totalMemorySpilled = totalMemorySpilled; }
}
