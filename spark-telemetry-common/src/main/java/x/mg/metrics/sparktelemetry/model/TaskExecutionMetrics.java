package x.mg.metrics.sparktelemetry.model;

/**
 * Task-level execution timing and memory metrics from Spark TaskMetrics.
 * Populated when spark.telemetry.metrics.task.execution=true.
 */
public class TaskExecutionMetrics {

    private long executorRunTime;
    private long executorCpuTime;
    private long executorDeserializeTime;
    private long executorDeserializeCpuTime;
    private long resultSerializationTime;
    private long jvmGcTime;
    private long schedulerDelay;
    private long resultSize;
    private long peakExecutionMemory;

    public long getExecutorRunTime() { return executorRunTime; }
    public void setExecutorRunTime(long executorRunTime) { this.executorRunTime = executorRunTime; }

    public long getExecutorCpuTime() { return executorCpuTime; }
    public void setExecutorCpuTime(long executorCpuTime) { this.executorCpuTime = executorCpuTime; }

    public long getExecutorDeserializeTime() { return executorDeserializeTime; }
    public void setExecutorDeserializeTime(long executorDeserializeTime) { this.executorDeserializeTime = executorDeserializeTime; }

    public long getExecutorDeserializeCpuTime() { return executorDeserializeCpuTime; }
    public void setExecutorDeserializeCpuTime(long executorDeserializeCpuTime) { this.executorDeserializeCpuTime = executorDeserializeCpuTime; }

    public long getResultSerializationTime() { return resultSerializationTime; }
    public void setResultSerializationTime(long resultSerializationTime) { this.resultSerializationTime = resultSerializationTime; }

    public long getJvmGcTime() { return jvmGcTime; }
    public void setJvmGcTime(long jvmGcTime) { this.jvmGcTime = jvmGcTime; }

    public long getSchedulerDelay() { return schedulerDelay; }
    public void setSchedulerDelay(long schedulerDelay) { this.schedulerDelay = schedulerDelay; }

    public long getResultSize() { return resultSize; }
    public void setResultSize(long resultSize) { this.resultSize = resultSize; }

    public long getPeakExecutionMemory() { return peakExecutionMemory; }
    public void setPeakExecutionMemory(long peakExecutionMemory) { this.peakExecutionMemory = peakExecutionMemory; }
}
