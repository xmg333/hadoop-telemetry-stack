package x.mg.metrics.sparktelemetry.model;

import java.util.List;

/**
 * Version-agnostic metric event model.
 * All Spark version adapters translate their version-specific events into this unified model.
 */
public class SparkMetricEvent {

    public enum EventType {
        TASK_END,
        STAGE_COMPLETE,
        JOB_START,
        JOB_END,
        PERIODIC_SYSTEM,
        SQL_EXECUTION
    }

    private EventType eventType;
    private long timestamp;
    private String applicationId;
    private String applicationName;
    private String user;
    private String queue;
    private String attemptId;
    private String executorId;
    private int stageId;
    private int stageAttemptNumber;
    private long taskId;
    private int taskAttemptNumber;
    private boolean taskSuccessful;
    private long taskDurationMs;

    // Task execution metrics (Category 1)
    private TaskExecutionMetrics taskExecutionMetrics;

    // Task info attributes (Category 3)
    private String taskHost;
    private String taskLocality;
    private boolean taskSpeculative;

    // Stage detailed metrics (Category 4)
    private int stageNumTasks;
    private long stageDurationMs;

    // Job lifecycle metrics (Category 5)
    private int jobId;
    private int jobNumStages;
    private long jobDurationMs;
    private boolean jobSuccessful;

    private IOMetrics ioMetrics;
    private MemoryMetrics memoryMetrics;
    private GCMetrics gcMetrics;

    // SQL query execution metrics (Category 6)
    private SqlExecutionMetrics sqlExecutionMetrics;
    private List<SqlTableIOMetrics> sqlTableIOMetrics;

    public EventType getEventType() { return eventType; }
    public void setEventType(EventType eventType) { this.eventType = eventType; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public String getApplicationId() { return applicationId; }
    public void setApplicationId(String applicationId) { this.applicationId = applicationId; }

    public String getApplicationName() { return applicationName; }
    public void setApplicationName(String applicationName) { this.applicationName = applicationName; }

    public String getUser() { return user; }
    public void setUser(String user) { this.user = user; }

    public String getQueue() { return queue; }
    public void setQueue(String queue) { this.queue = queue; }

    public String getAttemptId() { return attemptId; }
    public void setAttemptId(String attemptId) { this.attemptId = attemptId; }

    public String getExecutorId() { return executorId; }
    public void setExecutorId(String executorId) { this.executorId = executorId; }

    public int getStageId() { return stageId; }
    public void setStageId(int stageId) { this.stageId = stageId; }

    public int getStageAttemptNumber() { return stageAttemptNumber; }
    public void setStageAttemptNumber(int n) { this.stageAttemptNumber = n; }

    public long getTaskId() { return taskId; }
    public void setTaskId(long taskId) { this.taskId = taskId; }

    public int getTaskAttemptNumber() { return taskAttemptNumber; }
    public void setTaskAttemptNumber(int n) { this.taskAttemptNumber = n; }

    public boolean isTaskSuccessful() { return taskSuccessful; }
    public void setTaskSuccessful(boolean v) { this.taskSuccessful = v; }

    public long getTaskDurationMs() { return taskDurationMs; }
    public void setTaskDurationMs(long taskDurationMs) { this.taskDurationMs = taskDurationMs; }

    public TaskExecutionMetrics getTaskExecutionMetrics() { return taskExecutionMetrics; }
    public void setTaskExecutionMetrics(TaskExecutionMetrics m) { this.taskExecutionMetrics = m; }

    public String getTaskHost() { return taskHost; }
    public void setTaskHost(String taskHost) { this.taskHost = taskHost; }

    public String getTaskLocality() { return taskLocality; }
    public void setTaskLocality(String taskLocality) { this.taskLocality = taskLocality; }

    public boolean isTaskSpeculative() { return taskSpeculative; }
    public void setTaskSpeculative(boolean taskSpeculative) { this.taskSpeculative = taskSpeculative; }

    public int getStageNumTasks() { return stageNumTasks; }
    public void setStageNumTasks(int stageNumTasks) { this.stageNumTasks = stageNumTasks; }

    public long getStageDurationMs() { return stageDurationMs; }
    public void setStageDurationMs(long stageDurationMs) { this.stageDurationMs = stageDurationMs; }

    public int getJobId() { return jobId; }
    public void setJobId(int jobId) { this.jobId = jobId; }

    public int getJobNumStages() { return jobNumStages; }
    public void setJobNumStages(int jobNumStages) { this.jobNumStages = jobNumStages; }

    public long getJobDurationMs() { return jobDurationMs; }
    public void setJobDurationMs(long jobDurationMs) { this.jobDurationMs = jobDurationMs; }

    public boolean isJobSuccessful() { return jobSuccessful; }
    public void setJobSuccessful(boolean jobSuccessful) { this.jobSuccessful = jobSuccessful; }

    public IOMetrics getIoMetrics() { return ioMetrics; }
    public void setIoMetrics(IOMetrics ioMetrics) { this.ioMetrics = ioMetrics; }

    public MemoryMetrics getMemoryMetrics() { return memoryMetrics; }
    public void setMemoryMetrics(MemoryMetrics memoryMetrics) { this.memoryMetrics = memoryMetrics; }

    public GCMetrics getGcMetrics() { return gcMetrics; }
    public void setGcMetrics(GCMetrics gcMetrics) { this.gcMetrics = gcMetrics; }

    public SqlExecutionMetrics getSqlExecutionMetrics() { return sqlExecutionMetrics; }
    public void setSqlExecutionMetrics(SqlExecutionMetrics sqlExecutionMetrics) { this.sqlExecutionMetrics = sqlExecutionMetrics; }

    public List<SqlTableIOMetrics> getSqlTableIOMetrics() { return sqlTableIOMetrics; }
    public void setSqlTableIOMetrics(List<SqlTableIOMetrics> sqlTableIOMetrics) { this.sqlTableIOMetrics = sqlTableIOMetrics; }
}
