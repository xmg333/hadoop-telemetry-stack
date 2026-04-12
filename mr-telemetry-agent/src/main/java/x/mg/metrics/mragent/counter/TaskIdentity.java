package x.mg.metrics.mragent.counter;

/**
 * Task/job identity extracted from the MR task Context via reflection.
 */
public class TaskIdentity {

    private final String taskId;
    private final String jobId;
    private final String jobName;

    public TaskIdentity(String taskId, String jobId, String jobName) {
        this.taskId = taskId;
        this.jobId = jobId;
        this.jobName = jobName;
    }

    public String getTaskId() { return taskId; }
    public String getJobId() { return jobId; }
    public String getJobName() { return jobName; }

    public static TaskIdentity UNKNOWN = new TaskIdentity("unknown", "unknown", "unknown");
}
