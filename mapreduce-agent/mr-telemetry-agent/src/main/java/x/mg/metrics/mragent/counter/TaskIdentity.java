package x.mg.metrics.mragent.counter;

/**
 * Task/job identity extracted from the MR task Context via reflection.
 */
public class TaskIdentity {

    private final String taskId;
    private final String jobId;
    private final String jobName;
    private final String user;
    private final String queue;

    public TaskIdentity(String taskId, String jobId, String jobName, String user, String queue) {
        this.taskId = taskId;
        this.jobId = jobId;
        this.jobName = jobName;
        this.user = user;
        this.queue = queue;
    }

    public String getTaskId() { return taskId; }
    public String getJobId() { return jobId; }
    public String getJobName() { return jobName; }
    public String getUser() { return user; }
    public String getQueue() { return queue; }

    public static TaskIdentity UNKNOWN = new TaskIdentity("unknown", "unknown", "unknown", "unknown", "unknown");
}
