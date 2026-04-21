package x.mg.metrics.sparktelemetry.model;

/**
 * Query-level SQL execution metrics.
 * Captured by QueryExecutionListener for each SQL query completion.
 */
public class SqlExecutionMetrics {

    private long executionId;
    private String funcName;
    private String queryText;
    private boolean success;
    private String errorMessage;

    // Timing
    private long durationMs;

    // Join info
    private int joinCount;
    private String joinTypes;  // "broadcast,sort_merge" comma-separated

    // Shuffle summary
    private long shuffleBytesRead;
    private long shuffleBytesWritten;

    public long getExecutionId() { return executionId; }
    public void setExecutionId(long executionId) { this.executionId = executionId; }

    public String getFuncName() { return funcName; }
    public void setFuncName(String funcName) { this.funcName = funcName; }

    public String getQueryText() { return queryText; }
    public void setQueryText(String queryText) { this.queryText = queryText; }

    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public long getDurationMs() { return durationMs; }
    public void setDurationMs(long durationMs) { this.durationMs = durationMs; }

    public int getJoinCount() { return joinCount; }
    public void setJoinCount(int joinCount) { this.joinCount = joinCount; }

    public String getJoinTypes() { return joinTypes; }
    public void setJoinTypes(String joinTypes) { this.joinTypes = joinTypes; }

    public long getShuffleBytesRead() { return shuffleBytesRead; }
    public void setShuffleBytesRead(long shuffleBytesRead) { this.shuffleBytesRead = shuffleBytesRead; }

    public long getShuffleBytesWritten() { return shuffleBytesWritten; }
    public void setShuffleBytesWritten(long shuffleBytesWritten) { this.shuffleBytesWritten = shuffleBytesWritten; }
}
