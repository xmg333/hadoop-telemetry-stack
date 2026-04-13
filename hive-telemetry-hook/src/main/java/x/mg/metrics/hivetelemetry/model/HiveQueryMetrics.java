package x.mg.metrics.hivetelemetry.model;

import java.util.LinkedHashSet;
import java.util.Set;

public class HiveQueryMetrics {
    private long timestampMs;
    private String queryId;
    private String queryText;
    private String operationName;
    private String userName;
    private long durationMs;
    private boolean success;

    // IO metrics
    private long inputBytes;
    private long outputBytes;
    private long inputRows;
    private long outputRows;

    // Table lineage
    private final Set<String> inputTables = new LinkedHashSet<>();
    private final Set<String> outputTables = new LinkedHashSet<>();
    private final Set<String> inputDatabases = new LinkedHashSet<>();

    // Execution engine (mr, spark, tez)
    private String executionEngine;

    public long getTimestampMs() { return timestampMs; }
    public void setTimestampMs(long timestampMs) { this.timestampMs = timestampMs; }

    public String getQueryId() { return queryId; }
    public void setQueryId(String queryId) { this.queryId = queryId; }

    public String getQueryText() { return queryText; }
    public void setQueryText(String queryText) { this.queryText = queryText; }

    public String getOperationName() { return operationName; }
    public void setOperationName(String operationName) { this.operationName = operationName; }

    public String getUserName() { return userName; }
    public void setUserName(String userName) { this.userName = userName; }

    public long getDurationMs() { return durationMs; }
    public void setDurationMs(long durationMs) { this.durationMs = durationMs; }

    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }

    public long getInputBytes() { return inputBytes; }
    public void setInputBytes(long inputBytes) { this.inputBytes = inputBytes; }

    public long getOutputBytes() { return outputBytes; }
    public void setOutputBytes(long outputBytes) { this.outputBytes = outputBytes; }

    public long getInputRows() { return inputRows; }
    public void setInputRows(long inputRows) { this.inputRows = inputRows; }

    public long getOutputRows() { return outputRows; }
    public void setOutputRows(long outputRows) { this.outputRows = outputRows; }

    public Set<String> getInputTables() { return inputTables; }
    public void addInputTable(String table) { inputTables.add(table); }

    public Set<String> getOutputTables() { return outputTables; }
    public void addOutputTable(String table) { outputTables.add(table); }

    public Set<String> getInputDatabases() { return inputDatabases; }
    public void addInputDatabase(String db) { inputDatabases.add(db); }

    public String getExecutionEngine() { return executionEngine; }
    public void setExecutionEngine(String executionEngine) { this.executionEngine = executionEngine; }
}
