package x.mg.metrics.flink.model;

import java.io.Serializable;
import java.util.Map;

public class HiveQueryMetricRow implements Serializable {
    private static final long serialVersionUID = 1L;
    private long timestampMs;
    private String queryId;
    private String operation;
    private String userName;
    private String appName;
    private String queue;
    private String success;
    private String executionEngine;

    private Double durationMs;
    private Double successCount;
    private Double failureCount;
    private Double inputBytes;
    private Double outputBytes;
    private Double inputRows;
    private Double outputRows;

    // Text
    private String queryText;

    public static HiveQueryMetricRow fromLabels(long timestampMs, Map<String, String> labels) {
        HiveQueryMetricRow row = new HiveQueryMetricRow();
        row.timestampMs = timestampMs;
        row.queryId = labels.getOrDefault("hive.query.id", "unknown");
        row.operation = labels.getOrDefault("hive.query.operation", "unknown");
        row.userName = labels.getOrDefault("hive.query.user", "unknown");
        row.appName = labels.getOrDefault("hive.query.id", "");  // query_id as app_name
        row.queue = labels.getOrDefault("hive.query.queue", "");
        row.success = labels.getOrDefault("hive.query.success", "unknown");
        row.executionEngine = labels.getOrDefault("hive.query.execution_engine", "unknown");
        row.queryText = labels.getOrDefault("hive.query.sql_text", null);
        return row;
    }

    public void setMetricColumn(String columnName, double value) {
        switch (columnName) {
            case "duration_ms": durationMs = value; break;
            case "success_count": successCount = value; break;
            case "failure_count": failureCount = value; break;
            case "input_bytes": inputBytes = value; break;
            case "output_bytes": outputBytes = value; break;
            case "input_rows": inputRows = value; break;
            case "output_rows": outputRows = value; break;
        }
    }

    public long getTimestampMs() { return timestampMs; }
    public String getQueryId() { return queryId; }
    public String getOperation() { return operation; }
    public String getUserName() { return userName; }
    public String getAppName() { return appName; }
    public String getQueue() { return queue; }
    public String getSuccess() { return success; }
    public String getExecutionEngine() { return executionEngine; }
    public Double getDurationMs() { return durationMs; }
    public Double getSuccessCount() { return successCount; }
    public Double getFailureCount() { return failureCount; }
    public Double getInputBytes() { return inputBytes; }
    public Double getOutputBytes() { return outputBytes; }
    public Double getInputRows() { return inputRows; }
    public Double getOutputRows() { return outputRows; }
    public String getQueryText() { return queryText; }
}
