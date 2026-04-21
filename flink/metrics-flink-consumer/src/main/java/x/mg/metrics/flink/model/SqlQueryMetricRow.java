package x.mg.metrics.flink.model;

import java.io.Serializable;
import java.util.Map;

public class SqlQueryMetricRow implements Serializable {
    private static final long serialVersionUID = 1L;
    // Dimensions
    private long timestampMs;
    private String appId;
    private String executionId;
    private String appName;
    private String userName;
    private String queue;

    // Metrics
    private Double durationMs;
    private Double shuffleBytesRead;
    private Double shuffleBytesWritten;
    private Double joinCount;

    // Text
    private String queryText;

    public SqlQueryMetricRow() {}

    public static SqlQueryMetricRow fromLabels(long timestampMs, Map<String, String> labels) {
        SqlQueryMetricRow row = new SqlQueryMetricRow();
        row.timestampMs = timestampMs;
        row.appId = labels.getOrDefault("spark.app.id", "unknown");
        row.executionId = labels.getOrDefault("spark.sql.execution_id", "0");
        row.appName = labels.getOrDefault("spark.app.name", "");
        row.userName = labels.getOrDefault("spark.user", "");
        row.queue = labels.getOrDefault("spark.yarn.queue", "");
        row.queryText = labels.getOrDefault("spark.sql.query_text", null);
        return row;
    }

    public void setMetricColumn(String columnName, double value) {
        switch (columnName) {
            case "duration_ms": durationMs = value; break;
            case "shuffle_bytes_read": shuffleBytesRead = value; break;
            case "shuffle_bytes_written": shuffleBytesWritten = value; break;
            case "join_count": joinCount = value; break;
        }
    }

    // Getters
    public long getTimestampMs() { return timestampMs; }
    public String getAppId() { return appId; }
    public String getExecutionId() { return executionId; }
    public String getAppName() { return appName; }
    public String getUserName() { return userName; }
    public String getQueue() { return queue; }
    public Double getDurationMs() { return durationMs; }
    public Double getShuffleBytesRead() { return shuffleBytesRead; }
    public Double getShuffleBytesWritten() { return shuffleBytesWritten; }
    public Double getJoinCount() { return joinCount; }
    public String getQueryText() { return queryText; }
}
