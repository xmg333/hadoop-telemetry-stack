package x.mg.metrics.flink.model;

import java.util.Map;

public class SqlTableIoMetricRow {
    // Dimensions
    private long timestampMs;
    private String appId;
    private String executionId;
    private String tableName;
    private String operation;
    private String appName;
    private String userName;
    private String queue;

    // Metrics
    private Double bytes;
    private Double rows;
    private Double filesRead;
    private Double timeMs;

    public SqlTableIoMetricRow() {}

    public static SqlTableIoMetricRow fromLabels(long timestampMs, Map<String, String> labels) {
        SqlTableIoMetricRow row = new SqlTableIoMetricRow();
        row.timestampMs = timestampMs;
        row.appId = labels.getOrDefault("spark.app.id", "unknown");
        row.executionId = labels.getOrDefault("spark.sql.execution_id", "0");
        row.tableName = labels.getOrDefault("spark.sql.table_name", "unknown");
        row.operation = labels.getOrDefault("spark.sql.operation", "unknown");
        row.appName = labels.getOrDefault("spark.app.name", "");
        row.userName = labels.getOrDefault("spark.user", "");
        row.queue = labels.getOrDefault("spark.yarn.queue", "");
        return row;
    }

    public void setMetricColumn(String columnName, double value) {
        switch (columnName) {
            case "bytes": bytes = value; break;
            case "rows": rows = value; break;
            case "files_read": filesRead = value; break;
            case "time_ms": timeMs = value; break;
        }
    }

    // Getters
    public long getTimestampMs() { return timestampMs; }
    public String getAppId() { return appId; }
    public String getExecutionId() { return executionId; }
    public String getTableName() { return tableName; }
    public String getOperation() { return operation; }
    public String getAppName() { return appName; }
    public String getUserName() { return userName; }
    public String getQueue() { return queue; }
    public Double getBytes() { return bytes; }
    public Double getRows() { return rows; }
    public Double getFilesRead() { return filesRead; }
    public Double getTimeMs() { return timeMs; }
}
