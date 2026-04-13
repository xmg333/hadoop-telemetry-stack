package x.mg.metrics.flink.model;

import java.util.Map;

public class HiveTableIoMetricRow {
    private long timestampMs;
    private String queryId;
    private String tableName;
    private String tableType; // "input" or "output"
    private String operation;
    private String userName;
    private String executionEngine;

    private Double inputTableCount;
    private Double outputTableCount;

    public static HiveTableIoMetricRow fromLabels(long timestampMs, Map<String, String> labels) {
        HiveTableIoMetricRow row = new HiveTableIoMetricRow();
        row.timestampMs = timestampMs;
        row.queryId = labels.getOrDefault("hive.query.id", "unknown");
        row.operation = labels.getOrDefault("hive.query.operation", "unknown");
        row.userName = labels.getOrDefault("hive.query.user", "unknown");
        row.executionEngine = labels.getOrDefault("hive.query.execution_engine", "unknown");

        String inputTable = labels.get("hive.query.input_table");
        String outputTable = labels.get("hive.query.output_table");
        if (inputTable != null) {
            row.tableName = inputTable;
            row.tableType = "input";
        } else if (outputTable != null) {
            row.tableName = outputTable;
            row.tableType = "output";
        } else {
            row.tableName = "unknown";
            row.tableType = "unknown";
        }
        return row;
    }

    public void setMetricColumn(String columnName, double value) {
        switch (columnName) {
            case "input_table_count": inputTableCount = value; break;
            case "output_table_count": outputTableCount = value; break;
        }
    }

    public long getTimestampMs() { return timestampMs; }
    public String getQueryId() { return queryId; }
    public String getTableName() { return tableName; }
    public String getTableType() { return tableType; }
    public String getOperation() { return operation; }
    public String getUserName() { return userName; }
    public String getExecutionEngine() { return executionEngine; }
    public Double getInputTableCount() { return inputTableCount; }
    public Double getOutputTableCount() { return outputTableCount; }
}
