package x.mg.metrics.flink.model;

import java.util.Map;

public class JvmMemoryMetricRow {
    // Dimensions
    private long timestampMs;
    private String appId;
    private String executorId;

    // Metrics
    private Double heapUsed;
    private Double nonHeapUsed;

    public JvmMemoryMetricRow() {}

    public static JvmMemoryMetricRow fromLabels(long timestampMs, Map<String, String> labels) {
        JvmMemoryMetricRow row = new JvmMemoryMetricRow();
        row.timestampMs = timestampMs;
        row.appId = labels.getOrDefault("spark.app.id", "unknown");
        row.executorId = labels.get("spark.executor.id");
        return row;
    }

    public void setMetricColumn(String columnName, double value) {
        switch (columnName) {
            case "heap_used": heapUsed = value; break;
            case "non_heap_used": nonHeapUsed = value; break;
        }
    }

    public long getTimestampMs() { return timestampMs; }
    public String getAppId() { return appId; }
    public String getExecutorId() { return executorId; }
    public Double getHeapUsed() { return heapUsed; }
    public Double getNonHeapUsed() { return nonHeapUsed; }
}
