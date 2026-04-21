package x.mg.metrics.flink.model;

import java.io.Serializable;
import java.util.Map;

public class JvmMemoryMetricRow implements Serializable {
    private static final long serialVersionUID = 1L;
    // Dimensions
    private long timestampMs;
    private String appId;
    private String executorId;
    private String appName;
    private String userName;
    private String queue;

    // Metrics
    private Double heapUsed;
    private Double nonHeapUsed;

    public JvmMemoryMetricRow() {}

    public static JvmMemoryMetricRow fromLabels(long timestampMs, Map<String, String> labels) {
        JvmMemoryMetricRow row = new JvmMemoryMetricRow();
        row.timestampMs = timestampMs;
        row.appId = labels.getOrDefault("spark.app.id", "unknown");
        row.executorId = labels.getOrDefault("spark.executor.id", "unknown");
        row.appName = labels.getOrDefault("spark.app.name", "");
        row.userName = labels.getOrDefault("spark.user", "");
        row.queue = labels.getOrDefault("spark.yarn.queue", "");
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
    public String getAppName() { return appName; }
    public String getUserName() { return userName; }
    public String getQueue() { return queue; }
    public Double getHeapUsed() { return heapUsed; }
    public Double getNonHeapUsed() { return nonHeapUsed; }
}
