package x.mg.metrics.flink.model;

import java.util.Map;

public class JvmGcMetricRow {
    // Dimensions
    private long timestampMs;
    private String appId;
    private String executorId;
    private String gcName;
    private String appName;
    private String userName;
    private String queue;

    // Metrics
    private Double gcCount;
    private Double gcTimeMs;

    public JvmGcMetricRow() {}

    public static JvmGcMetricRow fromLabels(long timestampMs, Map<String, String> labels) {
        JvmGcMetricRow row = new JvmGcMetricRow();
        row.timestampMs = timestampMs;
        row.appId = labels.getOrDefault("spark.app.id", "unknown");
        row.executorId = labels.getOrDefault("spark.executor.id", "unknown");
        row.gcName = labels.getOrDefault("gc_name", "unknown");
        row.appName = labels.getOrDefault("spark.app.name", "");
        row.userName = labels.getOrDefault("spark.user", "");
        row.queue = labels.getOrDefault("spark.yarn.queue", "");
        return row;
    }

    public void setMetricColumn(String columnName, double value) {
        switch (columnName) {
            case "gc_count": gcCount = value; break;
            case "gc_time_ms": gcTimeMs = value; break;
        }
    }

    public long getTimestampMs() { return timestampMs; }
    public String getAppId() { return appId; }
    public String getExecutorId() { return executorId; }
    public String getGcName() { return gcName; }
    public String getAppName() { return appName; }
    public String getUserName() { return userName; }
    public String getQueue() { return queue; }
    public Double getGcCount() { return gcCount; }
    public Double getGcTimeMs() { return gcTimeMs; }
}
