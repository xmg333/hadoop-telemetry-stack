package x.mg.metrics.flink.model;

import java.util.Map;

public class JvmGcMetricRow {
    // Dimensions
    private long timestampMs;
    private String appId;
    private String executorId;
    private String gcName;

    // Metrics
    private Double gcCount;
    private Double gcTimeMs;

    public JvmGcMetricRow() {}

    public static JvmGcMetricRow fromLabels(long timestampMs, Map<String, String> labels) {
        JvmGcMetricRow row = new JvmGcMetricRow();
        row.timestampMs = timestampMs;
        row.appId = labels.getOrDefault("spark.app.id", "unknown");
        row.executorId = labels.get("spark.executor.id");
        row.gcName = labels.get("gc_name");
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
    public Double getGcCount() { return gcCount; }
    public Double getGcTimeMs() { return gcTimeMs; }
}
