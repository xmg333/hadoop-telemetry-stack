package x.mg.metrics.flink.model;

import java.util.Map;

public class JobMetricRow {
    // Dimensions
    private long timestampMs;
    private String appId;
    private int jobId;
    private String jobSuccess;

    // Metrics
    private Double durationMs;
    private Double numStages;

    public JobMetricRow() {}

    public static JobMetricRow fromLabels(long timestampMs, Map<String, String> labels) {
        JobMetricRow row = new JobMetricRow();
        row.timestampMs = timestampMs;
        row.appId = labels.getOrDefault("spark.app.id", "unknown");
        row.jobId = parseInt(labels.get("spark.job.id"), 0);
        row.jobSuccess = labels.get("spark.job.success");
        return row;
    }

    private static int parseInt(String s, int def) {
        try { return s != null ? Integer.parseInt(s) : def; }
        catch (NumberFormatException e) { return def; }
    }

    public void setMetricColumn(String columnName, double value) {
        switch (columnName) {
            case "duration_ms": durationMs = value; break;
            case "num_stages": numStages = value; break;
        }
    }

    public long getTimestampMs() { return timestampMs; }
    public String getAppId() { return appId; }
    public int getJobId() { return jobId; }
    public String getJobSuccess() { return jobSuccess; }
    public Double getDurationMs() { return durationMs; }
    public Double getNumStages() { return numStages; }
}
