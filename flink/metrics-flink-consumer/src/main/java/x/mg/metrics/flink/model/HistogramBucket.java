package x.mg.metrics.flink.model;

import java.io.Serializable;
import java.util.Map;

public class HistogramBucket implements Serializable {
    private static final long serialVersionUID = 1L;
    private long timestampMs;
    private String metricName;
    private double bucketLe;
    private long bucketCount;
    private Map<String, String> labels;

    public HistogramBucket() {}

    public HistogramBucket(long timestampMs, String metricName, double bucketLe, long bucketCount, Map<String, String> labels) {
        this.timestampMs = timestampMs;
        this.metricName = metricName;
        this.bucketLe = bucketLe;
        this.bucketCount = bucketCount;
        this.labels = labels;
    }

    public long getTimestampMs() { return timestampMs; }
    public void setTimestampMs(long timestampMs) { this.timestampMs = timestampMs; }

    public String getMetricName() { return metricName; }
    public void setMetricName(String metricName) { this.metricName = metricName; }

    public double getBucketLe() { return bucketLe; }
    public void setBucketLe(double bucketLe) { this.bucketLe = bucketLe; }

    public long getBucketCount() { return bucketCount; }
    public void setBucketCount(long bucketCount) { this.bucketCount = bucketCount; }

    public Map<String, String> getLabels() { return labels; }
    public void setLabels(Map<String, String> labels) { this.labels = labels; }

    @Override
    public String toString() {
        return "HistogramBucket{ts=" + timestampMs + ", name=" + metricName + ", le=" + bucketLe + ", count=" + bucketCount + "}";
    }
}
