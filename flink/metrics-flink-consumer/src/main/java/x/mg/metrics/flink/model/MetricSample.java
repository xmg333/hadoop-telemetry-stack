package x.mg.metrics.flink.model;

import java.io.Serializable;
import java.util.Map;

public class MetricSample implements Serializable {
    private static final long serialVersionUID = 1L;
    private long timestampMs;
    private String metricName;
    private MetricType metricType;
    private double value;
    private Map<String, String> labels;

    public MetricSample() {}

    public MetricSample(long timestampMs, String metricName, MetricType metricType, double value, Map<String, String> labels) {
        this.timestampMs = timestampMs;
        this.metricName = metricName;
        this.metricType = metricType;
        this.value = value;
        this.labels = labels;
    }

    public long getTimestampMs() { return timestampMs; }
    public void setTimestampMs(long timestampMs) { this.timestampMs = timestampMs; }

    public String getMetricName() { return metricName; }
    public void setMetricName(String metricName) { this.metricName = metricName; }

    public MetricType getMetricType() { return metricType; }
    public void setMetricType(MetricType metricType) { this.metricType = metricType; }

    public double getValue() { return value; }
    public void setValue(double value) { this.value = value; }

    public Map<String, String> getLabels() { return labels; }
    public void setLabels(Map<String, String> labels) { this.labels = labels; }

    @Override
    public String toString() {
        return "MetricSample{ts=" + timestampMs + ", name=" + metricName + ", type=" + metricType + ", value=" + value + "}";
    }
}
