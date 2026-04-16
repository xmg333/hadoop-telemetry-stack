package x.mg.metrics.flink.deserialize;

import io.opentelemetry.proto.metrics.v1.Histogram;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.metrics.v1.Gauge;
import x.mg.metrics.flink.model.HistogramBucket;
import x.mg.metrics.flink.model.MetricSample;
import x.mg.metrics.flink.model.MetricType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OtlpMetricsDeserializer {

    public static class DeserializationResult {
        private final List<MetricSample> samples;
        private final List<HistogramBucket> buckets;

        public DeserializationResult(List<MetricSample> samples, List<HistogramBucket> buckets) {
            this.samples = samples;
            this.buckets = buckets;
        }

        public List<MetricSample> getSamples() { return samples; }
        public List<HistogramBucket> getBuckets() { return buckets; }
    }

    public DeserializationResult deserialize(byte[] bytes) {
        List<MetricSample> samples = new ArrayList<>();
        List<HistogramBucket> buckets = new ArrayList<>();

        MetricsData data;
        try {
            data = MetricsData.parseFrom(bytes);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse OTLP MetricsData protobuf", e);
        }

        for (ResourceMetrics rm : data.getResourceMetricsList()) {
            // Extract resource attributes as base labels
            Map<String, String> resourceLabels = extractAttributes(rm.getResource().getAttributesList());

            for (ScopeMetrics sm : rm.getScopeMetricsList()) {
                for (Metric metric : sm.getMetricsList()) {
                    String metricName = metric.getName();

                    switch (metric.getDataCase()) {
                        case GAUGE:
                            processGauge(metric.getGauge(), metricName, resourceLabels, samples);
                            break;
                        case SUM:
                            processSum(metric.getSum(), metricName, resourceLabels, samples);
                            break;
                        case HISTOGRAM:
                            processHistogram(metric.getHistogram(), metricName, resourceLabels, samples, buckets);
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        return new DeserializationResult(samples, buckets);
    }

    private void processGauge(Gauge gauge, String metricName, Map<String, String> baseLabels, List<MetricSample> samples) {
        for (NumberDataPoint dp : gauge.getDataPointsList()) {
            long tsMs = dp.getTimeUnixNano() / 1_000_000;
            double value = dp.hasAsDouble() ? dp.getAsDouble() : (double) dp.getAsInt();
            Map<String, String> labels = mergeLabels(baseLabels, dp.getAttributesList());
            samples.add(new MetricSample(tsMs, metricName, MetricType.GAUGE, value, labels));
        }
    }

    private void processSum(Sum sum, String metricName, Map<String, String> baseLabels, List<MetricSample> samples) {
        MetricType type = sum.getIsMonotonic() ? MetricType.COUNTER : MetricType.GAUGE;
        for (NumberDataPoint dp : sum.getDataPointsList()) {
            long tsMs = dp.getTimeUnixNano() / 1_000_000;
            double value = dp.hasAsDouble() ? dp.getAsDouble() : (double) dp.getAsInt();
            Map<String, String> labels = mergeLabels(baseLabels, dp.getAttributesList());
            samples.add(new MetricSample(tsMs, metricName, type, value, labels));
        }
    }

    private void processHistogram(Histogram hist, String metricName, Map<String, String> baseLabels,
                                  List<MetricSample> samples, List<HistogramBucket> buckets) {
        for (HistogramDataPoint dp : hist.getDataPointsList()) {
            long tsMs = dp.getTimeUnixNano() / 1_000_000;
            Map<String, String> labels = mergeLabels(baseLabels, dp.getAttributesList());

            // Sum and count as MetricSample
            Map<String, String> sumLabels = new HashMap<>(labels);
            sumLabels.put("_histogram", "sum");
            samples.add(new MetricSample(tsMs, metricName, MetricType.HISTOGRAM_SUMMARY, dp.getSum(), sumLabels));

            Map<String, String> countLabels = new HashMap<>(labels);
            countLabels.put("_histogram", "count");
            samples.add(new MetricSample(tsMs, metricName, MetricType.HISTOGRAM_SUMMARY, (double) dp.getCount(), countLabels));

            // Explicit buckets
            List<Double> bounds = dp.getExplicitBoundsList();
            List<Long> counts = dp.getBucketCountsList();
            for (int i = 0; i < counts.size(); i++) {
                double le = (i < bounds.size()) ? bounds.get(i) : Double.POSITIVE_INFINITY;
                buckets.add(new HistogramBucket(tsMs, metricName, le, counts.get(i), labels));
            }
        }
    }

    private Map<String, String> extractAttributes(List<io.opentelemetry.proto.common.v1.KeyValue> attrs) {
        Map<String, String> map = new HashMap<>();
        for (io.opentelemetry.proto.common.v1.KeyValue kv : attrs) {
            map.put(kv.getKey(), extractValue(kv.getValue()));
        }
        return map;
    }

    private Map<String, String> mergeLabels(Map<String, String> base, List<io.opentelemetry.proto.common.v1.KeyValue> attrs) {
        Map<String, String> merged = new HashMap<>(base);
        for (io.opentelemetry.proto.common.v1.KeyValue kv : attrs) {
            merged.put(kv.getKey(), extractValue(kv.getValue()));
        }
        return merged;
    }

    private String extractValue(io.opentelemetry.proto.common.v1.AnyValue value) {
        switch (value.getValueCase()) {
            case STRING_VALUE: return value.getStringValue();
            case INT_VALUE: return String.valueOf(value.getIntValue());
            case DOUBLE_VALUE: return String.valueOf(value.getDoubleValue());
            case BOOL_VALUE: return String.valueOf(value.getBoolValue());
            default: return "";
        }
    }
}
