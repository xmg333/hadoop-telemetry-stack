package x.mg.metrics.sparktelemetry.otel;

import x.mg.metrics.sparktelemetry.model.SparkMetricEvent;

/**
 * Accepts SparkMetricEvents and records them as OTel metrics.
 */
public interface MetricRecorder {
    void record(SparkMetricEvent event);
}
