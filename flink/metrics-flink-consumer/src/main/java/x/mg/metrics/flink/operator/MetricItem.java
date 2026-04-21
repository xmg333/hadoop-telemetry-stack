package x.mg.metrics.flink.operator;

import x.mg.metrics.flink.model.HistogramBucket;
import x.mg.metrics.flink.model.MetricSample;

import java.io.Serializable;

public class MetricItem implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean isSample;
    private final MetricSample sample;
    private final HistogramBucket bucket;

    private MetricItem(MetricSample sample) {
        this.isSample = true;
        this.sample = sample;
        this.bucket = null;
    }

    private MetricItem(HistogramBucket bucket) {
        this.isSample = false;
        this.sample = null;
        this.bucket = bucket;
    }

    public static MetricItem ofSample(MetricSample sample) { return new MetricItem(sample); }
    public static MetricItem ofBucket(HistogramBucket bucket) { return new MetricItem(bucket); }

    public boolean isSample() { return isSample; }
    public MetricSample getSample() { return sample; }
    public HistogramBucket getBucket() { return bucket; }

    public String getMetricName() {
        return isSample ? sample.getMetricName() : bucket.getMetricName();
    }
}
