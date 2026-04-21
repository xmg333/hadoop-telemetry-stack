package x.mg.metrics.flink.operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import x.mg.metrics.flink.source.OtlpDeserializationSchema;

public class MetricRecordSplitFlatMap implements FlatMapFunction<OtlpDeserializationSchema.MetricRecord, MetricItem> {
    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(OtlpDeserializationSchema.MetricRecord record, Collector<MetricItem> out) throws Exception {
        for (x.mg.metrics.flink.model.MetricSample sample : record.getSamples()) {
            out.collect(MetricItem.ofSample(sample));
        }
        for (x.mg.metrics.flink.model.HistogramBucket bucket : record.getBuckets()) {
            out.collect(MetricItem.ofBucket(bucket));
        }
    }
}
