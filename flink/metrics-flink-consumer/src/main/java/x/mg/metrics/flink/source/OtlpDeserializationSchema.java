package x.mg.metrics.flink.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import x.mg.metrics.flink.deserialize.OtlpMetricsDeserializer;
import x.mg.metrics.flink.model.HistogramBucket;
import x.mg.metrics.flink.model.MetricSample;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class OtlpDeserializationSchema implements KafkaRecordDeserializationSchema<OtlpDeserializationSchema.MetricRecord> {
    private static final long serialVersionUID = 1L;

    private transient OtlpMetricsDeserializer deserializer;

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        deserializer = new OtlpMetricsDeserializer();
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<MetricRecord> out) throws IOException {
        if (deserializer == null) {
            deserializer = new OtlpMetricsDeserializer();
        }
        OtlpMetricsDeserializer.DeserializationResult result = deserializer.deserialize(record.value());
        out.collect(new MetricRecord(
            new ArrayList<>(result.getSamples()),
            new ArrayList<>(result.getBuckets())
        ));
    }

    @Override
    public TypeInformation<MetricRecord> getProducedType() {
        return TypeInformation.of(MetricRecord.class);
    }

    public static class MetricRecord implements Serializable {
        private static final long serialVersionUID = 1L;
        private final List<MetricSample> samples;
        private final List<HistogramBucket> buckets;

        public MetricRecord(List<MetricSample> samples, List<HistogramBucket> buckets) {
            this.samples = samples;
            this.buckets = buckets;
        }

        public List<MetricSample> getSamples() { return samples; }
        public List<HistogramBucket> getBuckets() { return buckets; }
    }
}
