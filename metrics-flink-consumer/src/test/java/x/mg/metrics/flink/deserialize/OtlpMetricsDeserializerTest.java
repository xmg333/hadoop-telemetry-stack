package x.mg.metrics.flink.deserialize;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.*;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.resource.v1.Resource;
import org.junit.jupiter.api.Test;
import x.mg.metrics.flink.model.HistogramBucket;
import x.mg.metrics.flink.model.MetricSample;
import x.mg.metrics.flink.model.MetricType;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OtlpMetricsDeserializerTest {

    private final OtlpMetricsDeserializer deserializer = new OtlpMetricsDeserializer();

    @Test
    void parsesGaugeMetric() {
        MetricsData data = MetricsData.newBuilder()
                .addResourceMetrics(ResourceMetrics.newBuilder()
                        .setResource(Resource.newBuilder()
                                .addAttributes(KeyValue.newBuilder().setKey("service.name").setValue(AnyValue.newBuilder().setStringValue("test-service").build()).build()))
                        .addScopeMetrics(ScopeMetrics.newBuilder()
                                .setScope(InstrumentationScope.newBuilder().setName("test").build())
                                .addMetrics(Metric.newBuilder()
                                        .setName("jvm.memory.heap_used")
                                        .setGauge(Gauge.newBuilder()
                                                .addDataPoints(NumberDataPoint.newBuilder()
                                                        .setTimeUnixNano(1_700_000_000_000_000_000L)
                                                        .setAsDouble(1024.0)
                                                        .addAttributes(KeyValue.newBuilder().setKey("host").setValue(AnyValue.newBuilder().setStringValue("host1").build()).build())
                                                        .build())))))
                .build();

        OtlpMetricsDeserializer.DeserializationResult result = deserializer.deserialize(data.toByteArray());
        List<MetricSample> samples = result.getSamples();

        assertEquals(1, samples.size());
        MetricSample s = samples.get(0);
        assertEquals("jvm.memory.heap_used", s.getMetricName());
        assertEquals(MetricType.GAUGE, s.getMetricType());
        assertEquals(1024.0, s.getValue(), 0.001);
        assertEquals(1_700_000_000_000L, s.getTimestampMs());
        assertEquals("test-service", s.getLabels().get("service.name"));
        assertEquals("host1", s.getLabels().get("host"));
        assertTrue(result.getBuckets().isEmpty());
    }

    @Test
    void parsesSumMonotonicAsCounter() {
        MetricsData data = MetricsData.newBuilder()
                .addResourceMetrics(ResourceMetrics.newBuilder()
                        .addScopeMetrics(ScopeMetrics.newBuilder()
                                .addMetrics(Metric.newBuilder()
                                        .setName("spark.task.io.bytes_read")
                                        .setSum(Sum.newBuilder()
                                                .setIsMonotonic(true)
                                                .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
                                                .addDataPoints(NumberDataPoint.newBuilder()
                                                        .setTimeUnixNano(2_000_000_000_000_000_000L)
                                                        .setAsInt(4096)
                                                        .build())))))
                .build();

        OtlpMetricsDeserializer.DeserializationResult result = deserializer.deserialize(data.toByteArray());
        assertEquals(1, result.getSamples().size());
        assertEquals(MetricType.COUNTER, result.getSamples().get(0).getMetricType());
        assertEquals(4096.0, result.getSamples().get(0).getValue(), 0.001);
    }

    @Test
    void parsesHistogramWithBuckets() {
        MetricsData data = MetricsData.newBuilder()
                .addResourceMetrics(ResourceMetrics.newBuilder()
                        .addScopeMetrics(ScopeMetrics.newBuilder()
                                .addMetrics(Metric.newBuilder()
                                        .setName("spark.task.duration_ms")
                                        .setHistogram(Histogram.newBuilder()
                                                .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
                                                .addDataPoints(HistogramDataPoint.newBuilder()
                                                        .setTimeUnixNano(3_000_000_000_000_000_000L)
                                                        .setCount(10)
                                                        .setSum(500.0)
                                                        .addExplicitBounds(100.0)
                                                        .addExplicitBounds(500.0)
                                                        .addBucketCounts(3)
                                                        .addBucketCounts(5)
                                                        .addBucketCounts(2))))))
                .build();

        OtlpMetricsDeserializer.DeserializationResult result = deserializer.deserialize(data.toByteArray());
        assertEquals(2, result.getSamples().size()); // sum + count
        assertEquals(3, result.getBuckets().size());

        HistogramBucket b0 = result.getBuckets().get(0);
        assertEquals(100.0, b0.getBucketLe(), 0.001);
        assertEquals(3, b0.getBucketCount());

        HistogramBucket b2 = result.getBuckets().get(2);
        assertEquals(Double.POSITIVE_INFINITY, b2.getBucketLe());
        assertEquals(2, b2.getBucketCount());
    }

    @Test
    void throwsOnInvalidProtobuf() {
        assertThrows(RuntimeException.class, () -> deserializer.deserialize(new byte[]{0x01, 0x02, 0x03}));
    }

    @Test
    void handlesEmptyMetricsData() {
        MetricsData data = MetricsData.newBuilder().build();
        OtlpMetricsDeserializer.DeserializationResult result = deserializer.deserialize(data.toByteArray());
        assertTrue(result.getSamples().isEmpty());
        assertTrue(result.getBuckets().isEmpty());
    }
}
