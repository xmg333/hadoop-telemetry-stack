package x.mg.metrics.sparktelemetry.otel;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableLongPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSumData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SplittingMetricExporterTest {

    private RecordingExporter recorder;

    @BeforeEach
    void setUp() {
        recorder = new RecordingExporter();
    }

    @Test
    void testNoSplitNeeded_FewDataPoints() {
        // 3 MetricData, each with 1 data point, threshold 10
        List<MetricData> metrics = createMetrics("metric", 3, 1);

        SplittingMetricExporter splitter = new SplittingMetricExporter(recorder, 10);
        CompletableResultCode result = splitter.export(metrics);

        assertTrue(result.join(5, TimeUnit.SECONDS).isSuccess());
        assertEquals(1, recorder.exportCount);
        assertEquals(3, recorder.totalDataPointsReceived());
    }

    @Test
    void testSplitIntoMultipleBatches() {
        // 4 MetricData, each with 5 data points = 20 total, threshold 10
        // Batch 1: md1(5) + md2(5) = 10 → flush
        // Batch 2: md3(5) + md4(5) = 10 → flush
        List<MetricData> metrics = createMetrics("metric", 4, 5);

        SplittingMetricExporter splitter = new SplittingMetricExporter(recorder, 10);
        CompletableResultCode result = splitter.export(metrics);

        assertTrue(result.join(5, TimeUnit.SECONDS).isSuccess());
        assertEquals(2, recorder.exportCount);
        assertEquals(20, recorder.totalDataPointsReceived());
    }

    @Test
    void testSingleLargeMetricDataExceedsThreshold() {
        // 1 MetricData with 15 data points, threshold 10
        // It should be sent as a single batch (can't split further)
        List<MetricData> metrics = createMetrics("large", 1, 15);
        // Add 2 more small ones
        metrics.addAll(createMetrics("small", 2, 3)); // 2×3 = 6 points

        SplittingMetricExporter splitter = new SplittingMetricExporter(recorder, 10);
        CompletableResultCode result = splitter.export(metrics);

        assertTrue(result.join(5, TimeUnit.SECONDS).isSuccess());
        // large(15) + small(6) = 21 total points
        // Batch 1: large(15) — can't split, goes alone
        // Batch 2: small1(3) + small2(3) = 6
        assertEquals(2, recorder.exportCount);
        assertEquals(21, recorder.totalDataPointsReceived());
    }

    @Test
    void testEmptyCollection() {
        SplittingMetricExporter splitter = new SplittingMetricExporter(recorder, 10);
        CompletableResultCode result = splitter.export(Collections.emptyList());

        assertTrue(result.isSuccess());
        assertEquals(0, recorder.exportCount);
    }

    @Test
    void testExactThreshold() {
        // 5 MetricData, each with 2 data points = 10 total, threshold 10
        List<MetricData> metrics = createMetrics("metric", 5, 2);

        SplittingMetricExporter splitter = new SplittingMetricExporter(recorder, 10);
        CompletableResultCode result = splitter.export(metrics);

        assertTrue(result.join(5, TimeUnit.SECONDS).isSuccess());
        assertEquals(1, recorder.exportCount);
        assertEquals(10, recorder.totalDataPointsReceived());
    }

    @Test
    void testFlushDelegates() {
        SplittingMetricExporter splitter = new SplittingMetricExporter(recorder, 10);
        CompletableResultCode result = splitter.flush();
        assertTrue(result.isSuccess());
        assertTrue(recorder.flushed);
    }

    @Test
    void testShutdownDelegates() {
        SplittingMetricExporter splitter = new SplittingMetricExporter(recorder, 10);
        CompletableResultCode result = splitter.shutdown();
        assertTrue(result.isSuccess());
        assertTrue(recorder.shutDown);
    }

    @Test
    void testAggregationTemporalityDelegates() {
        SplittingMetricExporter splitter = new SplittingMetricExporter(recorder, 10);
        AggregationTemporality temporality = splitter.getAggregationTemporality(
                io.opentelemetry.sdk.metrics.InstrumentType.COUNTER);
        assertEquals(AggregationTemporality.DELTA, temporality);
    }

    // --- Helper methods ---

    private List<MetricData> createMetrics(String namePrefix, int count, int pointsPerMetric) {
        List<MetricData> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add(createMetricData(namePrefix + "_" + i, pointsPerMetric));
        }
        return result;
    }

    private MetricData createMetricData(String name, int pointCount) {
        List<LongPointData> points = new ArrayList<>();
        for (int i = 0; i < pointCount; i++) {
            points.add(ImmutableLongPointData.create(
                    0, System.nanoTime(), Attributes.builder().put("idx", i).build(), i + 1));
        }
        return ImmutableMetricData.createLongSum(
                Resource.empty(), InstrumentationScopeInfo.empty(),
                name, "test description", "1",
                ImmutableSumData.create(true, AggregationTemporality.DELTA, points));
    }

    /**
     * A recording MetricExporter that tracks how many times export() is called
     * and how many total data points were received.
     */
    static class RecordingExporter implements MetricExporter {
        int exportCount = 0;
        boolean flushed = false;
        boolean shutDown = false;
        private final List<Integer> batchSizes = new ArrayList<>();

        @Override
        public CompletableResultCode export(Collection<MetricData> metrics) {
            exportCount++;
            int points = 0;
            for (MetricData md : metrics) {
                points += md.getData().getPoints().size();
            }
            batchSizes.add(points);
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode flush() {
            flushed = true;
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            shutDown = true;
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public AggregationTemporality getAggregationTemporality(io.opentelemetry.sdk.metrics.InstrumentType instrumentType) {
            return AggregationTemporality.DELTA;
        }

        int totalDataPointsReceived() {
            return batchSizes.stream().mapToInt(Integer::intValue).sum();
        }
    }
}
