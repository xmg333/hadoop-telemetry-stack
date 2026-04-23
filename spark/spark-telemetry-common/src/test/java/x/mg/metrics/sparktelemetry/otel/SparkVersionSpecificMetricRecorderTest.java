package x.mg.metrics.sparktelemetry.otel;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import x.mg.metrics.sparktelemetry.config.TelemetryConfig;
import x.mg.metrics.sparktelemetry.model.IOMetrics;
import x.mg.metrics.sparktelemetry.model.SparkMetricEvent;

import static org.junit.jupiter.api.Assertions.*;

class SparkVersionSpecificMetricRecorderTest {

    private InMemoryMetricReader metricReader;
    private MetricRecorder recorder;

    @BeforeEach
    void setUp() {
        metricReader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .registerMetricReader(metricReader)
                .build();
        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setMeterProvider(meterProvider)
                .build();
        recorder = new MetricRecorder(openTelemetry, new TelemetryConfig());
    }

    private MetricData findMetric(String name) {
        return metricReader.collectAllMetrics().stream()
                .filter(m -> m.getName().equals(name))
                .findFirst()
                .orElse(null);
    }

    private long getLongCounterSum(MetricData metric) {
        return metric.getLongSumData().getPoints().stream()
                .mapToLong(LongPointData::getValue)
                .sum();
    }

    private IOMetrics buildIOMetrics(int remoteReqsDuration, long remoteBytesReadToDisk) {
        IOMetrics io = new IOMetrics();
        io.setBytesRead(1000);
        io.setBytesWritten(2000);
        io.setRecordsRead(100);
        io.setRecordsWritten(200);
        io.setShuffleRemoteBytesRead(3000);
        io.setShuffleLocalBytesRead(4000);
        io.setShuffleFetchWaitTime(100);
        io.setShuffleRemoteBlocksFetched(5);
        io.setShuffleLocalBlocksFetched(8);
        io.setShuffleRecordsRead(400);
        io.setShuffleRemoteBytesReadToDisk(remoteBytesReadToDisk);
        io.setShuffleRemoteReqsDuration(remoteReqsDuration);
        io.setDiskBytesSpilled(600);
        io.setMemoryBytesSpilled(700);
        io.setShuffleBytesWritten(2000);
        return io;
    }

    private SparkMetricEvent buildTaskEvent(IOMetrics io) {
        SparkMetricEvent event = new SparkMetricEvent();
        event.setEventType(SparkMetricEvent.EventType.TASK_END);
        event.setTaskId(1);
        event.setStageId(1);
        event.setExecutorId("1");
        event.setTaskSuccessful(true);
        event.setTaskDurationMs(100);
        event.setIoMetrics(io);
        return event;
    }

    @Test
    void testSpark2_ShuffleWriteMetrics() {
        // Spark 2.x: remoteReqsDuration=0, remoteBytesReadToDisk=0
        IOMetrics io = buildIOMetrics(0, 0);
        recorder.record(buildTaskEvent(io));

        MetricData shuffleBytesWritten = findMetric("spark.task.shuffle.bytes_written");
        assertNotNull(shuffleBytesWritten);
        assertEquals(2000, getLongCounterSum(shuffleBytesWritten));

        MetricData shuffleLocalBlocks = findMetric("spark.task.shuffle.local_blocks_fetched");
        assertNotNull(shuffleLocalBlocks);
        assertEquals(8, getLongCounterSum(shuffleLocalBlocks));

        MetricData shuffleRecordsRead = findMetric("spark.task.shuffle.records_read");
        assertNotNull(shuffleRecordsRead);
        assertEquals(400, getLongCounterSum(shuffleRecordsRead));

        // remoteReqsDuration=0 should still be recorded
        MetricData remoteReqsDuration = findMetric("spark.task.shuffle.remote_reqs_duration_ms");
        assertNotNull(remoteReqsDuration);
        assertEquals(0, getLongCounterSum(remoteReqsDuration));
    }

    @Test
    void testSpark30_NoRemoteReqsDuration() {
        // Spark 3.0: remoteReqsDuration=0, uses bytesWritten API
        IOMetrics io = buildIOMetrics(0, 0);
        recorder.record(buildTaskEvent(io));

        MetricData shuffleBytesWritten = findMetric("spark.task.shuffle.bytes_written");
        assertNotNull(shuffleBytesWritten);
        assertEquals(2000, getLongCounterSum(shuffleBytesWritten));

        MetricData remoteBytesToDisk = findMetric("spark.task.shuffle.remote_bytes_read_to_disk");
        assertNotNull(remoteBytesToDisk);
        assertEquals(0, getLongCounterSum(remoteBytesToDisk));
    }

    @Test
    void testSpark32_NoRemoteReqsDuration() {
        // Spark 3.2: same as 3.0
        IOMetrics io = buildIOMetrics(0, 0);
        recorder.record(buildTaskEvent(io));

        MetricData shuffleBytesWritten = findMetric("spark.task.shuffle.bytes_written");
        assertNotNull(shuffleBytesWritten);
        assertEquals(2000, getLongCounterSum(shuffleBytesWritten));
    }

    @Test
    void testSpark35_WithRemoteReqsDuration() {
        // Spark 3.5: remoteReqsDuration=200, remoteBytesReadToDisk=4096
        IOMetrics io = buildIOMetrics(200, 4096);
        recorder.record(buildTaskEvent(io));

        MetricData shuffleBytesWritten = findMetric("spark.task.shuffle.bytes_written");
        assertNotNull(shuffleBytesWritten);
        assertEquals(2000, getLongCounterSum(shuffleBytesWritten));

        MetricData remoteBytesToDisk = findMetric("spark.task.shuffle.remote_bytes_read_to_disk");
        assertNotNull(remoteBytesToDisk);
        assertEquals(4096, getLongCounterSum(remoteBytesToDisk));

        MetricData remoteReqsDuration = findMetric("spark.task.shuffle.remote_reqs_duration_ms");
        assertNotNull(remoteReqsDuration);
        assertEquals(200, getLongCounterSum(remoteReqsDuration));
    }

    @Test
    void testSpark40_SameAs35() {
        // Spark 4.x: identical metrics to 3.5, Scala 2.13 doesn't affect Java layer
        IOMetrics io = buildIOMetrics(200, 4096);
        recorder.record(buildTaskEvent(io));

        String[] expectedMetrics = {
                "spark.task.shuffle.bytes_written",
                "spark.task.shuffle.local_blocks_fetched",
                "spark.task.shuffle.records_read",
                "spark.task.shuffle.remote_bytes_read_to_disk",
                "spark.task.shuffle.remote_reqs_duration_ms"
        };

        for (String metricName : expectedMetrics) {
            MetricData metric = findMetric(metricName);
            assertNotNull(metric, "Metric " + metricName + " should exist for Spark 4.x");
        }

        assertEquals(2000, getLongCounterSum(findMetric("spark.task.shuffle.bytes_written")));
        assertEquals(8, getLongCounterSum(findMetric("spark.task.shuffle.local_blocks_fetched")));
        assertEquals(400, getLongCounterSum(findMetric("spark.task.shuffle.records_read")));
        assertEquals(4096, getLongCounterSum(findMetric("spark.task.shuffle.remote_bytes_read_to_disk")));
        assertEquals(200, getLongCounterSum(findMetric("spark.task.shuffle.remote_reqs_duration_ms")));

        // Verify attribute keys are identical to Spark 3.5
        MetricData remoteReqs = findMetric("spark.task.shuffle.remote_reqs_duration_ms");
        for (LongPointData point : remoteReqs.getLongSumData().getPoints()) {
            Attributes attrs = point.getAttributes();
            assertEquals("1", attrs.get(AttributeKey.longKey("spark.stage.id")).toString());
            assertEquals("1", attrs.get(AttributeKey.longKey("spark.task.id")).toString());
            assertTrue(attrs.get(AttributeKey.booleanKey("spark.task.success")));
        }
    }
}
