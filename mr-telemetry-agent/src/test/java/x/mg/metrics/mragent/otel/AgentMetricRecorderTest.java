package x.mg.metrics.mragent.otel;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import org.junit.jupiter.api.Test;
import x.mg.metrics.mragent.counter.CounterMapping;
import x.mg.metrics.mragent.counter.TaskIdentity;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AgentMetricRecorderTest {

    /**
     * Creates a recorder with a non-routable OTel endpoint.
     * The SDK initializes but metrics won't actually be exported.
     * This is sufficient for testing that recordDeltas() doesn't throw.
     */
    private AgentMetricRecorder createRecorder() {
        OtlpGrpcMetricExporter exporter = OtlpGrpcMetricExporter.builder()
            .setEndpoint("http://localhost:9999")
            .build();

        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter)
                    .setInterval(Duration.ofMinutes(1))
                    .build())
            .build();

        OpenTelemetry otel = OpenTelemetrySdk.builder()
            .setMeterProvider(meterProvider)
            .build();

        return new AgentMetricRecorder(otel);
    }

    @Test
    void testRecordDeltas() {
        AgentMetricRecorder recorder = createRecorder();

        Map<String, Long> deltas = new HashMap<>();
        deltas.put("mr.task.io.hdfs_bytes_read", 2048L);
        deltas.put("mr.task.io.map_input_records", 100L);
        deltas.put("mr.task.cpu_time_ms", 500L);

        TaskIdentity identity = new TaskIdentity("task_001", "job_001", "test-job");
        assertDoesNotThrow(() -> recorder.recordDeltas(deltas, "map", identity));
    }

    @Test
    void testRecordDeltasEmpty() {
        AgentMetricRecorder recorder = createRecorder();

        Map<String, Long> deltas = new HashMap<>();
        TaskIdentity identity = new TaskIdentity("task_001", "job_001", "test-job");

        assertDoesNotThrow(() -> recorder.recordDeltas(deltas, "reduce", identity));
    }

    @Test
    void testRecordDeltasZeroValues() {
        AgentMetricRecorder recorder = createRecorder();

        Map<String, Long> deltas = new HashMap<>();
        deltas.put("mr.task.io.hdfs_bytes_read", 0L);

        TaskIdentity identity = new TaskIdentity("task_001", "job_001", "test-job");

        assertDoesNotThrow(() -> recorder.recordDeltas(deltas, "map", identity));
    }

    @Test
    void testRecordDeltasUnknownMetric() {
        AgentMetricRecorder recorder = createRecorder();

        Map<String, Long> deltas = new HashMap<>();
        deltas.put("unknown.metric", 100L);

        TaskIdentity identity = new TaskIdentity("task_001", "job_001", "test-job");

        // Unknown metric names should be silently ignored
        assertDoesNotThrow(() -> recorder.recordDeltas(deltas, "map", identity));
    }
}
