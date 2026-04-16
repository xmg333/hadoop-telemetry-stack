package x.mg.metrics.mragent.otel;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import x.mg.metrics.mragent.counter.CounterMapping;
import x.mg.metrics.mragent.counter.TaskIdentity;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Records MR task-level metrics as OTel delta counters.
 * Metric naming follows the mr.task.* pattern (mirrors the mr.job.* pattern in MRMetricRecorder).
 */
public class AgentMetricRecorder {

    private static final Logger LOG = Logger.getLogger(AgentMetricRecorder.class.getName());
    private static final String METER_NAME = "mr-telemetry-agent";

    private final Meter meter;
    private final Map<String, LongCounter> counters = new ConcurrentHashMap<>();
    private final LongHistogram durationHistogram;
    private final LongCounter successCounter;
    private final LongCounter failureCounter;

    public AgentMetricRecorder(OpenTelemetry openTelemetry) {
        this.meter = openTelemetry.getMeter(METER_NAME);

        // Pre-create all counters from CounterMapping definitions
        for (CounterMapping mapping : CounterMapping.ALL) {
            counters.put(mapping.otelName,
                meter.counterBuilder(mapping.otelName)
                    .setDescription(mapping.description)
                    .setUnit(mapping.unit)
                    .build());
        }

        // Task duration histogram
        this.durationHistogram = meter.histogramBuilder("mr.task.duration_ms")
            .ofLongs()
            .setDescription("MR task execution duration")
            .setUnit("ms")
            .build();

        // Task success/failure counters
        this.successCounter = meter.counterBuilder("mr.task.success")
            .setDescription("MR task completed successfully")
            .setUnit("{tasks}")
            .build();
        this.failureCounter = meter.counterBuilder("mr.task.failure")
            .setDescription("MR task failed with exception")
            .setUnit("{tasks}")
            .build();
    }

    /**
     * Record delta counter values.
     *
     * @param deltas   map of OTel metric name -> delta value
     * @param taskType "map" or "reduce"
     * @param identity task/job identity
     */
    public void recordDeltas(Map<String, Long> deltas, String taskType, TaskIdentity identity) {
        try {
            Attributes attrs = buildAttributes(taskType, identity);

            for (Map.Entry<String, Long> entry : deltas.entrySet()) {
                LongCounter counter = counters.get(entry.getKey());
                if (counter != null && entry.getValue() > 0) {
                    counter.add(entry.getValue(), attrs);
                }
            }
        } catch (Exception e) {
            // Silent failure: OTel Collector connection issues should not affect MR task execution
            LOG.log(Level.FINE, "Failed to record counter deltas: " + e.getMessage());
        }
    }

    private Attributes buildAttributes(String taskType, TaskIdentity identity) {
        AttributesBuilder builder = Attributes.builder()
            .put("mr.task.type", taskType)
            .put("mr.task.id", identity.getTaskId())
            .put("mr.job.id", identity.getJobId())
            .put("mr.job.name", identity.getJobName())
            .put("mr.job.user", identity.getUser())
            .put("mr.job.queue", identity.getQueue());
        return builder.build();
    }

    /**
     * Record task execution duration as a histogram.
     */
    public void recordDuration(long durationMs, String taskType, TaskIdentity identity) {
        try {
            Attributes attrs = buildAttributes(taskType, identity);
            durationHistogram.record(durationMs, attrs);
        } catch (Exception e) {
            LOG.log(Level.FINE, "Failed to record task duration: " + e.getMessage());
        }
    }

    /**
     * Record task completion result (success or failure).
     */
    public void recordTaskResult(boolean success, String taskType, TaskIdentity identity) {
        try {
            Attributes attrs = buildAttributes(taskType, identity);
            if (success) {
                successCounter.add(1, attrs);
            } else {
                failureCounter.add(1, attrs);
            }
        } catch (Exception e) {
            LOG.log(Level.FINE, "Failed to record task result: " + e.getMessage());
        }
    }
}
