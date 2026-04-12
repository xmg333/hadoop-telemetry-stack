package x.mg.metrics.mragent.counter;

import x.mg.metrics.mragent.config.AgentConfig;
import x.mg.metrics.mragent.otel.AgentMetricRecorder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Samples MR task counters at a configurable interval.
 * Started on Mapper.run()/Reducer.run() enter, stopped on exit.
 * Computes deltas from the previous sample and reports via OTel.
 */
public class TaskSampler {

    private static final Logger LOG = Logger.getLogger(TaskSampler.class.getName());

    private final Object context;
    private final String taskType;
    private final TaskIdentity identity;
    private final CounterReader counterReader;
    private final AgentMetricRecorder recorder;
    private final int samplingIntervalSecs;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Map<String, Long> previousCounters = new HashMap<>();

    public TaskSampler(Object context, String taskType, TaskIdentity identity,
                       CounterReader counterReader, AgentMetricRecorder recorder,
                       AgentConfig config) {
        this.context = context;
        this.taskType = taskType;
        this.identity = identity;
        this.counterReader = counterReader;
        this.recorder = recorder;
        this.samplingIntervalSecs = config.getSamplingIntervalSecs();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "mr-task-sampler");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            // Initial sample
            sample();
            scheduler.scheduleAtFixedRate(this::sample,
                samplingIntervalSecs, samplingIntervalSecs, TimeUnit.SECONDS);
            LOG.info("TaskSampler started: taskType=" + taskType
                + ", taskId=" + identity.getTaskId()
                + ", interval=" + samplingIntervalSecs + "s");
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            // Final sample before shutdown
            sample();
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            LOG.info("TaskSampler stopped: taskType=" + taskType
                + ", taskId=" + identity.getTaskId());
        }
    }

    private void sample() {
        try {
            Map<String, Long> current = counterReader.readCounters(context);
            if (current.isEmpty()) {
                System.err.println("[mr-telemetry-agent] Sample returned empty counters for type=" + taskType);
                return;
            }

            Map<String, Long> deltas = computeDeltas(previousCounters, current);
            if (!deltas.isEmpty()) {
                System.err.println("[mr-telemetry-agent] Recording deltas: " + deltas.size()
                    + " counters, taskType=" + taskType + ", taskId=" + identity.getTaskId()
                    + " deltas=" + deltas);
                recorder.recordDeltas(deltas, taskType, identity);
            } else {
                System.err.println("[mr-telemetry-agent] No deltas from " + current.size() + " counters");
            }

            previousCounters.clear();
            previousCounters.putAll(current);
        } catch (Exception e) {
            System.err.println("[mr-telemetry-agent] Counter sampling failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    static Map<String, Long> computeDeltas(Map<String, Long> prev, Map<String, Long> curr) {
        Map<String, Long> deltas = new HashMap<>();
        for (Map.Entry<String, Long> entry : curr.entrySet()) {
            Long previous = prev.get(entry.getKey());
            long delta = previous != null ? entry.getValue() - previous : entry.getValue();
            if (delta > 0) {
                deltas.put(entry.getKey(), delta);
            }
        }
        return deltas;
    }
}
