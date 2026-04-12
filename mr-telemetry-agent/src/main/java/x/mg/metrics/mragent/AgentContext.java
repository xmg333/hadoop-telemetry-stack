package x.mg.metrics.mragent;

import x.mg.metrics.mragent.config.AgentConfig;
import x.mg.metrics.mragent.counter.CounterReader;
import x.mg.metrics.mragent.counter.TaskIdentity;
import x.mg.metrics.mragent.counter.TaskSampler;
import x.mg.metrics.mragent.otel.AgentMetricRecorder;
import x.mg.metrics.mragent.otel.AgentOtelRegistry;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Singleton lifecycle manager for the MR Telemetry Agent.
 * Initialized lazily on first advice trigger (first Mapper/Reducer.run() call).
 *
 * Thread-safe: MR typically runs one mapper/reducer per JVM container,
 * but defensive synchronization is applied.
 */
public class AgentContext {

    private static final Logger LOG = Logger.getLogger(AgentContext.class.getName());
    private static final Object LOCK = new Object();
    private static volatile AgentContext instance;

    private final AgentConfig config;
    private final AgentOtelRegistry otelRegistry;
    private final AgentMetricRecorder metricRecorder;
    private final CounterReader counterReader;
    private volatile TaskSampler currentSampler;
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    private AgentContext() {
        this.config = new AgentConfig();
        this.counterReader = new CounterReader();
        this.otelRegistry = new AgentOtelRegistry(config);
        this.otelRegistry.start();
        this.metricRecorder = new AgentMetricRecorder(otelRegistry.getOpenTelemetry());
        this.initialized.set(true);
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "mr-agent-shutdown"));
        LOG.info("AgentContext initialized");
    }

    public static AgentContext getOrInit() {
        if (instance == null) {
            synchronized (LOCK) {
                if (instance == null) {
                    instance = new AgentContext();
                }
            }
        }
        return instance;
    }

    public void onRunEnter(Object context, String methodSignature) {
        String taskType = detectTaskType(methodSignature);
        TaskIdentity identity = counterReader.extractTaskIdentity(context);

        LOG.info("Task run enter: type=" + taskType
            + ", taskId=" + identity.getTaskId() + ", contextClass="
            + (context != null ? context.getClass().getName() : "null"));

        currentSampler = new TaskSampler(
            context, taskType, identity, counterReader, metricRecorder, config);
        currentSampler.start();
    }

    public void onRunExit(Object context, String methodSignature) {
        LOG.info("Task run exit: signature=" + methodSignature);

        if (currentSampler != null) {
            currentSampler.stop();
            currentSampler = null;
        }
        // Force flush metrics immediately — YARN containers may be SIGKILL'd
        // before shutdown hooks run, so we can't rely on shutdown hook alone.
        otelRegistry.forceFlush();
    }

    String detectTaskType(String methodSignature) {
        if (methodSignature == null) return "unknown";
        String sig = methodSignature.toLowerCase();
        if (sig.contains("mapper")) return "map";
        if (sig.contains("reducer")) return "reduce";
        return "unknown";
    }

    static void reset() {
        synchronized (LOCK) {
            if (instance != null) {
                instance.shutdown();
                instance = null;
            }
        }
    }

    public void shutdown() {
        if (currentSampler != null) {
            currentSampler.stop();
            currentSampler = null;
        }
        otelRegistry.stop();
        initialized.set(false);
    }

    // For testing
    AgentOtelRegistry getOtelRegistry() { return otelRegistry; }
    AgentMetricRecorder getMetricRecorder() { return metricRecorder; }
    CounterReader getCounterReader() { return counterReader; }
}
