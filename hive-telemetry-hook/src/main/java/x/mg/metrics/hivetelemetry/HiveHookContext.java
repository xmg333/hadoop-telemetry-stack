package x.mg.metrics.hivetelemetry;

import x.mg.metrics.hivetelemetry.config.HiveHookConfig;
import x.mg.metrics.hivetelemetry.otel.HiveMetricRecorder;
import x.mg.metrics.hivetelemetry.otel.HiveOtelRegistry;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Singleton lifecycle manager for the Hive Telemetry Hook.
 * Initialized lazily on first HiveTelemetryHook.run() call.
 * Thread-safe: HiveServer2 processes concurrent queries on different threads.
 */
public class HiveHookContext {
    private static final Logger LOG = Logger.getLogger(HiveHookContext.class.getName());
    private static final Object LOCK = new Object();
    private static volatile HiveHookContext instance;

    private final HiveHookConfig config;
    private final HiveOtelRegistry otelRegistry;
    private final HiveMetricRecorder metricRecorder;
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    private HiveHookContext() {
        this.config = new HiveHookConfig();
        this.otelRegistry = new HiveOtelRegistry(config);
        this.otelRegistry.start();
        this.metricRecorder = new HiveMetricRecorder(otelRegistry.getOpenTelemetry(), config);
        this.initialized.set(true);
        LOG.info("HiveHookContext initialized");
    }

    public static HiveHookContext getOrInit() {
        if (instance == null) {
            synchronized (LOCK) {
                if (instance == null) {
                    instance = new HiveHookContext();
                }
            }
        }
        return instance;
    }

    public HiveHookConfig getConfig() { return config; }
    public HiveMetricRecorder getMetricRecorder() { return metricRecorder; }
    public HiveOtelRegistry getOtelRegistry() { return otelRegistry; }

    static void reset() {
        synchronized (LOCK) {
            if (instance != null) {
                instance.shutdown();
                instance = null;
            }
        }
    }

    public void shutdown() {
        otelRegistry.forceFlush();
        otelRegistry.stop();
        initialized.set(false);
    }
}
