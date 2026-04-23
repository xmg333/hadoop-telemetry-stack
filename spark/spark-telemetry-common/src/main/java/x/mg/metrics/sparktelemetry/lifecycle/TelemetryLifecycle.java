package x.mg.metrics.sparktelemetry.lifecycle;

import io.opentelemetry.api.OpenTelemetry;
import x.mg.metrics.sparktelemetry.config.TelemetryConfig;
import x.mg.metrics.sparktelemetry.model.SparkMetricEvent;
import x.mg.metrics.sparktelemetry.otel.MetricRecorder;
import x.mg.metrics.sparktelemetry.otel.OtelRegistry;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Central lifecycle manager for the telemetry system.
 * Singleton per JVM (driver or executor).
 */
public class TelemetryLifecycle {

    private static final Logger LOG = Logger.getLogger(TelemetryLifecycle.class.getName());
    private static final Object LOCK = new Object();
    private static volatile TelemetryLifecycle instance;

    private final TelemetryConfig config;
    private final OtelRegistry otelRegistry;
    private final MetricRecorder metricRecorder;
    private final String appName;
    private final String user;
    private final String queue;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "telemetry-async-flush");
        t.setDaemon(true);
        return t;
    });

    // Bounded LRU cache for SQL text: executionId -> SQL text
    // SparkTelemetryListener.onOtherEvent puts, SparkTelemetryQueryExecutionListener gets and removes
    private final LinkedHashMap<Long, String> sqlTextCache = new LinkedHashMap<Long, String>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Long, String> eldest) {
            return size() > 1000;
        }
    };

    private TelemetryLifecycle(TelemetryConfig config, Map<String, String> sparkConfOverrides) {
        this.config = config;
        this.appName = sparkConfOverrides != null ? sparkConfOverrides.getOrDefault("spark.app.name", "") : "";
        String userVal = (sparkConfOverrides != null) ? sparkConfOverrides.getOrDefault("spark.user", "") : "";
        if (userVal.isEmpty()) userVal = System.getProperty("user.name", "");
        this.user = userVal;
        this.queue = (sparkConfOverrides != null) ? sparkConfOverrides.getOrDefault("spark.yarn.queue", "") : "";
        this.otelRegistry = new OtelRegistry(config);
        this.otelRegistry.start();
        this.metricRecorder = new MetricRecorder(otelRegistry.getOpenTelemetry(), config);
    }

    /**
     * Initialize the singleton instance with Spark conf overrides.
     */
    public static TelemetryLifecycle init(Map<String, String> sparkConfOverrides) {
        if (instance != null && instance.started.get()) {
            LOG.fine("TelemetryLifecycle already initialized");
            return instance;
        }
        synchronized (LOCK) {
            if (instance == null || !instance.started.get()) {
                TelemetryConfig config = new TelemetryConfig(sparkConfOverrides);
                instance = new TelemetryLifecycle(config, sparkConfOverrides);
                instance.started.set(true);
                LOG.info("TelemetryLifecycle initialized");
            }
        }
        return instance;
    }

    /**
     * Get the singleton instance. Must call init() first.
     */
    public static TelemetryLifecycle getInstance() {
        if (instance == null) {
            throw new IllegalStateException("TelemetryLifecycle not initialized. Call init() first.");
        }
        return instance;
    }

    /**
     * Check if initialized.
     */
    public static boolean isInitialized() {
        return instance != null && instance.started.get();
    }

    /**
     * Accept a metric event for recording.
     */
    public void accept(SparkMetricEvent event) {
        if (event == null || stopped.get()) return;
        try {
            metricRecorder.record(event);
        } catch (Exception e) {
            LOG.log(Level.FINE, "Failed to record metric event: " + e.getMessage(), e);
        }
    }

    /**
     * Get the current configuration.
     */
    public TelemetryConfig getConfig() {
        return config;
    }

    /**
     * Store SQL text for a Spark SQL execution.
     * Called by SparkTelemetryListener.onOtherEvent when SparkListenerSQLExecutionStart fires.
     */
    public void putSqlText(long executionId, String sqlText) {
        synchronized (sqlTextCache) {
            sqlTextCache.put(executionId, sqlText);
        }
    }

    /**
     * Retrieve and remove SQL text for a Spark SQL execution.
     * Called by SparkTelemetryQueryExecutionListener.extractMetrics.
     */
    public String getAndRemoveSqlText(long executionId) {
        synchronized (sqlTextCache) {
            return sqlTextCache.remove(executionId);
        }
    }

    public String getAppName() {
        return appName;
    }

    public String getUser() {
        return user;
    }

    public String getQueue() {
        return queue;
    }

    /**
     * Force flush pending metrics to the OTel exporter (blocking).
     * Used during shutdown to ensure all metrics are exported.
     */
    public void flush() {
        if (!stopped.get()) {
            otelRegistry.forceFlush();
        }
    }

    /**
     * Async flush — non-blocking version for use in SparkListener callbacks.
     * Avoids blocking the Spark DAGScheduler thread which causes job timeouts.
     */
    public void flushAsync() {
        if (!stopped.get()) {
            flushExecutor.submit(() -> {
                try {
                    otelRegistry.forceFlush();
                } catch (Exception e) {
                    LOG.log(Level.FINE, "Async flush failed: " + e.getMessage(), e);
                }
            });
        }
    }

    /**
     * Shutdown the telemetry system gracefully.
     */
    public void stop() {
        if (stopped.compareAndSet(false, true)) {
            LOG.info("Shutting down TelemetryLifecycle");
            // Drain pending async flush tasks before final synchronous flush
            flushExecutor.shutdown();
            try {
                if (!flushExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOG.warning("Async flush executor did not terminate in 5s, forcing shutdown");
                    flushExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                flushExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            otelRegistry.stop();
            started.set(false);
        }
    }

    /**
     * Reset the singleton for testing purposes.
     */
    public static void reset() {
        synchronized (LOCK) {
            if (instance != null) {
                instance.stop();
                instance = null;
            }
        }
    }
}
