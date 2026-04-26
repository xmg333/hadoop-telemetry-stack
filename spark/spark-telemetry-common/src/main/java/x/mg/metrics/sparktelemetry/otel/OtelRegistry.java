package x.mg.metrics.sparktelemetry.otel;

import io.opentelemetry.api.OpenTelemetry;

/**
 * Manages the OpenTelemetry SDK lifecycle: initialization, flushing, and shutdown.
 */
public interface OtelRegistry {
    void start();
    void stop();
    void forceFlush();
    OpenTelemetry getOpenTelemetry();
}
