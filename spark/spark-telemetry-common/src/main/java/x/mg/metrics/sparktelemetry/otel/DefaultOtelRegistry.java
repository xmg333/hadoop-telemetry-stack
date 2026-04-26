package x.mg.metrics.sparktelemetry.otel;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;
import io.opentelemetry.sdk.resources.Resource;
import x.mg.metrics.sparktelemetry.config.TelemetryConfig;

import io.opentelemetry.sdk.common.CompletableResultCode;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Initializes and manages the OpenTelemetry SDK pipeline:
 * MeterProvider → PeriodicMetricReader → OTLP (gRPC/HTTP) → OTel Collector
 */
public class DefaultOtelRegistry implements OtelRegistry {

    private static final Logger LOG = Logger.getLogger(DefaultOtelRegistry.class.getName());

    private OpenTelemetrySdk openTelemetrySdk;
    private SdkMeterProvider meterProvider;
    private TelemetryConfig config;

    public DefaultOtelRegistry(TelemetryConfig config) {
        this.config = config;
    }

    /**
     * Initialize the OTel SDK pipeline.
     */
    @Override
    public synchronized void start() {
        if (meterProvider != null) {
            LOG.warning("OtelRegistry already started, skipping");
            return;
        }

        try {
            // Build OTLP exporter
            OtlpGrpcMetricExporterBuilder exporterBuilder = OtlpGrpcMetricExporter.builder()
                    .setEndpoint(config.getOtelEndpoint())
                    .setAggregationTemporalitySelector(AggregationTemporalitySelector.deltaPreferred());

            // Build resource
            Resource resource = buildResource();

            // Build meter provider with periodic reader
            long intervalMs = config.getExportIntervalMs();
            meterProvider = SdkMeterProvider.builder()
                    .setResource(resource)
                    .registerMetricReader(
                            PeriodicMetricReader.builder(
                                    exporterBuilder.build()
                            ).setInterval(Duration.ofMillis(intervalMs)).build()
                    )
                    .build();

            openTelemetrySdk = OpenTelemetrySdk.builder()
                    .setMeterProvider(meterProvider)
                    .build();

            LOG.info("OTel SDK initialized: endpoint=" + config.getOtelEndpoint()
                    + ", interval=" + intervalMs + "ms"
                    + ", service=" + config.getServiceName());
        } catch (Exception e) {
            LOG.log(Level.WARNING, "OTel Collector connection failed, metrics will not be exported: " + e.getMessage(), e);
            // Use noop SDK so downstream code (MetricRecorder etc.) never sees null
            openTelemetrySdk = OpenTelemetrySdk.builder().build();
        }

        suppressOtelSdkErrorLogs();
    }

    private Resource buildResource() {
        AttributesBuilder attrBuilder = Attributes.builder()
                .put(AttributeKey.stringKey("service.name"), config.getServiceName());

        // Add any custom resource attributes from config
        try {
            com.typesafe.config.Config rawConfig = config.getRawConfig();
            if (rawConfig.hasPath("spark-telemetry.otel.resource.attributes")) {
                com.typesafe.config.Config attrs = rawConfig.getConfig("spark-telemetry.otel.resource.attributes");
                for (Map.Entry<String, com.typesafe.config.ConfigValue> entry : attrs.entrySet()) {
                    attrBuilder.put(entry.getKey(), entry.getValue().unwrapped().toString());
                }
            }
        } catch (Exception e) {
            LOG.log(Level.FINE, "No custom resource attributes found", e);
        }

        return Resource.create(attrBuilder.build());
    }

    /**
     * Get the OpenTelemetry instance for creating meters.
     */
    @Override
    public OpenTelemetry getOpenTelemetry() {
        return openTelemetrySdk;
    }

    /**
     * Downgrade OTel SDK internal ERROR logs (gRPC export failures etc.) to WARNING.
     * When the collector is unreachable, the SDK logs at ERROR which causes operational alarm,
     * but this is a telemetry concern — it should never look like a user application error.
     */
    private void suppressOtelSdkErrorLogs() {
        try {
            // The shade plugin relocates io.opentelemetry → x.mg.metrics.shaded.io.opentelemetry
            // so all OTel SDK loggers live under this prefix.
            Logger otelRoot = Logger.getLogger("x.mg.metrics.shaded.io.opentelemetry");
            if (otelRoot != null) {
                otelRoot.setLevel(Level.WARNING);
            }
        } catch (Exception e) {
            // Best effort — don't let log config break anything
        }
    }

    /**
     * Force flush all pending metrics to the exporter.
     * Should be called on job completion or before shutdown to ensure
     * short-lived tasks don't lose metrics between PeriodicMetricReader intervals.
     */
    @Override
    public synchronized void forceFlush() {
        if (meterProvider != null) {
            try {
                CompletableResultCode result =
                    meterProvider.forceFlush().join(10, TimeUnit.SECONDS);
                if (result.isSuccess()) {
                    LOG.info("OTel SDK forceFlush complete");
                } else {
                    LOG.warning("OTel SDK forceFlush returned failure");
                }
            } catch (Exception e) {
                // Stream reset during shutdown is expected when Spark tears down connections
                LOG.log(Level.FINE, "Error during OTel SDK forceFlush", e);
            }
        }
    }

    /**
     * Shutdown the OTel SDK pipeline gracefully.
     * Flushes pending metrics before closing.
     */
    @Override
    public synchronized void stop() {
        if (openTelemetrySdk != null) {
            try {
                // Flush pending metrics before shutdown to avoid losing data from short tasks
                if (meterProvider != null) {
                    forceFlush();
                }
                // close() on OpenTelemetrySdk closes all components including SdkMeterProvider.
                // Only call this once to avoid "Multiple close calls" warning.
                openTelemetrySdk.close();
                LOG.info("OTel SDK shutdown complete");
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Error during OTel SDK shutdown", e);
            }
            openTelemetrySdk = null;
            meterProvider = null;
        }
    }
}
