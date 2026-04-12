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
public class OtelRegistry {

    private static final Logger LOG = Logger.getLogger(OtelRegistry.class.getName());

    private OpenTelemetrySdk openTelemetrySdk;
    private SdkMeterProvider meterProvider;
    private TelemetryConfig config;

    public OtelRegistry(TelemetryConfig config) {
        this.config = config;
    }

    /**
     * Initialize the OTel SDK pipeline.
     */
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
            LOG.log(Level.SEVERE, "Failed to initialize OTel SDK", e);
            throw new RuntimeException("OTel SDK initialization failed", e);
        }
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
    public OpenTelemetry getOpenTelemetry() {
        return openTelemetrySdk;
    }

    /**
     * Force flush all pending metrics to the exporter.
     * Should be called on job completion or before shutdown to ensure
     * short-lived tasks don't lose metrics between PeriodicMetricReader intervals.
     */
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
                LOG.log(Level.WARNING, "Error during OTel SDK forceFlush", e);
            }
        }
    }

    /**
     * Shutdown the OTel SDK pipeline gracefully.
     * Flushes pending metrics before closing.
     */
    public synchronized void stop() {
        if (meterProvider != null) {
            try {
                // Flush pending metrics before shutdown to avoid losing data from short tasks
                forceFlush();
                meterProvider.close();
                LOG.info("OTel SDK shutdown complete");
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Error during OTel SDK shutdown", e);
            }
            meterProvider = null;
        }
        if (openTelemetrySdk != null) {
            openTelemetrySdk.close();
            openTelemetrySdk = null;
        }
    }
}
