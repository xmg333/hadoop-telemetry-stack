package x.mg.metrics.mragent.otel;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import x.mg.metrics.mragent.config.AgentConfig;

import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Initializes and manages the OTel SDK pipeline for the agent.
 * Pipeline: OtlpGrpcMetricExporter -> PeriodicMetricReader -> SdkMeterProvider
 */
public class AgentOtelRegistry {

    private static final Logger LOG = Logger.getLogger(AgentOtelRegistry.class.getName());

    private OpenTelemetrySdk openTelemetrySdk;
    private SdkMeterProvider meterProvider;
    private final AgentConfig config;

    public AgentOtelRegistry(AgentConfig config) {
        this.config = config;
    }

    public synchronized void start() {
        if (meterProvider != null) {
            LOG.warning("AgentOtelRegistry already started, skipping");
            return;
        }

        try {
            OtlpGrpcMetricExporter exporter = OtlpGrpcMetricExporter.builder()
                .setEndpoint(config.getOtelEndpoint())
                .setAggregationTemporalitySelector(AggregationTemporalitySelector.deltaPreferred())
                .build();

            Resource resource = Resource.create(
                Attributes.builder()
                    .put(AttributeKey.stringKey("service.name"), config.getServiceName())
                    .build()
            );

            long intervalMs = config.getExportIntervalMs();
            meterProvider = SdkMeterProvider.builder()
                .setResource(resource)
                .registerMetricReader(
                    PeriodicMetricReader.builder(exporter)
                        .setInterval(Duration.ofMillis(intervalMs))
                        .build()
                )
                .build();

            openTelemetrySdk = OpenTelemetrySdk.builder()
                .setMeterProvider(meterProvider)
                .build();

            LOG.info("Agent OTel SDK initialized: endpoint=" + config.getOtelEndpoint()
                + ", interval=" + intervalMs + "ms"
                + ", service=" + config.getServiceName());
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "OTel SDK init failed, metrics will not be exported: " + e.getMessage());
        }
    }

    public OpenTelemetry getOpenTelemetry() {
        return openTelemetrySdk;
    }

    public synchronized void forceFlush() {
        if (meterProvider != null) {
            try {
                CompletableResultCode result =
                    meterProvider.forceFlush().join(10, java.util.concurrent.TimeUnit.SECONDS);
                if (result.isSuccess()) {
                    LOG.info("Agent OTel SDK forceFlush complete");
                } else {
                    LOG.warning("Agent OTel SDK forceFlush returned failure");
                }
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Error during OTel SDK forceFlush", e);
            }
        }
    }

    public synchronized void stop() {
        if (meterProvider != null) {
            try {
                meterProvider.close();
                LOG.info("Agent OTel SDK shutdown complete");
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
