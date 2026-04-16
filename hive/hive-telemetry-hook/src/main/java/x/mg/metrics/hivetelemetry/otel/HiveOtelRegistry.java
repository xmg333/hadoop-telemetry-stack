package x.mg.metrics.hivetelemetry.otel;

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
import x.mg.metrics.hivetelemetry.config.HiveHookConfig;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HiveOtelRegistry {
    private static final Logger LOG = Logger.getLogger(HiveOtelRegistry.class.getName());

    private OpenTelemetrySdk openTelemetrySdk;
    private SdkMeterProvider meterProvider;
    private final HiveHookConfig config;

    public HiveOtelRegistry(HiveHookConfig config) {
        this.config = config;
    }

    public synchronized void start() {
        if (meterProvider != null) {
            LOG.warning("HiveOtelRegistry already started, skipping");
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

            LOG.info("Hive OTel SDK initialized: endpoint=" + config.getOtelEndpoint()
                + ", interval=" + intervalMs + "ms"
                + ", service=" + config.getServiceName());
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Hive OTel SDK init failed: " + e.getMessage());
        }
    }

    public OpenTelemetry getOpenTelemetry() {
        return openTelemetrySdk;
    }

    public synchronized void forceFlush() {
        if (meterProvider != null) {
            try {
                CompletableResultCode result =
                    meterProvider.forceFlush().join(10, TimeUnit.SECONDS);
                if (!result.isSuccess()) {
                    LOG.warning("Hive OTel SDK forceFlush returned failure");
                }
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Error during Hive OTel SDK forceFlush", e);
            }
        }
    }

    public synchronized void stop() {
        if (meterProvider != null) {
            try {
                meterProvider.close();
                LOG.info("Hive OTel SDK shutdown complete");
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Error during Hive OTel SDK shutdown", e);
            }
            meterProvider = null;
        }
        if (openTelemetrySdk != null) {
            openTelemetrySdk.close();
            openTelemetrySdk = null;
        }
    }
}
