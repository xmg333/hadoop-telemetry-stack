package x.mg.metrics.mrtelemetry;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import x.mg.metrics.mrtelemetry.config.MRCollectorConfig;
import x.mg.metrics.mrtelemetry.otel.MRMetricRecorder;
import x.mg.metrics.mrtelemetry.poller.CounterExtractor;
import x.mg.metrics.mrtelemetry.poller.HistoryServerClient;
import x.mg.metrics.mrtelemetry.poller.JobPoller;

import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main entry point for the MR Telemetry Collector.
 * Standalone Java application that polls MR History Server and exports metrics via OTel.
 */
public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        String configPath = args.length > 0 ? args[0] : null;

        try {
            // Load config
            MRCollectorConfig config = new MRCollectorConfig(configPath);
            LOG.info("MR Telemetry Collector starting...");
            LOG.info("History Server: " + config.getHistoryServerUrl());
            LOG.info("OTel Endpoint: " + config.getOtelEndpoint());

            // Initialize OTel SDK
            OtlpGrpcMetricExporterBuilder exporterBuilder = OtlpGrpcMetricExporter.builder()
                    .setEndpoint(config.getOtelEndpoint())
                    .setCompression("gzip")
                    .setAggregationTemporalitySelector(AggregationTemporalitySelector.deltaPreferred());

            Resource resource = Resource.create(
                    Attributes.builder()
                            .put(AttributeKey.stringKey("service.name"), config.getServiceName())
                            .build()
            );

            SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                    .setResource(resource)
                    .registerMetricReader(
                            PeriodicMetricReader.builder(exporterBuilder.build())
                                    .setInterval(Duration.ofMillis(config.getExportIntervalMs()))
                                    .build()
                    )
                    .build();

            OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                    .setMeterProvider(meterProvider)
                    .build();

            // Create components
            HistoryServerClient client = new HistoryServerClient(config);
            CounterExtractor extractor = new CounterExtractor();
            MRMetricRecorder recorder = new MRMetricRecorder(sdk);
            JobPoller poller = new JobPoller(config, client, extractor, recorder);

            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Shutting down...");
                poller.stop();
                meterProvider.close();
                sdk.close();
            }));

            // Start polling
            poller.start();

            // Keep main thread alive
            Thread.currentThread().join();

        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Fatal error", e);
            System.exit(1);
        }
    }
}
