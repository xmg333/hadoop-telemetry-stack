package x.mg.metrics.sparktelemetry.otel;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@link MetricExporter} wrapper that splits large metric batches into smaller chunks
 * to avoid exceeding gRPC message size limits.
 *
 * <p>When the total number of data points across all {@link MetricData} exceeds
 * {@code maxDataPointsPerBatch}, the collection is split into multiple sub-collections,
 * each exported separately via the delegate exporter.
 */
public class SplittingMetricExporter implements MetricExporter {

    private static final Logger LOG = Logger.getLogger(SplittingMetricExporter.class.getName());

    private final MetricExporter delegate;
    private final int maxDataPointsPerBatch;

    public SplittingMetricExporter(MetricExporter delegate, int maxDataPointsPerBatch) {
        this.delegate = delegate;
        this.maxDataPointsPerBatch = maxDataPointsPerBatch;
    }

    @Override
    public CompletableResultCode export(Collection<MetricData> metrics) {
        if (metrics.isEmpty()) {
            return CompletableResultCode.ofSuccess();
        }

        int totalPoints = 0;
        for (MetricData md : metrics) {
            totalPoints += md.getData().getPoints().size();
        }

        if (totalPoints <= maxDataPointsPerBatch) {
            return delegate.export(metrics);
        }

        // Split into batches
        List<CompletableResultCode> results = new ArrayList<>();
        List<MetricData> currentBatch = new ArrayList<>();
        int currentPoints = 0;

        for (MetricData md : metrics) {
            int mdPoints = md.getData().getPoints().size();

            // If current batch is non-empty and adding this MetricData would exceed the limit,
            // flush the current batch first
            if (!currentBatch.isEmpty() && currentPoints + mdPoints > maxDataPointsPerBatch) {
                results.add(delegate.export(new ArrayList<>(currentBatch)));
                LOG.log(Level.FINE, "SplittingMetricExporter: flushed batch with {0} data points",
                        currentPoints);
                currentBatch.clear();
                currentPoints = 0;
            }

            currentBatch.add(md);
            currentPoints += mdPoints;
        }

        // Flush remaining
        if (!currentBatch.isEmpty()) {
            results.add(delegate.export(new ArrayList<>(currentBatch)));
            LOG.log(Level.FINE, "SplittingMetricExporter: flushed final batch with {0} data points",
                    currentPoints);
        }

        LOG.log(Level.FINE, "SplittingMetricExporter: split {0} total data points into {1} batches",
                new Object[]{totalPoints, results.size()});

        // Aggregate results
        if (results.isEmpty()) {
            return CompletableResultCode.ofSuccess();
        }

        if (results.size() == 1) {
            return results.get(0);
        }

        final CompletableResultCode aggregate = new CompletableResultCode();
        final int total = results.size();
        final int[] pending = {total};
        final boolean[] failed = {false};

        for (CompletableResultCode result : results) {
            result.whenComplete(() -> {
                synchronized (pending) {
                    if (!result.isSuccess()) {
                        failed[0] = true;
                    }
                    pending[0]--;
                    if (pending[0] == 0) {
                        if (failed[0]) {
                            aggregate.fail();
                        } else {
                            aggregate.succeed();
                        }
                    }
                }
            });
        }

        return aggregate;
    }

    @Override
    public CompletableResultCode flush() {
        return delegate.flush();
    }

    @Override
    public CompletableResultCode shutdown() {
        return delegate.shutdown();
    }

    @Override
    public AggregationTemporality getAggregationTemporality(io.opentelemetry.sdk.metrics.InstrumentType instrumentType) {
        return delegate.getAggregationTemporality(instrumentType);
    }

    @Override
    public Aggregation getDefaultAggregation(io.opentelemetry.sdk.metrics.InstrumentType instrumentType) {
        return delegate.getDefaultAggregation(instrumentType);
    }
}
