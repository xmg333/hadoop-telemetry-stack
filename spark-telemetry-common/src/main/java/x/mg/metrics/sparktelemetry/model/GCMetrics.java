package x.mg.metrics.sparktelemetry.model;

import java.util.HashMap;
import java.util.Map;

/**
 * GC metrics captured from Dropwizard MetricRegistry.
 * Tracks count and time per GC collector name.
 */
public class GCMetrics {
    private final Map<String, GCCollectorStats> collectors = new HashMap<>();

    public void addCollector(String name, long count, long timeMs) {
        collectors.put(name, new GCCollectorStats(count, timeMs));
    }

    public Map<String, GCCollectorStats> getCollectors() {
        return collectors;
    }

    public long getTotalGcCount() {
        return collectors.values().stream().mapToLong(c -> c.count).sum();
    }

    public long getTotalGcTimeMs() {
        return collectors.values().stream().mapToLong(c -> c.timeMs).sum();
    }

    public static class GCCollectorStats {
        private final long count;
        private final long timeMs;

        public GCCollectorStats(long count, long timeMs) {
            this.count = count;
            this.timeMs = timeMs;
        }

        public long getCount() { return count; }
        public long getTimeMs() { return timeMs; }
    }
}
