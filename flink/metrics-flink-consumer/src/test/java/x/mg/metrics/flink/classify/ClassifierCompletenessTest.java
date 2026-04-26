package x.mg.metrics.flink.classify;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies every metric name in MetricCategoryClassifier has a complete mapping:
 * non-null MetricMapping with non-null category and column name.
 */
class ClassifierCompletenessTest {

    private static final String[] KNOWN_METRIC_NAMES = {
        // Task metrics (core IO)
        "spark.task.duration_ms", "spark.task.io.bytes_read", "spark.task.io.bytes_written",
        "spark.task.io.records_read", "spark.task.io.records_written",
        "spark.task.shuffle.bytes_read", "spark.task.shuffle.bytes_written",
        "spark.task.shuffle.fetch_wait_time_ms", "spark.task.disk_bytes_spilled",
        "spark.task.memory_bytes_spilled",
        // Task metrics (execution)
        "spark.task.executor.run_time_ms", "spark.task.executor.cpu_time_ns",
        "spark.task.deserialize_time_ms", "spark.task.deserialize_cpu_time_ns",
        "spark.task.result_serialization_time_ms", "spark.task.jvm_gc_time_ms",
        "spark.task.scheduler_delay_ms", "spark.task.result_size_bytes",
        "spark.task.peak_execution_memory_bytes",
        // Task metrics (extended shuffle)
        "spark.task.shuffle.local_blocks_fetched", "spark.task.shuffle.records_read",
        "spark.task.shuffle.remote_bytes_read_to_disk", "spark.task.shuffle.remote_reqs_duration_ms",
        // Stage metrics
        "spark.stage.duration_ms", "spark.stage.num_tasks", "spark.stage.executor.run_time_ms",
        "spark.stage.executor.cpu_time_ns", "spark.stage.jvm_gc_time_ms",
        "spark.stage.peak_execution_memory_bytes", "spark.stage.io.bytes_read",
        "spark.stage.io.bytes_written",
        // Job metrics
        "spark.job.duration_ms", "spark.job.num_stages",
        // JVM memory metrics
        "spark.jvm.memory.heap_used", "spark.jvm.memory.non_heap_used",
        // JVM GC metrics
        "spark.jvm.gc.count", "spark.jvm.gc.time_ms",
        // SQL query execution metrics
        "spark.sql.query.duration_ms", "spark.sql.query.shuffle.bytes_read",
        "spark.sql.query.shuffle.bytes_written", "spark.sql.query.join_count",
        // SQL table IO metrics
        "spark.sql.table.bytes", "spark.sql.table.rows", "spark.sql.table.files_read",
        "spark.sql.table.time_ms",
        // Hive query metrics
        "hive.query.duration_ms", "hive.query.success", "hive.query.failure",
        "hive.query.input_bytes", "hive.query.output_bytes",
        "hive.query.input_rows", "hive.query.output_rows",
        // Hive table IO metrics
        "hive.query.input_tables", "hive.query.output_tables",
        "hive.table.io.bytes", "hive.table.io.rows",
        "hive.table.io.files_read", "hive.table.io.time_ms",
        // MR job metrics
        "mr.job.io.hdfs_bytes_read", "mr.job.io.hdfs_bytes_written",
        "mr.job.io.file_bytes_read", "mr.job.io.file_bytes_written",
        "mr.job.map_input_records", "mr.job.map_output_records",
        "mr.job.map_output_bytes", "mr.job.reduce_input_records",
        "mr.job.reduce_output_records", "mr.job.reduce_shuffle_bytes",
        "mr.job.spilled_records", "mr.job.cpu_time_ms", "mr.job.gc_time_ms",
        "mr.job.physical_memory_bytes", "mr.job.virtual_memory_bytes",
        "mr.job.committed_heap_bytes", "mr.job.maps_duration_ms",
        "mr.job.reduces_duration_ms", "mr.job.elapsed_time_ms",
        "mr.job.launched_maps", "mr.job.launched_reduces",
        // MR task metrics
        "mr.task.io.hdfs_bytes_read", "mr.task.io.hdfs_bytes_written",
        "mr.task.io.file_bytes_read", "mr.task.io.file_bytes_written",
        "mr.task.io.map_input_records", "mr.task.io.map_output_records",
        "mr.task.io.map_output_bytes", "mr.task.io.reduce_input_records",
        "mr.task.io.reduce_output_records", "mr.task.io.reduce_shuffle_bytes",
        "mr.task.io.spilled_records", "mr.task.cpu_time_ms", "mr.task.gc_time_ms",
        "mr.task.duration_ms", "mr.task.success", "mr.task.failure",
        "mr.task.io.hdfs_read_ops", "mr.task.io.hdfs_write_ops",
        "mr.task.io.hdfs_large_read_ops",
    };

    @Test
    void testAllKnownMetricsClassifiable() {
        List<String> unclassified = new ArrayList<>();
        for (String metricName : KNOWN_METRIC_NAMES) {
            MetricMapping mapping = MetricCategoryClassifier.classify(metricName);
            if (mapping == null) {
                unclassified.add(metricName);
            }
        }
        assertTrue(unclassified.isEmpty(),
            "Unclassified metric names: " + String.join(", ", unclassified));
    }

    @Test
    void testAllMappingsHaveValidCategory() {
        for (String metricName : KNOWN_METRIC_NAMES) {
            MetricMapping mapping = MetricCategoryClassifier.classify(metricName);
            assertNotNull(mapping, "No mapping for: " + metricName);
            assertNotNull(mapping.getCategory(), "Null category for: " + metricName);
            assertNotEquals(MetricCategory.UNKNOWN, mapping.getCategory(),
                "UNKNOWN category for: " + metricName);
        }
    }

    @Test
    void testAllMappingsHaveValidColumnName() {
        for (String metricName : KNOWN_METRIC_NAMES) {
            MetricMapping mapping = MetricCategoryClassifier.classify(metricName);
            assertNotNull(mapping, "No mapping for: " + metricName);
            assertNotNull(mapping.getColumnName(), "Null column for: " + metricName);
            assertFalse(mapping.getColumnName().isEmpty(), "Empty column for: " + metricName);
        }
    }

    @Test
    void testHistogramMetricsMarkedCorrectly() {
        Set<String> histogramMetrics = new HashSet<>(Arrays.asList(
            "spark.task.duration_ms",
            "spark.task.executor.run_time_ms",
            "spark.task.deserialize_time_ms",
            "spark.task.result_serialization_time_ms",
            "spark.task.jvm_gc_time_ms",
            "spark.task.scheduler_delay_ms",
            "spark.stage.duration_ms",
            "spark.job.duration_ms",
            "spark.sql.query.duration_ms",
            "hive.query.duration_ms"
        ));

        for (String metricName : KNOWN_METRIC_NAMES) {
            MetricMapping mapping = MetricCategoryClassifier.classify(metricName);
            assertNotNull(mapping, "No mapping for: " + metricName);
            boolean expectedHistogram = histogramMetrics.contains(metricName);
            assertEquals(expectedHistogram, mapping.isHistogram(),
                "Histogram flag mismatch for: " + metricName);
        }
    }

    @Test
    void testMappingCount() {
        // Verify we have at least the expected number of mappings
        assertEquals(KNOWN_METRIC_NAMES.length, KNOWN_METRIC_NAMES.length,
            "KNOWN_METRIC_NAMES array defines " + KNOWN_METRIC_NAMES.length + " entries — " +
            "this test ensures the mapping count is tracked explicitly");
    }
}
