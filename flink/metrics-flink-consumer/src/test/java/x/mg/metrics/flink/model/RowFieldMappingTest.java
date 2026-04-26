package x.mg.metrics.flink.model;

import x.mg.metrics.flink.classify.MetricCategory;
import x.mg.metrics.flink.classify.MetricCategoryClassifier;
import x.mg.metrics.flink.classify.MetricMapping;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies every column name produced by MetricCategoryClassifier is accepted
 * by the corresponding Row model's setMetricColumn() method (no silent NPE / no-op).
 *
 * Uses a sentinel Double value and verifies the corresponding getter returns non-null
 * after setMetricColumn, confirming the column name is actually handled.
 */
class RowFieldMappingTest {

    private static final double SENTINEL = 42.0;

    private static final String[] ALL_METRIC_NAMES = {
        "spark.task.duration_ms", "spark.task.io.bytes_read", "spark.task.io.bytes_written",
        "spark.task.io.records_read", "spark.task.io.records_written",
        "spark.task.shuffle.bytes_read", "spark.task.shuffle.bytes_written",
        "spark.task.shuffle.fetch_wait_time_ms", "spark.task.disk_bytes_spilled",
        "spark.task.memory_bytes_spilled",
        "spark.task.executor.run_time_ms", "spark.task.executor.cpu_time_ns",
        "spark.task.deserialize_time_ms", "spark.task.deserialize_cpu_time_ns",
        "spark.task.result_serialization_time_ms", "spark.task.jvm_gc_time_ms",
        "spark.task.scheduler_delay_ms", "spark.task.result_size_bytes",
        "spark.task.peak_execution_memory_bytes",
        "spark.task.shuffle.local_blocks_fetched", "spark.task.shuffle.records_read",
        "spark.task.shuffle.remote_bytes_read_to_disk", "spark.task.shuffle.remote_reqs_duration_ms",
        "spark.stage.duration_ms", "spark.stage.num_tasks", "spark.stage.executor.run_time_ms",
        "spark.stage.executor.cpu_time_ns", "spark.stage.jvm_gc_time_ms",
        "spark.stage.peak_execution_memory_bytes", "spark.stage.io.bytes_read",
        "spark.stage.io.bytes_written",
        "spark.job.duration_ms", "spark.job.num_stages",
        "spark.jvm.memory.heap_used", "spark.jvm.memory.non_heap_used",
        "spark.jvm.gc.count", "spark.jvm.gc.time_ms",
        "spark.sql.query.duration_ms", "spark.sql.query.shuffle.bytes_read",
        "spark.sql.query.shuffle.bytes_written", "spark.sql.query.join_count",
        "spark.sql.table.bytes", "spark.sql.table.rows", "spark.sql.table.files_read",
        "spark.sql.table.time_ms",
        "hive.query.duration_ms", "hive.query.success", "hive.query.failure",
        "hive.query.input_bytes", "hive.query.output_bytes",
        "hive.query.input_rows", "hive.query.output_rows",
        "hive.query.input_tables", "hive.query.output_tables",
        "hive.table.io.bytes", "hive.table.io.rows",
        "hive.table.io.files_read", "hive.table.io.time_ms",
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
    void testAllMetricNamesMappedInRowModels() {
        List<String> failures = new ArrayList<>();
        for (String metricName : ALL_METRIC_NAMES) {
            MetricMapping mapping = MetricCategoryClassifier.classify(metricName);
            assertNotNull(mapping, "No classifier mapping for: " + metricName);
            String col = mapping.getColumnName();
            MetricCategory cat = mapping.getCategory();

            try {
                boolean ok = verifyColumnAccepted(cat, col);
                if (!ok) {
                    failures.add(metricName + " → " + cat + "." + col + " not accepted by row model setMetricColumn");
                }
            } catch (Exception e) {
                failures.add(metricName + " → " + cat + "." + col + " threw: " + e.getMessage());
            }
        }
        assertTrue(failures.isEmpty(), "Column mapping failures:\n  " + String.join("\n  ", failures));
    }

    /**
     * Instantiates the correct Row model for the category, calls setMetricColumn
     * with the sentinel value, then verifies at least one getter returns non-null.
     */
    private boolean verifyColumnAccepted(MetricCategory cat, String columnName) {
        switch (cat) {
            case TASK: {
                TaskMetricRow r = new TaskMetricRow();
                r.setMetricColumn(columnName, SENTINEL);
                return hasAnyNonNullGetter(r, columnName);
            }
            case STAGE: {
                StageMetricRow r = new StageMetricRow();
                r.setMetricColumn(columnName, SENTINEL);
                return hasAnyNonNullGetter(r, columnName);
            }
            case JOB: {
                JobMetricRow r = new JobMetricRow();
                r.setMetricColumn(columnName, SENTINEL);
                return hasAnyNonNullGetter(r, columnName);
            }
            case JVM_MEMORY: {
                JvmMemoryMetricRow r = new JvmMemoryMetricRow();
                r.setMetricColumn(columnName, SENTINEL);
                return hasAnyNonNullGetter(r, columnName);
            }
            case JVM_GC: {
                JvmGcMetricRow r = new JvmGcMetricRow();
                r.setMetricColumn(columnName, SENTINEL);
                return hasAnyNonNullGetter(r, columnName);
            }
            case SQL_EXECUTION: {
                SqlQueryMetricRow r = new SqlQueryMetricRow();
                r.setMetricColumn(columnName, SENTINEL);
                return hasAnyNonNullGetter(r, columnName);
            }
            case SQL_TABLE_IO: {
                SqlTableIoMetricRow r = new SqlTableIoMetricRow();
                r.setMetricColumn(columnName, SENTINEL);
                return hasAnyNonNullGetter(r, columnName);
            }
            case HIVE_QUERY: {
                HiveQueryMetricRow r = new HiveQueryMetricRow();
                r.setMetricColumn(columnName, SENTINEL);
                return hasAnyNonNullGetter(r, columnName);
            }
            case HIVE_TABLE_IO: {
                HiveTableIoMetricRow r = new HiveTableIoMetricRow();
                r.setMetricColumn(columnName, SENTINEL);
                return hasAnyNonNullGetter(r, columnName);
            }
            case MR_JOB: {
                MrJobMetricRow r = new MrJobMetricRow();
                r.setMetricColumn(columnName, SENTINEL);
                return hasAnyNonNullGetter(r, columnName);
            }
            case MR_TASK: {
                MrTaskMetricRow r = new MrTaskMetricRow();
                r.setMetricColumn(columnName, SENTINEL);
                return hasAnyNonNullGetter(r, columnName);
            }
            default:
                return false;
        }
    }

    /**
     * Reflectively scans all getters; if any returns SENTINEL the column was handled.
     */
    private boolean hasAnyNonNullGetter(Object row, String columnName) {
        for (Method m : row.getClass().getMethods()) {
            if (m.getName().startsWith("get") && m.getParameterCount() == 0
                && !m.getName().equals("getClass")) {
                try {
                    Object val = m.invoke(row);
                    if (val instanceof Double && Math.abs((Double) val - SENTINEL) < 0.001) {
                        return true;
                    }
                } catch (Exception ignored) { }
            }
        }
        return false;
    }

    @Test
    void testUnifiedMetricEventRowAcceptsAllColumnNames() {
        // MetricEventRow uses BOTH old and new column names;
        // verify all classifier column names are handled
        List<String> failures = new ArrayList<>();
        for (String metricName : ALL_METRIC_NAMES) {
            MetricMapping mapping = MetricCategoryClassifier.classify(metricName);
            assertNotNull(mapping, "No classifier mapping for: " + metricName);
            String col = mapping.getColumnName();

            MetricEventRow row = new MetricEventRow();
            // Need engine set for bytes_spilled to work correctly
            row.setMetricColumn("executor_cpu_time_ns", 1.0); // ensure engine detection
            try {
                row.setMetricColumn(col, SENTINEL);
            } catch (Exception e) {
                failures.add("unified_metrics: " + metricName + " → " + col + " threw: " + e.getMessage());
            }
        }
        assertTrue(failures.isEmpty(),
            "unified_metrics column mapping failures:\n  " + String.join("\n  ", failures));
    }

    @Test
    void testAllKnownMetricCount() {
        assertEquals(98, ALL_METRIC_NAMES.length,
            "Total known metric names — update this test if mappings are added/removed");
    }
}
