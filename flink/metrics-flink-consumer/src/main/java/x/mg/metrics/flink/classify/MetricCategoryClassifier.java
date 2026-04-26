package x.mg.metrics.flink.classify;

import java.util.HashMap;
import java.util.Map;

public class MetricCategoryClassifier {
    private static final Map<String, MetricMapping> MAPPINGS = new HashMap<>();

    static {
        // Task metrics (core IO)
        put("spark.task.duration_ms",                       MetricCategory.TASK, "duration_ms",                       true);
        put("spark.task.io.bytes_read",                     MetricCategory.TASK, "io_bytes_read",                     false);
        put("spark.task.io.bytes_written",                  MetricCategory.TASK, "io_bytes_written",                  false);
        put("spark.task.io.records_read",                   MetricCategory.TASK, "io_records_read",                   false);
        put("spark.task.io.records_written",                MetricCategory.TASK, "io_records_written",                false);
        put("spark.task.shuffle.bytes_read",                MetricCategory.TASK, "shuffle_bytes_read",                false);
        put("spark.task.shuffle.bytes_written",             MetricCategory.TASK, "shuffle_bytes_written",             false);
        put("spark.task.shuffle.fetch_wait_time_ms",        MetricCategory.TASK, "shuffle_fetch_wait_time_ms",        false);
        put("spark.task.disk_bytes_spilled",                MetricCategory.TASK, "disk_bytes_spilled",                false);
        put("spark.task.memory_bytes_spilled",              MetricCategory.TASK, "memory_bytes_spilled",              false);

        // Task metrics (execution)
        put("spark.task.executor.run_time_ms",              MetricCategory.TASK, "executor_run_time_ms",              true);
        put("spark.task.executor.cpu_time_ns",              MetricCategory.TASK, "executor_cpu_time_ns",              false);
        put("spark.task.deserialize_time_ms",               MetricCategory.TASK, "deserialize_time_ms",               true);
        put("spark.task.deserialize_cpu_time_ns",           MetricCategory.TASK, "deserialize_cpu_time_ns",           false);
        put("spark.task.result_serialization_time_ms",      MetricCategory.TASK, "result_serialization_time_ms",      true);
        put("spark.task.jvm_gc_time_ms",                    MetricCategory.TASK, "jvm_gc_time_ms",                    true);
        put("spark.task.scheduler_delay_ms",                MetricCategory.TASK, "scheduler_delay_ms",                true);
        put("spark.task.result_size_bytes",                 MetricCategory.TASK, "result_size_bytes",                 false);
        put("spark.task.peak_execution_memory_bytes",       MetricCategory.TASK, "peak_execution_memory_bytes",       false);

        // Task metrics (extended shuffle)
        put("spark.task.shuffle.local_blocks_fetched",      MetricCategory.TASK, "shuffle_local_blocks_fetched",      false);
        put("spark.task.shuffle.records_read",              MetricCategory.TASK, "shuffle_records_read",              false);
        put("spark.task.shuffle.remote_bytes_read_to_disk", MetricCategory.TASK, "shuffle_remote_bytes_read_to_disk", false);
        put("spark.task.shuffle.remote_reqs_duration_ms",   MetricCategory.TASK, "shuffle_remote_reqs_duration_ms",   false);

        // Stage metrics
        put("spark.stage.duration_ms",                      MetricCategory.STAGE, "duration_ms",                      true);
        put("spark.stage.num_tasks",                        MetricCategory.STAGE, "num_tasks",                        false);
        put("spark.stage.executor.run_time_ms",             MetricCategory.STAGE, "executor_run_time_ms",             false);
        put("spark.stage.executor.cpu_time_ns",             MetricCategory.STAGE, "executor_cpu_time_ns",             false);
        put("spark.stage.jvm_gc_time_ms",                   MetricCategory.STAGE, "jvm_gc_time_ms",                   false);
        put("spark.stage.peak_execution_memory_bytes",      MetricCategory.STAGE, "peak_execution_memory_bytes",      false);
        put("spark.stage.io.bytes_read",                     MetricCategory.STAGE, "io_bytes_read",                     false);
        put("spark.stage.io.bytes_written",                  MetricCategory.STAGE, "io_bytes_written",                  false);

        // Job metrics
        put("spark.job.duration_ms",                        MetricCategory.JOB, "duration_ms",                        true);
        put("spark.job.num_stages",                         MetricCategory.JOB, "num_stages",                          false);

        // JVM memory metrics
        put("spark.jvm.memory.heap_used",                   MetricCategory.JVM_MEMORY, "heap_used",                  false);
        put("spark.jvm.memory.non_heap_used",               MetricCategory.JVM_MEMORY, "non_heap_used",              false);

        // JVM GC metrics
        put("spark.jvm.gc.count",                           MetricCategory.JVM_GC, "gc_count",                       false);
        put("spark.jvm.gc.time_ms",                         MetricCategory.JVM_GC, "gc_time_ms",                     false);

        // SQL query execution metrics
        put("spark.sql.query.duration_ms",                  MetricCategory.SQL_EXECUTION, "duration_ms",            true);
        put("spark.sql.query.shuffle.bytes_read",           MetricCategory.SQL_EXECUTION, "shuffle_bytes_read",     false);
        put("spark.sql.query.shuffle.bytes_written",        MetricCategory.SQL_EXECUTION, "shuffle_bytes_written",  false);
        put("spark.sql.query.join_count",                   MetricCategory.SQL_EXECUTION, "join_count",             false);

        // SQL table IO metrics
        put("spark.sql.table.bytes",                        MetricCategory.SQL_TABLE_IO, "bytes",                   false);
        put("spark.sql.table.rows",                         MetricCategory.SQL_TABLE_IO, "rows",                    false);
        put("spark.sql.table.files_read",                   MetricCategory.SQL_TABLE_IO, "files_read",              false);
        put("spark.sql.table.time_ms",                      MetricCategory.SQL_TABLE_IO, "time_ms",                 false);

        // Hive query metrics
        put("hive.query.duration_ms",                       MetricCategory.HIVE_QUERY, "duration_ms",               true);
        put("hive.query.success",                           MetricCategory.HIVE_QUERY, "success_count",             false);
        put("hive.query.failure",                           MetricCategory.HIVE_QUERY, "failure_count",             false);
        put("hive.query.input_bytes",                       MetricCategory.HIVE_QUERY, "input_bytes",               false);
        put("hive.query.output_bytes",                      MetricCategory.HIVE_QUERY, "output_bytes",              false);
        put("hive.query.input_rows",                        MetricCategory.HIVE_QUERY, "input_rows",                false);
        put("hive.query.output_rows",                       MetricCategory.HIVE_QUERY, "output_rows",               false);

        // Hive table IO metrics
        put("hive.query.input_tables",                      MetricCategory.HIVE_TABLE_IO, "input_table_count",      false);
        put("hive.query.output_tables",                     MetricCategory.HIVE_TABLE_IO, "output_table_count",     false);
        put("hive.table.io.bytes",                          MetricCategory.HIVE_TABLE_IO, "bytes",                  false);
        put("hive.table.io.rows",                           MetricCategory.HIVE_TABLE_IO, "rows",                   false);
        put("hive.table.io.files_read",                     MetricCategory.HIVE_TABLE_IO, "files_read",             false);
        put("hive.table.io.time_ms",                        MetricCategory.HIVE_TABLE_IO, "time_ms",                false);

        // MR job metrics (IO)
        put("mr.job.io.hdfs_bytes_read",                    MetricCategory.MR_JOB, "hdfs_bytes_read",              false);
        put("mr.job.io.hdfs_bytes_written",                 MetricCategory.MR_JOB, "hdfs_bytes_written",            false);
        put("mr.job.io.file_bytes_read",                    MetricCategory.MR_JOB, "file_bytes_read",               false);
        put("mr.job.io.file_bytes_written",                 MetricCategory.MR_JOB, "file_bytes_written",            false);

        // MR job metrics (processing)
        put("mr.job.map_input_records",                     MetricCategory.MR_JOB, "map_input_records",             false);
        put("mr.job.map_output_records",                    MetricCategory.MR_JOB, "map_output_records",            false);
        put("mr.job.map_output_bytes",                      MetricCategory.MR_JOB, "map_output_bytes",              false);
        put("mr.job.reduce_input_records",                  MetricCategory.MR_JOB, "reduce_input_records",          false);
        put("mr.job.reduce_output_records",                 MetricCategory.MR_JOB, "reduce_output_records",         false);
        put("mr.job.reduce_shuffle_bytes",                  MetricCategory.MR_JOB, "reduce_shuffle_bytes",          false);
        put("mr.job.spilled_records",                       MetricCategory.MR_JOB, "spilled_records",               false);

        // MR job metrics (resource)
        put("mr.job.cpu_time_ms",                           MetricCategory.MR_JOB, "cpu_time_ms",                   false);
        put("mr.job.gc_time_ms",                            MetricCategory.MR_JOB, "gc_time_ms",                    false);
        put("mr.job.physical_memory_bytes",                 MetricCategory.MR_JOB, "physical_memory_bytes",         false);
        put("mr.job.virtual_memory_bytes",                  MetricCategory.MR_JOB, "virtual_memory_bytes",          false);
        put("mr.job.committed_heap_bytes",                  MetricCategory.MR_JOB, "committed_heap_bytes",          false);

        // MR job metrics (duration & count)
        put("mr.job.maps_duration_ms",                      MetricCategory.MR_JOB, "maps_duration_ms",              false);
        put("mr.job.reduces_duration_ms",                   MetricCategory.MR_JOB, "reduces_duration_ms",           false);
        put("mr.job.elapsed_time_ms",                       MetricCategory.MR_JOB, "elapsed_time_ms",               false);
        put("mr.job.launched_maps",                         MetricCategory.MR_JOB, "launched_maps",                 false);
        put("mr.job.launched_reduces",                      MetricCategory.MR_JOB, "launched_reduces",              false);

        // MR task metrics (IO)
        put("mr.task.io.hdfs_bytes_read",                   MetricCategory.MR_TASK, "hdfs_bytes_read",              false);
        put("mr.task.io.hdfs_bytes_written",                MetricCategory.MR_TASK, "hdfs_bytes_written",           false);
        put("mr.task.io.file_bytes_read",                   MetricCategory.MR_TASK, "file_bytes_read",              false);
        put("mr.task.io.file_bytes_written",                MetricCategory.MR_TASK, "file_bytes_written",           false);
        put("mr.task.io.map_input_records",                 MetricCategory.MR_TASK, "map_input_records",            false);
        put("mr.task.io.map_output_records",                MetricCategory.MR_TASK, "map_output_records",           false);
        put("mr.task.io.map_output_bytes",                  MetricCategory.MR_TASK, "map_output_bytes",             false);
        put("mr.task.io.reduce_input_records",              MetricCategory.MR_TASK, "reduce_input_records",         false);
        put("mr.task.io.reduce_output_records",             MetricCategory.MR_TASK, "reduce_output_records",        false);
        put("mr.task.io.reduce_shuffle_bytes",              MetricCategory.MR_TASK, "reduce_shuffle_bytes",         false);
        put("mr.task.io.spilled_records",                   MetricCategory.MR_TASK, "spilled_records",              false);

        // MR task metrics (resource)
        put("mr.task.cpu_time_ms",                          MetricCategory.MR_TASK, "cpu_time_ms",                  false);
        put("mr.task.gc_time_ms",                           MetricCategory.MR_TASK, "gc_time_ms",                   false);

        // MR task metrics (duration & result)
        put("mr.task.duration_ms",                          MetricCategory.MR_TASK, "duration_ms",                  false);
        put("mr.task.success",                              MetricCategory.MR_TASK, "success_count",                false);
        put("mr.task.failure",                              MetricCategory.MR_TASK, "failure_count",                false);

        // MR task metrics (file operations)
        put("mr.task.io.hdfs_read_ops",                     MetricCategory.MR_TASK, "hdfs_read_ops",                false);
        put("mr.task.io.hdfs_write_ops",                    MetricCategory.MR_TASK, "hdfs_write_ops",               false);
        put("mr.task.io.hdfs_large_read_ops",               MetricCategory.MR_TASK, "hdfs_large_read_ops",          false);
    }

    private static void put(String metricName, MetricCategory category, String columnName, boolean histogram) {
        MAPPINGS.put(metricName, new MetricMapping(category, columnName, histogram));
    }

    /**
     * Get the engine name for a metric category.
     * Used by MetricEventRow to populate the engine column in the wide table.
     */
    public static String getEngine(MetricCategory category) {
        switch (category) {
            case TASK:
            case STAGE:
            case JOB:
            case JVM_MEMORY:
            case JVM_GC:
            case SQL_EXECUTION:
            case SQL_TABLE_IO:
                return "SPARK";
            case MR_JOB:
            case MR_TASK:
                return "MR";
            case HIVE_QUERY:
            case HIVE_TABLE_IO:
                return "HIVE";
            default:
                return "UNKNOWN";
        }
    }

    public static MetricMapping classify(String metricName) {
        return MAPPINGS.get(metricName);
    }

    public static String extractGroupKey(MetricCategory category, Map<String, String> labels, long timestampMs) {
        StringBuilder sb = new StringBuilder();
        sb.append(timestampMs);
        switch (category) {
            case TASK:
                sb.append('|').append(labels.get("spark.app.id"))
                  .append('|').append(labels.get("spark.executor.id"))
                  .append('|').append(labels.get("spark.stage.id"))
                  .append('|').append(labels.get("spark.task.id"));
                break;
            case STAGE:
                sb.append('|').append(labels.get("spark.app.id"))
                  .append('|').append(labels.get("spark.executor.id"))
                  .append('|').append(labels.get("spark.stage.id"));
                break;
            case JOB:
                sb.append('|').append(labels.get("spark.app.id"))
                  .append('|').append(labels.get("spark.job.id"));
                break;
            case JVM_MEMORY:
                sb.append('|').append(labels.get("spark.app.id"))
                  .append('|').append(labels.get("spark.executor.id"));
                break;
            case JVM_GC:
                sb.append('|').append(labels.get("spark.app.id"))
                  .append('|').append(labels.get("spark.executor.id"))
                  .append('|').append(labels.get("gc_name"));
                break;
            case SQL_EXECUTION:
                // Use only app_id + execution_id as key (NOT timestamp).
                // OTLP exports duration_ms and join_count as separate data points with
                // potentially different timestamps; including timestampMs would split them
                // into different rows, preventing wide-row aggregation.
                sb.setLength(0); // clear timestamp prefix
                sb.append(labels.get("spark.app.id"))
                  .append('|').append(labels.get("spark.sql.execution_id"));
                break;
            case SQL_TABLE_IO:
                sb.append('|').append(labels.get("spark.app.id"))
                  .append('|').append(labels.get("spark.sql.execution_id"))
                  .append('|').append(labels.get("spark.sql.table_name"))
                  .append('|').append(labels.get("spark.sql.operation"));
                break;
            case HIVE_QUERY:
                sb.append('|').append(labels.get("hive.query.id"))
                  .append('|').append(labels.getOrDefault("hive.query.operation", "unknown"))
                  .append('|').append(labels.getOrDefault("hive.query.execution_engine", "unknown"));
                break;
            case HIVE_TABLE_IO:
                sb.append('|').append(labels.get("hive.query.id"))
                  .append('|').append(labels.getOrDefault("hive.query.operation", "unknown"))
                  .append('|').append(labels.getOrDefault("hive.query.input_table",
                        labels.getOrDefault("hive.query.output_table", "unknown")));
                break;
            case MR_JOB:
                // Use only job_id as key (NOT timestamp).
                // MR collector emits metrics for completed jobs; OTLP export time differs
                // from actual job finish time. Using job_id ensures all metrics for one job
                // aggregate into one row with the correct finish_time_ms as timestamp.
                sb.setLength(0); // clear timestamp prefix
                sb.append(labels.get("mr.job.id"));
                break;
            case MR_TASK:
                sb.setLength(0); // clear timestamp prefix
                sb.append(labels.get("mr.task.id"))
                  .append('|').append(labels.getOrDefault("mr.task.type", "unknown"))
                  .append('|').append(labels.get("mr.job.id"));
                break;
            default:
                break;
        }
        return sb.toString();
    }
}
