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
    }

    private static void put(String metricName, MetricCategory category, String columnName, boolean histogram) {
        MAPPINGS.put(metricName, new MetricMapping(category, columnName, histogram));
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
            default:
                break;
        }
        return sb.toString();
    }
}
