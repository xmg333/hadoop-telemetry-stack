package x.mg.metrics.mragent.counter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Mapping from Hadoop counter group/name to OTel metric name.
 */
public class CounterMapping {

    public final String groupName;
    public final String counterName;
    public final String otelName;
    public final String description;
    public final String unit;

    public CounterMapping(String groupName, String counterName,
                          String otelName, String description, String unit) {
        this.groupName = groupName;
        this.counterName = counterName;
        this.otelName = otelName;
        this.description = description;
        this.unit = unit;
    }

    // Hadoop counter group names
    public static final String TASK_COUNTER = "org.apache.hadoop.mapreduce.TaskCounter";
    public static final String FS_COUNTER = "org.apache.hadoop.mapreduce.FileSystemCounter";

    public static final List<CounterMapping> ALL = buildAll();

    private static List<CounterMapping> buildAll() {
        List<CounterMapping> mappings = new ArrayList<>();

        // TaskCounter
        mappings.add(new CounterMapping(TASK_COUNTER, "MAP_INPUT_RECORDS",
            "mr.task.io.map_input_records", "Map input records", "{records}"));
        mappings.add(new CounterMapping(TASK_COUNTER, "MAP_OUTPUT_RECORDS",
            "mr.task.io.map_output_records", "Map output records", "{records}"));
        mappings.add(new CounterMapping(TASK_COUNTER, "MAP_OUTPUT_BYTES",
            "mr.task.io.map_output_bytes", "Map output bytes", "By"));
        mappings.add(new CounterMapping(TASK_COUNTER, "REDUCE_INPUT_RECORDS",
            "mr.task.io.reduce_input_records", "Reduce input records", "{records}"));
        mappings.add(new CounterMapping(TASK_COUNTER, "REDUCE_OUTPUT_RECORDS",
            "mr.task.io.reduce_output_records", "Reduce output records", "{records}"));
        mappings.add(new CounterMapping(TASK_COUNTER, "REDUCE_SHUFFLE_BYTES",
            "mr.task.io.reduce_shuffle_bytes", "Reduce shuffle bytes", "By"));
        mappings.add(new CounterMapping(TASK_COUNTER, "SPILLED_RECORDS",
            "mr.task.io.spilled_records", "Spilled records", "{records}"));
        mappings.add(new CounterMapping(TASK_COUNTER, "CPU_MILLISECONDS",
            "mr.task.cpu_time_ms", "CPU time", "ms"));
        mappings.add(new CounterMapping(TASK_COUNTER, "GC_TIME_MILLIS",
            "mr.task.gc_time_ms", "GC time", "ms"));

        // FileSystemCounter - bytes
        mappings.add(new CounterMapping(FS_COUNTER, "HDFS_BYTES_READ",
            "mr.task.io.hdfs_bytes_read", "HDFS bytes read", "By"));
        mappings.add(new CounterMapping(FS_COUNTER, "HDFS_BYTES_WRITTEN",
            "mr.task.io.hdfs_bytes_written", "HDFS bytes written", "By"));
        mappings.add(new CounterMapping(FS_COUNTER, "FILE_BYTES_READ",
            "mr.task.io.file_bytes_read", "Local file bytes read", "By"));
        mappings.add(new CounterMapping(FS_COUNTER, "FILE_BYTES_WRITTEN",
            "mr.task.io.file_bytes_written", "Local file bytes written", "By"));

        // FileSystemCounter - file operations (Hadoop 2.7+)
        mappings.add(new CounterMapping(FS_COUNTER, "HDFS_READ_OPS",
            "mr.task.io.hdfs_read_ops", "HDFS read operations", "{ops}"));
        mappings.add(new CounterMapping(FS_COUNTER, "HDFS_WRITE_OPS",
            "mr.task.io.hdfs_write_ops", "HDFS write operations", "{ops}"));
        mappings.add(new CounterMapping(FS_COUNTER, "HDFS_LARGE_READ_OPS",
            "mr.task.io.hdfs_large_read_ops", "HDFS large read operations", "{ops}"));

        return Collections.unmodifiableList(mappings);
    }
}
