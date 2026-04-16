package x.mg.metrics.flink.classify;

public enum MetricCategory {
    TASK,
    STAGE,
    JOB,
    JVM_MEMORY,
    JVM_GC,
    SQL_EXECUTION,
    SQL_TABLE_IO,
    HIVE_QUERY,
    HIVE_TABLE_IO,
    MR_JOB,
    MR_TASK,
    UNKNOWN
}
