package x.mg.metrics.flink.model;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Contract test: verifies every OTel attribute key emitted by source adapters
 * (MetricRecorder, MRMetricRecorder, HiveMetricRecorder) is consumed by the
 * corresponding Row model's fromLabels() method.
 *
 * If a source adapter sets attribute "spark.foo" but no Row model reads it,
 * that's a silent data-loss bug.
 */
class AttributeCoverageTest {

    // ============================================================
    // Keys emitted by MetricRecorder (Spark) for TASK events
    // ============================================================
    private static final Set<String> SPARK_TASK_LABELS = new HashSet<>(Arrays.asList(
        "spark.app.id", "spark.app.name", "spark.user", "spark.yarn.queue",
        "spark.executor.id", "spark.stage.id", "spark.task.id",
        "spark.task.success", "spark.task.host", "spark.task.locality",
        "spark.task.speculative"
    ));

    // Keys emitted for STAGE events
    private static final Set<String> SPARK_STAGE_LABELS = new HashSet<>(Arrays.asList(
        "spark.app.id", "spark.app.name", "spark.user", "spark.yarn.queue",
        "spark.executor.id", "spark.stage.id"
    ));

    // Keys emitted for JOB events
    private static final Set<String> SPARK_JOB_LABELS = new HashSet<>(Arrays.asList(
        "spark.app.id", "spark.app.name", "spark.user", "spark.yarn.queue",
        "spark.job.id", "spark.job.success"
    ));

    // Keys emitted for JVM_MEMORY events
    private static final Set<String> SPARK_JVM_MEMORY_LABELS = new HashSet<>(Arrays.asList(
        "spark.app.id", "spark.app.name", "spark.user", "spark.yarn.queue",
        "spark.executor.id"
    ));

    // Keys emitted for JVM_GC events
    private static final Set<String> SPARK_JVM_GC_LABELS = new HashSet<>(Arrays.asList(
        "spark.app.id", "spark.app.name", "spark.user", "spark.yarn.queue",
        "spark.executor.id", "gc_name"
    ));

    // Keys emitted for SQL_EXECUTION events
    private static final Set<String> SPARK_SQL_LABELS = new HashSet<>(Arrays.asList(
        "spark.app.id", "spark.app.name", "spark.user", "spark.yarn.queue",
        "spark.sql.execution_id", "spark.sql.query_text"
    ));

    // Keys emitted for SQL_TABLE_IO events
    private static final Set<String> SPARK_TABLE_LABELS = new HashSet<>(Arrays.asList(
        "spark.app.id", "spark.app.name", "spark.user", "spark.yarn.queue",
        "spark.sql.execution_id", "spark.sql.table_name", "spark.sql.operation"
    ));

    // ============================================================
    // Keys emitted by HiveMetricRecorder (Hive)
    // ============================================================
    private static final Set<String> HIVE_QUERY_LABELS = new HashSet<>(Arrays.asList(
        "hive.query.id", "hive.query.operation", "hive.query.user",
        "hive.query.success", "hive.query.execution_engine",
        "hive.query.queue", "hive.query.sql_text"
    ));

    private static final Set<String> HIVE_TABLE_LABELS = new HashSet<>(Arrays.asList(
        "hive.query.id", "hive.query.operation", "hive.query.user",
        "hive.query.execution_engine", "hive.query.queue",
        "hive.query.input_table", "hive.query.output_table"
    ));

    // ============================================================
    // Keys emitted by MRMetricRecorder (MapReduce)
    // ============================================================
    private static final Set<String> MR_JOB_LABELS = new HashSet<>(Arrays.asList(
        "mr.job.id", "mr.job.name", "mr.job.user", "mr.job.state",
        "mr.job.queue", "mr.job.finish_time_ms", "mr.job.start_time_ms"
    ));

    private static final Set<String> MR_TASK_LABELS = new HashSet<>(Arrays.asList(
        "mr.task.id", "mr.task.type", "mr.task.state",
        "mr.job.id", "mr.job.name", "mr.job.user", "mr.job.queue",
        "mr.job.finish_time_ms", "mr.job.start_time_ms"
    ));

    // ============================================================
    // Tests
    // ============================================================

    @Test
    void testTaskMetricRowExtractsAllSparkTaskLabels() {
        Map<String, String> labels = new HashMap<>();
        for (String key : SPARK_TASK_LABELS) {
            labels.put(key, "test-" + key);
        }
        TaskMetricRow row = TaskMetricRow.fromLabels(0, labels);
        assertNotNull(row.getAppId());
        assertEquals("test-spark.app.id", row.getAppId());
        assertEquals("test-spark.app.name", row.getAppName());
        assertEquals("test-spark.user", row.getUserName());
        assertEquals("test-spark.yarn.queue", row.getQueue());
        assertEquals("test-spark.executor.id", row.getExecutorId());
        assertEquals(0, row.getStageId()); // non-numeric → 0
        assertEquals("test-spark.task.success", row.getTaskSuccess());
    }

    @Test
    void testTaskMetricRowHandlesMissingLabels() {
        // All labels missing → should not throw, use defaults
        Map<String, String> labels = new HashMap<>();
        TaskMetricRow row = TaskMetricRow.fromLabels(0, labels);
        assertNotNull(row.getAppId());
        assertEquals("unknown", row.getAppId());
        assertEquals("", row.getAppName());
        assertEquals("", row.getUserName());
        assertEquals("", row.getQueue());
        assertEquals("unknown", row.getExecutorId());
    }

    @Test
    void testStageMetricRowExtractsAllSparkStageLabels() {
        Map<String, String> labels = new HashMap<>();
        for (String key : SPARK_STAGE_LABELS) {
            labels.put(key, "test-" + key);
        }
        StageMetricRow row = StageMetricRow.fromLabels(0, labels);
        assertEquals("test-spark.app.id", row.getAppId());
        assertEquals("test-spark.app.name", row.getAppName());
        assertEquals("test-spark.user", row.getUserName());
        assertEquals("test-spark.yarn.queue", row.getQueue());
        assertEquals("test-spark.executor.id", row.getExecutorId());
    }

    @Test
    void testJobMetricRowExtractsAllSparkJobLabels() {
        Map<String, String> labels = new HashMap<>();
        for (String key : SPARK_JOB_LABELS) {
            labels.put(key, "test-" + key);
        }
        // job.id must be numeric for parseInt
        labels.put("spark.job.id", "42");
        JobMetricRow row = JobMetricRow.fromLabels(0, labels);
        assertEquals("test-spark.app.id", row.getAppId());
        assertEquals("test-spark.app.name", row.getAppName());
        assertEquals("test-spark.user", row.getUserName());
        assertEquals("test-spark.yarn.queue", row.getQueue());
        assertEquals(42, row.getJobId());
        assertEquals("test-spark.job.success", row.getJobSuccess());
    }

    @Test
    void testJvmMemoryMetricRowExtractsAllLabels() {
        Map<String, String> labels = new HashMap<>();
        for (String key : SPARK_JVM_MEMORY_LABELS) {
            labels.put(key, "test-" + key);
        }
        JvmMemoryMetricRow row = JvmMemoryMetricRow.fromLabels(0, labels);
        assertEquals("test-spark.app.id", row.getAppId());
        assertEquals("test-spark.app.name", row.getAppName());
        assertEquals("test-spark.user", row.getUserName());
        assertEquals("test-spark.yarn.queue", row.getQueue());
        assertEquals("test-spark.executor.id", row.getExecutorId());
    }

    @Test
    void testJvmGcMetricRowExtractsAllLabels() {
        Map<String, String> labels = new HashMap<>();
        for (String key : SPARK_JVM_GC_LABELS) {
            labels.put(key, "test-" + key);
        }
        JvmGcMetricRow row = JvmGcMetricRow.fromLabels(0, labels);
        assertEquals("test-spark.app.id", row.getAppId());
        assertEquals("test-spark.app.name", row.getAppName());
        assertEquals("test-spark.user", row.getUserName());
        assertEquals("test-spark.yarn.queue", row.getQueue());
        assertEquals("test-spark.executor.id", row.getExecutorId());
        assertEquals("test-gc_name", row.getGcName());
    }

    @Test
    void testSqlQueryMetricRowExtractsAllLabels() {
        Map<String, String> labels = new HashMap<>();
        for (String key : SPARK_SQL_LABELS) {
            labels.put(key, "test-" + key);
        }
        SqlQueryMetricRow row = SqlQueryMetricRow.fromLabels(0, labels);
        assertEquals("test-spark.app.id", row.getAppId());
        assertEquals("test-spark.app.name", row.getAppName());
        assertEquals("test-spark.user", row.getUserName());
        assertEquals("test-spark.yarn.queue", row.getQueue());
        assertEquals("test-spark.sql.execution_id", row.getExecutionId());
        assertEquals("test-spark.sql.query_text", row.getQueryText());
        assertEquals("test-spark.app.name", row.getAppName());
    }

    @Test
    void testSqlTableIoMetricRowExtractsAllLabels() {
        Map<String, String> labels = new HashMap<>();
        for (String key : SPARK_TABLE_LABELS) {
            labels.put(key, "test-" + key);
        }
        SqlTableIoMetricRow row = SqlTableIoMetricRow.fromLabels(0, labels);
        assertEquals("test-spark.app.id", row.getAppId());
        assertEquals("test-spark.app.name", row.getAppName());
        assertEquals("test-spark.user", row.getUserName());
        assertEquals("test-spark.yarn.queue", row.getQueue());
        assertEquals("test-spark.sql.execution_id", row.getExecutionId());
        assertEquals("test-spark.sql.table_name", row.getTableName());
        assertEquals("test-spark.sql.operation", row.getOperation());
    }

    @Test
    void testHiveQueryMetricRowExtractsAllLabels() {
        Map<String, String> labels = new HashMap<>();
        for (String key : HIVE_QUERY_LABELS) {
            labels.put(key, "test-" + key);
        }
        HiveQueryMetricRow row = HiveQueryMetricRow.fromLabels(0, labels);
        assertEquals("test-hive.query.id", row.getQueryId());
        assertEquals("test-hive.query.operation", row.getOperation());
        assertEquals("test-hive.query.user", row.getUserName());
        assertEquals("test-hive.query.success", row.getSuccess());
        assertEquals("test-hive.query.execution_engine", row.getExecutionEngine());
        assertEquals("test-hive.query.queue", row.getQueue());
        assertEquals("test-hive.query.sql_text", row.getQueryText());
        assertEquals("test-hive.query.id", row.getAppName()); // query_id as app_name
    }

    @Test
    void testHiveTableIoMetricRowExtractsAllLabels() {
        Map<String, String> labels = new HashMap<>();
        for (String key : HIVE_TABLE_LABELS) {
            labels.put(key, "test-" + key);
        }
        HiveTableIoMetricRow row = HiveTableIoMetricRow.fromLabels(0, labels);
        assertEquals("test-hive.query.id", row.getQueryId());
        assertEquals("test-hive.query.operation", row.getOperation());
        assertEquals("test-hive.query.user", row.getUserName());
        assertEquals("test-hive.query.execution_engine", row.getExecutionEngine());
        assertEquals("test-hive.query.queue", row.getQueue());
        assertEquals("test-hive.query.input_table", row.getTableName());
        assertEquals("input", row.getTableType());
        assertEquals("test-hive.query.id", row.getAppName()); // query_id as app_name
    }

    @Test
    void testHiveTableIoMetricRowPrefersInputTable() {
        Map<String, String> labels = new HashMap<>();
        labels.put("hive.query.input_table", "input_tbl");
        labels.put("hive.query.output_table", "output_tbl");
        HiveTableIoMetricRow row = HiveTableIoMetricRow.fromLabels(0, labels);
        assertEquals("input_tbl", row.getTableName());
        assertEquals("input", row.getTableType());
    }

    @Test
    void testHiveTableIoMetricRowFallsBackToOutputTable() {
        Map<String, String> labels = new HashMap<>();
        labels.put("hive.query.output_table", "output_tbl");
        HiveTableIoMetricRow row = HiveTableIoMetricRow.fromLabels(0, labels);
        assertEquals("output_tbl", row.getTableName());
        assertEquals("output", row.getTableType());
    }

    @Test
    void testMrJobMetricRowExtractsAllLabels() {
        Map<String, String> labels = new HashMap<>();
        for (String key : MR_JOB_LABELS) {
            labels.put(key, "test-" + key);
        }
        MrJobMetricRow row = MrJobMetricRow.fromLabels(0, labels);
        assertEquals("test-mr.job.id", row.getJobId());
        assertEquals("test-mr.job.name", row.getJobName());
        assertEquals("test-mr.job.user", row.getUserName());
        assertEquals("test-mr.job.state", row.getState());
        assertEquals("test-mr.job.queue", row.getQueue());
        assertEquals("test-mr.job.name", row.getAppName()); // job_name as app_name
    }

    @Test
    void testMrTaskMetricRowExtractsAllLabels() {
        Map<String, String> labels = new HashMap<>();
        for (String key : MR_TASK_LABELS) {
            labels.put(key, "test-" + key);
        }
        MrTaskMetricRow row = MrTaskMetricRow.fromLabels(0, labels);
        assertEquals("test-mr.task.id", row.getTaskId());
        assertEquals("test-mr.task.type", row.getTaskType());
        assertEquals("test-mr.task.state", row.getState());
        assertEquals("test-mr.job.id", row.getJobId());
        assertEquals("test-mr.job.name", row.getJobName());
        assertEquals("test-mr.job.user", row.getUserName());
        assertEquals("test-mr.job.queue", row.getQueue());
    }

    @Test
    void testDefaultValuesWhenAllLabelsMissing() {
        // Every fromLabels() should return a non-null row with defaults
        Map<String, String> empty = new HashMap<>();

        assertNotNull(TaskMetricRow.fromLabels(0, empty));
        assertNotNull(StageMetricRow.fromLabels(0, empty));
        assertNotNull(JobMetricRow.fromLabels(0, empty));
        assertNotNull(JvmMemoryMetricRow.fromLabels(0, empty));
        assertNotNull(JvmGcMetricRow.fromLabels(0, empty));
        assertNotNull(SqlQueryMetricRow.fromLabels(0, empty));
        assertNotNull(SqlTableIoMetricRow.fromLabels(0, empty));
        assertNotNull(HiveQueryMetricRow.fromLabels(0, empty));
        assertNotNull(HiveTableIoMetricRow.fromLabels(0, empty));
        assertNotNull(MrJobMetricRow.fromLabels(0, empty));
        assertNotNull(MrTaskMetricRow.fromLabels(0, empty));
    }
}
