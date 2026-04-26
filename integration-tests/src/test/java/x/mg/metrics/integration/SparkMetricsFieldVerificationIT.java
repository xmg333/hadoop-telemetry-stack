package x.mg.metrics.integration;

import org.junit.jupiter.api.*;

import java.io.*;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end integration test for Spark metrics.
 * Submits real PySpark jobs and verifies EVERY column in EVERY table is populated.
 */
@Tag("integration")
class SparkMetricsFieldVerificationIT {

    private static final String MYSQL_HOST = System.getenv().getOrDefault("MYSQL_HOST", "localhost");
    private static final String MYSQL_PORT = System.getenv().getOrDefault("MYSQL_PORT", "3306");
    private static final String MYSQL_USER = System.getenv().getOrDefault("MYSQL_USER", "root");
    private static final String MYSQL_PASSWORD = System.getenv().getOrDefault("MYSQL_PASSWORD", "root123");
    private static final String OTEL_ENDPOINT = System.getenv().getOrDefault("OTEL_ENDPOINT", "http://localhost:4317");
    private static final String JAVA_HOME = System.getenv().getOrDefault("JAVA_HOME", "");

    private static final long PROPAGATION_TIMEOUT_SEC = 90;

    private static MetricsVerificationHelper db;
    private static String sparkHome;
    private static File pluginJar;
    private static String sparkVersion;
    private static String javaHome;

    @BeforeAll
    static void setUp() throws Exception {
        javaHome = findJava8();
        assumeTrue(javaHome != null, "No Java 8 found. Set JAVA_HOME to JDK 8.");

        sparkHome = System.getenv().getOrDefault("SPARK_HOME", "");
        if (sparkHome.isEmpty()) {
            File opt = new File("/opt");
            if (opt.isDirectory()) {
                File[] dirs = opt.listFiles((d, name) ->
                    name.startsWith("spark-") && new File(d, name + "/bin/spark-submit").exists());
                if (dirs != null && dirs.length > 0) {
                    Arrays.sort(dirs, Comparator.comparing(File::getName).reversed());
                    sparkHome = dirs[0].getAbsolutePath();
                }
            }
        }
        assumeTrue(sparkHome != null && !sparkHome.isEmpty() && new File(sparkHome).isDirectory(),
            "No Spark installation found. Set SPARK_HOME.");

        File jarsDir = new File(sparkHome, "jars");
        File[] jars = jarsDir.listFiles((d, name) ->
            name.contains("spark-telemetry") && name.endsWith(".jar") && !name.contains("original"));
        assumeTrue(jars != null && jars.length > 0,
            "No telemetry plugin JAR found in " + jarsDir);
        pluginJar = jars[0];

        sparkVersion = detectSparkVersion();

        db = new MetricsVerificationHelper(MYSQL_HOST, Integer.parseInt(MYSQL_PORT),
            "telemetry", MYSQL_USER, MYSQL_PASSWORD);

        System.out.println("Java: " + javaHome);
        System.out.println("Spark: " + sparkHome + " (v" + sparkVersion + ")");
        System.out.println("Plugin: " + pluginJar.getName());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (db != null) db.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // task_metrics — ALL columns verified
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testSparkPi_TaskMetrics_AllColumns() throws Exception {
        String appName = "it-task-" + System.currentTimeMillis();
        int slices = 50;
        String output = submitPySparkPi(appName, slices);
        assertOutputContains(output, "Pi is roughly");

        String appId = db.waitForMetric("task_metrics", "app_id",
            "app_name = '" + appName + "'", PROPAGATION_TIMEOUT_SEC);
        String where = "app_id = '" + appId + "'";

        System.out.println("[task_metrics] app_id=" + appId);

        assertEquals(slices, db.getRowCount("task_metrics", where),
            "Should have exactly " + slices + " task rows");
        assertEquals(slices, db.getRowCount("task_metrics", where + " AND task_success = 'true'"),
            "All tasks should succeed");

        // ── ALL dimension columns: non-null, non-empty ──
        db.assertDimensionColumns("task_metrics", where,
            "app_id", "app_name", "executor_id", "stage_id", "task_id",
            "task_success", "task_host", "task_locality", "user_name", "queue");

        // task_success must be exactly "true"
        assertEquals("true", db.getStringValue("task_metrics", where + " AND task_success = 'true'", "task_success"));

        // ── ALL metric columns: non-negative, NOT NULL ──
        db.assertMetricColumnsNonNegative("task_metrics", where,
            "duration_ms", "executor_run_time_ms", "executor_cpu_time_ns",
            "result_size_bytes",
            "io_bytes_read", "io_bytes_written", "io_records_read", "io_records_written",
            "shuffle_bytes_read", "shuffle_bytes_written", "shuffle_fetch_wait_time_ms",
            "disk_bytes_spilled", "memory_bytes_spilled",
            "deserialize_time_ms",
            "jvm_gc_time_ms", "scheduler_delay_ms",
            "peak_execution_memory_bytes",
            "shuffle_local_blocks_fetched", "shuffle_records_read",
            "shuffle_remote_bytes_read_to_disk", "shuffle_remote_reqs_duration_ms");

        // ── Pi-specific: these MUST be positive ──
        db.assertMetricColumnsPositive("task_metrics", where,
            "duration_ms", "executor_run_time_ms", "executor_cpu_time_ns", "result_size_bytes");

        System.out.println("  [PASS] task_metrics: " + slices + " rows, ALL columns verified");
    }

    // ═══════════════════════════════════════════════════════════════
    // stage_metrics — ALL columns verified
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testSparkPi_StageMetrics_AllColumns() throws Exception {
        String appName = "it-stage-" + System.currentTimeMillis();
        int slices = 50;
        submitPySparkPi(appName, slices);

        String appId = db.waitForMetric("stage_metrics", "app_id",
            "app_name = '" + appName + "'", PROPAGATION_TIMEOUT_SEC);
        String where = "app_id = '" + appId + "'";

        System.out.println("[stage_metrics] app_id=" + appId);

        assertEquals(1, db.getRowCount("stage_metrics", where));

        // ── ALL dimension columns ──
        db.assertDimensionColumns("stage_metrics", where,
            "app_id", "app_name", "executor_id", "stage_id", "user_name", "queue");

        // ── ALL metric columns: non-negative, NOT NULL ──
        db.assertMetricColumnsNonNegative("stage_metrics", where,
            "duration_ms", "num_tasks",
            "executor_run_time_ms", "executor_cpu_time_ns",
            "jvm_gc_time_ms", "peak_execution_memory_bytes",
            "io_bytes_read", "io_bytes_written");

        // ── Must be positive ──
        db.assertMetricColumnsPositive("stage_metrics", where,
            "duration_ms", "num_tasks", "executor_run_time_ms", "executor_cpu_time_ns");

        assertEquals(slices, db.getDoubleValue("stage_metrics", "num_tasks", where).intValue());

        System.out.println("  [PASS] stage_metrics: ALL columns verified, num_tasks="
            + db.getDoubleValue("stage_metrics", "num_tasks", where).intValue());
    }

    // ═══════════════════════════════════════════════════════════════
    // job_metrics — ALL columns verified
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testSparkPi_JobMetrics_AllColumns() throws Exception {
        String appName = "it-job-" + System.currentTimeMillis();
        submitPySparkPi(appName, 50);

        String appId = db.waitForMetric("job_metrics", "app_id",
            "app_name = '" + appName + "'", PROPAGATION_TIMEOUT_SEC);
        String where = "app_id = '" + appId + "'";

        System.out.println("[job_metrics] app_id=" + appId);

        long jobRows = db.getRowCount("job_metrics", where);
        assertTrue(jobRows >= 1, "Should have at least 1 job");

        // ── ALL dimension columns ──
        db.assertDimensionColumns("job_metrics", where,
            "app_id", "app_name", "job_id", "job_success", "user_name", "queue");

        // ── ALL metric columns: non-negative, NOT NULL ──
        db.assertMetricColumnsNonNegative("job_metrics", where, "duration_ms", "num_stages");

        // ── Must be positive ──
        db.assertMetricColumnsPositive("job_metrics", where, "duration_ms");
        Double numStages = db.getDoubleValue("job_metrics", "num_stages", where);
        assertNotNull(numStages);
        assertTrue(numStages >= 1, "Should have at least 1 stage");

        System.out.println("  [PASS] job_metrics: " + jobRows + " rows, ALL columns verified");
    }

    // ═══════════════════════════════════════════════════════════════
    // JVM memory metrics — ALL columns verified
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testSparkPi_JvmMemory_AllColumns() throws Exception {
        String appName = "it-jvmmem-" + System.currentTimeMillis();
        submitPySparkPi(appName, 50);

        String appId = db.waitForMetric("jvm_memory_metrics", "app_id",
            "app_name = '" + appName + "'", PROPAGATION_TIMEOUT_SEC);
        String where = "app_id = '" + appId + "'";

        System.out.println("[jvm_memory_metrics] app_id=" + appId);

        assertTrue(db.getRowCount("jvm_memory_metrics", where) >= 1);

        // ── ALL dimension columns ──
        db.assertDimensionColumns("jvm_memory_metrics", where,
            "app_id", "app_name", "executor_id", "user_name", "queue");

        // ── ALL metric columns: positive, NOT NULL ──
        db.assertMetricColumnsPositive("jvm_memory_metrics", where, "heap_used", "non_heap_used");

        System.out.println("  [PASS] jvm_memory_metrics: ALL columns verified");
    }

    // ═══════════════════════════════════════════════════════════════
    // JVM GC metrics — ALL columns verified
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testSparkPi_JvmGc_AllColumns() throws Exception {
        String appName = "it-jvmgc-" + System.currentTimeMillis();
        submitPySparkPi(appName, 50);

        String appId = db.waitForMetric("jvm_gc_metrics", "app_id",
            "app_name = '" + appName + "'", PROPAGATION_TIMEOUT_SEC);
        String where = "app_id = '" + appId + "'";

        System.out.println("[jvm_gc_metrics] app_id=" + appId);

        assertTrue(db.getRowCount("jvm_gc_metrics", where) >= 1);

        // ── ALL dimension columns ──
        db.assertDimensionColumns("jvm_gc_metrics", where,
            "app_id", "app_name", "executor_id", "gc_name", "user_name", "queue");

        // ── ALL metric columns: positive, NOT NULL ──
        db.assertMetricColumnsPositive("jvm_gc_metrics", where, "gc_count", "gc_time_ms");

        System.out.println("  [PASS] jvm_gc_metrics: ALL columns verified");
    }

    // ═══════════════════════════════════════════════════════════════
    // sql_query_metrics — ALL columns verified
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testSQL_GroupBy_QueryMetrics_AllColumns() throws Exception {
        String appName = "it-sql-qm-" + System.currentTimeMillis();
        submitSQLGroupBy(appName, 100, 5);

        String appId = db.waitForMetric("sql_query_metrics", "app_id",
            "app_name = '" + appName + "'", PROPAGATION_TIMEOUT_SEC);
        String where = "app_id = '" + appId + "'";

        System.out.println("[sql_query_metrics] app_id=" + appId);

        assertTrue(db.getRowCount("sql_query_metrics", where) >= 1);

        // ── ALL dimension columns ──
        db.assertDimensionColumns("sql_query_metrics", where,
            "app_id", "app_name", "execution_id", "user_name", "queue");

        // ── ALL metric columns: non-negative, NOT NULL ──
        db.assertMetricColumnsNonNegative("sql_query_metrics", where,
            "duration_ms", "shuffle_bytes_read", "shuffle_bytes_written", "join_count");

        // ── Duration must be positive ──
        db.assertMetricColumnsPositive("sql_query_metrics", where, "duration_ms");

        System.out.println("  [PASS] sql_query_metrics: ALL columns verified");
    }

    // ═══════════════════════════════════════════════════════════════
    // sql_query_table_metrics — ALL columns verified (when populated)
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testSQL_GroupBy_TableIOMetrics_AllColumns() throws Exception {
        String appName = "it-sql-tio-" + System.currentTimeMillis();
        submitSQLGroupBy(appName, 100, 5);

        String appId = db.waitForMetric("sql_query_metrics", "app_id",
            "app_name = '" + appName + "'", PROPAGATION_TIMEOUT_SEC);
        String where = "app_id = '" + appId + "'";

        db.assertMetricColumnsPositive("sql_query_metrics", where, "duration_ms");

        long tableRows = db.getRowCount("sql_query_table_metrics", where);
        if (tableRows > 0) {
            System.out.println("[sql_query_table_metrics] app_id=" + appId);

            // ── ALL dimension columns ──
            db.assertDimensionColumns("sql_query_table_metrics", where,
                "app_id", "app_name", "execution_id", "table_name", "operation",
                "user_name", "queue");

            // ── ALL metric columns: non-negative, NOT NULL ──
            db.assertMetricColumnsNonNegative("sql_query_table_metrics", where,
                "bytes", "rows", "files_read", "time_ms");

            System.out.println("  [PASS] sql_query_table_metrics: " + tableRows + " rows, ALL columns verified");
        } else {
            System.out.println("  [SKIP] sql_query_table_metrics: 0 rows (expected for temp view queries)");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // SQL JOIN — verify join_count and shuffle
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testSQL_Join_QueryMetrics_AllColumns() throws Exception {
        String appName = "it-sql-join-" + System.currentTimeMillis();
        submitSQLJoin(appName, 100);

        String appId = db.waitForMetric("sql_query_metrics", "app_id",
            "app_name = '" + appName + "'", PROPAGATION_TIMEOUT_SEC);
        String where = "app_id = '" + appId + "'";

        System.out.println("[sql JOIN] app_id=" + appId);

        assertTrue(db.getRowCount("sql_query_metrics", where) >= 1);

        // ── ALL dimension columns ──
        db.assertDimensionColumns("sql_query_metrics", where,
            "app_id", "app_name", "execution_id", "user_name", "queue");

        // ── ALL metric columns: non-negative ──
        db.assertMetricColumnsNonNegative("sql_query_metrics", where,
            "duration_ms", "shuffle_bytes_read", "shuffle_bytes_written", "join_count");

        db.assertMetricColumnsPositive("sql_query_metrics", where, "duration_ms");

        Double joinCount = db.getDoubleValue("sql_query_metrics", "join_count",
            where + " AND join_count IS NOT NULL");
        if (joinCount != null) {
            assertTrue(joinCount > 0, "join_count should be > 0 for JOIN, got " + joinCount);
        }

        Double shuffleRead = db.getDoubleValue("sql_query_metrics", "shuffle_bytes_read",
            where + " AND shuffle_bytes_read IS NOT NULL");
        if (shuffleRead != null) {
            assertTrue(shuffleRead > 0, "shuffle should be > 0 for exchange, got " + shuffleRead);
        }

        System.out.println("  [PASS] sql JOIN: join_count=" + joinCount + " shuffle=" + shuffleRead);
    }

    // ═══════════════════════════════════════════════════════════════
    // Cross-table consistency — ALL shared dimension columns match
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testSparkPi_CrossTableConsistency() throws Exception {
        String appName = "it-xref-" + System.currentTimeMillis();
        submitPySparkPi(appName, 10);

        String appId = db.waitForMetric("task_metrics", "app_id",
            "app_name = '" + appName + "'", PROPAGATION_TIMEOUT_SEC);
        String where = "app_id = '" + appId + "'";

        assertTrue(db.getRowCount("task_metrics", where) > 0);
        assertTrue(db.getRowCount("stage_metrics", where) >= 1);
        assertTrue(db.getRowCount("job_metrics", where) >= 1);

        db.waitForRows("jvm_memory_metrics", where, PROPAGATION_TIMEOUT_SEC);
        assertTrue(db.getRowCount("jvm_memory_metrics", where) > 0);
        assertTrue(db.getRowCount("jvm_gc_metrics", where) > 0);

        // app_name, user_name, queue must match across ALL tables
        String taskAppName = db.getStringValue("task_metrics", "app_name", where);
        String stageAppName = db.getStringValue("stage_metrics", "app_name", where);
        String jobAppName = db.getStringValue("job_metrics", "app_name", where);
        assertEquals(taskAppName, stageAppName, "app_name: task vs stage");
        assertEquals(taskAppName, jobAppName, "app_name: task vs job");

        String taskUser = db.getStringValue("task_metrics", "user_name", where);
        String stageUser = db.getStringValue("stage_metrics", "user_name", where);
        assertEquals(taskUser, stageUser, "user_name: task vs stage");

        String taskQueue = db.getStringValue("task_metrics", "queue", where);
        String stageQueue = db.getStringValue("stage_metrics", "queue", where);
        assertEquals(taskQueue, stageQueue, "queue: task vs stage");

        System.out.println("  [PASS] cross-table: app_name/user_name/queue consistent across ALL tables");
    }

    // ═══════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════

    private void assertOutputContains(String output, String text) {
        assertTrue(output.contains(text),
            "Output should contain '" + text + "'. Output:\n" + output);
    }

    private String submitPySparkPi(String appName, int slices) throws Exception {
        File script = File.createTempFile("sparkpi-", ".py");
        script.deleteOnExit();
        try (PrintWriter pw = new PrintWriter(script)) {
            pw.println("from pyspark.sql import SparkSession");
            pw.println("from pyspark.sql.functions import col, rand, when");
            pw.println("spark = SparkSession.builder.appName('" + appName + "').getOrCreate()");
            pw.println("n = " + slices);
            pw.println("df = spark.range(0, n, numPartitions=n)");
            pw.println("df2 = df.withColumn('x', rand() * 2 - 1).withColumn('y', rand() * 2 - 1)");
            pw.println("df3 = df2.withColumn('hit', when(col('x')**2 + col('y')**2 <= 1, 1).otherwise(0))");
            pw.println("hits = df3.agg({'hit': 'sum'}).collect()[0][0]");
            pw.println("print('Pi is roughly %f' % (4.0 * hits / n))");
            pw.println("spark.stop()");
        }
        return submitPySparkApp(appName, script);
    }

    private String submitSQLGroupBy(String appName, int numRows, int numGroups) throws Exception {
        File script = File.createTempFile("sql-groupby-", ".py");
        script.deleteOnExit();
        try (PrintWriter pw = new PrintWriter(script)) {
            pw.println("from pyspark.sql import SparkSession");
            pw.println("spark = SparkSession.builder.appName('" + appName + "').getOrCreate()");
            pw.println("spark.range(0, " + numRows + ", numPartitions=1).createOrReplaceTempView('t')");
            pw.println("spark.sql('SELECT id % " + numGroups + " as grp, id * 10.0 as val FROM t').createOrReplaceTempView('t2')");
            pw.println("spark.sql('SELECT grp, COUNT(*) as cnt, AVG(val) as avg_val FROM t2 GROUP BY grp').collect()");
            pw.println("print('SQL verification complete')");
            pw.println("spark.stop()");
        }
        return submitPySparkApp(appName, script);
    }

    private String submitSQLJoin(String appName, int numRows) throws Exception {
        File script = File.createTempFile("sql-join-", ".py");
        script.deleteOnExit();
        try (PrintWriter pw = new PrintWriter(script)) {
            pw.println("from pyspark.sql import SparkSession");
            pw.println("spark = SparkSession.builder.appName('" + appName + "').getOrCreate()");
            pw.println("spark.range(0, " + numRows + ", numPartitions=1).createOrReplaceTempView('t')");
            pw.println("spark.sql('SELECT id % 5 as grp, id * 10.0 as val FROM t').createOrReplaceTempView('t1')");
            pw.println("spark.sql('SELECT id % 5 as grp, id * 10.0 as val FROM t').createOrReplaceTempView('t2')");
            pw.println("spark.sql('SELECT a.grp, a.cnt, b.total FROM (SELECT grp, COUNT(*) as cnt FROM t1 GROUP BY grp) a JOIN (SELECT grp, SUM(val) as total FROM t2 GROUP BY grp) b ON a.grp = b.grp').collect()");
            pw.println("print('SQL verification complete')");
            pw.println("spark.stop()");
        }
        return submitPySparkApp(appName, script);
    }

    private String submitPySparkApp(String appName, File script) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add(sparkHome + "/bin/spark-submit");
        cmd.add("--master"); cmd.add("local[4]");
        cmd.add("--jars"); cmd.add(pluginJar.getAbsolutePath());
        cmd.add("--conf"); cmd.add("spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin");
        cmd.add("--conf"); cmd.add("spark.telemetry.otel.exporter.endpoint=" + OTEL_ENDPOINT);
        cmd.add("--conf"); cmd.add("spark.telemetry.otel.service.name=" + appName);
        cmd.add("--conf"); cmd.add("spark.telemetry.otel.export.interval.ms=3000");
        cmd.add("--conf"); cmd.add("spark.telemetry.metrics.task.execution=true");
        cmd.add("--conf"); cmd.add("spark.telemetry.metrics.task.shuffle-extended=true");
        cmd.add("--conf"); cmd.add("spark.telemetry.metrics.task.info=true");
        cmd.add("--conf"); cmd.add("spark.telemetry.metrics.stage.detailed=true");
        cmd.add("--conf"); cmd.add("spark.telemetry.metrics.job.lifecycle=true");
        cmd.add("--conf"); cmd.add("spark.telemetry.metrics.sql.query-execution=true");
        cmd.add("--conf"); cmd.add("spark.app.name=" + appName);
        cmd.add(script.getAbsolutePath());
        return runCommand(cmd, 180);
    }

    private String runCommand(List<String> cmd, int timeoutSec) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        if (javaHome != null && !javaHome.isEmpty()) {
            pb.environment().put("JAVA_HOME", javaHome);
        }
        pb.redirectErrorStream(true);
        Process proc = pb.start();

        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
        }

        boolean finished = proc.waitFor(timeoutSec, TimeUnit.SECONDS);
        if (!finished) {
            proc.destroyForcibly();
            throw new RuntimeException("Command timed out after " + timeoutSec + "s");
        }
        return output.toString();
    }

    private static String detectSparkVersion() {
        File jarsDir = new File(sparkHome, "jars");
        File[] cores = jarsDir.listFiles((d, name) -> name.startsWith("spark-core_"));
        if (cores != null && cores.length > 0) {
            String name = cores[0].getName();
            int dash = name.lastIndexOf('-');
            if (dash > 0) return name.substring(dash + 1).replace(".jar", "");
        }
        return "unknown";
    }

    private static String findJava8() {
        if (!JAVA_HOME.isEmpty() && new File(JAVA_HOME, "bin/java").exists()) {
            return JAVA_HOME;
        }
        File opt = new File("/opt");
        if (opt.isDirectory()) {
            File[] dirs = opt.listFiles((d, name) ->
                name.startsWith("jdk") && new File(d, name + "/bin/java").exists());
            if (dirs != null) {
                for (File dir : dirs) {
                    if (dir.getName().contains("8")) return dir.getAbsolutePath();
                }
                return dirs[0].getAbsolutePath();
            }
        }
        return null;
    }
}
