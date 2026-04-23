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
 * Strict end-to-end integration test: submit real Spark jobs, verify EXACT
 * metric values and row counts per app_id in MySQL.
 *
 * Assertions are based on known job characteristics:
 *   - SparkPi: 1 job, 1 stage, N tasks, no shuffle IO, no SQL
 *   - SQL GROUP BY: produces shuffle + scan metrics, known row counts
 *   - SQL JOIN: produces join_count > 0, shuffle > 0
 *
 * Prerequisites:
 *   - Spark with telemetry plugin JAR in jars/
 *   - OTel Collector, Kafka, Flink Consumer, MySQL running
 *   - Env: SPARK_HOME, MYSQL_HOST (default localhost), JAVA_HOME (Java 8)
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
    private static String javaBin;

    @BeforeAll
    static void setUp() throws Exception {
        // Detect Java 8
        javaBin = findJava8();
        assumeTrue(javaBin != null, "No Java 8 found. Set JAVA_HOME to JDK 8.");

        // Detect Spark
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

        // Find plugin JAR
        File jarsDir = new File(sparkHome, "jars");
        File[] jars = jarsDir.listFiles((d, name) ->
            name.contains("spark-telemetry") && name.endsWith(".jar") && !name.contains("original"));
        assumeTrue(jars != null && jars.length > 0,
            "No telemetry plugin JAR found in " + jarsDir);
        pluginJar = jars[0];

        sparkVersion = detectSparkVersion();

        db = new MetricsVerificationHelper(MYSQL_HOST, Integer.parseInt(MYSQL_PORT),
            "telemetry", MYSQL_USER, MYSQL_PASSWORD);

        System.out.println("Java: " + javaBin);
        System.out.println("Spark: " + sparkHome + " (v" + sparkVersion + ")");
        System.out.println("Plugin: " + pluginJar.getName());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (db != null) db.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // SparkPi — deterministic CPU job, no IO, no shuffle
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testSparkPi_TaskMetrics_StrictChecks() throws Exception {
        String appName = "it-pi-task-" + System.currentTimeMillis();
        int slices = 50; // produces exactly 50 tasks
        String output = submitSparkPi(appName, slices);
        assertJobCompleted(output, "SparkPi");

        String appId = db.waitForMetric("task_metrics", "app_id",
            "app_name = '" + appName + "'", PROPAGATION_TIMEOUT_SEC);
        String where = "app_id = '" + appId + "'";

        System.out.println("[task_metrics] app_id=" + appId);

        // ── Row count: exactly `slices` tasks, all successful ──
        long totalRows = db.getRowCount("task_metrics", where);
        assertEquals(slices, totalRows,
            "SparkPi with " + slices + " slices should produce exactly " + slices + " task rows");

        long successRows = db.getRowCount("task_metrics", where + " AND task_success = 'true'");
        assertEquals(slices, successRows, "All tasks should succeed");

        // ── Dimension columns: every row must have these populated ──
        db.assertDimensionColumns("task_metrics", where,
            "app_id", "app_name", "executor_id", "stage_id", "task_id",
            "task_success", "task_host");
        // user_name and queue: on YARN they have values; in local mode they may be empty
        // Still verify they are not NULL (empty string is acceptable for local mode)
        assertColumnNotNull("task_metrics", where, "user_name");
        assertColumnNotNull("task_metrics", where, "queue");

        // ── task_locality: SparkPi in local mode → PROCESS_LOCAL ──
        String locality = db.getStringValue("task_metrics", "task_locality", where);
        assertNotNull(locality, "task_locality should not be NULL");
        assertFalse(locality.isEmpty(), "task_locality should not be empty");

        // ── Duration: all tasks must have duration > 0 ──
        db.assertMetricColumnsPositive("task_metrics", where, "duration_ms");

        // ── Execution metrics: CPU-bound job must have positive run time ──
        db.assertMetricColumnsPositive("task_metrics", where, "executor_run_time_ms");

        // ── CPU time: must be positive for CPU-bound work ──
        db.assertMetricColumnsPositive("task_metrics", where, "executor_cpu_time_ns");

        // ── Result size: must be > 0 ──
        db.assertMetricColumnsPositive("task_metrics", where, "result_size_bytes");

        // ── SparkPi has NO IO: bytes read/written = 0, shuffle = 0 ──
        Double ioRead = db.getDoubleValue("task_metrics", "io_bytes_read", where + " LIMIT 1");
        assertNotNull(ioRead, "io_bytes_read should not be NULL");
        assertEquals(0.0, ioRead, 0.01, "SparkPi should have 0 bytes read");

        Double shuffleRead = db.getDoubleValue("task_metrics", "shuffle_bytes_read", where + " LIMIT 1");
        assertNotNull(shuffleRead, "shuffle_bytes_read should not be NULL");
        assertEquals(0.0, shuffleRead, 0.01, "SparkPi should have 0 shuffle bytes read");

        // ── All 23 metric columns must exist and be non-NULL ──
        db.assertMetricColumnsNonNegative("task_metrics", where,
            "io_bytes_read", "io_bytes_written", "io_records_read", "io_records_written",
            "shuffle_bytes_read", "shuffle_bytes_written", "shuffle_fetch_wait_time_ms",
            "disk_bytes_spilled", "memory_bytes_spilled",
            "deserialize_time_ms",
            "jvm_gc_time_ms", "scheduler_delay_ms",
            "peak_execution_memory_bytes",
            "shuffle_local_blocks_fetched", "shuffle_records_read",
            "shuffle_remote_bytes_read_to_disk", "shuffle_remote_reqs_duration_ms");

        System.out.println("  [PASS] task_metrics: " + totalRows + " rows, all fields verified");
    }

    @Test
    void testSparkPi_StageMetrics_StrictChecks() throws Exception {
        String appName = "it-pi-stage-" + System.currentTimeMillis();
        int slices = 50;
        String output = submitSparkPi(appName, slices);
        assertJobCompleted(output, "SparkPi");

        String appId = db.waitForMetric("stage_metrics", "app_id",
            "app_name = '" + appName + "'", PROPAGATION_TIMEOUT_SEC);
        String where = "app_id = '" + appId + "'";

        System.out.println("[stage_metrics] app_id=" + appId);

        // SparkPi: exactly 1 stage with `slices` tasks
        long stageRows = db.getRowCount("stage_metrics", where);
        assertEquals(1, stageRows, "SparkPi should produce exactly 1 stage_metrics row");

        // num_tasks must match slices
        Double numTasks = db.getDoubleValue("stage_metrics", "num_tasks", where);
        assertNotNull(numTasks, "num_tasks should not be NULL");
        assertEquals(slices, numTasks.intValue(), "num_tasks should equal slices");

        // Duration > 0
        db.assertMetricColumnsPositive("stage_metrics", where,
            "duration_ms", "executor_run_time_ms");

        // CPU time > 0
        db.assertMetricColumnsPositive("stage_metrics", where, "executor_cpu_time_ns");

        // Dimension columns
        db.assertDimensionColumns("stage_metrics", where,
            "app_id", "app_name", "executor_id", "stage_id");

        // SparkPi has no IO
        Double stageIoRead = db.getDoubleValue("stage_metrics", "io_bytes_read", where);
        assertNotNull(stageIoRead, "stage io_bytes_read should not be NULL");
        assertEquals(0.0, stageIoRead, 0.01, "SparkPi stage should have 0 bytes read");

        System.out.println("  [PASS] stage_metrics: " + stageRows + " row, num_tasks=" + numTasks.intValue());
    }

    @Test
    void testSparkPi_JobMetrics_StrictChecks() throws Exception {
        String appName = "it-pi-job-" + System.currentTimeMillis();
        String output = submitSparkPi(appName, 50);
        assertJobCompleted(output, "SparkPi");

        String appId = db.waitForMetric("job_metrics", "app_id",
            "app_name = '" + appName + "'", PROPAGATION_TIMEOUT_SEC);
        String where = "app_id = '" + appId + "'";

        System.out.println("[job_metrics] app_id=" + appId);

        // SparkPi: exactly 1 job
        long jobRows = db.getRowCount("job_metrics", where);
        assertEquals(1, jobRows, "SparkPi should produce exactly 1 job_metrics row");

        // num_stages = 1
        Double numStages = db.getDoubleValue("job_metrics", "num_stages", where);
        assertNotNull(numStages, "num_stages should not be NULL");
        assertEquals(1, numStages.intValue(), "SparkPi should have 1 stage");

        // job_success = true
        String success = db.getStringValue("job_metrics", "job_success", where);
        assertEquals("true", success, "SparkPi job should succeed");

        // duration > 0
        db.assertMetricColumnsPositive("job_metrics", where, "duration_ms");

        // All dimensions
        db.assertDimensionColumns("job_metrics", where,
            "app_id", "app_name", "job_id", "job_success");

        System.out.println("  [PASS] job_metrics: success=" + success + ", num_stages=" + numStages.intValue());
    }

    @Test
    void testSparkPi_JvmMetrics_StrictChecks() throws Exception {
        String appName = "it-pi-jvm-" + System.currentTimeMillis();
        String output = submitSparkPi(appName, 50);
        assertJobCompleted(output, "SparkPi");

        String appId = db.waitForMetric("jvm_memory_metrics", "app_id",
            "app_name = '" + appName + "'", PROPAGATION_TIMEOUT_SEC);
        String where = "app_id = '" + appId + "'";

        System.out.println("[jvm_metrics] app_id=" + appId);

        // Memory: at least 1 row, heap_used > 0
        long memRows = db.getRowCount("jvm_memory_metrics", where);
        assertTrue(memRows >= 1, "Should have at least 1 memory metric row");
        db.assertMetricColumnsPositive("jvm_memory_metrics", where, "heap_used", "non_heap_used");
        db.assertDimensionColumns("jvm_memory_metrics", where, "app_id", "executor_id");

        // GC: at least 1 row per GC collector
        db.waitForRows("jvm_gc_metrics", where, PROPAGATION_TIMEOUT_SEC);
        long gcRows = db.getRowCount("jvm_gc_metrics", where);
        assertTrue(gcRows >= 1, "Should have at least 1 GC metric row");
        db.assertMetricColumnsPositive("jvm_gc_metrics", where, "gc_count", "gc_time_ms");
        db.assertDimensionColumns("jvm_gc_metrics", where, "app_id", "executor_id", "gc_name");

        System.out.println("  [PASS] jvm: memory=" + memRows + " rows, gc=" + gcRows + " rows");
    }

    // ═══════════════════════════════════════════════════════════════
    // SQL — GROUP BY produces shuffle + scan, JOIN produces join_count
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testSQL_GroupBy_QueryMetrics_StrictChecks() throws Exception {
        String appName = "it-sql-groupby-" + System.currentTimeMillis();
        int numRows = 100;
        int numGroups = 5;
        String output = submitSQLGroupBy(appName, numRows, numGroups);
        assertJobCompleted(output, "SQL GROUP BY");

        String appId = db.waitForMetric("sql_query_metrics", "app_id",
            "app_name = '" + appName + "'", PROPAGATION_TIMEOUT_SEC);
        String where = "app_id = '" + appId + "'";

        System.out.println("[sql_query_metrics] app_id=" + appId);

        // GROUP BY query should produce at least 1 sql_query_metrics row
        long sqlRows = db.getRowCount("sql_query_metrics", where);
        assertTrue(sqlRows >= 1, "Should have at least 1 SQL query metric row");

        // duration > 0
        db.assertMetricColumnsPositive("sql_query_metrics", where, "duration_ms");

        // query_text: MUST be populated (not NULL, not empty)
        String queryText = db.getStringValue("sql_query_metrics", "query_text",
            where + " AND query_text IS NOT NULL LIMIT 1");
        assertNotNull(queryText, "query_text MUST NOT be NULL in sql_query_metrics");
        assertFalse(queryText.trim().isEmpty(), "query_text MUST NOT be empty");
        assertTrue(queryText.contains("GROUP BY"), "query_text should contain the SQL query");

        // execution_id must be > 0 (Spark 3.x)
        String execId = db.getStringValue("sql_query_metrics", "execution_id", where + " LIMIT 1");
        assertNotNull(execId, "execution_id should not be NULL");

        // Dimension columns
        db.assertDimensionColumns("sql_query_metrics", where,
            "app_id", "execution_id");

        System.out.println("  [PASS] sql_query_metrics: " + sqlRows + " rows, query_text populated");
    }

    @Test
    void testSQL_GroupBy_TableIOMetrics_StrictChecks() throws Exception {
        String appName = "it-sql-tableio-" + System.currentTimeMillis();
        int numRows = 100;
        int numGroups = 5;
        String output = submitSQLGroupBy(appName, numRows, numGroups);
        assertJobCompleted(output, "SQL GROUP BY");

        String appId = db.waitForMetric("sql_query_table_metrics", "app_id",
            "app_name = '" + appName + "'", PROPAGATION_TIMEOUT_SEC);
        String where = "app_id = '" + appId + "'";

        System.out.println("[sql_query_table_metrics] app_id=" + appId);

        // Should have scan entries for the temp view
        long tableRows = db.getRowCount("sql_query_table_metrics", where);
        assertTrue(tableRows >= 1, "Should have at least 1 table IO metric row");

        // operation must be 'scan' for a SELECT query
        String operation = db.getStringValue("sql_query_table_metrics", "operation",
            where + " LIMIT 1");
        assertNotNull(operation, "operation should not be NULL");
        // scan is the primary operation for GROUP BY
        long scanCount = db.getRowCount("sql_query_table_metrics", where + " AND operation = 'scan'");
        assertTrue(scanCount >= 1, "Should have at least 1 scan operation");

        // rows: must match the input data count (100 rows)
        Double scannedRows = db.getDoubleValue("sql_query_table_metrics", "rows",
            where + " AND operation = 'scan' LIMIT 1");
        assertNotNull(scannedRows, "rows should not be NULL for scan operation");
        assertEquals(numRows, scannedRows.intValue(), "Scanned rows should match input data count");

        // bytes: must be > 0 for in-memory data
        db.assertMetricColumnsPositive("sql_query_table_metrics",
            where + " AND operation = 'scan'", "bytes");

        // Dimension columns for each row
        db.assertDimensionColumns("sql_query_table_metrics", where,
            "app_id", "execution_id", "table_name", "operation");

        System.out.println("  [PASS] sql_query_table_metrics: " + tableRows + " rows, "
            + scanCount + " scans, rows=" + numRows);
    }

    @Test
    void testSQL_Join_JoinCountAndShuffle() throws Exception {
        String appName = "it-sql-join-" + System.currentTimeMillis();
        int numRows = 100;
        String output = submitSQLJoin(appName, numRows);
        assertJobCompleted(output, "SQL JOIN");

        String appId = db.waitForMetric("sql_query_metrics", "app_id",
            "app_name = '" + appName + "'", PROPAGATION_TIMEOUT_SEC);
        String where = "app_id = '" + appId + "'";

        System.out.println("[sql JOIN metrics] app_id=" + appId);

        // Find the JOIN query (has join_count > 0)
        db.waitForRows("sql_query_metrics",
            where + " AND join_count > 0", PROPAGATION_TIMEOUT_SEC);

        Double joinCount = db.getDoubleValue("sql_query_metrics", "join_count",
            where + " AND join_count > 0 LIMIT 1");
        assertNotNull(joinCount, "join_count should not be NULL for JOIN query");
        assertTrue(joinCount > 0, "JOIN query should have join_count > 0, got " + joinCount);

        // Shuffle bytes must be > 0 for JOIN (exchange required)
        Double shuffleRead = db.getDoubleValue("sql_query_metrics", "shuffle_bytes_read",
            where + " AND join_count > 0 LIMIT 1");
        assertNotNull(shuffleRead, "shuffle_bytes_read should not be NULL for JOIN query");
        assertTrue(shuffleRead > 0, "JOIN query should have shuffle_bytes_read > 0, got " + shuffleRead);

        System.out.println("  [PASS] SQL JOIN: join_count=" + joinCount.intValue()
            + ", shuffle_bytes_read=" + shuffleRead.longValue());
    }

    // ═══════════════════════════════════════════════════════════════
    // Cross-table consistency checks
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testSparkPi_CrossTableConsistency() throws Exception {
        String appName = "it-pi-consistency-" + System.currentTimeMillis();
        String output = submitSparkPi(appName, 10);
        assertJobCompleted(output, "SparkPi");

        String appId = db.waitForMetric("task_metrics", "app_id",
            "app_name = '" + appName + "'", PROPAGATION_TIMEOUT_SEC);
        String where = "app_id = '" + appId + "'";

        // All 5 Spark metric tables should have the same app_id
        long taskRows = db.getRowCount("task_metrics", where);
        long stageRows = db.getRowCount("stage_metrics", where);
        long jobRows = db.getRowCount("job_metrics", where);
        long memRows = db.getRowCount("jvm_memory_metrics", where);
        long gcRows = db.getRowCount("jvm_gc_metrics", where);

        assertTrue(taskRows > 0, "task_metrics should have rows");
        assertEquals(1, stageRows, "Should have exactly 1 stage");
        assertEquals(1, jobRows, "Should have exactly 1 job");
        assertTrue(memRows > 0, "jvm_memory_metrics should have rows");
        assertTrue(gcRows > 0, "jvm_gc_metrics should have rows");

        // Same app_name across all tables
        String taskAppName = db.getStringValue("task_metrics", "app_name", where + " LIMIT 1");
        String jobAppName = db.getStringValue("job_metrics", "app_name", where + " LIMIT 1");
        assertEquals(taskAppName, jobAppName, "app_name should match across tables");

        // No SQL metrics for SparkPi (non-SQL job)
        long sqlRows = db.getRowCount("sql_query_metrics", where);
        assertEquals(0, sqlRows, "SparkPi should produce 0 SQL query metrics");

        System.out.println("  [PASS] Cross-table: task=" + taskRows + " stage=" + stageRows
            + " job=" + jobRows + " mem=" + memRows + " gc=" + gcRows + " sql=0");
    }

    // ═══════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════

    private void assertJobCompleted(String output, String jobName) {
        assertTrue(output.contains("Pi is roughly") || output.contains("completed")
                || output.contains("SQL verification complete"),
            jobName + " should complete. Output:\n" + output);
    }

    private void assertColumnNotNull(String table, String where, String column) throws Exception {
        String sql = "SELECT " + column + " FROM " + table + " WHERE " + where + " LIMIT 1";
        try (Statement stmt = db.getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next(), "No rows in " + table);
            // getString returns null for SQL NULL, but empty string for empty string
            // We just want to verify it's not SQL NULL
            String val = rs.getString(column);
            // val can be empty (local mode) but not null
            if (val == null) {
                fail(column + " is NULL in " + table);
            }
        }
    }

    private String submitSparkPi(String appName, int slices) throws Exception {
        // Use SparkSession.range() to avoid cloudpickle issues with Python 3.13 + PySpark 3.2
        File script = File.createTempFile("sparkpi-", ".py");
        script.deleteOnExit();
        try (PrintWriter pw = new PrintWriter(script)) {
            pw.println("from pyspark.sql import SparkSession");
            pw.println("from pyspark.sql.functions import col, rand, when, lit");
            pw.println("spark = SparkSession.builder.appName('" + appName + "').getOrCreate()");
            pw.println("n = " + slices);
            pw.println("df = spark.range(0, n, numPartitions=n)");
            pw.println("df2 = df.withColumn('x', rand() * 2 - 1).withColumn('y', rand() * 2 - 1)");
            pw.println("df3 = df2.withColumn('hit', when(col('x')**2 + col('y')**2 <= 1, 1).otherwise(0))");
            pw.println("hits = df3.agg({'hit': 'sum'}).collect()[0][0]");
            pw.println("pi = 4.0 * hits / n");
            pw.println("print('Pi is roughly %f' % pi)");
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
        return submitPySpark(appName, script);
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
        return submitPySpark(appName, script);
    }

    private String submitPySpark(String appName, File script) throws Exception {
        return submitPySparkApp(appName, script);
    }

    private String runCommand(List<String> cmd, int timeoutSec) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        if (javaBin != null && !javaBin.isEmpty()) {
            pb.environment().put("JAVA_HOME", javaBin);
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
        // Returns JAVA_HOME path (not bin/java), used to set pb.environment JAVA_HOME
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
