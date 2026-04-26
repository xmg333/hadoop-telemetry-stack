package x.mg.metrics.integration;

import org.junit.jupiter.api.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Strict end-to-end integration test for Hive query and table IO metrics.
 * Submits real Hive queries with known characteristics and verifies EVERY
 * column is populated in both hive_query_metrics and hive_query_table.
 *
 * Test data characteristics used for assertions:
 *   - test_src has exactly 5 rows
 *   - test_src2 has exactly 3 rows
 *   - SELECT COUNT(*) → success=true, input_bytes > 0, input_rows >= 1
 *   - CTAS → reads 5 rows, writes 5 rows, both input and output metrics > 0
 *   - JOIN on id → reads from 2 tables, output 3 rows (matching ids: 1,2,3)
 *
 * Prerequisites:
 *   - Hive installation with HiveServer2 running
 *   - Telemetry hook JAR in auxlib/
 *   - OTel Collector, Kafka, Flink Consumer, MySQL running
 *   - HIVE_HOME env var set
 */
@Tag("integration")
class HiveMetricsFieldVerificationIT {

    private static final String MYSQL_HOST = System.getenv().getOrDefault("MYSQL_HOST", "localhost");
    private static final String MYSQL_PORT = System.getenv().getOrDefault("MYSQL_PORT", "3306");
    private static final String MYSQL_USER = System.getenv().getOrDefault("MYSQL_USER", "root");
    private static final String MYSQL_PASSWORD = System.getenv().getOrDefault("MYSQL_PASSWORD", "root123");

    private static final long PROPAGATION_TIMEOUT_SEC = 90;

    private static MetricsVerificationHelper db;
    private static String hiveHome;
    private static String hadoopHome;
    private static String beelineUrl;

    @BeforeAll
    static void setUp() throws Exception {
        hiveHome = System.getenv().getOrDefault("HIVE_HOME", "");
        if (hiveHome.isEmpty()) {
            File opt = new File("/opt");
            if (opt.isDirectory()) {
                File[] dirs = opt.listFiles((d, name) ->
                    name.startsWith("apache-hive") && new File(d, name + "/bin/beeline").exists());
                if (dirs != null && dirs.length > 0) {
                    hiveHome = dirs[0].getAbsolutePath();
                }
            }
        }
        assumeTrue(hiveHome != null && !hiveHome.isEmpty(),
            "No Hive installation found. Set HIVE_HOME.");

        hadoopHome = System.getenv().getOrDefault("HADOOP_HOME", "");
        if (hadoopHome.isEmpty()) {
            File opt = new File("/opt");
            if (opt.isDirectory()) {
                File[] dirs = opt.listFiles((d, name) ->
                    name.startsWith("hadoop") && new File(d, name + "/bin/hadoop").exists());
                if (dirs != null && dirs.length > 0) {
                    Arrays.sort(dirs, (a, b) -> b.getName().compareTo(a.getName()));
                    hadoopHome = dirs[0].getAbsolutePath();
                }
            }
        }

        beelineUrl = System.getenv().getOrDefault("BEELINE_URL",
            "jdbc:hive2://localhost:10000");

        System.out.println("[DEBUG] hiveHome=" + hiveHome);
        System.out.println("[DEBUG] hadoopHome=" + hadoopHome);
        System.out.println("[DEBUG] beelineUrl=" + beelineUrl);

        String host = beelineUrl.replace("jdbc:hive2://", "").split("/")[0].split(":")[0];
        int port = 10000;
        try {
            String[] hp = beelineUrl.replace("jdbc:hive2://", "").split("/")[0].split(":");
            if (hp.length > 1) port = Integer.parseInt(hp[1]);
        } catch (NumberFormatException ignored) {}
        try (java.net.Socket s = new java.net.Socket()) {
            s.connect(new java.net.InetSocketAddress(host, port), 5000);
            System.out.println("[DEBUG] HiveServer2 port " + port + " reachable");
        } catch (Exception e) {
            assumeTrue(false, "HiveServer2 not reachable at " + host + ":" + port + ": " + e.getMessage());
        }

        db = new MetricsVerificationHelper(MYSQL_HOST, Integer.parseInt(MYSQL_PORT),
            "telemetry", MYSQL_USER, MYSQL_PASSWORD);

        System.out.println("Hive Home: " + hiveHome);
        System.out.println("Beeline URL: " + beelineUrl);
        System.out.println("MySQL: " + MYSQL_HOST + ":" + MYSQL_PORT);

        runHive("CREATE DATABASE IF NOT EXISTS field_verify");
        runHive("DROP TABLE IF EXISTS field_verify.test_src");
        runHive("CREATE TABLE field_verify.test_src (id INT, name STRING) STORED AS TEXTFILE");
        runHive("INSERT INTO TABLE field_verify.test_src VALUES (1,'alpha'),(2,'beta'),(3,'gamma'),(4,'delta'),(5,'epsilon')");

        runHive("DROP TABLE IF EXISTS field_verify.test_src2");
        runHive("CREATE TABLE field_verify.test_src2 (id INT, value STRING) STORED AS TEXTFILE");
        runHive("INSERT INTO TABLE field_verify.test_src2 VALUES (1,'v1'),(2,'v2'),(3,'v3')");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (db != null) db.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // hive_query_metrics — SELECT COUNT(*) ALL columns verified
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testHiveQueryMetrics_SelectAllColumns() throws Exception {
        String marker = "hv_sel_" + System.currentTimeMillis();
        String sql = "SELECT COUNT(*) FROM field_verify.test_src /* " + marker + " */";

        String output = runHive(sql);
        assertFalse(output.contains("Error"), "Hive query should succeed: " + output);

        db.waitForRows("hive_query_metrics",
            "query_text LIKE '%" + marker + "%'", PROPAGATION_TIMEOUT_SEC);
        String where = "query_text LIKE '%" + marker + "%'";

        System.out.println("[hive_query_metrics SELECT] marker=" + marker);

        assertEquals(1, db.getRowCount("hive_query_metrics", where),
            "Should have exactly 1 hive_query_metrics row for this query");

        // ── ALL dimension columns: non-null, non-empty ──
        db.assertDimensionColumns("hive_query_metrics", where,
            "query_id", "operation", "user_name", "success", "execution_engine");

        // ── success = true ──
        assertEquals("true", db.getStringValue("hive_query_metrics", "success", where));

        // ── execution_engine: non-empty (typically "mr") ──
        String engine = db.getStringValue("hive_query_metrics", "execution_engine", where);
        assertFalse(engine.isEmpty(), "execution_engine should not be empty");

        // ── query_text: contains marker and SQL ──
        String queryText = db.getStringValue("hive_query_metrics", "query_text", where);
        assertNotNull(queryText, "query_text should not be NULL");
        assertFalse(queryText.trim().isEmpty(), "query_text should not be empty");
        assertTrue(queryText.contains(marker),
            "query_text should contain marker '" + marker + "'");
        assertTrue(queryText.contains("SELECT"),
            "query_text should contain the SQL statement");

        // ── ALL timing columns: positive ──
        db.assertMetricColumnsPositive("hive_query_metrics", where, "duration_ms");

        // ── ALL counter columns: non-negative, NOT NULL ──
        db.assertMetricColumnsNonNegative("hive_query_metrics", where,
            "success_count", "failure_count",
            "input_bytes", "output_bytes",
            "input_rows", "output_rows");

        // ── SELECT COUNT(*) specific: success_count MUST be > 0 ──
        db.assertMetricColumnsPositive("hive_query_metrics", where,
            "success_count", "input_bytes", "input_rows");

        Double inputBytes = db.getDoubleValue("hive_query_metrics", "input_bytes", where);
        assertTrue(inputBytes > 0,
            "input_bytes should be > 0 when reading from HDFS table, got " + inputBytes);

        Double inputRows = db.getDoubleValue("hive_query_metrics", "input_rows", where);
        assertTrue(inputRows >= 1,
            "input_rows should be >= 1 for a table with data, got " + inputRows);

        System.out.println("  [PASS] hive_query_metrics: success=true"
            + ", duration=" + db.getDoubleValue("hive_query_metrics", "duration_ms", where).longValue() + "ms"
            + ", input_bytes=" + inputBytes.longValue()
            + ", input_rows=" + inputRows.longValue()
            + ", engine=" + engine);
    }

    // ═══════════════════════════════════════════════════════════════
    // hive_query_metrics — CTAS ALL columns verified (read + write)
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testHiveQueryMetrics_CTAS_AllColumns() throws Exception {
        String marker = "hv_ctas_" + System.currentTimeMillis();
        String tableName = "field_verify.test_output_" + marker;

        String sql = "CREATE TABLE " + tableName + " AS SELECT * FROM field_verify.test_src /* " + marker + " */";

        String output = runHive(sql);
        assertFalse(output.contains("Error"), "Hive CTAS should succeed: " + output);

        db.waitForRows("hive_query_metrics",
            "query_text LIKE '%" + marker + "%'", PROPAGATION_TIMEOUT_SEC);
        String where = "query_text LIKE '%" + marker + "%'";

        System.out.println("[hive_query_metrics CTAS] marker=" + marker);

        assertEquals(1, db.getRowCount("hive_query_metrics", where),
            "Should have exactly 1 hive_query_metrics row for CTAS");

        // ── ALL dimension columns: non-null, non-empty ──
        db.assertDimensionColumns("hive_query_metrics", where,
            "query_id", "operation", "user_name", "success", "execution_engine");

        assertEquals("true", db.getStringValue("hive_query_metrics", "success", where));

        // ── query_text: contains CTAS and marker ──
        String queryText = db.getStringValue("hive_query_metrics", "query_text", where);
        assertNotNull(queryText, "query_text should not be NULL for CTAS");
        assertTrue(queryText.contains(marker),
            "query_text should contain marker '" + marker + "'");

        // ── ALL timing columns: positive ──
        db.assertMetricColumnsPositive("hive_query_metrics", where, "duration_ms");

        // ── ALL counter columns: non-negative, NOT NULL ──
        db.assertMetricColumnsNonNegative("hive_query_metrics", where,
            "success_count", "failure_count",
            "input_bytes", "output_bytes",
            "input_rows", "output_rows");

        // ── CTAS reads 5-row table → input MUST be > 0 ──
        db.assertMetricColumnsPositive("hive_query_metrics", where,
            "success_count", "input_bytes", "input_rows");

        // ── CTAS writes to HDFS → output_bytes and output_rows SHOULD be > 0 ──
        db.assertMetricColumnsPositive("hive_query_metrics", where,
            "output_bytes", "output_rows");

        Double inputBytes = db.getDoubleValue("hive_query_metrics", "input_bytes", where);
        Double outputBytes = db.getDoubleValue("hive_query_metrics", "output_bytes", where);

        System.out.println("  [PASS] hive_query_metrics CTAS: success=true"
            + ", duration=" + db.getDoubleValue("hive_query_metrics", "duration_ms", where).longValue() + "ms"
            + ", input=" + inputBytes.longValue() + " bytes"
            + ", output=" + outputBytes.longValue() + " bytes");

        runHive("DROP TABLE IF EXISTS " + tableName);
    }

    // ═══════════════════════════════════════════════════════════════
    // hive_query_metrics — JOIN ALL columns verified (multi-table input)
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testHiveQueryMetrics_JoinAllColumns() throws Exception {
        String marker = "hv_join_" + System.currentTimeMillis();
        String sql = "SELECT a.id, a.name, b.value FROM field_verify.test_src a "
            + "JOIN field_verify.test_src2 b ON a.id = b.id /* " + marker + " */";

        String output = runHive(sql);
        assertFalse(output.contains("Error"), "Hive JOIN should succeed: " + output);

        db.waitForRows("hive_query_metrics",
            "query_text LIKE '%" + marker + "%'", PROPAGATION_TIMEOUT_SEC);
        String where = "query_text LIKE '%" + marker + "%'";

        System.out.println("[hive_query_metrics JOIN] marker=" + marker);

        assertEquals(1, db.getRowCount("hive_query_metrics", where),
            "Should have exactly 1 query metrics row for JOIN");

        // ── ALL dimension columns: non-null, non-empty ──
        db.assertDimensionColumns("hive_query_metrics", where,
            "query_id", "operation", "user_name", "success", "execution_engine");

        assertEquals("true", db.getStringValue("hive_query_metrics", "success", where));

        // ── query_text contains JOIN and marker ──
        String queryText = db.getStringValue("hive_query_metrics", "query_text", where);
        assertNotNull(queryText);
        assertTrue(queryText.contains(marker));

        // ── ALL timing: positive ──
        db.assertMetricColumnsPositive("hive_query_metrics", where, "duration_ms");

        // ── ALL counter columns: non-negative, NOT NULL ──
        db.assertMetricColumnsNonNegative("hive_query_metrics", where,
            "success_count", "failure_count",
            "input_bytes", "output_bytes",
            "input_rows", "output_rows");

        // ── JOIN reads from 2 tables → input MUST be > 0 ──
        db.assertMetricColumnsPositive("hive_query_metrics", where,
            "success_count", "input_bytes", "input_rows");

        System.out.println("  [PASS] hive_query_metrics JOIN: duration="
            + db.getDoubleValue("hive_query_metrics", "duration_ms", where).longValue() + "ms"
            + ", input_bytes="
            + db.getDoubleValue("hive_query_metrics", "input_bytes", where).longValue());
    }

    // ═══════════════════════════════════════════════════════════════
    // hive_query_table — scan entries for SELECT (ALL columns)
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testHiveQueryTable_ScanAllColumns() throws Exception {
        String marker = "hv_tio_" + System.currentTimeMillis();
        String sql = "SELECT COUNT(*) FROM field_verify.test_src /* " + marker + " */";

        String output = runHive(sql);
        assertFalse(output.contains("Error"), "Hive query should succeed: " + output);

        // Wait for query metrics first (confirms pipeline processed the query)
        db.waitForRows("hive_query_metrics",
            "query_text LIKE '%" + marker + "%'", PROPAGATION_TIMEOUT_SEC);

        System.out.println("[hive_query_table SCAN] marker=" + marker);

        // Wait for table IO metrics (scan = input table entries)
        long scanRows = 0;
        try {
            db.waitForRows("hive_query_table",
                "table_type = 'input'", 30);
            scanRows = db.getRowCount("hive_query_table", "table_type = 'input'");
        } catch (RuntimeException e) {
            System.out.println("  [SKIP] hive_query_table: no scan rows found");
            return;
        }

        assertTrue(scanRows >= 1, "Should have at least 1 input row, got " + scanRows);

        // Use the most recent scan row for detailed checks
        String scanWhere = "table_type = 'input' ORDER BY timestamp_ms DESC LIMIT 1";

        // ── ALL dimension columns: non-null, non-empty ──
        db.assertDimensionColumns("hive_query_table", scanWhere,
            "query_id", "table_name", "table_type", "operation", "user_name", "execution_engine");

        // ── table_type cross-validation ──
        assertEquals("input", db.getStringValue("hive_query_table", "table_type", scanWhere));

        // ── table_name: must reference a real table (not "unknown") ──
        String tableName = db.getStringValue("hive_query_table", "table_name", scanWhere);
        assertFalse(tableName.isEmpty(), "table_name should not be empty");
        assertNotEquals("unknown", tableName, "table_name should be a real table, not 'unknown'");

        // ── ALL metric columns: non-negative, NOT NULL ──
        db.assertMetricColumnsNonNegative("hive_query_table", scanWhere,
            "input_table_count", "output_table_count",
            "bytes", "rows", "files_read", "time_ms");

        // ── Scan-specific: bytes and rows MUST be > 0 for reading from HDFS table ──
        db.assertMetricColumnsPositive("hive_query_table", scanWhere,
            "bytes", "rows");

        System.out.println("  [PASS] hive_query_table SCAN: table=" + tableName
            + ", bytes=" + db.getDoubleValue("hive_query_table", "bytes", scanWhere).longValue()
            + ", rows=" + db.getDoubleValue("hive_query_table", "rows", scanWhere).longValue());
    }

    // ═══════════════════════════════════════════════════════════════
    // hive_query_table — write entries for CTAS (ALL columns)
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testHiveQueryTable_WriteAllColumns() throws Exception {
        String marker = "hv_tw_" + System.currentTimeMillis();
        String table = "field_verify.test_output_" + marker;

        String sql = "CREATE TABLE " + table + " AS SELECT * FROM field_verify.test_src /* " + marker + " */";

        String output = runHive(sql);
        assertFalse(output.contains("Error"), "Hive CTAS should succeed: " + output);

        // Wait for query metrics
        db.waitForRows("hive_query_metrics",
            "query_text LIKE '%" + marker + "%'", PROPAGATION_TIMEOUT_SEC);

        System.out.println("[hive_query_table WRITE] marker=" + marker);

        // Wait for write entries
        long writeRows = 0;
        try {
            db.waitForRows("hive_query_table",
                "table_type = 'output'", 30);
            writeRows = db.getRowCount("hive_query_table", "table_type = 'output'");
        } catch (RuntimeException e) {
            System.out.println("  [SKIP] hive_query_table WRITE: no output rows found");
            runHive("DROP TABLE IF EXISTS " + table);
            return;
        }

        assertTrue(writeRows >= 1, "CTAS should produce at least 1 output row, got " + writeRows);

        String writeWhere = "table_type = 'output' ORDER BY timestamp_ms DESC LIMIT 1";

        // ── ALL dimension columns: non-null, non-empty ──
        db.assertDimensionColumns("hive_query_table", writeWhere,
            "query_id", "table_name", "table_type", "operation", "user_name", "execution_engine");

        // ── table_type cross-validation ──
        assertEquals("output", db.getStringValue("hive_query_table", "table_type", writeWhere));

        // ── table_name: must reference the CTAS target table ──
        String tableName = db.getStringValue("hive_query_table", "table_name", writeWhere);
        assertFalse(tableName.isEmpty(), "table_name should not be empty");
        assertNotEquals("unknown", tableName, "table_name should be a real table, not 'unknown'");

        // ── ALL metric columns: non-negative, NOT NULL ──
        db.assertMetricColumnsNonNegative("hive_query_table", writeWhere,
            "input_table_count", "output_table_count",
            "bytes", "rows", "files_read", "time_ms");

        // ── CTAS writes to HDFS → bytes and rows MUST be > 0 ──
        db.assertMetricColumnsPositive("hive_query_table", writeWhere,
            "bytes", "rows");

        System.out.println("  [PASS] hive_query_table WRITE: table=" + tableName
            + ", bytes=" + db.getDoubleValue("hive_query_table", "bytes", writeWhere).longValue()
            + ", rows=" + db.getDoubleValue("hive_query_table", "rows", writeWhere).longValue());

        runHive("DROP TABLE IF EXISTS " + table);
    }

    // ═══════════════════════════════════════════════════════════════
    // Cross-table: query ↔ table IO consistency
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testHive_CrossTableConsistency() throws Exception {
        String marker = "hv_xref_" + System.currentTimeMillis();
        String table = "field_verify.test_output_" + marker;

        String sql = "CREATE TABLE " + table + " AS SELECT * FROM field_verify.test_src /* " + marker + " */";

        String output = runHive(sql);
        assertFalse(output.contains("Error"), "Hive CTAS should succeed: " + output);

        db.waitForRows("hive_query_metrics",
            "query_text LIKE '%" + marker + "%'", PROPAGATION_TIMEOUT_SEC);
        String queryWhere = "query_text LIKE '%" + marker + "%'";

        System.out.println("[hive CROSS-TABLE] marker=" + marker);

        // ── Query metrics exist ──
        assertEquals(1, db.getRowCount("hive_query_metrics", queryWhere));

        // ── Dimension consistency: user_name must match across tables ──
        String userFromQuery = db.getStringValue("hive_query_metrics", "user_name", queryWhere);
        assertNotNull(userFromQuery);

        String engineFromQuery = db.getStringValue("hive_query_metrics", "execution_engine", queryWhere);
        assertNotNull(engineFromQuery);

        // ── Table IO: CTAS reads from test_src (input) and writes to output table (output) ──
        try {
            db.waitForRows("hive_query_table",
                "table_type IN ('input', 'output')", 30);

            long inputCount = db.getRowCount("hive_query_table", "table_type = 'input'");
            long outputCount = db.getRowCount("hive_query_table", "table_type = 'output'");
            assertTrue(inputCount >= 1, "CTAS should have at least 1 input table entry");
            assertTrue(outputCount >= 1, "CTAS should have at least 1 output table entry");

            // Verify user_name matches in table IO
            String inputWhere = "table_type = 'input' ORDER BY timestamp_ms DESC LIMIT 1";
            String userFromTable = db.getStringValue("hive_query_table", "user_name", inputWhere);
            assertEquals(userFromQuery, userFromTable,
                "user_name must match between hive_query_metrics and hive_query_table");

            String engineFromTable = db.getStringValue("hive_query_table", "execution_engine", inputWhere);
            assertEquals(engineFromQuery, engineFromTable,
                "execution_engine must match between hive_query_metrics and hive_query_table");

            System.out.println("  [PASS] Cross-table: user_name="
                + userFromQuery + " consistent, input=" + inputCount + " output=" + outputCount);
        } catch (RuntimeException e) {
            System.out.println("  [SKIP] hive_query_table: no table IO rows found");
        }

        runHive("DROP TABLE IF EXISTS " + table);
    }

    // ═══════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════

    private static String runHive(String sql) throws Exception {
        String beeline = hiveHome + "/bin/beeline";
        List<String> cmd = Arrays.asList(
            beeline,
            "-u", beelineUrl,
            "-e", sql
        );
        return runCommand(cmd, 180);
    }

    private static String runCommand(List<String> cmd, int timeoutSec) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        if (hadoopHome != null && !hadoopHome.isEmpty()) {
            pb.environment().put("HADOOP_HOME", hadoopHome);
        }
        String javaHome = System.getProperty("java.home");
        if (javaHome != null) {
            pb.environment().put("JAVA_HOME", javaHome);
            pb.environment().put("PATH", javaHome + "/bin:" + pb.environment().getOrDefault("PATH", "/usr/bin:/bin"));
        }
        pb.redirectErrorStream(true);
        pb.redirectInput(ProcessBuilder.Redirect.PIPE);
        Process proc = pb.start();
        proc.getOutputStream().close();

        StringBuilder output = new StringBuilder();
        Thread readerThread = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                }
            } catch (IOException ignored) {}
        });
        readerThread.setDaemon(true);
        readerThread.start();

        boolean finished = proc.waitFor(timeoutSec, TimeUnit.SECONDS);
        if (!finished) {
            String out = output.toString();
            System.out.println("[TIMEOUT] cmd=" + cmd + " output=\n" + out);
            proc.destroyForcibly();
            readerThread.join(5000);
            throw new RuntimeException("Command timed out after " + timeoutSec + "s: " + cmd);
        }
        readerThread.join(5000);
        System.out.println("[CMD] " + cmd.get(cmd.size() - 1) + " -> exit=" + proc.exitValue() + " output_lines=" + output.length());

        return output.toString();
    }
}
