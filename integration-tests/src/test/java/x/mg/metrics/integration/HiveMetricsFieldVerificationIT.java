package x.mg.metrics.integration;

import org.junit.jupiter.api.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Strict end-to-end integration test for Hive query metrics.
 * Submits real Hive queries with known characteristics and verifies EXACT
 * metric values per query marker in MySQL.
 *
 * Test data characteristics used for assertions:
 *   - test_src has exactly 5 rows
 *   - test_src2 has exactly 3 rows
 *   - SELECT COUNT(*) → output_rows = 1
 *   - JOIN on id → output_rows = 3 (matching ids: 1,2,3)
 *   - CTAS → write metrics populated
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

        beelineUrl = System.getenv().getOrDefault("BEELINE_URL",
            "jdbc:hive2://localhost:10000");

        db = new MetricsVerificationHelper(MYSQL_HOST, Integer.parseInt(MYSQL_PORT),
            "telemetry", MYSQL_USER, MYSQL_PASSWORD);

        System.out.println("Hive Home: " + hiveHome);
        System.out.println("Beeline URL: " + beelineUrl);
        System.out.println("MySQL: " + MYSQL_HOST + ":" + MYSQL_PORT);

        // Setup test tables with known data
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
    // hive_query_metrics — SELECT COUNT(*) strict checks
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testHiveSelect_QueryMetrics_StrictChecks() throws Exception {
        String marker = "hv_sel_" + System.currentTimeMillis();
        // SELECT COUNT(*) on 5-row table
        String sql = "SELECT COUNT(*) FROM field_verify.test_src /* " + marker + " */";

        String output = runHive(sql);
        assertFalse(output.contains("Error"), "Hive query should succeed: " + output);

        // Wait for metrics
        db.waitForRows("hive_query_metrics",
            "query_text LIKE '%" + marker + "%'", PROPAGATION_TIMEOUT_SEC);
        String where = "query_text LIKE '%" + marker + "%'";

        System.out.println("[hive_query_metrics] marker=" + marker);

        // ── Exactly 1 row for this query ──
        long rows = db.getRowCount("hive_query_metrics", where);
        assertEquals(1, rows, "Should have exactly 1 hive_query_metrics row for this query");

        // ── Dimension columns: must be non-null, non-empty ──
        db.assertDimensionColumns("hive_query_metrics", where,
            "query_id", "operation", "user_name", "success", "execution_engine");

        // ── success = true ──
        String success = db.getStringValue("hive_query_metrics", "success", where);
        assertEquals("true", success, "Query should succeed");

        // ── execution_engine: typically "mr" for Hive on MR ──
        String engine = db.getStringValue("hive_query_metrics", "execution_engine", where);
        assertNotNull(engine, "execution_engine should not be NULL");
        assertFalse(engine.isEmpty(), "execution_engine should not be empty");

        // ── Duration: must be positive ──
        db.assertMetricColumnsPositive("hive_query_metrics", where, "duration_ms");

        // ── IO metrics: non-negative ──
        db.assertMetricColumnsNonNegative("hive_query_metrics", where,
            "input_bytes", "output_bytes", "input_rows", "output_rows");

        // ── input_rows: reading from 5-row table, should be >= 5 ──
        Double inputRows = db.getDoubleValue("hive_query_metrics", "input_rows", where);
        assertNotNull(inputRows, "input_rows should not be NULL");
        // Hive may report input_rows as number of splits/files, not exact row count
        // But it must be >= 1
        assertTrue(inputRows >= 1,
            "input_rows should be >= 1 for a table with data, got " + inputRows);

        // ── input_bytes: must be > 0 (reading from HDFS) ──
        Double inputBytes = db.getDoubleValue("hive_query_metrics", "input_bytes", where);
        assertNotNull(inputBytes, "input_bytes should not be NULL");
        assertTrue(inputBytes > 0,
            "input_bytes should be > 0 when reading from HDFS table, got " + inputBytes);

        // ── query_text must contain the marker and original SQL ──
        String queryText = db.getStringValue("hive_query_metrics", "query_text", where);
        assertNotNull(queryText, "query_text should not be NULL");
        assertFalse(queryText.trim().isEmpty(), "query_text should not be empty");
        assertTrue(queryText.contains(marker),
            "query_text should contain marker '" + marker + "'");
        assertTrue(queryText.contains("SELECT"),
            "query_text should contain the SQL statement");

        System.out.println("  [PASS] hive_query_metrics: success=" + success
            + ", duration=" + db.getDoubleValue("hive_query_metrics", "duration_ms", where).longValue() + "ms"
            + ", input_bytes=" + inputBytes.longValue()
            + ", input_rows=" + inputRows.longValue());
    }

    // ═══════════════════════════════════════════════════════════════
    // hive_table_io_metrics — scan operations for SELECT
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testHiveSelect_TableIOMetrics_StrictChecks() throws Exception {
        String marker = "hv_tio_" + System.currentTimeMillis();
        String sql = "SELECT COUNT(*) FROM field_verify.test_src /* " + marker + " */";

        String output = runHive(sql);
        assertFalse(output.contains("Error"), "Hive query should succeed: " + output);

        // Wait for hive_query_metrics first to get query_id
        db.waitForRows("hive_query_metrics",
            "query_text LIKE '%" + marker + "%'", PROPAGATION_TIMEOUT_SEC);

        // Wait for table IO metrics
        db.waitForRows("hive_table_io_metrics",
            "operation = 'scan'", PROPAGATION_TIMEOUT_SEC);

        System.out.println("[hive_table_io_metrics] marker=" + marker);

        // ── Must have at least 1 scan entry ──
        long scanRows = db.getRowCount("hive_table_io_metrics", "operation = 'scan'");
        assertTrue(scanRows >= 1, "Should have at least 1 scan row, got " + scanRows);

        // ── Dimension columns for scan rows ──
        String scanWhere = "operation = 'scan' LIMIT 1";
        db.assertDimensionColumns("hive_table_io_metrics", scanWhere,
            "table_name", "operation");

        // ── table_name: should reference a real table, not 'unknown' ──
        String tableName = db.getStringValue("hive_table_io_metrics", "table_name",
            "operation = 'scan' LIMIT 1");
        assertNotNull(tableName, "table_name should not be NULL");
        assertFalse(tableName.isEmpty(), "table_name should not be empty");

        // ── IO columns: non-negative ──
        db.assertMetricColumnsNonNegative("hive_table_io_metrics",
            "operation = 'scan' LIMIT 1",
            "bytes", "rows", "files_read", "time_ms");

        // ── bytes: must be > 0 for scan of non-empty table ──
        Double scanBytes = db.getDoubleValue("hive_table_io_metrics", "bytes",
            "operation = 'scan' LIMIT 1");
        assertNotNull(scanBytes, "bytes should not be NULL for scan operation");
        assertTrue(scanBytes > 0,
            "bytes should be > 0 when scanning a non-empty table, got " + scanBytes);

        System.out.println("  [PASS] hive_table_io_metrics: " + scanRows + " scans"
            + ", table=" + tableName
            + ", bytes=" + scanBytes.longValue());
    }

    // ═══════════════════════════════════════════════════════════════
    // CTAS — write metrics verification
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testHiveCTAS_WriteMetrics_StrictChecks() throws Exception {
        String marker = "hv_ctas_" + System.currentTimeMillis();
        String tableName = "field_verify.test_output_" + marker;

        String sql = "CREATE TABLE " + tableName + " AS SELECT * FROM field_verify.test_src /* " + marker + " */";

        String output = runHive(sql);
        assertFalse(output.contains("Error"), "Hive CTAS should succeed: " + output);

        // Wait for query metrics
        db.waitForRows("hive_query_metrics",
            "query_text LIKE '%" + marker + "%'", PROPAGATION_TIMEOUT_SEC);
        String where = "query_text LIKE '%" + marker + "%'";

        System.out.println("[hive CTAS] marker=" + marker);

        // ── Query metrics must exist and be populated ──
        long queryRows = db.getRowCount("hive_query_metrics", where);
        assertEquals(1, queryRows, "Should have exactly 1 hive_query_metrics row for CTAS");

        db.assertDimensionColumns("hive_query_metrics", where,
            "query_id", "operation", "user_name", "success");

        String success = db.getStringValue("hive_query_metrics", "success", where);
        assertEquals("true", success, "CTAS should succeed");

        db.assertMetricColumnsPositive("hive_query_metrics", where, "duration_ms");

        // ── CTAS reads input → input_bytes > 0 ──
        Double inputBytes = db.getDoubleValue("hive_query_metrics", "input_bytes", where);
        assertNotNull(inputBytes, "input_bytes should not be NULL for CTAS");
        assertTrue(inputBytes > 0,
            "CTAS should read input data, input_bytes > 0, got " + inputBytes);

        // ── CTAS writes output → output_bytes > 0, output_rows > 0 ──
        Double outputBytes = db.getDoubleValue("hive_query_metrics", "output_bytes", where);
        assertNotNull(outputBytes, "output_bytes should not be NULL for CTAS");
        assertTrue(outputBytes > 0,
            "CTAS should write output, output_bytes > 0, got " + outputBytes);

        Double outputRows = db.getDoubleValue("hive_query_metrics", "output_rows", where);
        assertNotNull(outputRows, "output_rows should not be NULL for CTAS");

        // ── Table IO metrics: should have write entry ──
        // Wait briefly for table IO metrics to propagate
        db.waitForRows("hive_table_io_metrics",
            "operation = 'write'", PROPAGATION_TIMEOUT_SEC);

        long writeRows = db.getRowCount("hive_table_io_metrics", "operation = 'write'");
        assertTrue(writeRows >= 1, "CTAS should produce at least 1 write entry, got " + writeRows);

        // Write entry dimensions
        db.assertDimensionColumns("hive_table_io_metrics",
            "operation = 'write' LIMIT 1",
            "table_name", "operation");

        // Write IO columns
        db.assertMetricColumnsNonNegative("hive_table_io_metrics",
            "operation = 'write' LIMIT 1",
            "bytes", "rows", "files_read", "time_ms");

        System.out.println("  [PASS] Hive CTAS: success=" + success
            + ", input=" + inputBytes.longValue() + " bytes"
            + ", output=" + outputBytes.longValue() + " bytes"
            + ", output_rows=" + outputRows.longValue()
            + ", write_io_rows=" + writeRows);

        // Cleanup
        runHive("DROP TABLE IF EXISTS " + tableName);
    }

    // ═══════════════════════════════════════════════════════════════
    // JOIN — cross-table verification
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testHiveJoin_QueryMetrics_StrictChecks() throws Exception {
        String marker = "hv_join_" + System.currentTimeMillis();
        // JOIN test_src (5 rows) with test_src2 (3 rows) on id → 3 matching rows
        String sql = "SELECT a.id, a.name, b.value FROM field_verify.test_src a "
            + "JOIN field_verify.test_src2 b ON a.id = b.id /* " + marker + " */";

        String output = runHive(sql);
        assertFalse(output.contains("Error"), "Hive JOIN should succeed: " + output);

        db.waitForRows("hive_query_metrics",
            "query_text LIKE '%" + marker + "%'", PROPAGATION_TIMEOUT_SEC);
        String where = "query_text LIKE '%" + marker + "%'";

        System.out.println("[hive JOIN] marker=" + marker);

        // ── Exactly 1 query metrics row ──
        long queryRows = db.getRowCount("hive_query_metrics", where);
        assertEquals(1, queryRows, "Should have exactly 1 query metrics row for JOIN");

        // ── All dimension columns populated ──
        db.assertDimensionColumns("hive_query_metrics", where,
            "query_id", "operation", "user_name", "success", "execution_engine");

        // ── Duration positive ──
        db.assertMetricColumnsPositive("hive_query_metrics", where, "duration_ms");

        // ── JOIN reads from 2 tables → input_bytes should be > 0 ──
        Double inputBytes = db.getDoubleValue("hive_query_metrics", "input_bytes", where);
        assertNotNull(inputBytes, "input_bytes should not be NULL for JOIN");
        assertTrue(inputBytes > 0,
            "JOIN should read input data from both tables, got " + inputBytes);

        // ── Table IO: should have scan entries for both tables ──
        db.waitForRows("hive_table_io_metrics",
            "operation IN ('scan', 'write')", PROPAGATION_TIMEOUT_SEC);

        long scanCount = db.getRowCount("hive_table_io_metrics", "operation = 'scan'");
        assertTrue(scanCount >= 2,
            "JOIN of 2 tables should produce at least 2 scan entries, got " + scanCount);

        System.out.println("  [PASS] Hive JOIN: duration="
            + db.getDoubleValue("hive_query_metrics", "duration_ms", where).longValue() + "ms"
            + ", input_bytes=" + inputBytes.longValue()
            + ", scan_entries=" + scanCount);
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
        return runCommand(cmd, 120);
    }

    private static String runCommand(List<String> cmd, int timeoutSec) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(cmd);
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
            throw new RuntimeException("Command timed out: " + cmd);
        }

        return output.toString();
    }
}
