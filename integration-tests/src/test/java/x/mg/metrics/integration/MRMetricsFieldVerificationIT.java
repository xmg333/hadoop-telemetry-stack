package x.mg.metrics.integration;

import org.junit.jupiter.api.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Strict end-to-end integration test for MR job and task metrics.
 * Submits real MapReduce WordCount jobs and verifies EVERY column is populated.
 *
 * WordCount characteristics used for assertions:
 *   - Reads input text → map_input_records > 0
 *   - Produces map output → map_output_records > 0, map_output_bytes > 0
 *   - Shuffles to reducers → reduce_input_records > 0, reduce_shuffle_bytes > 0
 *   - Writes output → reduce_output_records > 0, hdfs_bytes_written > 0
 *   - Small input → typically 1-2 maps, 1 reduce
 *
 * Prerequisites:
 *   - Hadoop installation with YARN/Local mode running
 *   - MR Collector polling History Server (provides per-task counters via recordTask())
 *   - OTel Collector, Kafka, Flink Consumer, MySQL running
 *   - HADOOP_HOME env var set
 */
@Tag("integration")
class MRMetricsFieldVerificationIT {

    private static final String MYSQL_HOST = System.getenv().getOrDefault("MYSQL_HOST", "localhost");
    private static final String MYSQL_PORT = System.getenv().getOrDefault("MYSQL_PORT", "3306");
    private static final String MYSQL_USER = System.getenv().getOrDefault("MYSQL_USER", "root");
    private static final String MYSQL_PASSWORD = System.getenv().getOrDefault("MYSQL_PASSWORD", "root123");

    private static final long PROPAGATION_TIMEOUT_SEC = 180;

    private static MetricsVerificationHelper db;
    private static String hadoopHome;

    @BeforeAll
    static void setUp() throws Exception {
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
        assumeTrue(hadoopHome != null && !hadoopHome.isEmpty(),
            "No Hadoop installation found. Set HADOOP_HOME.");

        try {
            ProcessBuilder pb = new ProcessBuilder(hadoopHome + "/bin/hadoop", "fs", "-ls", "/");
            pb.redirectErrorStream(true);
            Process proc = pb.start();
            boolean finished = proc.waitFor(15, TimeUnit.SECONDS);
            assumeTrue(finished && proc.exitValue() == 0,
                "HDFS not reachable. Start HDFS and YARN first.");
        } catch (Exception e) {
            assumeTrue(false, "HDFS not reachable: " + e.getMessage());
        }

        db = new MetricsVerificationHelper(MYSQL_HOST, Integer.parseInt(MYSQL_PORT),
            "telemetry", MYSQL_USER, MYSQL_PASSWORD);

        System.out.println("Hadoop Home: " + hadoopHome);
        System.out.println("MySQL: " + MYSQL_HOST + ":" + MYSQL_PORT);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (db != null) db.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // mr_job_metrics — ALL columns verified
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testMRJobMetrics_AllColumnsPopulated() throws Exception {
        String jobName = "it-mr-job-" + System.currentTimeMillis();
        MRJobResult result = submitMRWordCount(jobName);

        System.out.println("Waiting for mr_job_metrics: " + result.jobId);

        String foundJobId = db.waitForMetric("mr_job_metrics", "job_id",
            "job_id = '" + result.jobId + "'", PROPAGATION_TIMEOUT_SEC);
        String where = "job_id = '" + result.jobId + "'";

        System.out.println("[mr_job_metrics] job_id=" + foundJobId);

        assertEquals(1, db.getRowCount("mr_job_metrics", where),
            "Should have exactly 1 mr_job_metrics row per job");

        // ── ALL dimension columns: non-null, non-empty ──
        db.assertDimensionColumns("mr_job_metrics", where,
            "job_id", "job_name", "user_name", "state", "queue");

        assertEquals("SUCCEEDED",
            db.getStringValue("mr_job_metrics", "state", where));

        // ── ALL timing columns: positive ──
        db.assertMetricColumnsPositive("mr_job_metrics", where,
            "start_time_ms", "finish_time_ms", "elapsed_time_ms");

        Double startTime = db.getDoubleValue("mr_job_metrics", "start_time_ms", where);
        Double finishTime = db.getDoubleValue("mr_job_metrics", "finish_time_ms", where);
        assertTrue(finishTime > startTime,
            "finish_time_ms (" + finishTime + ") should be > start_time_ms (" + startTime + ")");

        // ── ALL IO counter columns: non-negative, NOT NULL ──
        db.assertMetricColumnsNonNegative("mr_job_metrics", where,
            "hdfs_bytes_read", "hdfs_bytes_written",
            "file_bytes_read", "file_bytes_written",
            "map_input_records", "map_output_records", "map_output_bytes",
            "reduce_input_records", "reduce_output_records", "reduce_shuffle_bytes",
            "spilled_records",
            "cpu_time_ms", "gc_time_ms",
            "physical_memory_bytes", "virtual_memory_bytes", "committed_heap_bytes");

        // ── WordCount-specific: these MUST be > 0 ──
        db.assertMetricColumnsPositive("mr_job_metrics", where,
            "hdfs_bytes_read", "hdfs_bytes_written",
            "map_input_records", "map_output_records", "map_output_bytes",
            "reduce_input_records", "reduce_output_records", "reduce_shuffle_bytes",
            "spilled_records", "cpu_time_ms",
            "launched_maps", "launched_reduces",
            "maps_duration_ms", "reduces_duration_ms");

        System.out.println("  [PASS] mr_job_metrics: all "
            + "hdfs_read=" + db.getDoubleValue("mr_job_metrics", "hdfs_bytes_read", where).longValue()
            + ", map_in=" + db.getDoubleValue("mr_job_metrics", "map_input_records", where).longValue()
            + ", cpu=" + db.getDoubleValue("mr_job_metrics", "cpu_time_ms", where).longValue()
            + "ms, maps=" + db.getDoubleValue("mr_job_metrics", "launched_maps", where).intValue()
            + ", reduces=" + db.getDoubleValue("mr_job_metrics", "launched_reduces", where).intValue());
    }

    // ═══════════════════════════════════════════════════════════════
    // mr_task_metrics MAP — ALL columns verified
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testMRTaskMetrics_MAP_AllColumnsPopulated() throws Exception {
        String jobName = "it-mr-map-" + System.currentTimeMillis();
        MRJobResult result = submitMRWordCount(jobName);

        db.waitForRows("mr_task_metrics", "job_id = '" + result.jobId + "'",
            PROPAGATION_TIMEOUT_SEC);
        String where = "job_id = '" + result.jobId + "' AND task_type = 'MAP'";

        System.out.println("[mr_task_metrics MAP] job_id=" + result.jobId);

        long mapCount = db.getRowCount("mr_task_metrics", where);
        assertTrue(mapCount >= 1, "Should have at least 1 MAP task, got " + mapCount);

        // ── ALL dimension columns: non-null, non-empty ──
        db.assertDimensionColumns("mr_task_metrics", where,
            "task_id", "task_type", "job_id", "job_name", "user_name", "state");

        assertEquals("MAP", db.getStringValue("mr_task_metrics", "task_type", where));
        assertEquals("SUCCEEDED", db.getStringValue("mr_task_metrics", "state", where));

        // ── task_id cross-validation ──
        String taskId = db.getStringValue("mr_task_metrics", "task_id", where);
        assertNotNull(taskId);
        assertTrue(taskId.contains("_m_"),
            "MAP task_id must contain '_m_', got: " + taskId);
        assertTrue(taskId.matches("attempt_\\d+_\\d+_m_\\d+_\\d+"),
            "task_id must match Hadoop attempt pattern: " + taskId);

        // ── ALL metric columns: non-negative, NOT NULL ──
        db.assertMetricColumnsNonNegative("mr_task_metrics", where,
            "hdfs_bytes_read", "hdfs_bytes_written",
            "file_bytes_read", "file_bytes_written",
            "map_input_records", "map_output_records", "map_output_bytes",
            "reduce_input_records", "reduce_output_records", "reduce_shuffle_bytes",
            "spilled_records", "cpu_time_ms", "gc_time_ms",
            "duration_ms", "success_count",
            "hdfs_read_ops", "hdfs_write_ops", "hdfs_large_read_ops");

        // ── MAP-specific: MUST be positive for WordCount ──
        db.assertMetricColumnsPositive("mr_task_metrics", where,
            "hdfs_bytes_read",
            "map_input_records", "map_output_records", "map_output_bytes",
            "cpu_time_ms", "duration_ms", "success_count");

        System.out.println("  [PASS] MAP task: taskId=" + taskId
            + ", hdfs_read=" + db.getDoubleValue("mr_task_metrics", "hdfs_bytes_read", where).longValue()
            + ", map_in=" + db.getDoubleValue("mr_task_metrics", "map_input_records", where).longValue()
            + ", map_out=" + db.getDoubleValue("mr_task_metrics", "map_output_records", where).longValue()
            + ", cpu=" + db.getDoubleValue("mr_task_metrics", "cpu_time_ms", where).longValue());
    }

    // ═══════════════════════════════════════════════════════════════
    // mr_task_metrics REDUCE — ALL columns verified
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testMRTaskMetrics_REDUCE_AllColumnsPopulated() throws Exception {
        String jobName = "it-mr-reduce-" + System.currentTimeMillis();
        MRJobResult result = submitMRWordCount(jobName);

        db.waitForRows("mr_task_metrics", "job_id = '" + result.jobId + "'",
            PROPAGATION_TIMEOUT_SEC);
        String where = "job_id = '" + result.jobId + "' AND task_type = 'REDUCE'";

        System.out.println("[mr_task_metrics REDUCE] job_id=" + result.jobId);

        long reduceCount = db.getRowCount("mr_task_metrics", where);
        assertTrue(reduceCount >= 1, "Should have at least 1 REDUCE task, got " + reduceCount);

        // ── ALL dimension columns: non-null, non-empty ──
        db.assertDimensionColumns("mr_task_metrics", where,
            "task_id", "task_type", "job_id", "job_name", "user_name", "state");

        assertEquals("REDUCE", db.getStringValue("mr_task_metrics", "task_type", where));
        assertEquals("SUCCEEDED", db.getStringValue("mr_task_metrics", "state", where));

        // ── task_id cross-validation ──
        String taskId = db.getStringValue("mr_task_metrics", "task_id", where);
        assertNotNull(taskId);
        assertTrue(taskId.contains("_r_"),
            "REDUCE task_id must contain '_r_', got: " + taskId);
        assertFalse(taskId.contains("_m_"),
            "REDUCE task_id must NOT contain '_m_' (container reuse bug), got: " + taskId);
        assertTrue(taskId.matches("attempt_\\d+_\\d+_r_\\d+_\\d+"),
            "task_id must match Hadoop attempt pattern: " + taskId);

        // ── ALL metric columns: non-negative, NOT NULL ──
        db.assertMetricColumnsNonNegative("mr_task_metrics", where,
            "hdfs_bytes_read", "hdfs_bytes_written",
            "file_bytes_read", "file_bytes_written",
            "map_input_records", "map_output_records", "map_output_bytes",
            "reduce_input_records", "reduce_output_records", "reduce_shuffle_bytes",
            "spilled_records", "cpu_time_ms", "gc_time_ms",
            "duration_ms", "success_count",
            "hdfs_read_ops", "hdfs_write_ops", "hdfs_large_read_ops");

        // ── REDUCE-specific: MUST be positive for WordCount ──
        db.assertMetricColumnsPositive("mr_task_metrics", where,
            "hdfs_bytes_written",
            "reduce_input_records", "reduce_output_records", "reduce_shuffle_bytes",
            "file_bytes_read", "file_bytes_written",
            "spilled_records",
            "cpu_time_ms", "duration_ms", "success_count");

        System.out.println("  [PASS] REDUCE task: taskId=" + taskId
            + ", hdfs_write=" + db.getDoubleValue("mr_task_metrics", "hdfs_bytes_written", where).longValue()
            + ", reduce_in=" + db.getDoubleValue("mr_task_metrics", "reduce_input_records", where).longValue()
            + ", shuffle=" + db.getDoubleValue("mr_task_metrics", "reduce_shuffle_bytes", where).longValue()
            + ", file_write=" + db.getDoubleValue("mr_task_metrics", "file_bytes_written", where).longValue());
    }

    // ═══════════════════════════════════════════════════════════════
    // Cross-table consistency
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testMR_CrossTableConsistency() throws Exception {
        String jobName = "it-mr-xref-" + System.currentTimeMillis();
        MRJobResult result = submitMRWordCount(jobName);

        db.waitForRows("mr_task_metrics", "job_id = '" + result.jobId + "'",
            PROPAGATION_TIMEOUT_SEC);

        String jobWhere = "job_id = '" + result.jobId + "'";
        String taskWhere = "job_id = '" + result.jobId + "'";

        // ── Dimension consistency across tables ──
        String jobNameFromJob = db.getStringValue("mr_job_metrics", "job_name", jobWhere);
        String jobNameFromTask = db.getStringValue("mr_task_metrics", "job_name", taskWhere);
        assertEquals(jobNameFromJob, jobNameFromTask,
            "job_name must match between mr_job_metrics and mr_task_metrics");

        String userFromJob = db.getStringValue("mr_job_metrics", "user_name", jobWhere);
        String userFromTask = db.getStringValue("mr_task_metrics", "user_name", taskWhere);
        assertEquals(userFromJob, userFromTask,
            "user_name must match between mr_job_metrics and mr_task_metrics");

        // ── Must have both MAP and REDUCE tasks ──
        long mapCount = db.getRowCount("mr_task_metrics", taskWhere + " AND task_type = 'MAP'");
        long reduceCount = db.getRowCount("mr_task_metrics", taskWhere + " AND task_type = 'REDUCE'");
        assertTrue(mapCount >= 1, "Should have at least 1 MAP task");
        assertTrue(reduceCount >= 1, "Should have at least 1 REDUCE task");

        System.out.println("  [PASS] Cross-table: job_name/user consistent, MAP=" + mapCount + " REDUCE=" + reduceCount);
    }

    // ═══════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════

    private static class MRJobResult {
        final String jobId;
        final String output;

        MRJobResult(String jobId, String output) {
            this.jobId = jobId;
            this.output = output;
        }
    }

    private MRJobResult submitMRWordCount(String jobName) throws Exception {
        String hadoop = hadoopHome + "/bin/hadoop";
        String inputPath = "/tmp/it-mr/" + jobName + "/input";
        String outputPath = "/tmp/it-mr/" + jobName + "/output";

        runCommand(Arrays.asList(hadoop, "fs", "rm", "-R", "-skipTrash",
            "/tmp/it-mr/" + jobName), 30);
        runCommand(Arrays.asList(hadoop, "fs", "mkdir", "-p", inputPath), 30);
        runCommand(Arrays.asList("bash", "-c",
            "echo 'hello world hello spark telemetry metrics verify field population test word count data hadoop mapreduce yarn hdfs' | "
            + hadoop + " fs -put -f - " + inputPath + "/data.txt"), 30);

        File libDir = new File(hadoopHome, "share/hadoop/mapreduce");
        File[] jars = libDir.listFiles((d, name) ->
            name.startsWith("hadoop-mapreduce-examples-") && name.endsWith(".jar"));
        assumeTrue(jars != null && jars.length > 0,
            "No hadoop-mapreduce-examples JAR found in " + libDir);
        String examplesJar = jars[0].getAbsolutePath();

        String output = runCommand(Arrays.asList(
            hadoop, "jar", examplesJar,
            "wordcount",
            "-D", "mapreduce.job.name=" + jobName,
            inputPath, outputPath
        ), 120);

        Pattern p = Pattern.compile("job_\\d+_\\d+");
        Matcher m = p.matcher(output);
        assertTrue(m.find(), "Should find job ID in output:\n" + output);

        return new MRJobResult(m.group(), output);
    }

    private String runCommand(List<String> cmd, int timeoutSec) throws Exception {
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
