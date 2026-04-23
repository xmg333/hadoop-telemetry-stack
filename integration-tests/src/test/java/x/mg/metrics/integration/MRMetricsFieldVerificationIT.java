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
 * Strict end-to-end integration test for MR job metrics.
 * Submits real MapReduce WordCount jobs and verifies EXACT metric values
 * and row counts per job_id in MySQL.
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
 *   - MR Collector polling History Server
 *   - OTel Collector, Kafka, Flink Consumer, MySQL running
 *   - HADOOP_HOME env var set
 */
@Tag("integration")
class MRMetricsFieldVerificationIT {

    private static final String MYSQL_HOST = System.getenv().getOrDefault("MYSQL_HOST", "localhost");
    private static final String MYSQL_PORT = System.getenv().getOrDefault("MYSQL_PORT", "3306");
    private static final String MYSQL_USER = System.getenv().getOrDefault("MYSQL_USER", "root");
    private static final String MYSQL_PASSWORD = System.getenv().getOrDefault("MYSQL_PASSWORD", "root123");

    private static final long PROPAGATION_TIMEOUT_SEC = 120;

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
                    hadoopHome = dirs[0].getAbsolutePath();
                }
            }
        }
        assumeTrue(hadoopHome != null && !hadoopHome.isEmpty(),
            "No Hadoop installation found. Set HADOOP_HOME.");

        // Check HDFS is reachable
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
    // mr_job_metrics — strict per-job_id verification
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testMRJobMetrics_AllFieldsPopulated_StrictChecks() throws Exception {
        String jobName = "it-mr-job-" + System.currentTimeMillis();
        MRJobResult result = submitMRWordCount(jobName);

        System.out.println("Waiting for mr_job_metrics: " + result.jobId);

        String foundJobId = db.waitForMetric("mr_job_metrics", "job_id",
            "job_id = '" + result.jobId + "'", PROPAGATION_TIMEOUT_SEC);
        String where = "job_id = '" + result.jobId + "'";

        System.out.println("[mr_job_metrics] job_id=" + foundJobId);

        // ── Exactly 1 row for this job ──
        long jobRows = db.getRowCount("mr_job_metrics", where);
        assertEquals(1, jobRows, "Should have exactly 1 mr_job_metrics row per job");

        // ── Dimension columns: must be non-null and non-empty ──
        db.assertDimensionColumns("mr_job_metrics", where,
            "job_id", "job_name", "user_name", "state", "queue");

        // job_name must match what we set
        String actualJobName = db.getStringValue("mr_job_metrics", "job_name", where);
        assertNotNull(actualJobName, "job_name should not be NULL");
        assertTrue(actualJobName.contains(jobName),
            "job_name should contain '" + jobName + "', got: " + actualJobName);

        // state should be SUCCEEDED
        String state = db.getStringValue("mr_job_metrics", "state", where);
        assertEquals("SUCCEEDED", state, "Job state should be SUCCEEDED");

        // ── Timing columns: must be positive ──
        db.assertMetricColumnsPositive("mr_job_metrics", where,
            "start_time_ms", "finish_time_ms", "elapsed_time_ms");

        // ── start < finish ──
        Double startTime = db.getDoubleValue("mr_job_metrics", "start_time_ms", where);
        Double finishTime = db.getDoubleValue("mr_job_metrics", "finish_time_ms", where);
        assertNotNull(startTime, "start_time_ms should not be NULL");
        assertNotNull(finishTime, "finish_time_ms should not be NULL");
        assertTrue(finishTime > startTime,
            "finish_time_ms (" + finishTime + ") should be > start_time_ms (" + startTime + ")");

        // ── IO: WordCount reads input text → must have positive read metrics ──
        db.assertMetricColumnsPositive("mr_job_metrics", where,
            "hdfs_bytes_read", "map_input_records");

        // ── Map output: WordCount produces (word, 1) pairs → must be positive ──
        db.assertMetricColumnsPositive("mr_job_metrics", where,
            "map_output_records", "map_output_bytes");

        // ── Reduce: WordCount has a reducer → must have positive shuffle + output ──
        db.assertMetricColumnsPositive("mr_job_metrics", where,
            "reduce_input_records", "reduce_output_records",
            "reduce_shuffle_bytes");

        // ── HDFS write: output written to HDFS ──
        db.assertMetricColumnsPositive("mr_job_metrics", where,
            "hdfs_bytes_written");

        // ── Resource metrics: non-negative ──
        db.assertMetricColumnsNonNegative("mr_job_metrics", where,
            "file_bytes_read", "file_bytes_written",
            "spilled_records",
            "cpu_time_ms", "gc_time_ms",
            "physical_memory_bytes", "virtual_memory_bytes");

        // ── Task counts: WordCount has at least 1 map and 1 reduce ──
        db.assertMetricColumnsPositive("mr_job_metrics", where,
            "launched_maps", "launched_reduces");

        // ── Duration: maps and reduces must have positive duration ──
        db.assertMetricColumnsPositive("mr_job_metrics", where,
            "maps_duration_ms", "reduces_duration_ms");

        System.out.println("  [PASS] mr_job_metrics: state=" + state
            + ", maps=" + db.getDoubleValue("mr_job_metrics", "launched_maps", where).intValue()
            + ", reduces=" + db.getDoubleValue("mr_job_metrics", "launched_reduces", where).intValue()
            + ", hdfs_read=" + db.getDoubleValue("mr_job_metrics", "hdfs_bytes_read", where).longValue()
            + ", map_input=" + db.getDoubleValue("mr_job_metrics", "map_input_records", where).longValue());
    }

    // ═══════════════════════════════════════════════════════════════
    // mr_task_metrics — strict per-task verification
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testMRTaskMetrics_AllFieldsPopulated_StrictChecks() throws Exception {
        String jobName = "it-mr-task-" + System.currentTimeMillis();
        MRJobResult result = submitMRWordCount(jobName);

        db.waitForRows("mr_task_metrics", "job_id = '" + result.jobId + "'",
            PROPAGATION_TIMEOUT_SEC);
        String where = "job_id = '" + result.jobId + "'";

        System.out.println("[mr_task_metrics] job_id=" + result.jobId);

        // ── Must have at least 1 MAP and 1 REDUCE task ──
        long mapCount = db.getRowCount("mr_task_metrics",
            where + " AND task_type = 'MAP'");
        long reduceCount = db.getRowCount("mr_task_metrics",
            where + " AND task_type = 'REDUCE'");
        long totalTasks = db.getRowCount("mr_task_metrics", where);

        assertTrue(mapCount >= 1, "Should have at least 1 MAP task, got " + mapCount);
        assertTrue(reduceCount >= 1, "Should have at least 1 REDUCE task, got " + reduceCount);
        assertEquals(mapCount + reduceCount, totalTasks,
            "Total tasks should equal MAP + REDUCE counts");

        // ── Dimension columns for ALL tasks ──
        db.assertDimensionColumns("mr_task_metrics", where,
            "task_id", "task_type", "job_id", "job_name", "user_name", "queue");

        // ── MAP tasks: must have positive duration ──
        String mapWhere = where + " AND task_type = 'MAP'";
        db.assertMetricColumnsPositive("mr_task_metrics", mapWhere, "duration_ms");

        // MAP: must have read input (wordcount reads from HDFS)
        db.assertMetricColumnsPositive("mr_task_metrics", mapWhere,
            "hdfs_bytes_read", "map_input_records", "map_output_records", "map_output_bytes");

        // ── REDUCE tasks: must have positive duration ──
        String reduceWhere = where + " AND task_type = 'REDUCE'";
        db.assertMetricColumnsPositive("mr_task_metrics", reduceWhere, "duration_ms");

        // REDUCE: must have shuffle input + output
        db.assertMetricColumnsPositive("mr_task_metrics", reduceWhere,
            "reduce_input_records", "reduce_output_records", "reduce_shuffle_bytes");

        // REDUCE: writes to HDFS
        db.assertMetricColumnsPositive("mr_task_metrics", reduceWhere,
            "hdfs_bytes_written");

        // ── Resource counters — non-negative for all tasks ──
        db.assertMetricColumnsNonNegative("mr_task_metrics", where,
            "file_bytes_read", "file_bytes_written",
            "spilled_records",
            "cpu_time_ms", "gc_time_ms");

        // ── Cross-check: map_input_records > 0 (we wrote actual words) ──
        Double mapInputRecords = db.getDoubleValue("mr_task_metrics", "map_input_records",
            mapWhere + " LIMIT 1");
        assertNotNull(mapInputRecords, "map_input_records should not be NULL for MAP task");
        assertTrue(mapInputRecords > 0,
            "MAP task should have map_input_records > 0, got " + mapInputRecords);

        // ── Cross-check: reduce_output_records > 0 (wordcount outputs unique words) ──
        Double reduceOutputRecords = db.getDoubleValue("mr_task_metrics", "reduce_output_records",
            reduceWhere + " LIMIT 1");
        assertNotNull(reduceOutputRecords, "reduce_output_records should not be NULL for REDUCE task");
        assertTrue(reduceOutputRecords > 0,
            "REDUCE task should have reduce_output_records > 0, got " + reduceOutputRecords);

        System.out.println("  [PASS] mr_task_metrics: MAP=" + mapCount + " REDUCE=" + reduceCount
            + " total=" + totalTasks);
    }

    // ═══════════════════════════════════════════════════════════════
    // Cross-table consistency: job ↔ task
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testMR_CrossTableConsistency() throws Exception {
        String jobName = "it-mr-xref-" + System.currentTimeMillis();
        MRJobResult result = submitMRWordCount(jobName);

        db.waitForRows("mr_task_metrics", "job_id = '" + result.jobId + "'",
            PROPAGATION_TIMEOUT_SEC);

        String jobWhere = "job_id = '" + result.jobId + "'";
        String taskWhere = "job_id = '" + result.jobId + "'";

        // ── job_name must match across tables ──
        String jobNameFromJob = db.getStringValue("mr_job_metrics", "job_name", jobWhere);
        String jobNameFromTask = db.getStringValue("mr_task_metrics", "job_name", taskWhere + " LIMIT 1");
        assertEquals(jobNameFromJob, jobNameFromTask,
            "job_name must match between mr_job_metrics and mr_task_metrics");

        // ── user_name must match across tables ──
        String userFromJob = db.getStringValue("mr_job_metrics", "user_name", jobWhere);
        String userFromTask = db.getStringValue("mr_task_metrics", "user_name", taskWhere + " LIMIT 1");
        assertEquals(userFromJob, userFromTask,
            "user_name must match between mr_job_metrics and mr_task_metrics");

        // ── launched_maps in job = number of MAP tasks in task table ──
        Double launchedMaps = db.getDoubleValue("mr_job_metrics", "launched_maps", jobWhere);
        long mapTasks = db.getRowCount("mr_task_metrics", taskWhere + " AND task_type = 'MAP'");
        assertEquals(launchedMaps.intValue(), mapTasks,
            "launched_maps in job_metrics should equal MAP task count");

        // ── launched_reduces in job = number of REDUCE tasks in task table ──
        Double launchedReduces = db.getDoubleValue("mr_job_metrics", "launched_reduces", jobWhere);
        long reduceTasks = db.getRowCount("mr_task_metrics", taskWhere + " AND task_type = 'REDUCE'");
        assertEquals(launchedReduces.intValue(), reduceTasks,
            "launched_reduces in job_metrics should equal REDUCE task count");

        System.out.println("  [PASS] Cross-table: job_name/user match, maps=" + mapTasks
            + " reduces=" + reduceTasks);
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

        // Clean up previous runs
        runCommand(Arrays.asList(hadoop, "fs", "rm", "-rf", "-skipTrash",
            "/tmp/it-mr/" + jobName), 30);

        // Create input with known content
        runCommand(Arrays.asList(hadoop, "fs", "mkdir", "-p", inputPath), 30);
        runCommand(Arrays.asList("bash", "-c",
            "echo 'hello world hello spark telemetry metrics verify field population test word count' | "
            + hadoop + " fs -put -f - " + inputPath + "/data.txt"), 30);

        // Find examples JAR
        File libDir = new File(hadoopHome, "share/hadoop/mapreduce");
        File[] jars = libDir.listFiles((d, name) ->
            name.startsWith("hadoop-mapreduce-examples-") && name.endsWith(".jar"));
        assumeTrue(jars != null && jars.length > 0,
            "No hadoop-mapreduce-examples JAR found in " + libDir);
        String examplesJar = jars[0].getAbsolutePath();

        // Submit wordcount
        String output = runCommand(Arrays.asList(
            hadoop, "jar", examplesJar,
            "wordcount",
            "-D", "mapreduce.job.name=" + jobName,
            inputPath, outputPath
        ), 120);

        // Extract job ID
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
