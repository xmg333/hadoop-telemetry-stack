package x.mg.metrics.integration;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Bare-metal Hadoop cluster integration tests.
 * Verifies Hadoop/YARN/HistoryServer services via HTTP REST API,
 * and MR Collector polling + metric export on the bare-metal node.
 *
 * Prerequisites:
 *   - Hadoop installation (HADOOP_HOME or /opt/hadoop-*)
 *   - YARN running (ResourceManager on port 8088)
 *   - History Server running (port 19888)
 *   - OTel Collector, Kafka, Flink Consumer, MySQL running
 */
@Tag("integration")
class HadoopClusterIT {

    private static final String MYSQL_HOST = System.getenv().getOrDefault("MYSQL_HOST", "localhost");
    private static final String MYSQL_PORT = System.getenv().getOrDefault("MYSQL_PORT", "3306");
    private static final String MYSQL_USER = System.getenv().getOrDefault("MYSQL_USER", "root");
    private static final String MYSQL_PASSWORD = System.getenv().getOrDefault("MYSQL_PASSWORD", "root123");
    private static final String OTEL_ENDPOINT = System.getenv().getOrDefault("OTEL_ENDPOINT", "http://localhost:4317");
    private static final long PROPAGATION_TIMEOUT_SEC = 120;

    private static String hadoopHome;
    private static String javaHome;
    private static String mrCollectorJar;
    private static MetricsVerificationHelper db;

    @BeforeAll
    static void setUp() throws Exception {
        hadoopHome = findHadoopHome();
        assumeTrue(hadoopHome != null, "No Hadoop installation found. Set HADOOP_HOME.");

        javaHome = findJava8();
        assumeTrue(javaHome != null, "No Java 8 found. Set JAVA_HOME.");

        // Find MR Collector dist JAR
        File jar = findMrCollectorJar();
        assumeTrue(jar != null, "MR Collector dist JAR not found. Run: mvn clean package -DskipTests");
        mrCollectorJar = jar.getAbsolutePath();

        db = new MetricsVerificationHelper(MYSQL_HOST, Integer.parseInt(MYSQL_PORT),
            "telemetry", MYSQL_USER, MYSQL_PASSWORD);

        System.out.println("Hadoop: " + hadoopHome);
        System.out.println("Java: " + javaHome);
        System.out.println("MR Collector: " + mrCollectorJar);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (db != null) db.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // Infrastructure checks
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testHistoryServerApiReturnsJobs() throws Exception {
        int code = httpGetCode("http://localhost:19888/ws/v1/history/mapreduce/jobs");
        assertTrue(code == 200 || code == 404,
            "History Server should be reachable on port 19888 (got " + code + ")");
        System.out.println("[PASS] History Server reachable (HTTP " + code + ")");
    }

    @Test
    void testYarnResourceManagerUp() throws Exception {
        int code = httpGetCode("http://localhost:8088/ws/v1/cluster/info");
        assumeTrue(code == 200, "YARN ResourceManager not reachable (got " + code + ")");
        System.out.println("[PASS] YARN ResourceManager running (HTTP " + code + ")");
    }

    // ═══════════════════════════════════════════════════════════════
    // MR Collector: submit job → poll History Server → verify MySQL
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testMRCollectorPollsAndExportsMetrics() throws Exception {
        // 1. Submit a WordCount MR job
        String jobName = "it-collector-" + System.currentTimeMillis();
        String jobId = submitMRWordCount(jobName);
        assertNotNull(jobId, "Should get a job ID from MR submission");
        System.out.println("Submitted MR job: " + jobId);

        // 2. Verify job appears in History Server
        String jobsJson = httpGet("http://localhost:19888/ws/v1/history/mapreduce/jobs");
        assertTrue(jobsJson.contains("job"), "History Server should list jobs");

        // 3. Run MR Collector to poll and export
        String collectorOutput = runMRCollector();
        System.out.println("MR Collector output: " + collectorOutput.substring(0, Math.min(500, collectorOutput.length())));

        // 4. Verify mr_job_metrics in MySQL
        String where = "job_name = '" + jobName + "'";
        String dbJobId = db.waitForMetric("mr_job_metrics", "job_id", where, PROPAGATION_TIMEOUT_SEC);
        assertNotNull(dbJobId, "mr_job_metrics should have row for job " + jobName);

        // Verify key fields
        db.assertDimensionColumns("mr_job_metrics", "job_id = '" + dbJobId + "'",
            "job_id", "job_name", "user_name", "queue", "job_status");
        db.assertMetricColumnsPositive("mr_job_metrics", "job_id = '" + dbJobId + "'",
            "duration_ms", "maps_total", "reduces_total");

        System.out.println("[PASS] MR Collector: job_id=" + dbJobId + " in MySQL");
    }

    // ═══════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════

    private String submitMRWordCount(String jobName) throws Exception {
        List<String> env = buildEnv();
        String hadoopBin = hadoopHome + "/bin/hadoop";

        // Prepare HDFS input
        String inputPath = "/tmp/" + jobName + "/input";
        String outputPath = "/tmp/" + jobName + "/output";
        runCommand(env, hadoopBin, "fs", "-rm", "-r", "-f", outputPath);
        runCommand(env, hadoopBin, "fs", "-rm", "-r", "-f", inputPath);
        runCommand(env, hadoopBin, "fs", "-mkdir", "-p", inputPath);

        // Write input data locally, then upload
        File inputFile = File.createTempFile("mr-input-", ".txt");
        inputFile.deleteOnExit();
        try (PrintWriter pw = new PrintWriter(inputFile)) {
            for (int i = 0; i < 100; i++) pw.println("hello world foo bar baz qux");
        }
        runCommand(env, hadoopBin, "fs", "-put", "-f", inputFile.getAbsolutePath(), inputPath + "/");

        // Find examples JAR
        File examplesJar = findExamplesJar();
        assumeTrue(examplesJar != null, "hadoop-mapreduce-examples JAR not found in " + hadoopHome);

        // Submit WordCount
        String output = runCommand(env, 180,
            hadoopBin, "jar", examplesJar.getAbsolutePath(), "wordcount",
            "-D", "mapreduce.job.name=" + jobName,
            inputPath, outputPath);

        // Extract job ID
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("job_\\d+_\\d+").matcher(output);
        return m.find() ? m.group() : null;
    }

    private String runMRCollector() throws Exception {
        // Run MR Collector with a single poll cycle (exit after first poll)
        List<String> cmd = new ArrayList<>();
        cmd.add(javaHome + "/bin/java");
        cmd.add("-jar");
        cmd.add(mrCollectorJar);
        cmd.add("--history-server-url=http://localhost:19888");
        cmd.add("--otel-exporter-endpoint=" + OTEL_ENDPOINT);
        cmd.add("--poll-interval-seconds=0");  // Single poll then exit
        return runCommand(buildEnv(), 120, cmd.toArray(new String[0]));
    }

    private List<String> buildEnv() {
        List<String> env = new ArrayList<>();
        env.add("JAVA_HOME=" + javaHome);
        env.add("HADOOP_HOME=" + hadoopHome);
        env.add("PATH=" + javaHome + "/bin:" + hadoopHome + "/bin:/usr/bin:/bin");
        env.add("HADOOP_CONF_DIR=" + hadoopHome + "/etc/hadoop");
        return env;
    }

    private File findExamplesJar() {
        File libDir = new File(hadoopHome, "share/hadoop/mapreduce");
        if (libDir.isDirectory()) {
            File[] jars = libDir.listFiles((d, name) ->
                name.startsWith("hadoop-mapreduce-examples-") && name.endsWith(".jar")
                    && !name.contains("sources") && !name.contains("javadoc"));
            if (jars != null && jars.length > 0) return jars[0];
        }
        return null;
    }

    static String findHadoopHome() {
        String env = System.getenv().getOrDefault("HADOOP_HOME", "");
        if (!env.isEmpty() && new File(env, "bin/hadoop").exists()) return env;
        File opt = new File("/opt");
        if (opt.isDirectory()) {
            File[] dirs = opt.listFiles((d, name) ->
                name.startsWith("hadoop") && new File(d, name + "/bin/hadoop").exists());
            if (dirs != null && dirs.length > 0) return dirs[0].getAbsolutePath();
        }
        return null;
    }

    static String findJava8() {
        String env = System.getenv().getOrDefault("JAVA_HOME", "");
        if (!env.isEmpty() && new File(env, "bin/java").exists()) return env;
        File opt = new File("/opt");
        if (opt.isDirectory()) {
            File[] dirs = opt.listFiles((d, name) ->
                name.startsWith("jdk") && new File(d, name + "/bin/java").exists());
            if (dirs != null) {
                for (File dir : dirs) {
                    if (dir.getName().contains("8")) return dir.getAbsolutePath();
                }
            }
        }
        return null;
    }

    static File findMrCollectorJar() {
        File[] roots = {new File(".."), new File(".")};
        for (File root : roots) {
            File distDir = new File(root, "mapreduce-collector/mr-telemetry-dist/target");
            if (distDir.isDirectory()) {
                File[] jars = distDir.listFiles((d, name) ->
                    name.startsWith("mr-telemetry-dist-") && name.endsWith(".jar") && !name.contains("original"));
                if (jars != null && jars.length > 0) return jars[0];
            }
        }
        return null;
    }

    private String runCommand(List<String> extraEnv, int timeoutSec, String... cmd) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.environment().clear();
        for (String e : extraEnv) {
            String[] parts = e.split("=", 2);
            pb.environment().put(parts[0], parts[1]);
        }
        pb.redirectErrorStream(true);
        Process p = pb.start();
        StringBuilder output = new StringBuilder();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String line;
            while ((line = r.readLine()) != null) output.append(line).append("\n");
        }
        boolean finished = p.waitFor(timeoutSec, TimeUnit.SECONDS);
        if (!finished) {
            p.destroyForcibly();
            throw new RuntimeException("Command timed out after " + timeoutSec + "s: " + String.join(" ", cmd));
        }
        return output.toString();
    }

    private String runCommand(List<String> extraEnv, String... cmd) throws Exception {
        return runCommand(extraEnv, 60, cmd);
    }

    private static int httpGetCode(String urlStr) {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            return conn.getResponseCode();
        } catch (Exception e) {
            return -1;
        }
    }

    private static String httpGet(String urlStr) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(10000);
        try (BufferedReader r = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            return r.lines().collect(Collectors.joining("\n"));
        }
    }
}
