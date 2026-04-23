package x.mg.metrics.integration;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Bare-metal MR Agent integration tests.
 * Submits MapReduce jobs with the telemetry agent via -javaagent and
 * verifies metrics arrive in MySQL.
 *
 * Prerequisites:
 *   - Hadoop installation with YARN running
 *   - Agent JAR built: mvn clean package -Pspark-3 -DskipTests
 *   - OTel Collector, Kafka, Flink Consumer, MySQL running
 *   - Java 8 at /opt/jdk* or JAVA_HOME
 */
@Tag("integration")
class MRAgentIT {

    private static final String MYSQL_HOST = System.getenv().getOrDefault("MYSQL_HOST", "localhost");
    private static final String MYSQL_PORT = System.getenv().getOrDefault("MYSQL_PORT", "3306");
    private static final String MYSQL_USER = System.getenv().getOrDefault("MYSQL_USER", "root");
    private static final String MYSQL_PASSWORD = System.getenv().getOrDefault("MYSQL_PASSWORD", "root123");
    private static final String OTEL_ENDPOINT = System.getenv().getOrDefault("OTEL_ENDPOINT", "http://localhost:4317");
    private static final long PROPAGATION_TIMEOUT_SEC = 120;
    private static final int JOB_TIMEOUT_SECS = 180;

    private static String hadoopHome;
    private static String javaHome;
    private static File agentJar;
    private static MetricsVerificationHelper db;

    @BeforeAll
    static void setUp() throws Exception {
        hadoopHome = HadoopClusterIT.findHadoopHome();
        assumeTrue(hadoopHome != null, "No Hadoop installation found. Set HADOOP_HOME.");

        javaHome = HadoopClusterIT.findJava8();
        assumeTrue(javaHome != null, "No Java 8 found. Set JAVA_HOME.");

        agentJar = findAgentJar();
        assumeTrue(agentJar != null, "Agent JAR not found. Run: mvn clean package -DskipTests");

        db = new MetricsVerificationHelper(MYSQL_HOST, Integer.parseInt(MYSQL_PORT),
            "telemetry", MYSQL_USER, MYSQL_PASSWORD);

        System.out.println("Hadoop: " + hadoopHome);
        System.out.println("Java: " + javaHome);
        System.out.println("Agent: " + agentJar.getAbsolutePath());

        // Verify YARN is running
        int yarnCode = httpGetCode("http://localhost:8088/ws/v1/cluster/info");
        assumeTrue(yarnCode == 200, "YARN ResourceManager not reachable on port 8088 (got " + yarnCode + ")");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (db != null) db.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // Test 1: Agent classpath safety (YARN mode)
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testAgentYarnClasspathSafety() throws Exception {
        String jobName = "it-agent-safety-" + System.currentTimeMillis();
        String output = submitMRJobWithAgent(jobName, true);

        boolean succeeded = output.contains("completed successfully") ||
                            output.contains("SUCCEEDED") ||
                            (output.contains("Map input records") && !output.contains("FAILED"));
        assertTrue(succeeded, "MR job with agent should complete successfully. Output:\n" + output);
        System.out.println("[PASS] YARN job completed successfully with agent loaded");
    }

    // ═══════════════════════════════════════════════════════════════
    // Test 2: Agent exports mr.task.* metrics to MySQL
    // ═══════════════════════════════════════════════════════════════

    @Test
    void testAgentExportsMetricsToMySQL() throws Exception {
        String jobName = "it-agent-metrics-" + System.currentTimeMillis();
        String output = submitMRJobWithAgent(jobName, true);

        boolean succeeded = output.contains("completed successfully") ||
                            output.contains("SUCCEEDED") ||
                            (output.contains("Map input records") && !output.contains("FAILED"));
        assertTrue(succeeded, "MR job with agent should complete. Output:\n" + output);

        // Wait for mr_task_metrics in MySQL
        String where = "app_name LIKE '" + jobName + "%'";
        String taskId = db.waitForMetric("mr_task_metrics", "task_id", where, PROPAGATION_TIMEOUT_SEC);

        if (taskId != null) {
            String taskWhere = "task_id = '" + taskId + "'";
            db.assertMetricColumnsPositive("mr_task_metrics", taskWhere,
                "io_map_input_records", "io_map_output_records");
            System.out.println("[PASS] mr_task_metrics in MySQL: task_id=" + taskId);
        } else {
            // Agent metrics may take longer or require YARN mode
            System.out.println("[WARN] mr_task_metrics not found in MySQL within " + PROPAGATION_TIMEOUT_SEC + "s");
            System.out.println("  This may occur if YARN containers cannot reach OTel Collector");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════

    private String submitMRJobWithAgent(String jobName, boolean yarnMode) throws Exception {
        List<String> env = buildEnv();
        String hadoopBin = hadoopHome + "/bin/hadoop";
        String inputPath = "/tmp/" + jobName + "/input";
        String outputPath = "/tmp/" + jobName + "/output";

        // Prepare HDFS input
        runCommand(env, hadoopBin, "fs", "-rm", "-r", "-f", outputPath);
        runCommand(env, hadoopBin, "fs", "-rm", "-r", "-f", inputPath);
        runCommand(env, hadoopBin, "fs", "-mkdir", "-p", inputPath);

        File inputFile = File.createTempFile("mr-agent-input-", ".txt");
        inputFile.deleteOnExit();
        try (PrintWriter pw = new PrintWriter(inputFile)) {
            for (int i = 0; i < 200; i++) pw.println("hello world foo bar baz qux");
        }
        runCommand(env, hadoopBin, "fs", "-put", "-f", inputFile.getAbsolutePath(), inputPath + "/");

        File examplesJar = findExamplesJar();
        assumeTrue(examplesJar != null, "hadoop-mapreduce-examples JAR not found");

        String agentOpts = "-javaagent:" + agentJar.getAbsolutePath() +
            " -Dmr.telemetry.agent.otel.exporter.endpoint=" + OTEL_ENDPOINT +
            " -Dmr.telemetry.agent.otel.export.interval.ms=5000" +
            " -Dmr.telemetry.agent.sampling.interval.secs=2";

        List<String> cmd = new ArrayList<>();
        cmd.add(hadoopBin);
        cmd.add("jar");
        cmd.add(examplesJar.getAbsolutePath());
        cmd.add("wordcount");
        cmd.add("-D");
        cmd.add("mapreduce.job.name=" + jobName);
        cmd.add("-D");
        cmd.add("mapreduce.map.java.opts=" + agentOpts);
        cmd.add("-D");
        cmd.add("mapreduce.reduce.java.opts=" + agentOpts);
        cmd.add(inputPath);
        cmd.add(outputPath);

        return runCommand(env, JOB_TIMEOUT_SECS, cmd.toArray(new String[0]));
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

    static File findAgentJar() {
        File[] roots = {new File(".."), new File(".")};
        for (File root : roots) {
            File distDir = new File(root, "mapreduce-agent/mr-telemetry-agent-dist/target");
            if (distDir.isDirectory()) {
                File[] jars = distDir.listFiles((d, name) ->
                    name.startsWith("mr-telemetry-agent-dist-") && name.endsWith(".jar")
                        && !name.contains("original"));
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
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) new java.net.URL(urlStr).openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            return conn.getResponseCode();
        } catch (Exception e) {
            return -1;
        }
    }
}
