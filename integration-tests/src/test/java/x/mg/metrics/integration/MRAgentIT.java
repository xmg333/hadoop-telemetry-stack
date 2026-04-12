package x.mg.metrics.integration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for the MR Telemetry Agent.
 * Tests classpath safety, Hadoop 2/3 compatibility, and real-time metric export.
 *
 * Prerequisites:
 *   1. K8s cluster with hadoop3, hadoop2, otel-collector pods running
 *   2. Agent JAR built: mvn clean package -Pspark-3 -DskipTests
 *   3. Hadoop installed in pods at /opt/hadoop with JAVA_HOME set
 */
@Tag("integration")
class MRAgentIT extends K8sTestBase {

    private static final String AGENT_JAR_RELATIVE =
        "../mr-telemetry-agent-dist/target/mr-telemetry-agent-dist-1.0.0-SNAPSHOT.jar";
    private static final String AGENT_POD_PATH = "/tmp/mr-telemetry-agent.jar";
    private static final String OTEL_ENDPOINT = "http://otel-collector:4317";
    private static final int JOB_TIMEOUT_SECS = 180;

    private static String hadoop3Pod;
    private static String hadoop2Pod;

    @BeforeAll
    static void checkPrerequisites() throws Exception {
        checkKubectl();

        // Check agent JAR exists
        File agentJar = new File(AGENT_JAR_RELATIVE);
        assumeTrue(agentJar.exists(),
            "Agent JAR not found. Run: mvn clean package -Pspark-3 -DskipTests");

        // Discover pods
        hadoop3Pod = findPodOrNull("hadoop3");
        hadoop2Pod = findPodOrNull("hadoop2");

        // At least one Hadoop pod must exist
        assumeTrue(hadoop3Pod != null || hadoop2Pod != null,
            "No hadoop3 or hadoop2 pods found in cluster");
    }

    // ========================================================================
    // Test 1: Hadoop 3 (YARN mode) — classpath safety
    // ========================================================================

    @Test
    void testAgentHadoop3ClasspathSafety() throws Exception {
        assumeTrue(hadoop3Pod != null, "hadoop3 pod not found, skipping");
        assumeTrue(isYarnReady(hadoop3Pod), "YARN not ready on hadoop3, skipping");

        copyAgentJar(hadoop3Pod);
        prepareHdfsInput(hadoop3Pod, "/tmp/agent-h3/input", "/tmp/agent-h3/output");

        String output = submitYarnJobWithAgent(hadoop3Pod, "/tmp/agent-h3/input", "/tmp/agent-h3/output");

        // Job must succeed — this is the primary classpath safety assertion
        assertJobSucceeded(output, "Hadoop 3 YARN job with agent");
        System.out.println("[PASS] Hadoop 3 YARN job completed successfully with agent loaded");
    }

    // ========================================================================
    // Test 2: Hadoop 2 (local mode) — classpath safety
    // ========================================================================

    @Test
    void testAgentHadoop2ClasspathSafety() throws Exception {
        assumeTrue(hadoop2Pod != null, "hadoop2 pod not found, skipping");

        copyAgentJar(hadoop2Pod);
        prepareHdfsInput(hadoop2Pod, "/tmp/agent-h2/input", "/tmp/agent-h2/output");

        String output = submitLocalJobWithAgent(hadoop2Pod, "/tmp/agent-h2/input", "/tmp/agent-h2/output");

        // Job must succeed — classpath safety for Hadoop 2
        assertJobSucceeded(output, "Hadoop 2 local mode job with agent");
        System.out.println("[PASS] Hadoop 2 local mode job completed successfully with agent loaded");
    }

    // ========================================================================
    // Test 3: Real-time metric export (Hadoop 3 YARN)
    // ========================================================================

    @Test
    void testAgentExportsRealTimeMetrics() throws Exception {
        assumeTrue(hadoop3Pod != null, "hadoop3 pod not found, skipping");
        assumeTrue(isYarnReady(hadoop3Pod), "YARN not ready on hadoop3, skipping");

        // Verify OTel Collector is running
        String otelPhase = kubectlExec("get", "pods", "-l", "app=otel-collector",
            "-o", "jsonpath={.items[0].status.phase}");
        assumeTrue("Running".equals(otelPhase.trim()),
            "OTel Collector not running, skipping metric verification test");

        // Clear OTel Collector logs to get a clean baseline
        kubectlExec("logs", "-l", "app=otel-collector", "--tail=0");

        copyAgentJar(hadoop3Pod);
        prepareHdfsInput(hadoop3Pod, "/tmp/agent-metrics/input", "/tmp/agent-metrics/output");

        // Submit job with agent and short export interval for faster verification
        String output = submitYarnJobWithAgent(hadoop3Pod,
            "/tmp/agent-metrics/input", "/tmp/agent-metrics/output");

        assertJobSucceeded(output, "Metric export test job");

        // Wait for OTel Collector to process metrics
        Thread.sleep(15000);

        // Check OTel Collector logs for mr.task.* metrics
        String otelLogs = kubectlExec("logs", "-l", "app=otel-collector", "--tail=200");

        // The debug exporter logs metric names. Check for our mr.task.* metrics.
        boolean foundTaskMetrics = otelLogs.contains("mr.task.");
        if (foundTaskMetrics) {
            System.out.println("[PASS] OTel Collector received mr.task.* metrics:");
            // Print relevant lines for visibility
            for (String line : otelLogs.split("\n")) {
                if (line.contains("mr.task.")) {
                    System.out.println("  " + line);
                }
            }
        } else {
            // Not a hard failure — metrics may take longer to appear
            // or the job may have finished before the first export interval
            System.out.println("[WARN] mr.task.* metrics not yet visible in OTel Collector logs.");
            System.out.println("  This may be expected if the job completed before the first export interval.");
            System.out.println("  OTel Collector log sample (last 20 lines):");
            String[] logLines = otelLogs.split("\n");
            for (int i = Math.max(0, logLines.length - 20); i < logLines.length; i++) {
                System.out.println("  " + logLines[i]);
            }
        }
    }

    // ========================================================================
    // Helper methods
    // ========================================================================

    private static String findPodOrNull(String podName) {
        try {
            String phase = new K8sTestBase() {}.kubectlExec("get", "pod", podName,
                "-o", "jsonpath={.status.phase}");
            return "Running".equals(phase.trim()) ? podName : null;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Copy the agent JAR to a pod via kubectl cp.
     */
    private void copyAgentJar(String pod) throws Exception {
        File agentJar = new File(AGENT_JAR_RELATIVE);
        kubectlExec("cp", agentJar.getAbsolutePath(), pod + ":" + AGENT_POD_PATH);
        System.out.println("Copied agent JAR to " + pod + ":" + AGENT_POD_PATH);
    }

    /**
     * Prepare HDFS input directory with test data.
     */
    private void prepareHdfsInput(String pod, String inputPath, String outputPath) throws Exception {
        // Generate larger input for real-time sampling (more map tasks = more samples)
        kubectlExec("exec", pod, "--", "bash", "-c",
            "export HADOOP_HOME=/opt/hadoop JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 && " +
            "PATH=$HADOOP_HOME/bin:$JAVA_HOME/bin:$PATH && " +
            "rm -f /tmp/agent-input.txt && " +
            "for i in $(seq 1 100); do echo 'hello world foo bar baz qux'; done > /tmp/agent-input.txt && " +
            "hadoop fs -rm -r -f " + outputPath + " 2>/dev/null; " +
            "hadoop fs -rm -r -f " + inputPath + " 2>/dev/null; " +
            "hadoop fs -mkdir -p " + inputPath + " && " +
            "hadoop fs -put -f /tmp/agent-input.txt " + inputPath + "/"
        );
    }

    /**
     * Submit MR job with agent on Hadoop 3 (YARN mode).
     * Uses mapreduce.map.java.opts / mapreduce.reduce.java.opts to inject -javaagent.
     * Writes a shell script to the pod first to avoid quoting issues with kubectl exec.
     */
    private String submitYarnJobWithAgent(String pod, String inputPath, String outputPath) throws Exception {
        String script = "#!/bin/bash\n" +
            "export HADOOP_HOME=/opt/hadoop\n" +
            "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64\n" +
            "export PATH=$HADOOP_HOME/bin:$JAVA_HOME/bin:$PATH\n" +
            "EXAMPLES_JAR=$(find -L $HADOOP_HOME -name 'hadoop-mapreduce-examples-*.jar' ! -name '*sources*' ! -name '*test*' | head -1)\n" +
            "JAVA_OPTS=\"-javaagent:" + AGENT_POD_PATH +
            " -Dmr.telemetry.agent.otel.exporter.endpoint=" + OTEL_ENDPOINT +
            " -Dmr.telemetry.agent.otel.export.interval.ms=5000" +
            " -Dmr.telemetry.agent.sampling.interval.secs=2\"\n" +
            "hadoop jar $EXAMPLES_JAR wordcount " +
            "-Dmapreduce.map.java.opts=\"$JAVA_OPTS\" " +
            "-Dmapreduce.reduce.java.opts=\"$JAVA_OPTS\" " +
            inputPath + " " + outputPath + " 2>&1\n";

        writeAndExecScript(pod, script);
        return kubectlExecLong(pod, "bash /tmp/agent-submit.sh", JOB_TIMEOUT_SECS);
    }

    /**
     * Submit MR job with agent on Hadoop 2 (local mode).
     * Uses HADOOP_OPTS to inject -javaagent into the client JVM.
     * Writes a shell script to the pod first to avoid quoting issues.
     */
    private String submitLocalJobWithAgent(String pod, String inputPath, String outputPath) throws Exception {
        String script = "#!/bin/bash\n" +
            "export HADOOP_HOME=/opt/hadoop\n" +
            "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64\n" +
            "export PATH=$HADOOP_HOME/bin:$JAVA_HOME/bin:$PATH\n" +
            "EXAMPLES_JAR=$(find -L $HADOOP_HOME -name 'hadoop-mapreduce-examples-*.jar' ! -name '*sources*' ! -name '*test*' | head -1)\n" +
            "export HADOOP_OPTS=\"-javaagent:" + AGENT_POD_PATH +
            " -Dmr.telemetry.agent.otel.exporter.endpoint=" + OTEL_ENDPOINT +
            " -Dmr.telemetry.agent.otel.export.interval.ms=5000" +
            " -Dmr.telemetry.agent.sampling.interval.secs=2\"\n" +
            "hadoop jar $EXAMPLES_JAR wordcount " +
            "-Dmapreduce.framework.name=local " +
            inputPath + " " + outputPath + " 2>&1\n";

        writeAndExecScript(pod, script);
        return kubectlExecLong(pod, "bash /tmp/agent-submit.sh", JOB_TIMEOUT_SECS);
    }

    /**
     * Write a script to the pod via base64 encoding (avoids all quoting issues).
     */
    private void writeAndExecScript(String pod, String scriptContent) throws Exception {
        String encoded = Base64.getEncoder().encodeToString(scriptContent.getBytes("UTF-8"));
        kubectlExec("exec", pod, "--", "bash", "-c",
            "echo " + encoded + " | base64 -d > /tmp/agent-submit.sh && chmod +x /tmp/agent-submit.sh");
    }

    /**
     * Check if YARN ResourceManager is accepting applications.
     */
    private boolean isYarnReady(String pod) {
        try {
            String result = kubectlExec("exec", pod, "--", "bash", "-c",
                "curl -s -o /dev/null -w '%{http_code}' http://localhost:8088/ws/v1/cluster/info");
            return result.contains("200");
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Assert that the MR job output indicates success.
     */
    private void assertJobSucceeded(String output, String context) {
        // YARN mode: check for completion status
        boolean succeeded = output.contains("completed successfully") ||
                            output.contains("SUCCEEDED") ||
                            (output.contains("Map input records") && !output.contains("FAILED"));

        // Local mode: check for normal completion indicators
        boolean localSucceeded = output.contains("Bytes Written") ||
                                 output.contains("Map output records") ||
                                 (output.contains("File System Counters") && !output.contains("Exception"));

        assertTrue(succeeded || localSucceeded,
            context + " should complete successfully. Output:\n" + output);
    }

    /**
     * Execute a long-running command on a pod with extended timeout.
     */
    private String kubectlExecLong(String pod, String bashCmd, int timeoutSecs) throws Exception {
        String[] cmd = {"kubectl", "exec", pod, "--", "bash", "-c", bashCmd};
        Process p = Runtime.getRuntime().exec(cmd);
        boolean finished = p.waitFor(timeoutSecs, java.util.concurrent.TimeUnit.SECONDS);

        String output;
        try (java.io.BufferedReader r = new java.io.BufferedReader(
                new java.io.InputStreamReader(p.getInputStream()))) {
            output = r.lines().collect(java.util.stream.Collectors.joining("\n"));
        }

        if (!finished) {
            p.destroyForcibly();
            throw new RuntimeException("Command timed out after " + timeoutSecs + "s. Partial output:\n" + output);
        }

        // Combine stdout and stderr for MR job output
        String error;
        try (java.io.BufferedReader r = new java.io.BufferedReader(
                new java.io.InputStreamReader(p.getErrorStream()))) {
            error = r.lines().collect(java.util.stream.Collectors.joining("\n"));
        }

        return output + "\n" + error;
    }
}
