package x.mg.metrics.integration;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Bare-metal infrastructure smoke tests.
 * Verifies all required backend services are running (Docker containers,
 * Hadoop/YARN, History Server) before other IT tests execute.
 *
 * Prerequisites:
 *   - Docker containers: otel-collector, kafka, mysql
 *   - Hadoop YARN running (optional, detected via port 8088)
 *   - Hadoop History Server running (optional, detected via port 19888)
 */
@Tag("integration")
class InfrastructureIT {

    @Test
    void testOTelCollectorReachable() throws Exception {
        boolean reachable = isTcpPortOpen("localhost", 4317, 5000);
        assertTrue(reachable, "OTel Collector should be reachable on port 4317");
        System.out.println("[PASS] OTel Collector reachable on port 4317");
    }

    @Test
    void testKafkaContainerRunning() throws Exception {
        String output = dockerPs();
        assertTrue(output.contains("kafka"), "Kafka container should be running. docker ps: " + output);
        System.out.println("[PASS] Kafka container running");
    }

    @Test
    void testMySQLContainerRunning() throws Exception {
        String output = dockerPs();
        assertTrue(output.contains("mysql"), "MySQL container should be running. docker ps: " + output);
        System.out.println("[PASS] MySQL container running");
    }

    @Test
    void testOTelCollectorContainerRunning() throws Exception {
        String output = dockerPs();
        assertTrue(output.contains("otel-collector") || output.contains("otel"),
            "OTel Collector container should be running. docker ps: " + output);
        System.out.println("[PASS] OTel Collector container running");
    }

    @Test
    void testHadoopHistoryServerReachable() throws Exception {
        String hadoopHome = findHadoopHome();
        assumeTrue(hadoopHome != null, "No Hadoop installation found");

        int code = httpGetCode("http://localhost:19888/ws/v1/history/mapreduce/jobs");
        // 200 = jobs listed, 404 = no jobs yet but server is up
        assertTrue(code == 200 || code == 404,
            "History Server should be reachable on port 19888 (got " + code + ")");
        System.out.println("[PASS] History Server reachable (HTTP " + code + ")");
    }

    @Test
    void testYarnResourceManagerReachable() throws Exception {
        String hadoopHome = findHadoopHome();
        assumeTrue(hadoopHome != null, "No Hadoop installation found");

        int code = httpGetCode("http://localhost:8088/ws/v1/cluster/info");
        // 200 = RM running
        assumeTrue(code == 200,
            "YARN ResourceManager not reachable on port 8088 (got " + code + "). YARN may not be running.");
        System.out.println("[PASS] YARN ResourceManager reachable (HTTP " + code + ")");
    }

    // ═══════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════

    private static String dockerPs() throws Exception {
        ProcessBuilder pb = new ProcessBuilder("docker", "ps", "--format", "{{.Names}}");
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String output;
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            output = r.lines().collect(java.util.stream.Collectors.joining("\n"));
        }
        p.waitFor(10, TimeUnit.SECONDS);
        return output;
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

    private static boolean isTcpPortOpen(String host, int port, int timeoutMs) {
        try (java.net.Socket socket = new java.net.Socket()) {
            socket.connect(new java.net.InetSocketAddress(host, port), timeoutMs);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
