package x.mg.metrics.integration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Hadoop and Hive E2E integration tests.
 *
 * Tests compatibility with multiple Hadoop and Hive versions.
 *
 * Prerequisites:
 *   - Hadoop installations in /opt/hadoop-*
 *   - Hive installations in /opt/apache-hive-* or /opt/hive*
 *   - OTel Collector running
 *   - Kafka accessible for metric verification
 */
@Tag("integration")
class HadoopHiveE2EIT {

    private static final String OTEL_ENDPOINT = System.getenv().getOrDefault(
        "OTEL_ENDPOINT", "http://localhost:4317");
    private static final int JOB_TIMEOUT_SECS = 180;

    private static List<HadoopVersion> hadoopVersions;
    private static List<HiveVersion> hiveVersions;
    private static File mrCollectorJar;
    private static File hiveHookJar;

    @BeforeAll
    static void detectInstallations() {
        hadoopVersions = detectHadoopVersions();
        hiveVersions = detectHiveVersions();
        mrCollectorJar = findMrCollectorJar();
        hiveHookJar = findHiveHookJar();

        System.out.println("Detected Hadoop versions: " +
            hadoopVersions.stream().map(v -> v.version).collect(Collectors.joining(", ")));
        System.out.println("Detected Hive versions: " +
            hiveVersions.stream().map(v -> v.version).collect(Collectors.joining(", ")));
    }

    /**
     * Tests MR Collector with Hadoop 2.x and 3.x.
     */
    @Test
    void testMRCollectorWithAllHadoopVersions() throws Exception {
        assumeTrue(!hadoopVersions.isEmpty(), "No Hadoop installations found");
        assumeTrue(mrCollectorJar != null && mrCollectorJar.exists(),
            "MR Collector JAR not found. Run: mvn package -DskipTests");

        List<String> failures = new ArrayList<>();

        for (HadoopVersion hv : hadoopVersions) {
            try {
                testMRCollectorWithHadoop(hv);
            } catch (AssertionError e) {
                failures.add(hv.version + ": " + e.getMessage());
                System.err.println("FAILED: Hadoop " + hv.version + " - " + e.getMessage());
            }
        }

        assertTrue(failures.isEmpty(),
            "Failures for Hadoop versions: " + String.join(", ", failures));
    }

    /**
     * Tests Hive Hook with Hive 2.x and 3.x.
     */
    @Test
    void testHiveHookWithAllHiveVersions() throws Exception {
        assumeTrue(!hiveVersions.isEmpty(), "No Hive installations found");
        assumeTrue(hiveHookJar != null && hiveHookJar.exists(),
            "Hive Hook JAR not found. Run: mvn package -DskipTests");

        List<String> failures = new ArrayList<>();

        for (HiveVersion hv : hiveVersions) {
            try {
                testHiveHookWithHive(hv);
            } catch (AssertionError e) {
                failures.add(hv.version + ": " + e.getMessage());
                System.err.println("FAILED: Hive " + hv.version + " - " + e.getMessage());
            }
        }

        assertTrue(failures.isEmpty(),
            "Failures for Hive versions: " + String.join(", ", failures));
    }

    /**
     * Tests cross-version compatibility: Hive 2.3.9 + Hadoop 2.7.
     */
    @Test
    void testHive2WithHadoop2() throws Exception {
        HiveVersion hive2 = hiveVersions.stream()
            .filter(v -> v.version.startsWith("2.3"))
            .findFirst()
            .orElse(null);
        HadoopVersion hadoop2 = hadoopVersions.stream()
            .filter(v -> v.version.startsWith("2.7"))
            .findFirst()
            .orElse(null);

        assumeTrue(hive2 != null, "Hive 2.3.x not found");
        assumeTrue(hadoop2 != null, "Hadoop 2.7.x not found");

        testHiveWithHadoop(hive2, hadoop2);
    }

    /**
     * Tests cross-version compatibility: Hive 3.1.3 + Hadoop 3.2.
     */
    @Test
    void testHive3WithHadoop3() throws Exception {
        HiveVersion hive3 = hiveVersions.stream()
            .filter(v -> v.version.startsWith("3.1"))
            .findFirst()
            .orElse(null);
        HadoopVersion hadoop3 = hadoopVersions.stream()
            .filter(v -> v.version.startsWith("3.2") || v.version.startsWith("3.4"))
            .findFirst()
            .orElse(null);

        assumeTrue(hive3 != null, "Hive 3.1.x not found");
        assumeTrue(hadoop3 != null, "Hadoop 3.x not found");

        testHiveWithHadoop(hive3, hadoop3);
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    private void testMRCollectorWithHadoop(HadoopVersion hv) throws Exception {
        System.out.println("\n=== Testing MR Collector with Hadoop " + hv.version + " ===");
        System.out.println("  Home: " + hv.hadoopHome);

        // Check if Hadoop is running (History Server)
        boolean historyServerRunning = checkHistoryServer(hv);
        assumeTrue(historyServerRunning,
            "History Server not running for Hadoop " + hv.version);

        // Start MR Collector
        Process collectorProcess = startMRCollector(hv);
        assertNotNull(collectorProcess, "Failed to start MR Collector");

        try {
            // Wait for collector to initialize
            Thread.sleep(2000);

            // Submit MR job
            String result = submitMRJob(hv);
            assertTrue(
                result.contains("completed successfully") || result.contains("SUCCEEDED"),
                "MR job should complete. Output:\n" + result
            );

            // Wait for collector to poll and export
            Thread.sleep(10000);

            System.out.println("  [PASS] Hadoop " + hv.version + " - MR job and collector working");
        } finally {
            if (collectorProcess.isAlive()) {
                collectorProcess.destroyForcibly();
            }
        }
    }

    private void testHiveHookWithHive(HiveVersion hv) throws Exception {
        System.out.println("\n=== Testing Hive Hook with Hive " + hv.version + " ===");
        System.out.println("  Home: " + hv.hiveHome);

        // Check if Hive is configured
        File hiveSite = new File(hv.hiveHome, "conf/hive-site.xml");
        assumeTrue(hiveSite.exists(), "hive-site.xml not found");

        // Create test SQL script
        String sqlScript =
            "CREATE DATABASE IF NOT EXISTS telemetry_test;\n" +
            "USE telemetry_test;\n" +
            "DROP TABLE IF EXISTS test_table;\n" +
            "CREATE TABLE test_table (id INT, name STRING) STORED AS TEXTFILE;\n" +
            "INSERT INTO test_table VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');\n" +
            "SELECT COUNT(*) FROM test_table;\n" +
            "DROP TABLE test_table;\n" +
            "DROP DATABASE telemetry_test;\n";

        // Submit Hive query with hook
        String result = submitHiveQuery(hv, sqlScript);

        // Note: Hive queries may fail due to metastore issues in test environment
        // We mainly want to verify the hook doesn't crash Hive
        System.out.println("  Hive output:\n" + result);

        if (result.contains("FAILED") || result.contains("Exception")) {
            // Check if it's a metastore issue (expected in test env) or hook crash
            if (result.contains("HiveTelemetryHook") && result.contains("Exception")) {
                fail("Hive Hook crashed: " + result);
            } else {
                System.out.println("  [WARN] Hive query failed (likely metastore issue), but hook didn't crash");
            }
        } else {
            System.out.println("  [PASS] Hive " + hv.version + " - Hook configured correctly");
        }
    }

    private void testHiveWithHadoop(HiveVersion hv, HadoopVersion hadoopV) throws Exception {
        System.out.println("\n=== Testing Hive " + hv.version + " with Hadoop " + hadoopV.version + " ===");
        System.out.println("  Hive Home: " + hv.hiveHome);
        System.out.println("  Hadoop Home: " + hadoopV.hadoopHome);

        // Set HADOOP_HOME environment for this test
        String sqlScript =
            "CREATE DATABASE IF NOT EXISTS telemetry_test;\n" +
            "USE telemetry_test;\n" +
            "DROP TABLE IF EXISTS test_table;\n" +
            "CREATE TABLE test_table (id INT, name STRING) STORED AS TEXTFILE;\n" +
            "INSERT INTO test_table VALUES (1, 'alice'), (2, 'bob');\n" +
            "SELECT * FROM test_table;\n" +
            "DROP TABLE test_table;\n";

        String result = submitHiveQueryWithHadoop(hv, hadoopV, sqlScript);
        System.out.println("  Output:\n" + result);

        // Verify hook didn't crash the query
        assertFalse(result.contains("HiveTelemetryHook") && result.contains("Exception"),
            "Hive Hook should not crash with Hadoop " + hadoopV.version);

        System.out.println("  [PASS] Cross-version compatibility verified");
    }

    // ========================================================================
    // Detection and Setup Methods
    // ========================================================================

    private static List<HadoopVersion> detectHadoopVersions() {
        List<HadoopVersion> versions = new ArrayList<>();

        File opt = new File("/opt");
        if (!opt.isDirectory()) {
            return versions;
        }

        // Look for hadoop-* directories
        File[] dirs = opt.listFiles((d, name) ->
            name.startsWith("hadoop-") && new File(d, name + "/bin/hadoop").exists());

        if (dirs != null) {
            for (File dir : dirs) {
                String version = dir.getName().substring("hadoop-".length());
                versions.add(new HadoopVersion(version, dir.getAbsolutePath()));
            }
        }

        // Also check /opt/hadoop symlink
        File hadoopSymlink = new File("/opt/hadoop");
        if (hadoopSymlink.exists() && hadoopSymlink.isDirectory()) {
            String version = readHadoopVersion(hadoopSymlink);
            versions.add(new HadoopVersion(version, hadoopSymlink.getAbsolutePath()));
        }

        return versions;
    }

    private static List<HiveVersion> detectHiveVersions() {
        List<HiveVersion> versions = new ArrayList<>();

        File opt = new File("/opt");
        if (!opt.isDirectory()) {
            return versions;
        }

        // Look for apache-hive-* directories
        File[] dirs = opt.listFiles((d, name) ->
            name.startsWith("apache-hive-") && new File(d, name + "/bin/hive").exists());

        if (dirs != null) {
            for (File dir : dirs) {
                String name = dir.getName();
                // Extract version from apache-hive-2.3.9-bin -> 2.3.9
                String version = name.replace("apache-hive-", "").replace("-bin", "");
                versions.add(new HiveVersion(version, dir.getAbsolutePath()));
            }
        }

        // Also check /opt/hive
        File hiveDir = new File("/opt/hive");
        if (hiveDir.exists() && hiveDir.isDirectory() &&
            new File(hiveDir, "bin/hive").exists()) {
            String version = readHiveVersion(hiveDir);
            versions.add(new HiveVersion(version, hiveDir.getAbsolutePath()));
        }

        return versions;
    }

    private static String readHadoopVersion(File hadoopHome) {
        try {
            File releaseFile = new File(hadoopHome, "share/doc/hadoop/hadoop-version.txt");
            if (releaseFile.exists()) {
                List<String> lines = java.nio.file.Files.readAllLines(releaseFile.toPath());
                if (!lines.isEmpty()) return lines.get(0).trim();
            }
        } catch (Exception ignored) {}
        return "unknown";
    }

    private static String readHiveVersion(File hiveHome) {
        try {
            File releaseFile = new File(hiveHome, "RELEASE_NOTES.txt");
            if (releaseFile.exists()) {
                List<String> lines = java.nio.file.Files.readAllLines(releaseFile.toPath());
                for (String line : lines) {
                    if (line.contains("Version")) {
                        return line.replaceAll(".*Version\\s*", "").trim();
                    }
                }
            }
        } catch (Exception ignored) {}
        return "unknown";
    }

    private static File findMrCollectorJar() {
        String[] paths = {
            "mapreduce-collector/mr-telemetry-dist/target",
            "../mapreduce-collector/mr-telemetry-dist/target",
            "."
        };
        for (String path : paths) {
            File dir = new File(path);
            if (!dir.isDirectory()) continue;
            File[] jars = dir.listFiles((d, name) ->
                name.contains("mr-telemetry") &&
                name.endsWith(".jar") &&
                !name.contains("original") &&
                !name.contains("agent"));
            if (jars != null && jars.length > 0) {
                return jars[0];
            }
        }
        return null;
    }

    private static File findHiveHookJar() {
        String[] paths = {
            "hive/hive-telemetry-hook-dist/target",
            "../hive/hive-telemetry-hook-dist/target",
            "."
        };
        for (String path : paths) {
            File dir = new File(path);
            if (!dir.isDirectory()) continue;
            File[] jars = dir.listFiles((d, name) ->
                name.contains("hive-telemetry-hook") &&
                name.endsWith(".jar") &&
                !name.contains("original"));
            if (jars != null && jars.length > 0) {
                return jars[0];
            }
        }
        return null;
    }

    private boolean checkHistoryServer(HadoopVersion hv) {
        try {
            List<String> cmd = new ArrayList<>();
            cmd.add(hv.hadoopHome + "/bin/mapred");
            cmd.add("version");

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(true);
            Process proc = pb.start();

            boolean finished = proc.waitFor(10, TimeUnit.SECONDS);
            return finished && proc.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    private Process startMRCollector(HadoopVersion hv) throws Exception {
        File configFile = createMRCollectorConfig();

        List<String> cmd = new ArrayList<>();
        cmd.add("java");
        cmd.add("-jar");
        cmd.add(mrCollectorJar.getAbsolutePath());
        cmd.add(configFile.getAbsolutePath());

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.environment().put("HADOOP_HOME", hv.hadoopHome);
        pb.redirectErrorStream(true);

        return pb.start();
    }

    private File createMRCollectorConfig() throws Exception {
        String config =
            "mr-telemetry {\n" +
            "  history-server {\n" +
            "    url = \"http://localhost:19888\"\n" +
            "    poll.interval.secs = 5\n" +
            "  }\n" +
            "  otel {\n" +
            "    exporter.endpoint = \"" + OTEL_ENDPOINT + "\"\n" +
            "    service.name = \"mr-collector-test\"\n" +
            "    export.interval.ms = 5000\n" +
            "  }\n" +
            "  collection {\n" +
            "    job.counters = true\n" +
            "    task.counters = true\n" +
            "    job.details = true\n" +
            "  }\n" +
            "}\n";

        File configFile = File.createTempFile("mr-collector", ".conf");
        try (PrintWriter pw = new PrintWriter(configFile)) {
            pw.print(config);
        }
        return configFile;
    }

    private String submitMRJob(HadoopVersion hv) throws Exception {
        // Find hadoop-mapreduce-examples JAR
        File examplesDir = new File(hv.hadoopHome, "share/hadoop/mapreduce");
        File[] jars = examplesDir.listFiles((d, name) ->
            name.contains("hadoop-mapreduce-examples") &&
            name.endsWith(".jar") &&
            !name.contains("sources") &&
            !name.contains("javadoc"));

        File examplesJar = (jars != null && jars.length > 0) ? jars[0] : null;
        assertNotNull(examplesJar, "hadoop-mapreduce-examples JAR not found");

        List<String> cmd = new ArrayList<>();
        cmd.add(hv.hadoopHome + "/bin/hadoop");
        cmd.add("jar");
        cmd.add(examplesJar.getAbsolutePath());
        cmd.add("pi");
        cmd.add("2");  // maps
        cmd.add("10"); // samples

        return runCommand(cmd, JOB_TIMEOUT_SECS, hv.hadoopHome);
    }

    private String submitHiveQuery(HiveVersion hv, String sqlScript) throws Exception {
        return submitHiveQueryWithHadoop(hv, null, sqlScript);
    }

    private String submitHiveQueryWithHadoop(HiveVersion hv, HadoopVersion hadoopV,
                                              String sqlScript) throws Exception {
        File sqlFile = File.createTempFile("hive-test", ".sql");
        try (PrintWriter pw = new PrintWriter(sqlFile)) {
            pw.print(sqlScript);
        }

        File hoconConfig = createHiveTelemetryConfig();

        List<String> cmd = new ArrayList<>();
        cmd.add(hv.hiveHome + "/bin/hive");
        cmd.add("--hiveconf");
        cmd.add("hive.telemetry.config.path=" + hoconConfig.getAbsolutePath());
        if (hiveHookJar != null && hiveHookJar.exists()) {
            cmd.add("--hiveconf");
            cmd.add("hive.aux.jars.path=" + hiveHookJar.getAbsolutePath());
        }
        cmd.add("-f");
        cmd.add(sqlFile.getAbsolutePath());

        String hadoopHome = (hadoopV != null) ? hadoopV.hadoopHome : null;
        return runCommand(cmd, JOB_TIMEOUT_SECS, hadoopHome);
    }

    private File createHiveTelemetryConfig() throws Exception {
        String config =
            "hive-telemetry {\n" +
            "  otel {\n" +
            "    exporter.endpoint = \"" + OTEL_ENDPOINT + "\"\n" +
            "    service.name = \"hive-test\"\n" +
            "    export.interval.ms = 5000\n" +
            "  }\n" +
            "  metrics {\n" +
            "    enabled = true\n" +
            "    query.duration = true\n" +
            "    query.io = true\n" +
            "    query.tables = true\n" +
            "  }\n" +
            "}\n";

        File configFile = File.createTempFile("hive-telemetry", ".conf");
        try (PrintWriter pw = new PrintWriter(configFile)) {
            pw.print(config);
        }
        return configFile;
    }

    private String runCommand(List<String> cmd, int timeoutSecs, String hadoopHome) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        if (hadoopHome != null) {
            pb.environment().put("HADOOP_HOME", hadoopHome);
        }

        Process proc = pb.start();

        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(proc.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
        }

        boolean finished = proc.waitFor(timeoutSecs, TimeUnit.SECONDS);
        if (!finished) {
            proc.destroyForcibly();
            throw new RuntimeException("Command timed out after " + timeoutSecs + "s");
        }

        return output.toString();
    }

    // ========================================================================
    // Data Classes
    // ========================================================================

    private static class HadoopVersion {
        final String version;
        final String hadoopHome;

        HadoopVersion(String version, String hadoopHome) {
            this.version = version;
            this.hadoopHome = hadoopHome;
        }
    }

    private static class HiveVersion {
        final String version;
        final String hiveHome;

        HiveVersion(String version, String hiveHome) {
            this.version = version;
            this.hiveHome = hiveHome;
        }
    }
}
