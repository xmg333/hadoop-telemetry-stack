package x.mg.metrics.integration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Multi-version Spark integration test.
 *
 * This test detects available Spark installations and runs the telemetry
 * plugin against each version to verify compatibility.
 *
 * Prerequisites:
 *   - Spark installations in /opt/spark-*
 *   - OTel Collector running and accessible
 *   - Plugin JARs built for each Spark version
 */
@Tag("integration")
class SparkMultiVersionIT {

    private static final String OTEL_ENDPOINT = System.getenv().getOrDefault(
        "OTEL_ENDPOINT", "http://localhost:4317");
    private static final int JOB_TIMEOUT_SECS = 120;

    /**
     * Tests all detected Spark versions.
     */
    @Test
    void testAllSparkVersions() throws Exception {
        List<SparkVersion> versions = detectSparkVersions();
        assumeTrue(!versions.isEmpty(),
            "No Spark installations found in /opt/spark-*");

        System.out.println("Detected Spark versions: " +
            versions.stream().map(v -> v.version).collect(Collectors.joining(", ")));

        List<String> failures = new ArrayList<>();

        for (SparkVersion sv : versions) {
            try {
                testSparkVersion(sv);
            } catch (AssertionError e) {
                failures.add(sv.version + ": " + e.getMessage());
                System.err.println("FAILED: Spark " + sv.version + " - " + e.getMessage());
            }
        }

        assertTrue(failures.isEmpty(),
            "Failures for Spark versions: " + String.join(", ", failures));
    }

    /**
     * Tests a specific Spark version.
     */
    private void testSparkVersion(SparkVersion sv) throws Exception {
        System.out.println("\n=== Testing Spark " + sv.version + " ===");
        System.out.println("  Home: " + sv.sparkHome);
        System.out.println("  Plugin JAR: " + sv.pluginJar);

        // Find examples JAR
        File examplesJar = findExamplesJar(sv.sparkHome);
        assertNotNull(examplesJar,
            "No spark-examples JAR found in " + sv.sparkHome + "/examples/jars/");

        // Submit SparkPi job
        String result = submitSparkPi(sv, examplesJar);

        // Verify job completed successfully
        assertTrue(
            result.contains("Pi is roughly") || result.contains("completed successfully"),
            "SparkPi job should complete successfully. Output:\n" + result
        );

        System.out.println("  [PASS] Spark " + sv.version + " - Job completed successfully");
    }

    /**
     * Detects available Spark installations.
     */
    private List<SparkVersion> detectSparkVersions() {
        List<SparkVersion> versions = new ArrayList<>();

        File opt = new File("/opt");
        if (!opt.isDirectory()) {
            return versions;
        }

        File[] dirs = opt.listFiles((d, name) -> name.startsWith("spark-"));
        if (dirs == null) {
            return versions;
        }

        for (File dir : dirs) {
            if (!dir.isDirectory()) continue;

            // Extract version from directory name (e.g., spark-3.2.0 -> 3.2.0)
            String name = dir.getName();
            String version = name.substring("spark-".length());

            // Find plugin JAR
            File pluginJar = findPluginJar(dir);
            if (pluginJar != null) {
                versions.add(new SparkVersion(version, dir.getAbsolutePath(), pluginJar));
            } else {
                System.err.println("Warning: No plugin JAR found for " + name);
            }
        }

        return versions;
    }

    /**
     * Finds the plugin JAR in the Spark installation.
     */
    private File findPluginJar(File sparkHome) {
        File jarsDir = new File(sparkHome, "jars");
        if (!jarsDir.isDirectory()) {
            return null;
        }

        File[] jars = jarsDir.listFiles((d, name) ->
            name.contains("spark-telemetry") &&
            name.endsWith(".jar") &&
            !name.contains("original"));

        return (jars != null && jars.length > 0) ? jars[0] : null;
    }

    /**
     * Finds the examples JAR in the Spark installation.
     */
    private File findExamplesJar(String sparkHome) {
        File examplesDir = new File(sparkHome, "examples/jars");
        if (!examplesDir.isDirectory()) {
            return null;
        }

        File[] jars = examplesDir.listFiles((d, name) ->
            name.startsWith("spark-examples") && name.endsWith(".jar"));

        return (jars != null && jars.length > 0) ? jars[0] : null;
    }

    /**
     * Submits SparkPi job with telemetry plugin.
     */
    private String submitSparkPi(SparkVersion sv, File examplesJar) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add(sv.sparkHome + "/bin/spark-submit");
        cmd.add("--master");
        cmd.add("local[2]");
        cmd.add("--class");
        cmd.add("org.apache.spark.examples.SparkPi");
        cmd.add("--jars");
        cmd.add(sv.pluginJar.getAbsolutePath());
        cmd.add("--conf");
        cmd.add("spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin");
        cmd.add("--conf");
        cmd.add("spark.telemetry.otel.exporter.endpoint=" + OTEL_ENDPOINT);
        cmd.add("--conf");
        cmd.add("spark.telemetry.otel.service.name=spark-" + sv.version + "-test");
        cmd.add("--conf");
        cmd.add("spark.telemetry.otel.export.interval.ms=5000");
        cmd.add(examplesJar.getAbsolutePath());
        cmd.add("100");  // slices

        return runCommand(cmd, JOB_TIMEOUT_SECS);
    }

    /**
     * Runs a command and returns output.
     */
    private String runCommand(List<String> cmd, int timeoutSecs) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);

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

    /**
     * Represents a detected Spark version.
     */
    private static class SparkVersion {
        final String version;
        final String sparkHome;
        final File pluginJar;

        SparkVersion(String version, String sparkHome, File pluginJar) {
            this.version = version;
            this.sparkHome = sparkHome;
            this.pluginJar = pluginJar;
        }
    }
}
