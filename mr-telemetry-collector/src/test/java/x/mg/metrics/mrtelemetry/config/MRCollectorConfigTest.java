package x.mg.metrics.mrtelemetry.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class MRCollectorConfigTest {

    @Test
    void testDefaultValues() {
        MRCollectorConfig config = new MRCollectorConfig();

        assertEquals("http://localhost:19888", config.getHistoryServerUrl());
        assertEquals(30, config.getPollIntervalSecs());
        assertEquals(10, config.getConnectTimeoutSecs());
        assertEquals(30, config.getReadTimeoutSecs());
        assertEquals("http://localhost:4317", config.getOtelEndpoint());
        assertEquals("grpc", config.getOtelProtocol());
        assertEquals("mr-telemetry-collector", config.getServiceName());
        assertEquals(10000L, config.getExportIntervalMs());
        assertEquals("/tmp/mr-telemetry-state.json", config.getStateFile());
        assertTrue(config.isJobCounters());
        assertFalse(config.isTaskCounters());
        assertTrue(config.isJobDetails());
    }

    @Test
    void testShouldAcceptJobAll() {
        MRCollectorConfig config = new MRCollectorConfig();

        // Default config includes ".*" for all users and job names
        assertTrue(config.shouldAcceptJob("wordcount", "testuser"));
        assertTrue(config.shouldAcceptJob("teragen", "hadoop"));
        assertTrue(config.shouldAcceptJob("any-job", "any-user"));
    }

    @Test
    void testShouldAcceptJobExcludeUser(@TempDir Path tempDir) throws IOException {
        File confFile = tempDir.resolve("exclude-user.conf").toFile();
        try (FileWriter w = new FileWriter(confFile)) {
            w.write("mr-telemetry.filter.user.exclude = [\"system\", \"hive\"]\n");
        }

        MRCollectorConfig config = new MRCollectorConfig(confFile.getAbsolutePath());

        // Regular user still accepted
        assertTrue(config.shouldAcceptJob("wordcount", "testuser"));

        // Excluded users rejected
        assertFalse(config.shouldAcceptJob("wordcount", "system"));
        assertFalse(config.shouldAcceptJob("wordcount", "hive"));
    }

    @Test
    void testShouldAcceptJobExcludeName(@TempDir Path tempDir) throws IOException {
        File confFile = tempDir.resolve("exclude-name.conf").toFile();
        try (FileWriter w = new FileWriter(confFile)) {
            w.write("mr-telemetry.filter.job.name.exclude = [\"temp.*\", \"test_.*\"]\n");
        }

        MRCollectorConfig config = new MRCollectorConfig(confFile.getAbsolutePath());

        // Regular job accepted
        assertTrue(config.shouldAcceptJob("wordcount", "testuser"));

        // Excluded job name patterns rejected
        assertFalse(config.shouldAcceptJob("temp_cleanup", "testuser"));
        assertFalse(config.shouldAcceptJob("test_job", "testuser"));
    }
}
