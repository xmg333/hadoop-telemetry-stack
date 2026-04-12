package x.mg.metrics.mrtelemetry.poller;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import x.mg.metrics.mrtelemetry.config.MRCollectorConfig;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class HistoryServerClientTest {

    private MockWebServer server;
    private HistoryServerClient client;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();

        String url = server.url("").toString();
        // Remove trailing slash since HistoryServerClient strips it anyway
        if (url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }

        // Create a temp config pointing to the mock server
        File confFile = tempDir.resolve("test.conf").toFile();
        try (FileWriter w = new FileWriter(confFile)) {
            w.write("mr-telemetry.history-server.url = \"" + url + "\"\n");
            w.write("mr-telemetry.history-server.connect.timeout.secs = 5\n");
            w.write("mr-telemetry.history-server.read.timeout.secs = 5\n");
        }

        MRCollectorConfig config = new MRCollectorConfig(confFile.getAbsolutePath());
        client = new HistoryServerClient(config);
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
    }

    @Test
    void testGetJobs() throws IOException, InterruptedException {
        String jobsJson = "{\"jobs\":{\"job\":[{\"id\":\"job_001\",\"name\":\"wordcount\","
                + "\"user\":\"testuser\",\"state\":\"SUCCEEDED\"}]}}";

        server.enqueue(new MockResponse()
                .setBody(jobsJson)
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json"));

        String response = client.getJobs();

        assertTrue(response.contains("job_001"));
        assertTrue(response.contains("wordcount"));
        assertTrue(response.contains("testuser"));

        RecordedRequest request = server.takeRequest();
        assertEquals("/ws/v1/history/mapreduce/jobs", request.getPath());
        assertEquals("GET", request.getMethod());
    }

    @Test
    void testGetJobCounters() throws IOException, InterruptedException {
        String countersJson = "{\"jobCounters\":{\"counterGroup\":[]}}";

        server.enqueue(new MockResponse()
                .setBody(countersJson)
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json"));

        String response = client.getJobCounters("job_001");

        assertTrue(response.contains("jobCounters"));

        RecordedRequest request = server.takeRequest();
        assertEquals("/ws/v1/history/mapreduce/jobs/job_001/counters", request.getPath());
        assertEquals("GET", request.getMethod());
    }

    @Test
    void testErrorHandling() {
        server.enqueue(new MockResponse().setResponseCode(500));

        IOException exception = assertThrows(IOException.class, () -> client.getJobs());
        assertTrue(exception.getMessage().contains("HTTP 500"));
    }
}
