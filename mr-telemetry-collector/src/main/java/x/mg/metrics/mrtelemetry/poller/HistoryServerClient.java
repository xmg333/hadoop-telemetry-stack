package x.mg.metrics.mrtelemetry.poller;

import x.mg.metrics.mrtelemetry.config.MRCollectorConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * HTTP client for MR History Server REST API.
 * Uses only java.net.HttpURLConnection (no external HTTP library dependency).
 */
public class HistoryServerClient {
    private static final Logger LOG = Logger.getLogger(HistoryServerClient.class.getName());

    private final String baseUrl;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;

    public HistoryServerClient(MRCollectorConfig config) {
        String url = config.getHistoryServerUrl();
        this.baseUrl = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
        this.connectTimeoutMs = config.getConnectTimeoutSecs() * 1000;
        this.readTimeoutMs = config.getReadTimeoutSecs() * 1000;
    }

    /**
     * GET /ws/v1/history/mapreduce/jobs
     */
    public String getJobs() throws IOException {
        return get("/ws/v1/history/mapreduce/jobs");
    }

    /**
     * GET /ws/v1/history/mapreduce/jobs/{jobId}/counters
     */
    public String getJobCounters(String jobId) throws IOException {
        return get("/ws/v1/history/mapreduce/jobs/" + jobId + "/counters");
    }

    /**
     * GET /ws/v1/history/mapreduce/jobs/{jobId}/tasks
     */
    public String getTasks(String jobId) throws IOException {
        return get("/ws/v1/history/mapreduce/jobs/" + jobId + "/tasks");
    }

    /**
     * GET /ws/v1/history/mapreduce/jobs/{jobId}
     */
    public String getJob(String jobId) throws IOException {
        return get("/ws/v1/history/mapreduce/jobs/" + jobId);
    }

    /**
     * GET /ws/v1/history/mapreduce/jobs/{jobId}/tasks/{taskId}/counters
     */
    public String getTaskCounters(String jobId, String taskId) throws IOException {
        return get("/ws/v1/history/mapreduce/jobs/" + jobId + "/tasks/" + taskId + "/counters");
    }

    private String get(String path) throws IOException {
        String urlStr = baseUrl + path;
        LOG.fine("GET " + urlStr);
        HttpURLConnection conn = null;
        try {
            URL url = new URL(urlStr);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");
            conn.setConnectTimeout(connectTimeoutMs);
            conn.setReadTimeout(readTimeoutMs);

            int code = conn.getResponseCode();
            if (code != 200) {
                throw new IOException("HTTP " + code + " from " + urlStr);
            }

            StringBuilder sb = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
            }
            return sb.toString();
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }
}
