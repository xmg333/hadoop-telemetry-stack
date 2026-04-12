package x.mg.metrics.mrtelemetry.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class MRCollectorConfig {
    private final Config resolvedConfig;

    public MRCollectorConfig() {
        this(null);
    }

    public MRCollectorConfig(String configPath) {
        Config defaults = ConfigFactory.parseMap(buildDefaults());
        Config fileConfig = loadHoconFile(configPath);
        this.resolvedConfig = fileConfig.withFallback(defaults).resolve();
    }

    private Config loadHoconFile(String configPath) {
        if (configPath != null && new File(configPath).exists()) {
            return ConfigFactory.parseFile(new File(configPath));
        }
        // Try classpath
        Config cp = ConfigFactory.parseResources("mr-collector.conf");
        return cp;
    }

    private Map<String, Object> buildDefaults() {
        Map<String, Object> d = new HashMap<>();
        d.put(MRConfigKeys.HS_URL, MRConfigKeys.DEFAULT_HS_URL);
        d.put(MRConfigKeys.HS_POLL_INTERVAL_SECS, MRConfigKeys.DEFAULT_POLL_INTERVAL_SECS);
        d.put(MRConfigKeys.HS_CONNECT_TIMEOUT_SECS, MRConfigKeys.DEFAULT_CONNECT_TIMEOUT_SECS);
        d.put(MRConfigKeys.HS_READ_TIMEOUT_SECS, MRConfigKeys.DEFAULT_READ_TIMEOUT_SECS);
        d.put(MRConfigKeys.OTEL_EXPORTER_ENDPOINT, MRConfigKeys.DEFAULT_ENDPOINT);
        d.put(MRConfigKeys.OTEL_EXPORTER_PROTOCOL, "grpc");
        d.put(MRConfigKeys.OTEL_SERVICE_NAME, MRConfigKeys.DEFAULT_SERVICE_NAME);
        d.put(MRConfigKeys.OTEL_EXPORT_INTERVAL_MS, MRConfigKeys.DEFAULT_EXPORT_INTERVAL_MS);
        d.put(MRConfigKeys.STATE_FILE, MRConfigKeys.DEFAULT_STATE_FILE);
        d.put(MRConfigKeys.FILTER_USER_INCLUDE, Collections.singletonList(".*"));
        d.put(MRConfigKeys.FILTER_USER_EXCLUDE, Collections.emptyList());
        d.put(MRConfigKeys.FILTER_JOB_NAME_INCLUDE, Collections.singletonList(".*"));
        d.put(MRConfigKeys.FILTER_JOB_NAME_EXCLUDE, Collections.emptyList());
        d.put(MRConfigKeys.COLLECTION_JOB_COUNTERS, true);
        d.put(MRConfigKeys.COLLECTION_TASK_COUNTERS, false);
        d.put(MRConfigKeys.COLLECTION_JOB_DETAILS, true);
        return d;
    }

    public String getHistoryServerUrl() { return getString(MRConfigKeys.HS_URL, MRConfigKeys.DEFAULT_HS_URL); }
    public int getPollIntervalSecs() { return getInt(MRConfigKeys.HS_POLL_INTERVAL_SECS, MRConfigKeys.DEFAULT_POLL_INTERVAL_SECS); }
    public int getConnectTimeoutSecs() { return getInt(MRConfigKeys.HS_CONNECT_TIMEOUT_SECS, MRConfigKeys.DEFAULT_CONNECT_TIMEOUT_SECS); }
    public int getReadTimeoutSecs() { return getInt(MRConfigKeys.HS_READ_TIMEOUT_SECS, MRConfigKeys.DEFAULT_READ_TIMEOUT_SECS); }
    public String getOtelEndpoint() { return getString(MRConfigKeys.OTEL_EXPORTER_ENDPOINT, MRConfigKeys.DEFAULT_ENDPOINT); }
    public String getOtelProtocol() { return getString(MRConfigKeys.OTEL_EXPORTER_PROTOCOL, "grpc"); }
    public String getServiceName() { return getString(MRConfigKeys.OTEL_SERVICE_NAME, MRConfigKeys.DEFAULT_SERVICE_NAME); }
    public long getExportIntervalMs() { return getLong(MRConfigKeys.OTEL_EXPORT_INTERVAL_MS, MRConfigKeys.DEFAULT_EXPORT_INTERVAL_MS); }
    public String getStateFile() { return getString(MRConfigKeys.STATE_FILE, MRConfigKeys.DEFAULT_STATE_FILE); }
    public boolean isJobCounters() { return getBoolean(MRConfigKeys.COLLECTION_JOB_COUNTERS, true); }
    public boolean isTaskCounters() { return getBoolean(MRConfigKeys.COLLECTION_TASK_COUNTERS, false); }
    public boolean isJobDetails() { return getBoolean(MRConfigKeys.COLLECTION_JOB_DETAILS, true); }
    public Config getRawConfig() { return resolvedConfig; }

    public boolean shouldAcceptJob(String jobName, String user) {
        List<String> userIncludes = getStringOrStringList(MRConfigKeys.FILTER_USER_INCLUDE);
        List<String> userExcludes = getStringOrStringList(MRConfigKeys.FILTER_USER_EXCLUDE);
        List<String> nameIncludes = getStringOrStringList(MRConfigKeys.FILTER_JOB_NAME_INCLUDE);
        List<String> nameExcludes = getStringOrStringList(MRConfigKeys.FILTER_JOB_NAME_EXCLUDE);

        boolean userOk = user == null || (userIncludes.isEmpty() || userIncludes.stream().anyMatch(p -> Pattern.matches(p, user)))
                && userExcludes.stream().noneMatch(p -> Pattern.matches(p, user));
        boolean nameOk = jobName == null || (nameIncludes.isEmpty() || nameIncludes.stream().anyMatch(p -> Pattern.matches(p, jobName)))
                && nameExcludes.stream().noneMatch(p -> Pattern.matches(p, jobName));
        return userOk && nameOk;
    }

    private String getString(String path, String defaultValue) {
        try { return resolvedConfig.getString(path); } catch (Exception e) { return defaultValue; }
    }
    private int getInt(String path, int defaultValue) {
        try { return resolvedConfig.getInt(path); } catch (Exception e) { return defaultValue; }
    }
    private long getLong(String path, long defaultValue) {
        try { return resolvedConfig.getLong(path); } catch (Exception e) { return defaultValue; }
    }
    private boolean getBoolean(String path, boolean defaultValue) {
        try { return resolvedConfig.getBoolean(path); } catch (Exception e) { return defaultValue; }
    }
    private List<String> getStringOrStringList(String path) {
        try { return resolvedConfig.getStringList(path); }
        catch (Exception e) {
            try {
                String s = resolvedConfig.getString(path);
                return s != null && !s.isEmpty() ? Collections.singletonList(s) : Collections.emptyList();
            } catch (Exception e2) { return Collections.emptyList(); }
        }
    }
}
