package x.mg.metrics.hivetelemetry.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.util.*;
import java.util.regex.Pattern;

public class HiveHookConfig {
    private final Config resolvedConfig;

    public HiveHookConfig() {
        // Three-tier merge: HiveConf overrides > HOCON file > defaults
        Map<String, Object> hiveConfMap = loadHiveConfOverrides();
        Config overrides = ConfigFactory.parseMap(hiveConfMap);
        Config fileConfig = loadHoconFile();
        Config defaults = ConfigFactory.parseMap(buildDefaults());
        this.resolvedConfig = overrides.withFallback(fileConfig).withFallback(defaults).resolve();
    }

    private Map<String, Object> loadHiveConfOverrides() {
        Map<String, Object> map = new HashMap<>();
        try {
            org.apache.hadoop.hive.conf.HiveConf hiveConf = new org.apache.hadoop.hive.conf.HiveConf();
            for (Map.Entry<String, String> entry : hiveConf) {
                String key = entry.getKey();
                if (key.startsWith("hive.telemetry.")) {
                    // Convert hive.telemetry.foo.bar -> hive-telemetry.foo.bar
                    String configKey = "hive-telemetry" + key.substring("hive.telemetry".length());
                    map.put(configKey, parseValue(entry.getValue()));
                }
            }
        } catch (Exception e) {
            // HiveConf may not be fully available in all contexts
        }
        return map;
    }

    private Object parseValue(String value) {
        if (value == null) return "";
        if ("true".equalsIgnoreCase(value)) return true;
        if ("false".equalsIgnoreCase(value)) return false;
        try { return Long.parseLong(value); } catch (NumberFormatException ignored) {}
        try { return Double.parseDouble(value); } catch (NumberFormatException ignored) {}
        return value;
    }

    private Config loadHoconFile() {
        // Check for explicit config path from HiveConf
        String configPath = null;
        try {
            org.apache.hadoop.hive.conf.HiveConf hiveConf = new org.apache.hadoop.hive.conf.HiveConf();
            configPath = hiveConf.get(HiveHookConfigKeys.CONFIG_PATH);
        } catch (Exception ignored) {}

        if (configPath != null && new File(configPath).exists()) {
            return ConfigFactory.parseFile(new File(configPath));
        }
        return ConfigFactory.parseResources("hive-telemetry.conf");
    }

    private Map<String, Object> buildDefaults() {
        Map<String, Object> d = new HashMap<>();
        d.put(HiveHookConfigKeys.OTEL_EXPORTER_ENDPOINT, HiveHookConfigKeys.DEFAULT_ENDPOINT);
        d.put(HiveHookConfigKeys.OTEL_SERVICE_NAME, HiveHookConfigKeys.DEFAULT_SERVICE_NAME);
        d.put(HiveHookConfigKeys.OTEL_EXPORT_INTERVAL_MS, HiveHookConfigKeys.DEFAULT_EXPORT_INTERVAL_MS);
        d.put(HiveHookConfigKeys.METRICS_ENABLED, true);
        d.put(HiveHookConfigKeys.METRICS_QUERY_DURATION, true);
        d.put(HiveHookConfigKeys.METRICS_QUERY_IO, true);
        d.put(HiveHookConfigKeys.METRICS_QUERY_TABLES, true);
        d.put(HiveHookConfigKeys.FILTER_USER_INCLUDE, Collections.singletonList(".*"));
        d.put(HiveHookConfigKeys.FILTER_USER_EXCLUDE, Collections.emptyList());
        d.put(HiveHookConfigKeys.FILTER_OPERATION_INCLUDE, Collections.singletonList(".*"));
        d.put(HiveHookConfigKeys.FILTER_OPERATION_EXCLUDE, Collections.emptyList());
        return d;
    }

    public boolean isEnabled() { return getBoolean(HiveHookConfigKeys.METRICS_ENABLED, true); }
    public boolean isQueryDuration() { return getBoolean(HiveHookConfigKeys.METRICS_QUERY_DURATION, true); }
    public boolean isQueryIO() { return getBoolean(HiveHookConfigKeys.METRICS_QUERY_IO, true); }
    public boolean isQueryTables() { return getBoolean(HiveHookConfigKeys.METRICS_QUERY_TABLES, true); }
    public String getOtelEndpoint() { return getString(HiveHookConfigKeys.OTEL_EXPORTER_ENDPOINT, HiveHookConfigKeys.DEFAULT_ENDPOINT); }
    public String getServiceName() { return getString(HiveHookConfigKeys.OTEL_SERVICE_NAME, HiveHookConfigKeys.DEFAULT_SERVICE_NAME); }
    public long getExportIntervalMs() { return getLong(HiveHookConfigKeys.OTEL_EXPORT_INTERVAL_MS, HiveHookConfigKeys.DEFAULT_EXPORT_INTERVAL_MS); }

    public boolean shouldAccept(String user, String operation) {
        List<String> userIncludes = getStringOrStringList(HiveHookConfigKeys.FILTER_USER_INCLUDE);
        List<String> userExcludes = getStringOrStringList(HiveHookConfigKeys.FILTER_USER_EXCLUDE);
        List<String> opIncludes = getStringOrStringList(HiveHookConfigKeys.FILTER_OPERATION_INCLUDE);
        List<String> opExcludes = getStringOrStringList(HiveHookConfigKeys.FILTER_OPERATION_EXCLUDE);

        boolean userOk = user == null
                || (userIncludes.isEmpty() || userIncludes.stream().anyMatch(p -> Pattern.matches(p, user)))
                && userExcludes.stream().noneMatch(p -> Pattern.matches(p, user));
        boolean opOk = operation == null
                || (opIncludes.isEmpty() || opIncludes.stream().anyMatch(p -> Pattern.matches(p, operation)))
                && opExcludes.stream().noneMatch(p -> Pattern.matches(p, operation));
        return userOk && opOk;
    }

    private String getString(String path, String defaultValue) {
        try { return resolvedConfig.getString(path); } catch (Exception e) { return defaultValue; }
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
