package x.mg.metrics.sparktelemetry.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;

import java.io.File;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Three-tier configuration loader:
 * 1. Spark conf properties (spark.telemetry.*) - highest priority
 * 2. telemetry.conf HOCON file (classpath or specified path)
 * 3. Built-in defaults
 */
public class TelemetryConfig {

    private final Config resolvedConfig;
    private final Map<String, String> sparkConfOverrides;

    public TelemetryConfig() {
        this(Collections.emptyMap());
    }

    public TelemetryConfig(Map<String, String> sparkConfOverrides) {
        this.sparkConfOverrides = sparkConfOverrides != null ? sparkConfOverrides : Collections.emptyMap();
        this.resolvedConfig = loadConfig();
    }

    private Config loadConfig() {
        // 1. Load defaults
        Config defaults = ConfigFactory.parseMap(buildDefaults());

        // 2. Load HOCON file
        Config fileConfig = loadHoconFile();

        // 3. Apply Spark conf overrides
        Config overrides = ConfigFactory.parseMap(buildSparkConfMap());

        // Merge: overrides > file > defaults
        return overrides.withFallback(fileConfig).withFallback(defaults).resolve();
    }

    private Config loadHoconFile() {
        String configPath = sparkConfOverrides.get(ConfigKeys.CONFIG_PATH_KEY);
        if (configPath != null && new File(configPath).exists()) {
            return ConfigFactory.parseFile(new File(configPath));
        }
        // Try classpath
        return ConfigFactory.parseResources("telemetry.conf");
    }

    private Map<String, Object> buildSparkConfMap() {
        Map<String, Object> map = new HashMap<>();
        for (Map.Entry<String, String> entry : sparkConfOverrides.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(ConfigKeys.SPARK_CONF_PREFIX) && !key.equals(ConfigKeys.CONFIG_PATH_KEY)) {
                // Convert spark.telemetry.foo.bar → spark-telemetry.foo.bar
                String configKey = ConfigKeys.PREFIX + key.substring(ConfigKeys.SPARK_CONF_PREFIX.length() - 1);
                map.put(configKey, parseValue(entry.getValue()));
            }
        }
        return map;
    }

    private Object parseValue(String value) {
        if (value == null) return "";
        if (value.equalsIgnoreCase("true")) return true;
        if (value.equalsIgnoreCase("false")) return false;
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ignored) {}
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException ignored) {}
        return value;
    }

    private Map<String, Object> buildDefaults() {
        Map<String, Object> defaults = new HashMap<>();
        defaults.put(ConfigKeys.PREFIX + ".otel.exporter.endpoint", ConfigKeys.DEFAULT_ENDPOINT);
        defaults.put(ConfigKeys.PREFIX + ".otel.exporter.protocol", ConfigKeys.DEFAULT_PROTOCOL);
        defaults.put(ConfigKeys.PREFIX + ".otel.service.name", ConfigKeys.DEFAULT_SERVICE_NAME);
        defaults.put(ConfigKeys.PREFIX + ".otel.export.interval.ms", ConfigKeys.DEFAULT_EXPORT_INTERVAL_MS);
        defaults.put(ConfigKeys.PREFIX + ".metrics.listener.enabled", true);
        defaults.put(ConfigKeys.PREFIX + ".metrics.listener.capture.task-end", true);
        defaults.put(ConfigKeys.PREFIX + ".metrics.listener.capture.stage-complete", true);
        defaults.put(ConfigKeys.PREFIX + ".metrics.listener.capture.job-end", false);
        defaults.put(ConfigKeys.PREFIX + ".metrics.system.enabled", true);
        defaults.put(ConfigKeys.PREFIX + ".metrics.system.capture.jvm-memory", true);
        defaults.put(ConfigKeys.PREFIX + ".metrics.system.capture.jvm-gc", true);
        defaults.put(ConfigKeys.PREFIX + ".metrics.system.capture.buffer-pools", true);
        defaults.put(ConfigKeys.PREFIX + ".metrics.system.capture.executor-memory", true);
        defaults.put(ConfigKeys.PREFIX + ".metrics.task.execution", true);
        defaults.put(ConfigKeys.PREFIX + ".metrics.task.shuffle-extended", true);
        defaults.put(ConfigKeys.PREFIX + ".metrics.task.info", true);
        defaults.put(ConfigKeys.PREFIX + ".metrics.stage.detailed", false);
        defaults.put(ConfigKeys.PREFIX + ".metrics.job.lifecycle", false);
        defaults.put(ConfigKeys.PREFIX + ".filter.app.name.include", Collections.singletonList(".*"));
        defaults.put(ConfigKeys.PREFIX + ".filter.app.name.exclude", Collections.emptyList());
        return defaults;
    }

    // ---- Typed getters ----

    public String getOtelEndpoint() {
        return getString(ConfigKeys.PREFIX + ".otel.exporter.endpoint", ConfigKeys.DEFAULT_ENDPOINT);
    }

    public String getOtelProtocol() {
        return getString(ConfigKeys.PREFIX + ".otel.exporter.protocol", ConfigKeys.DEFAULT_PROTOCOL);
    }

    public String getServiceName() {
        return getString(ConfigKeys.PREFIX + ".otel.service.name", ConfigKeys.DEFAULT_SERVICE_NAME);
    }

    public long getExportIntervalMs() {
        return getLong(ConfigKeys.PREFIX + ".otel.export.interval.ms", ConfigKeys.DEFAULT_EXPORT_INTERVAL_MS);
    }

    public boolean isListenerEnabled() {
        return getBoolean(ConfigKeys.PREFIX + ".metrics.listener.enabled", true);
    }

    public boolean isCaptureTaskEnd() {
        return getBoolean(ConfigKeys.PREFIX + ".metrics.listener.capture.task-end", true);
    }

    public boolean isCaptureStageComplete() {
        return getBoolean(ConfigKeys.PREFIX + ".metrics.listener.capture.stage-complete", true);
    }

    public boolean isCaptureJobEnd() {
        return getBoolean(ConfigKeys.PREFIX + ".metrics.listener.capture.job-end", false);
    }

    public boolean isSystemMetricsEnabled() {
        return getBoolean(ConfigKeys.PREFIX + ".metrics.system.enabled", true);
    }

    public boolean isCaptureJvmMemory() {
        return getBoolean(ConfigKeys.PREFIX + ".metrics.system.capture.jvm-memory", true);
    }

    public boolean isCaptureJvmGc() {
        return getBoolean(ConfigKeys.PREFIX + ".metrics.system.capture.jvm-gc", true);
    }

    public boolean isCaptureBufferPools() {
        return getBoolean(ConfigKeys.PREFIX + ".metrics.system.capture.buffer-pools", true);
    }

    public boolean isCaptureExecutorMemory() {
        return getBoolean(ConfigKeys.PREFIX + ".metrics.system.capture.executor-memory", true);
    }

    public boolean isCaptureTaskExecution() {
        return getBoolean(ConfigKeys.METRICS_TASK_EXECUTION, true);
    }

    public boolean isCaptureTaskShuffleExtended() {
        return getBoolean(ConfigKeys.METRICS_TASK_SHUFFLE_EXTENDED, true);
    }

    public boolean isCaptureTaskInfo() {
        return getBoolean(ConfigKeys.METRICS_TASK_INFO, true);
    }

    public boolean isCaptureStageDetailed() {
        return getBoolean(ConfigKeys.METRICS_STAGE_DETAILED, false);
    }

    public boolean isCaptureJobLifecycle() {
        return getBoolean(ConfigKeys.METRICS_JOB_LIFECYCLE, false);
    }

    public boolean shouldAcceptApp(String appName) {
        if (appName == null) return true;
        List<String> includes = getStringOrStringList(ConfigKeys.PREFIX + ".filter.app.name.include");
        List<String> excludes = getStringOrStringList(ConfigKeys.PREFIX + ".filter.app.name.exclude");
        boolean included = includes.isEmpty() || includes.stream().anyMatch(p -> Pattern.matches(p, appName));
        boolean excluded = excludes.stream().anyMatch(p -> Pattern.matches(p, appName));
        return included && !excluded;
    }

    // ---- Raw access ----

    public Config getRawConfig() {
        return resolvedConfig;
    }

    private String getString(String path, String defaultValue) {
        try {
            return resolvedConfig.getString(path);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private long getLong(String path, long defaultValue) {
        try {
            return resolvedConfig.getLong(path);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private boolean getBoolean(String path, boolean defaultValue) {
        try {
            return resolvedConfig.getBoolean(path);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private List<String> getStringList(String path) {
        try {
            return resolvedConfig.getStringList(path);
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    /**
     * Get a config value as a list of strings, falling back to treating a single string as a one-element list.
     */
    private List<String> getStringOrStringList(String path) {
        try {
            return resolvedConfig.getStringList(path);
        } catch (Exception e) {
            try {
                String single = resolvedConfig.getString(path);
                if (single != null && !single.isEmpty()) {
                    return Collections.singletonList(single);
                }
            } catch (Exception e2) {
                // ignore
            }
            return Collections.emptyList();
        }
    }
}
