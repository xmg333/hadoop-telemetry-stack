package x.mg.metrics.diagnostic.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * 诊断配置
 */
public class DiagnosticConfig {

    private final Config config;

    public DiagnosticConfig() {
        this.config = loadConfig();
    }

    public DiagnosticConfig(String configPath) {
        this.config = loadConfigFile(configPath);
    }

    private Config loadConfig() {
        Config defaults = ConfigFactory.parseMap(buildDefaults());
        Config fileConfig = loadConfigFile(null);
        return fileConfig.withFallback(defaults).resolve();
    }

    private Config loadConfigFile(String configPath) {
        if (configPath != null) {
            return ConfigFactory.parseFile(new java.io.File(configPath));
        }
        return ConfigFactory.load();
    }

    private java.util.Map<String, Object> buildDefaults() {
        java.util.Map<String, Object> d = new java.util.HashMap<>();

        // OTel Collector
        d.put("diagnostic.otel-collector.endpoint", "http://localhost:4317");
        d.put("diagnostic.otel-collector.health-check-port", 13133);
        d.put("diagnostic.otel-collector.timeout-ms", 5000);

        // Kafka
        d.put("diagnostic.kafka.bootstrap-servers", "localhost:9092");
        d.put("diagnostic.kafka.metrics-topic", "telemetry-metrics");
        d.put("diagnostic.kafka.traces-topic", "telemetry-traces");
        d.put("diagnostic.kafka.timeout-ms", 5000);

        // MySQL
        d.put("diagnostic.mysql.host", "localhost");
        d.put("diagnostic.mysql.port", 3306);
        d.put("diagnostic.mysql.database", "telemetry");
        d.put("diagnostic.mysql.username", "metrics");
        d.put("diagnostic.mysql.password", "metrics");
        d.put("diagnostic.mysql.timeout-ms", 5000);

        // Spark
        d.put("diagnostic.spark.plugins-config.enabled", true);

        // Hive
        d.put("diagnostic.hive.hook-config.enabled", true);

        // MR Collector
        d.put("diagnostic.mr-collector.enabled", true);
        d.put("diagnostic.mr-collector.history-server-url", "http://localhost:19888");
        d.put("diagnostic.mr-collector.poll-interval-secs", 30);

        // Hive
        d.put("diagnostic.hive.hook-jar-path", "");
        d.put("diagnostic.hive.aux-jars-path", "");

        return d;
    }

    // OTel Collector
    public String getOtelCollectorEndpoint() {
        return config.getString("diagnostic.otel-collector.endpoint");
    }

    public int getOtelCollectorHealthCheckPort() {
        return config.getInt("diagnostic.otel-collector.health-check-port");
    }

    public int getOtelCollectorTimeoutMs() {
        return config.getInt("diagnostic.otel-collector.timeout-ms");
    }

    // Kafka
    public String getKafkaBootstrapServers() {
        return config.getString("diagnostic.kafka.bootstrap-servers");
    }

    public String getKafkaMetricsTopic() {
        return config.getString("diagnostic.kafka.metrics-topic");
    }

    public String getKafkaTracesTopic() {
        return config.getString("diagnostic.kafka.traces-topic");
    }

    public int getKafkaTimeoutMs() {
        return config.getInt("diagnostic.kafka.timeout-ms");
    }

    // MySQL
    public String getMysqlHost() {
        return config.getString("diagnostic.mysql.host");
    }

    public int getMysqlPort() {
        return config.getInt("diagnostic.mysql.port");
    }

    public String getMysqlDatabase() {
        return config.getString("diagnostic.mysql.database");
    }

    public String getMysqlUsername() {
        return config.getString("diagnostic.mysql.username");
    }

    public String getMysqlPassword() {
        return config.getString("diagnostic.mysql.password");
    }

    public int getMysqlTimeoutMs() {
        return config.getInt("diagnostic.mysql.timeout-ms");
    }

    // Spark
    public boolean isSparkPluginsConfigEnabled() {
        return config.getBoolean("diagnostic.spark.plugins-config.enabled");
    }

    // Hive
    public boolean isHiveHookConfigEnabled() {
        return config.getBoolean("diagnostic.hive.hook-config.enabled");
    }

    // MR Collector
    public boolean isMrCollectorEnabled() {
        return config.getBoolean("diagnostic.mr-collector.enabled");
    }

    public String getMrCollectorHistoryServerUrl() {
        return config.getString("diagnostic.mr-collector.history-server-url");
    }

    public int getMrCollectorPollIntervalSecs() {
        return config.getInt("diagnostic.mr-collector.poll-interval-secs");
    }

    // Hive
    public String getHiveHookJarPath() {
        try { return config.getString("diagnostic.hive.hook-jar-path"); }
        catch (Exception e) { return ""; }
    }

    public String getHiveAuxJarsPath() {
        try { return config.getString("diagnostic.hive.aux-jars-path"); }
        catch (Exception e) { return ""; }
    }

    public Config getRawConfig() {
        return config;
    }
}
