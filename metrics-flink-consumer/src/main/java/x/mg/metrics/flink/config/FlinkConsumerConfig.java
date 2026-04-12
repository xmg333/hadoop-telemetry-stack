package x.mg.metrics.flink.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkConsumerConfig {
    private final Config resolvedConfig;

    public FlinkConsumerConfig(String configPath) {
        Config defaults = ConfigFactory.parseMap(buildDefaults());
        Config fileConfig = loadHoconFile(configPath);
        this.resolvedConfig = fileConfig.withFallback(defaults).resolve();
    }

    private Config loadHoconFile(String configPath) {
        if (configPath != null && new File(configPath).exists()) {
            return ConfigFactory.parseFile(new File(configPath));
        }
        return ConfigFactory.parseResources("flink-consumer.conf");
    }

    private Map<String, Object> buildDefaults() {
        Map<String, Object> d = new HashMap<>();
        d.put(FlinkConsumerConfigKeys.KAFKA_BOOTSTRAP_SERVERS, FlinkConsumerConfigKeys.DEFAULT_KAFKA_BOOTSTRAP);
        d.put(FlinkConsumerConfigKeys.KAFKA_TOPIC, FlinkConsumerConfigKeys.DEFAULT_KAFKA_TOPIC);
        d.put(FlinkConsumerConfigKeys.KAFKA_GROUP_ID, FlinkConsumerConfigKeys.DEFAULT_KAFKA_GROUP_ID);
        d.put(FlinkConsumerConfigKeys.KAFKA_STARTUP_MODE, FlinkConsumerConfigKeys.DEFAULT_KAFKA_STARTUP_MODE);
        d.put(FlinkConsumerConfigKeys.KAFKA_CHECKPOINT_PATH, FlinkConsumerConfigKeys.DEFAULT_KAFKA_CHECKPOINT_PATH);
        d.put(FlinkConsumerConfigKeys.SINK_TYPE, FlinkConsumerConfigKeys.DEFAULT_SINK_TYPE);
        d.put(FlinkConsumerConfigKeys.SINK_MYSQL_URL, FlinkConsumerConfigKeys.DEFAULT_MYSQL_URL);
        d.put(FlinkConsumerConfigKeys.SINK_MYSQL_USER, FlinkConsumerConfigKeys.DEFAULT_MYSQL_USER);
        d.put(FlinkConsumerConfigKeys.SINK_MYSQL_PASSWORD, FlinkConsumerConfigKeys.DEFAULT_MYSQL_PASSWORD);
        d.put(FlinkConsumerConfigKeys.SINK_MYSQL_BATCH_SIZE, FlinkConsumerConfigKeys.DEFAULT_MYSQL_BATCH_SIZE);
        d.put(FlinkConsumerConfigKeys.SINK_MYSQL_FLUSH_INTERVAL_MS, FlinkConsumerConfigKeys.DEFAULT_MYSQL_FLUSH_INTERVAL_MS);
        d.put(FlinkConsumerConfigKeys.SINK_CLICKHOUSE_URL, FlinkConsumerConfigKeys.DEFAULT_CLICKHOUSE_URL);
        d.put(FlinkConsumerConfigKeys.SINK_CLICKHOUSE_USER, FlinkConsumerConfigKeys.DEFAULT_CLICKHOUSE_USER);
        d.put(FlinkConsumerConfigKeys.SINK_CLICKHOUSE_PASSWORD, FlinkConsumerConfigKeys.DEFAULT_CLICKHOUSE_PASSWORD);
        d.put(FlinkConsumerConfigKeys.SINK_CLICKHOUSE_BATCH_SIZE, FlinkConsumerConfigKeys.DEFAULT_CLICKHOUSE_BATCH_SIZE);
        d.put(FlinkConsumerConfigKeys.SINK_CLICKHOUSE_FLUSH_INTERVAL_MS, FlinkConsumerConfigKeys.DEFAULT_CLICKHOUSE_FLUSH_INTERVAL_MS);
        d.put(FlinkConsumerConfigKeys.FILTER_METRIC_NAME_INCLUDE, Collections.singletonList(".*"));
        d.put(FlinkConsumerConfigKeys.FILTER_METRIC_NAME_EXCLUDE, Collections.emptyList());
        d.put(FlinkConsumerConfigKeys.PROCESSING_PARALLELISM, FlinkConsumerConfigKeys.DEFAULT_PARALLELISM);
        return d;
    }

    // Kafka
    public String getKafkaBootstrapServers() { return getString(FlinkConsumerConfigKeys.KAFKA_BOOTSTRAP_SERVERS, FlinkConsumerConfigKeys.DEFAULT_KAFKA_BOOTSTRAP); }
    public String getKafkaTopic() { return getString(FlinkConsumerConfigKeys.KAFKA_TOPIC, FlinkConsumerConfigKeys.DEFAULT_KAFKA_TOPIC); }
    public String getKafkaGroupId() { return getString(FlinkConsumerConfigKeys.KAFKA_GROUP_ID, FlinkConsumerConfigKeys.DEFAULT_KAFKA_GROUP_ID); }
    public String getKafkaStartupMode() { return getString(FlinkConsumerConfigKeys.KAFKA_STARTUP_MODE, FlinkConsumerConfigKeys.DEFAULT_KAFKA_STARTUP_MODE); }
    public String getKafkaCheckpointPath() { return getString(FlinkConsumerConfigKeys.KAFKA_CHECKPOINT_PATH, FlinkConsumerConfigKeys.DEFAULT_KAFKA_CHECKPOINT_PATH); }

    // Sink
    public String getSinkType() { return getString(FlinkConsumerConfigKeys.SINK_TYPE, FlinkConsumerConfigKeys.DEFAULT_SINK_TYPE); }
    public String getMysqlUrl() { return getString(FlinkConsumerConfigKeys.SINK_MYSQL_URL, FlinkConsumerConfigKeys.DEFAULT_MYSQL_URL); }
    public String getMysqlUser() { return getString(FlinkConsumerConfigKeys.SINK_MYSQL_USER, FlinkConsumerConfigKeys.DEFAULT_MYSQL_USER); }
    public String getMysqlPassword() { return getString(FlinkConsumerConfigKeys.SINK_MYSQL_PASSWORD, FlinkConsumerConfigKeys.DEFAULT_MYSQL_PASSWORD); }
    public int getMysqlBatchSize() { return getInt(FlinkConsumerConfigKeys.SINK_MYSQL_BATCH_SIZE, FlinkConsumerConfigKeys.DEFAULT_MYSQL_BATCH_SIZE); }
    public int getMysqlFlushIntervalMs() { return getInt(FlinkConsumerConfigKeys.SINK_MYSQL_FLUSH_INTERVAL_MS, FlinkConsumerConfigKeys.DEFAULT_MYSQL_FLUSH_INTERVAL_MS); }
    public String getClickhouseUrl() { return getString(FlinkConsumerConfigKeys.SINK_CLICKHOUSE_URL, FlinkConsumerConfigKeys.DEFAULT_CLICKHOUSE_URL); }
    public String getClickhouseUser() { return getString(FlinkConsumerConfigKeys.SINK_CLICKHOUSE_USER, FlinkConsumerConfigKeys.DEFAULT_CLICKHOUSE_USER); }
    public String getClickhousePassword() { return getString(FlinkConsumerConfigKeys.SINK_CLICKHOUSE_PASSWORD, FlinkConsumerConfigKeys.DEFAULT_CLICKHOUSE_PASSWORD); }
    public int getClickhouseBatchSize() { return getInt(FlinkConsumerConfigKeys.SINK_CLICKHOUSE_BATCH_SIZE, FlinkConsumerConfigKeys.DEFAULT_CLICKHOUSE_BATCH_SIZE); }
    public int getClickhouseFlushIntervalMs() { return getInt(FlinkConsumerConfigKeys.SINK_CLICKHOUSE_FLUSH_INTERVAL_MS, FlinkConsumerConfigKeys.DEFAULT_CLICKHOUSE_FLUSH_INTERVAL_MS); }

    // Convenience: get active batch size/flush interval based on sink type
    public int getBatchSize() { return "clickhouse".equalsIgnoreCase(getSinkType()) ? getClickhouseBatchSize() : getMysqlBatchSize(); }
    public int getFlushIntervalMs() { return "clickhouse".equalsIgnoreCase(getSinkType()) ? getClickhouseFlushIntervalMs() : getMysqlFlushIntervalMs(); }
    public String getJdbcUrl() { return "clickhouse".equalsIgnoreCase(getSinkType()) ? getClickhouseUrl() : getMysqlUrl(); }
    public String getJdbcUser() { return "clickhouse".equalsIgnoreCase(getSinkType()) ? getClickhouseUser() : getMysqlUser(); }
    public String getJdbcPassword() { return "clickhouse".equalsIgnoreCase(getSinkType()) ? getClickhousePassword() : getMysqlPassword(); }

    // Filter
    public List<String> getMetricNameIncludes() { return getStringList(FlinkConsumerConfigKeys.FILTER_METRIC_NAME_INCLUDE); }
    public List<String> getMetricNameExcludes() { return getStringList(FlinkConsumerConfigKeys.FILTER_METRIC_NAME_EXCLUDE); }

    // Processing
    public int getParallelism() { return getInt(FlinkConsumerConfigKeys.PROCESSING_PARALLELISM, FlinkConsumerConfigKeys.DEFAULT_PARALLELISM); }

    public Config getRawConfig() { return resolvedConfig; }

    private String getString(String path, String defaultValue) {
        try { return resolvedConfig.getString(path); } catch (Exception e) { return defaultValue; }
    }
    private int getInt(String path, int defaultValue) {
        try { return resolvedConfig.getInt(path); } catch (Exception e) { return defaultValue; }
    }
    private List<String> getStringList(String path) {
        try { return resolvedConfig.getStringList(path); }
        catch (Exception e) {
            try {
                String s = resolvedConfig.getString(path);
                return s != null && !s.isEmpty() ? Collections.singletonList(s) : Collections.emptyList();
            } catch (Exception e2) { return Collections.emptyList(); }
        }
    }
}
