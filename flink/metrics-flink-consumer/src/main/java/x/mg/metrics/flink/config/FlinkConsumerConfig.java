package x.mg.metrics.flink.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkConsumerConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private transient final Config resolvedConfig;

    // Kafka
    private final String kafkaBootstrapServers;
    private final String kafkaTopic;
    private final String kafkaGroupId;
    private final String kafkaStartupMode;
    private final String kafkaCheckpointPath;

    // Sink
    private final String sinkType;
    private final String mysqlUrl;
    private final String mysqlUser;
    private final String mysqlPassword;
    private final int mysqlBatchSize;
    private final int mysqlFlushIntervalMs;
    private final String clickhouseUrl;
    private final String clickhouseUser;
    private final String clickhousePassword;
    private final int clickhouseBatchSize;
    private final int clickhouseFlushIntervalMs;

    // Filter
    private final List<String> metricNameIncludes;
    private final List<String> metricNameExcludes;

    // Processing
    private final int parallelism;

    public FlinkConsumerConfig(String configPath) {
        Config defaults = ConfigFactory.parseMap(buildDefaults());
        Config fileConfig = loadHoconFile(configPath);
        this.resolvedConfig = fileConfig.withFallback(defaults).resolve();

        // Kafka
        this.kafkaBootstrapServers = getString(FlinkConsumerConfigKeys.KAFKA_BOOTSTRAP_SERVERS, FlinkConsumerConfigKeys.DEFAULT_KAFKA_BOOTSTRAP);
        this.kafkaTopic = getString(FlinkConsumerConfigKeys.KAFKA_TOPIC, FlinkConsumerConfigKeys.DEFAULT_KAFKA_TOPIC);
        this.kafkaGroupId = getString(FlinkConsumerConfigKeys.KAFKA_GROUP_ID, FlinkConsumerConfigKeys.DEFAULT_KAFKA_GROUP_ID);
        this.kafkaStartupMode = getString(FlinkConsumerConfigKeys.KAFKA_STARTUP_MODE, FlinkConsumerConfigKeys.DEFAULT_KAFKA_STARTUP_MODE);
        this.kafkaCheckpointPath = getString(FlinkConsumerConfigKeys.KAFKA_CHECKPOINT_PATH, FlinkConsumerConfigKeys.DEFAULT_KAFKA_CHECKPOINT_PATH);

        // Sink
        this.sinkType = getString(FlinkConsumerConfigKeys.SINK_TYPE, FlinkConsumerConfigKeys.DEFAULT_SINK_TYPE);
        this.mysqlUrl = getString(FlinkConsumerConfigKeys.SINK_MYSQL_URL, FlinkConsumerConfigKeys.DEFAULT_MYSQL_URL);
        this.mysqlUser = getString(FlinkConsumerConfigKeys.SINK_MYSQL_USER, FlinkConsumerConfigKeys.DEFAULT_MYSQL_USER);
        this.mysqlPassword = getString(FlinkConsumerConfigKeys.SINK_MYSQL_PASSWORD, FlinkConsumerConfigKeys.DEFAULT_MYSQL_PASSWORD);
        this.mysqlBatchSize = getInt(FlinkConsumerConfigKeys.SINK_MYSQL_BATCH_SIZE, FlinkConsumerConfigKeys.DEFAULT_MYSQL_BATCH_SIZE);
        this.mysqlFlushIntervalMs = getInt(FlinkConsumerConfigKeys.SINK_MYSQL_FLUSH_INTERVAL_MS, FlinkConsumerConfigKeys.DEFAULT_MYSQL_FLUSH_INTERVAL_MS);
        this.clickhouseUrl = getString(FlinkConsumerConfigKeys.SINK_CLICKHOUSE_URL, FlinkConsumerConfigKeys.DEFAULT_CLICKHOUSE_URL);
        this.clickhouseUser = getString(FlinkConsumerConfigKeys.SINK_CLICKHOUSE_USER, FlinkConsumerConfigKeys.DEFAULT_CLICKHOUSE_USER);
        this.clickhousePassword = getString(FlinkConsumerConfigKeys.SINK_CLICKHOUSE_PASSWORD, FlinkConsumerConfigKeys.DEFAULT_CLICKHOUSE_PASSWORD);
        this.clickhouseBatchSize = getInt(FlinkConsumerConfigKeys.SINK_CLICKHOUSE_BATCH_SIZE, FlinkConsumerConfigKeys.DEFAULT_CLICKHOUSE_BATCH_SIZE);
        this.clickhouseFlushIntervalMs = getInt(FlinkConsumerConfigKeys.SINK_CLICKHOUSE_FLUSH_INTERVAL_MS, FlinkConsumerConfigKeys.DEFAULT_CLICKHOUSE_FLUSH_INTERVAL_MS);

        // Filter
        this.metricNameIncludes = new ArrayList<>(getStringList(FlinkConsumerConfigKeys.FILTER_METRIC_NAME_INCLUDE));
        this.metricNameExcludes = new ArrayList<>(getStringList(FlinkConsumerConfigKeys.FILTER_METRIC_NAME_EXCLUDE));

        // Processing
        this.parallelism = getInt(FlinkConsumerConfigKeys.PROCESSING_PARALLELISM, FlinkConsumerConfigKeys.DEFAULT_PARALLELISM);
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
    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public String getKafkaTopic() { return kafkaTopic; }
    public String getKafkaGroupId() { return kafkaGroupId; }
    public String getKafkaStartupMode() { return kafkaStartupMode; }
    public String getKafkaCheckpointPath() { return kafkaCheckpointPath; }

    // Sink
    public String getSinkType() { return sinkType; }
    public String getMysqlUrl() { return mysqlUrl; }
    public String getMysqlUser() { return mysqlUser; }
    public String getMysqlPassword() { return mysqlPassword; }
    public int getMysqlBatchSize() { return mysqlBatchSize; }
    public int getMysqlFlushIntervalMs() { return mysqlFlushIntervalMs; }
    public String getClickhouseUrl() { return clickhouseUrl; }
    public String getClickhouseUser() { return clickhouseUser; }
    public String getClickhousePassword() { return clickhousePassword; }
    public int getClickhouseBatchSize() { return clickhouseBatchSize; }
    public int getClickhouseFlushIntervalMs() { return clickhouseFlushIntervalMs; }

    // Convenience: get active batch size/flush interval based on sink type
    public int getBatchSize() { return "clickhouse".equalsIgnoreCase(sinkType) ? clickhouseBatchSize : mysqlBatchSize; }
    public int getFlushIntervalMs() { return "clickhouse".equalsIgnoreCase(sinkType) ? clickhouseFlushIntervalMs : mysqlFlushIntervalMs; }
    public String getJdbcUrl() { return "clickhouse".equalsIgnoreCase(sinkType) ? clickhouseUrl : mysqlUrl; }
    public String getJdbcUser() { return "clickhouse".equalsIgnoreCase(sinkType) ? clickhouseUser : mysqlUser; }
    public String getJdbcPassword() { return "clickhouse".equalsIgnoreCase(sinkType) ? clickhousePassword : mysqlPassword; }

    // Filter
    public List<String> getMetricNameIncludes() { return metricNameIncludes; }
    public List<String> getMetricNameExcludes() { return metricNameExcludes; }

    // Processing
    public int getParallelism() { return parallelism; }

    @SuppressWarnings("unused")
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
