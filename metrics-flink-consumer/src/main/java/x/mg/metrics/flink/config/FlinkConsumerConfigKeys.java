package x.mg.metrics.flink.config;

public final class FlinkConsumerConfigKeys {
    private FlinkConsumerConfigKeys() {}

    public static final String PREFIX = "flink-consumer";

    // Kafka
    public static final String KAFKA_BOOTSTRAP_SERVERS = "flink-consumer.kafka.bootstrap.servers";
    public static final String KAFKA_TOPIC = "flink-consumer.kafka.topic";
    public static final String KAFKA_GROUP_ID = "flink-consumer.kafka.group.id";
    public static final String KAFKA_STARTUP_MODE = "flink-consumer.kafka.startup.mode";
    public static final String KAFKA_CHECKPOINT_PATH = "flink-consumer.kafka.checkpoint.path";

    // Sink
    public static final String SINK_TYPE = "flink-consumer.sink.type";
    public static final String SINK_MYSQL_URL = "flink-consumer.sink.mysql.url";
    public static final String SINK_MYSQL_USER = "flink-consumer.sink.mysql.user";
    public static final String SINK_MYSQL_PASSWORD = "flink-consumer.sink.mysql.password";
    public static final String SINK_MYSQL_BATCH_SIZE = "flink-consumer.sink.mysql.batch.size";
    public static final String SINK_MYSQL_FLUSH_INTERVAL_MS = "flink-consumer.sink.mysql.flush.interval.ms";
    public static final String SINK_CLICKHOUSE_URL = "flink-consumer.sink.clickhouse.url";
    public static final String SINK_CLICKHOUSE_USER = "flink-consumer.sink.clickhouse.user";
    public static final String SINK_CLICKHOUSE_PASSWORD = "flink-consumer.sink.clickhouse.password";
    public static final String SINK_CLICKHOUSE_BATCH_SIZE = "flink-consumer.sink.clickhouse.batch.size";
    public static final String SINK_CLICKHOUSE_FLUSH_INTERVAL_MS = "flink-consumer.sink.clickhouse.flush.interval.ms";

    // Filter
    public static final String FILTER_METRIC_NAME_INCLUDE = "flink-consumer.filter.metric.name.include";
    public static final String FILTER_METRIC_NAME_EXCLUDE = "flink-consumer.filter.metric.name.exclude";

    // Processing
    public static final String PROCESSING_PARALLELISM = "flink-consumer.processing.parallelism";

    // Defaults
    public static final String DEFAULT_KAFKA_BOOTSTRAP = "localhost:9092";
    public static final String DEFAULT_KAFKA_TOPIC = "telemetry-metrics";
    public static final String DEFAULT_KAFKA_GROUP_ID = "flink-metrics-consumer";
    public static final String DEFAULT_KAFKA_STARTUP_MODE = "latest-offset";
    public static final String DEFAULT_KAFKA_CHECKPOINT_PATH = "/tmp/flink-consumer-checkpoint.txt";
    public static final String DEFAULT_SINK_TYPE = "mysql";
    public static final String DEFAULT_MYSQL_URL = "jdbc:mysql://localhost:3306/metrics_db";
    public static final String DEFAULT_MYSQL_USER = "metrics";
    public static final String DEFAULT_MYSQL_PASSWORD = "metrics";
    public static final int DEFAULT_MYSQL_BATCH_SIZE = 1000;
    public static final int DEFAULT_MYSQL_FLUSH_INTERVAL_MS = 5000;
    public static final String DEFAULT_CLICKHOUSE_URL = "jdbc:clickhouse://localhost:8123/metrics_db";
    public static final String DEFAULT_CLICKHOUSE_USER = "default";
    public static final String DEFAULT_CLICKHOUSE_PASSWORD = "";
    public static final int DEFAULT_CLICKHOUSE_BATCH_SIZE = 5000;
    public static final int DEFAULT_CLICKHOUSE_FLUSH_INTERVAL_MS = 3000;
    public static final int DEFAULT_PARALLELISM = 2;
}
