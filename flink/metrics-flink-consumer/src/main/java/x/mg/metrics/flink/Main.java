package x.mg.metrics.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import x.mg.metrics.flink.config.FlinkConsumerConfig;
import x.mg.metrics.flink.operator.AccumulatingProcessFunction;
import x.mg.metrics.flink.operator.MetricRecordSplitFlatMap;
import x.mg.metrics.flink.filter.MetricNameFilter;
import x.mg.metrics.flink.sink.FlinkCategoryJdbcSink;
import x.mg.metrics.flink.source.OtlpDeserializationSchema;

import java.util.logging.Logger;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {
        String configPath = args.length > 0 ? args[0] : null;
        FlinkConsumerConfig config = new FlinkConsumerConfig(configPath);

        LOG.info("Metrics Flink Consumer starting...");
        LOG.info("Kafka: " + config.getKafkaBootstrapServers() + " / " + config.getKafkaTopic());
        LOG.info("Sink: " + config.getSinkType() + " -> " + config.getJdbcUrl());
        LOG.info("Parallelism: " + config.getParallelism());
        LOG.info("Startup mode: " + config.getKafkaStartupMode());

        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.getParallelism());

        // Enable checkpointing for exactly-once offset commit
        long checkpointIntervalMs = Math.max(config.getFlushIntervalMs(), 5000);
        env.enableCheckpointing(checkpointIntervalMs);
        env.getCheckpointConfig().setCheckpointStorage("file://" + config.getKafkaCheckpointPath());
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        LOG.info("Checkpoint interval: " + checkpointIntervalMs + "ms, path: " + config.getKafkaCheckpointPath());

        // Build KafkaSource using Flink connector
        KafkaSource<OtlpDeserializationSchema.MetricRecord> kafkaSource = KafkaSource
            .<OtlpDeserializationSchema.MetricRecord>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setTopics(config.getKafkaTopic())
            .setGroupId(config.getKafkaGroupId())
            .setStartingOffsets(resolveStartupMode(config.getKafkaStartupMode()))
            .setDeserializer(new OtlpDeserializationSchema())
            .build();

        // Build pipeline
        DataStream<OtlpDeserializationSchema.MetricRecord> rawStream = env.fromSource(
            kafkaSource, WatermarkStrategy.noWatermarks(), "OTLP Kafka Source");

        MetricNameFilter nameFilter = new MetricNameFilter(
            config.getMetricNameIncludes(), config.getMetricNameExcludes());

        rawStream
            .flatMap(new MetricRecordSplitFlatMap())
            .filter(item -> nameFilter.test(item.getMetricName()))
            .keyBy(item -> "all")
            .process(new AccumulatingProcessFunction(config.getBatchSize(), config.getFlushIntervalMs()))
            .addSink(new FlinkCategoryJdbcSink(
                config.getJdbcUrl(), config.getJdbcUser(), config.getJdbcPassword(),
                config.getBatchSize(), config.getFlushIntervalMs(),
                "clickhouse".equalsIgnoreCase(config.getSinkType())))
            .name(config.getSinkType().toUpperCase() + " Sink");

        env.execute("Metrics Flink Consumer");
    }

    private static OffsetsInitializer resolveStartupMode(String mode) {
        if ("latest-offset".equals(mode)) {
            return OffsetsInitializer.latest();
        }
        return OffsetsInitializer.earliest();
    }
}
