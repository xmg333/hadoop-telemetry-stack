package x.mg.metrics.flink;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import x.mg.metrics.flink.config.FlinkConsumerConfig;
import x.mg.metrics.flink.deserialize.OtlpMetricsDeserializer;
import x.mg.metrics.flink.filter.MetricNameFilter;
import x.mg.metrics.flink.model.HistogramBucket;
import x.mg.metrics.flink.model.MetricSample;
import x.mg.metrics.flink.sink.CategoryJdbcSink;
import x.mg.metrics.flink.source.OffsetCheckpoint;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class.getName());
    private static volatile boolean running = true;

    public static void main(String[] args) throws Exception {
        String configPath = args.length > 0 ? args[0] : null;
        FlinkConsumerConfig config = new FlinkConsumerConfig(configPath);

        LOG.info("Metrics Consumer starting...");
        LOG.info("Kafka: " + config.getKafkaBootstrapServers() + " / " + config.getKafkaTopic());
        LOG.info("Sink: " + config.getSinkType() + " -> " + config.getJdbcUrl());
        LOG.info("Checkpoint: " + config.getKafkaCheckpointPath());
        LOG.info("Startup mode: " + config.getKafkaStartupMode());

        // Load checkpoint
        OffsetCheckpoint checkpoint = new OffsetCheckpoint(config.getKafkaCheckpointPath());
        Map<TopicPartition, Long> savedOffsets = checkpoint.load();

        // Build Kafka consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getKafkaBootstrapServers());
        props.put("group.id", config.getKafkaGroupId());
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        props.put("fetch.max.bytes", "52428800");
        props.put("max.partition.fetch.bytes", "10485760");
        props.put("fetch.max.wait.ms", "5000");
        props.put("session.timeout.ms", "30000");
        props.put("request.timeout.ms", "60000");

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);

        // Discover and assign all partitions
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(config.getKafkaTopic());
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            LOG.severe("Topic not found: " + config.getKafkaTopic());
            consumer.close();
            return;
        }
        List<TopicPartition> allPartitions = new ArrayList<>();
        for (PartitionInfo pi : partitionInfos) {
            allPartitions.add(new TopicPartition(config.getKafkaTopic(), pi.partition()));
        }
        consumer.assign(allPartitions);
        LOG.info("Assigned to " + allPartitions.size() + " partitions: " + allPartitions);

        // Seek to offsets: checkpoint > startup.mode
        if (!savedOffsets.isEmpty()) {
            for (TopicPartition tp : allPartitions) {
                Long offset = savedOffsets.get(tp);
                if (offset != null) {
                    consumer.seek(tp, offset);
                    LOG.info("  " + tp + " -> checkpoint offset " + offset);
                } else {
                    // Partition not in checkpoint, use startup mode
                    if ("latest-offset".equals(config.getKafkaStartupMode())) {
                        consumer.seekToEnd(Collections.singletonList(tp));
                    } else {
                        consumer.seekToBeginning(Collections.singletonList(tp));
                    }
                    LOG.info("  " + tp + " -> " + config.getKafkaStartupMode());
                }
            }
        } else {
            String mode = config.getKafkaStartupMode();
            if ("latest-offset".equals(mode)) {
                consumer.seekToEnd(allPartitions);
                LOG.info("No checkpoint, seeking to end (latest-offset)");
            } else {
                consumer.seekToBeginning(allPartitions);
                LOG.info("No checkpoint, seeking to beginning (earliest-offset)");
            }
        }

        // Build deserializer and filter
        OtlpMetricsDeserializer deserializer = new OtlpMetricsDeserializer();
        MetricNameFilter nameFilter = new MetricNameFilter(
                config.getMetricNameIncludes(),
                config.getMetricNameExcludes());

        // Open category JDBC sink
        CategoryJdbcSink sink = new CategoryJdbcSink(config);
        sink.open();

        // Track current offsets per partition for checkpoint
        Map<TopicPartition, Long> currentOffsets = new HashMap<>();

        // Shutdown hook for graceful exit
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received...");
            running = false;
        }, "shutdown-hook"));

        long totalRecords = 0;
        long lastCheckpointRecordCount = 0;

        // Main loop
        while (running) {
            try {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(5));
                if (records.isEmpty()) continue;

                LOG.info("Polled " + records.count() + " records");

                for (ConsumerRecord<byte[], byte[]> record : records) {
                    try {
                        OtlpMetricsDeserializer.DeserializationResult result = deserializer.deserialize(record.value());
                        for (MetricSample sample : result.getSamples()) {
                            if (nameFilter.test(sample.getMetricName())) {
                                sink.invoke(sample);
                            }
                        }
                        for (HistogramBucket bucket : result.getBuckets()) {
                            if (nameFilter.test(bucket.getMetricName())) {
                                sink.invoke(bucket);
                            }
                        }
                        totalRecords++;

                        // Track max offset per partition (offset + 1 = next offset to consume)
                        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                        currentOffsets.put(tp, record.offset() + 1);
                    } catch (Exception e) {
                        LOG.log(Level.WARNING, "Failed to deserialize record at offset " + record.offset(), e);
                    }
                }

                // Flush sink and save checkpoint
                try {
                    sink.flush();
                    // Only update checkpoint after successful flush
                    for (Map.Entry<TopicPartition, Long> e : currentOffsets.entrySet()) {
                        checkpoint.update(e.getKey(), e.getValue());
                    }
                    checkpoint.save();
                    lastCheckpointRecordCount = totalRecords;
                } catch (Exception e) {
                    LOG.log(Level.WARNING, "Flush/checkpoint failed, will retry next cycle", e);
                    // Don't update checkpoint - on restart, will reprocess these records
                }

                if (totalRecords % 100 == 0) {
                    LOG.info("Total records processed: " + totalRecords + " (checkpointed at " + lastCheckpointRecordCount + ")");
                }
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Poll error", e);
                if (!running) break;
            }
        }

        // Final cleanup
        LOG.info("Shutting down...");
        try {
            sink.flush();
            for (Map.Entry<TopicPartition, Long> e : currentOffsets.entrySet()) {
                checkpoint.update(e.getKey(), e.getValue());
            }
            checkpoint.save();
            LOG.info("Final checkpoint saved");
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Final flush/checkpoint failed", e);
        }

        sink.close();
        consumer.close();
        LOG.info("Metrics Consumer stopped. Total records: " + totalRecords);
    }
}
