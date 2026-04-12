package x.mg.metrics.flink.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class FlinkConsumerConfigTest {

    @Test
    void loadsDefaults() {
        FlinkConsumerConfig config = new FlinkConsumerConfig(null);
        assertEquals("localhost:9092", config.getKafkaBootstrapServers());
        assertEquals("telemetry-metrics", config.getKafkaTopic());
        assertEquals("mysql", config.getSinkType());
        assertEquals(2, config.getParallelism());
    }

    @Test
    void loadsFromFile(@TempDir Path tempDir) throws Exception {
        File conf = tempDir.resolve("test.conf").toFile();
        try (PrintWriter pw = new PrintWriter(conf)) {
            pw.println("flink-consumer {");
            pw.println("  kafka { bootstrap.servers = \"kafka:9092\", topic = \"test-topic\" }");
            pw.println("  sink { type = clickhouse }");
            pw.println("}");
        }
        FlinkConsumerConfig config = new FlinkConsumerConfig(conf.getAbsolutePath());
        assertEquals("kafka:9092", config.getKafkaBootstrapServers());
        assertEquals("test-topic", config.getKafkaTopic());
        assertEquals("clickhouse", config.getSinkType());
        assertEquals("jdbc:clickhouse://localhost:8123/metrics_db", config.getJdbcUrl());
    }

    @Test
    void sinkTypeDeterminesJdbcParams() {
        FlinkConsumerConfig config = new FlinkConsumerConfig(null);
        // Default is mysql
        assertEquals("jdbc:mysql://localhost:3306/metrics_db", config.getJdbcUrl());
        assertEquals("metrics", config.getJdbcUser());
    }
}
