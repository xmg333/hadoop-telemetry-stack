package x.mg.metrics.diagnostic.checks;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Kafka 检查器 — 返回多个检查项
 */
public class KafkaChecker {

    private final String bootstrapServers;
    private final String metricsTopic;
    private final String tracesTopic;
    private final int timeoutMs;

    public KafkaChecker(String bootstrapServers, String metricsTopic, String tracesTopic, int timeoutMs) {
        this.bootstrapServers = bootstrapServers;
        this.metricsTopic = metricsTopic;
        this.tracesTopic = tracesTopic;
        this.timeoutMs = timeoutMs;
    }

    public List<CheckItem> check() {
        List<CheckItem> items = new ArrayList<>();
        items.add(CheckItem.ok("Bootstrap servers: " + bootstrapServers));

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("request.timeout.ms", timeoutMs);
        props.put("default.api.timeout.ms", timeoutMs);

        try (AdminClient adminClient = AdminClient.create(props)) {
            ListTopicsResult topicsResult = adminClient.listTopics();
            KafkaFuture<Set<String>> topicsFuture = topicsResult.names();
            Set<String> existingTopics = topicsFuture.get();

            items.add(CheckItem.ok("Broker 连接成功，共 " + existingTopics.size() + " 个 topic"));

            // Metrics topic
            if (existingTopics.contains(metricsTopic)) {
                items.add(CheckItem.ok("Topic '" + metricsTopic + "': 存在"));
            } else {
                items.add(CheckItem.warn("Topic '" + metricsTopic + "': 不存在",
                    "OTel Collector 首次导出时会自动创建，或手动创建：\n" +
                    "  kafka-topics.sh --create --topic " + metricsTopic + " --partitions 3 --replication-factor 1"));
            }

            // Traces topic
            if (existingTopics.contains(tracesTopic)) {
                items.add(CheckItem.ok("Topic '" + tracesTopic + "': 存在"));
            } else {
                items.add(CheckItem.warn("Topic '" + tracesTopic + "': 不存在",
                    "若需要 traces pipeline，请创建：\n" +
                    "  kafka-topics.sh --create --topic " + tracesTopic + " --partitions 3 --replication-factor 1"));
            }

        } catch (Exception e) {
            items.add(CheckItem.fail("Kafka 连接失败: " + e.getMessage(),
                "请检查 Kafka broker 是否运行：kubectl get pods -l app=kafka\n" +
                "  或检查 bootstrap.servers 配置"));
        }

        return items;
    }
}
