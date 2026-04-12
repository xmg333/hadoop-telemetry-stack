package x.mg.metrics.flink.source;

import org.apache.kafka.common.TopicPartition;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Persists Kafka consumer offsets to a local file for checkpoint-based recovery.
 * Format: one line per partition as {@code topic:partition=offset}.
 * Saves are atomic (write to .tmp then rename).
 */
public class OffsetCheckpoint {
    private static final Logger LOG = Logger.getLogger(OffsetCheckpoint.class.getName());

    private final File checkpointFile;
    private final Map<TopicPartition, Long> pendingOffsets = new HashMap<>();

    public OffsetCheckpoint(String path) {
        this.checkpointFile = new File(path);
    }

    /**
     * Load saved offsets from checkpoint file.
     *
     * @return map of TopicPartition → next offset to consume, empty if no checkpoint exists
     */
    public Map<TopicPartition, Long> load() {
        Map<TopicPartition, Long> result = new HashMap<>();
        if (!checkpointFile.exists()) {
            LOG.info("No checkpoint file found: " + checkpointFile.getAbsolutePath());
            return result;
        }
        try {
            List<String> lines = Files.readAllLines(checkpointFile.toPath());
            for (String line : lines) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                int eqIdx = line.indexOf('=');
                int colonIdx = line.lastIndexOf(':', eqIdx - 1);
                if (eqIdx < 0 || colonIdx < 0) continue;
                String topic = line.substring(0, colonIdx);
                int partition = Integer.parseInt(line.substring(colonIdx + 1, eqIdx));
                long offset = Long.parseLong(line.substring(eqIdx + 1));
                result.put(new TopicPartition(topic, partition), offset);
            }
            LOG.info("Loaded checkpoint: " + result.size() + " partitions from " + checkpointFile.getAbsolutePath());
            for (Map.Entry<TopicPartition, Long> e : result.entrySet()) {
                LOG.info("  " + e.getKey().topic() + ":" + e.getKey().partition() + " -> offset " + e.getValue());
            }
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Failed to load checkpoint file, starting fresh", e);
        }
        return result;
    }

    /**
     * Update the offset for a single partition.
     */
    public void update(TopicPartition tp, long offset) {
        pendingOffsets.put(tp, offset);
    }

    /**
     * Atomically save all pending offsets to the checkpoint file.
     */
    public void save() {
        if (pendingOffsets.isEmpty()) return;
        try {
            File tmpFile = new File(checkpointFile.getAbsolutePath() + ".tmp");
            try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(tmpFile)))) {
                writer.println("# flink-consumer offset checkpoint");
                writer.println("# " + new java.util.Date());
                for (Map.Entry<TopicPartition, Long> e : pendingOffsets.entrySet()) {
                    TopicPartition tp = e.getKey();
                    writer.println(tp.topic() + ":" + tp.partition() + "=" + e.getValue());
                }
            }
            Files.move(tmpFile.toPath(), checkpointFile.toPath(),
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Failed to save checkpoint", e);
        }
    }
}
