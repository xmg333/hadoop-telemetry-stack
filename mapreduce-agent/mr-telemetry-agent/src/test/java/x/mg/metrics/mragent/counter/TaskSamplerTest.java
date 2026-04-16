package x.mg.metrics.mragent.counter;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TaskSamplerTest {

    @Test
    void testComputeDeltasFirstSample() {
        Map<String, Long> prev = new HashMap<>();
        Map<String, Long> curr = new HashMap<>();
        curr.put("mr.task.io.hdfs_bytes_read", 1000L);
        curr.put("mr.task.cpu_time_ms", 500L);

        Map<String, Long> deltas = TaskSampler.computeDeltas(prev, curr);

        // First sample: all values reported as deltas
        assertEquals(1000L, deltas.get("mr.task.io.hdfs_bytes_read"));
        assertEquals(500L, deltas.get("mr.task.cpu_time_ms"));
    }

    @Test
    void testComputeDeltasIncremental() {
        Map<String, Long> prev = new HashMap<>();
        prev.put("mr.task.io.hdfs_bytes_read", 1000L);
        prev.put("mr.task.cpu_time_ms", 500L);

        Map<String, Long> curr = new HashMap<>();
        curr.put("mr.task.io.hdfs_bytes_read", 2500L);
        curr.put("mr.task.cpu_time_ms", 800L);

        Map<String, Long> deltas = TaskSampler.computeDeltas(prev, curr);

        assertEquals(1500L, deltas.get("mr.task.io.hdfs_bytes_read"));
        assertEquals(300L, deltas.get("mr.task.cpu_time_ms"));
    }

    @Test
    void testComputeDeltasNoChange() {
        Map<String, Long> prev = new HashMap<>();
        prev.put("mr.task.io.hdfs_bytes_read", 1000L);

        Map<String, Long> curr = new HashMap<>();
        curr.put("mr.task.io.hdfs_bytes_read", 1000L);

        Map<String, Long> deltas = TaskSampler.computeDeltas(prev, curr);

        // No change means no delta (delta=0 filtered out)
        assertNull(deltas.get("mr.task.io.hdfs_bytes_read"));
        assertTrue(deltas.isEmpty());
    }

    @Test
    void testComputeDeltasEmpty() {
        Map<String, Long> prev = new HashMap<>();
        Map<String, Long> curr = new HashMap<>();

        Map<String, Long> deltas = TaskSampler.computeDeltas(prev, curr);

        assertTrue(deltas.isEmpty());
    }
}
