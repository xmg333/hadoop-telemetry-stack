package x.mg.metrics.mragent.counter;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests CounterReader using mock objects that mimic the Hadoop API.
 * CounterReader uses reflection via context.getClass().getMethod(...),
 * so mock classes with matching method signatures work as stand-ins.
 */
class CounterReaderTest {

    // --- Mock Hadoop API classes ---

    public static class MockCounter {
        private final long value;
        public MockCounter(long value) { this.value = value; }
        public long getValue() { return value; }
    }

    public static class MockCounterGroup {
        private final Map<String, MockCounter> counters = new HashMap<>();
        public void addCounter(String name, long value) { counters.put(name, new MockCounter(value)); }
        public MockCounter findCounter(String name) { return counters.get(name); }
    }

    public static class MockCounters {
        private final Map<String, MockCounterGroup> groups = new HashMap<>();
        public void addGroup(String name, MockCounterGroup group) { groups.put(name, group); }
        public MockCounterGroup getGroup(String name) { return groups.get(name); }
    }

    public static class MockConfiguration {
        private final Map<String, String> props = new HashMap<>();
        public void set(String key, String value) { props.put(key, value); }
        public String get(String key, String defaultValue) { return props.getOrDefault(key, defaultValue); }
    }

    public static class MockTaskAttemptID {
        private final String id;
        public MockTaskAttemptID(String id) { this.id = id; }
        @Override public String toString() { return id; }
    }

    public static class MockJobID {
        private final String id;
        public MockJobID(String id) { this.id = id; }
        @Override public String toString() { return id; }
    }

    public static class MockContext {
        private final MockCounters counters;
        private final MockConfiguration configuration;
        private final MockTaskAttemptID taskAttemptID;
        private final MockJobID jobID;

        public MockContext(MockCounters counters, MockConfiguration configuration,
                           MockTaskAttemptID taskAttemptID, MockJobID jobID) {
            this.counters = counters;
            this.configuration = configuration;
            this.taskAttemptID = taskAttemptID;
            this.jobID = jobID;
        }

        public MockCounters getCounters() { return counters; }
        public MockConfiguration getConfiguration() { return configuration; }
        public MockTaskAttemptID getTaskAttemptID() { return taskAttemptID; }
        public MockJobID getJobID() { return jobID; }

        /** New MapReduce API: getCounter(groupName, counterName) */
        public MockCounter getCounter(String groupName, String counterName) {
            MockCounterGroup group = counters.getGroup(groupName);
            if (group == null) return null;
            return group.findCounter(counterName);
        }
    }

    // --- Helper to build a mock context ---

    private MockContext buildMockContext() {
        MockCounterGroup taskGroup = new MockCounterGroup();
        taskGroup.addCounter("MAP_INPUT_RECORDS", 1000L);
        taskGroup.addCounter("MAP_OUTPUT_RECORDS", 950L);
        taskGroup.addCounter("MAP_OUTPUT_BYTES", 102400L);
        taskGroup.addCounter("SPILLED_RECORDS", 10L);
        taskGroup.addCounter("CPU_MILLISECONDS", 5000L);
        taskGroup.addCounter("GC_TIME_MILLIS", 200L);

        MockCounterGroup fsGroup = new MockCounterGroup();
        fsGroup.addCounter("HDFS_BYTES_READ", 2048000L);
        fsGroup.addCounter("HDFS_BYTES_WRITTEN", 512000L);
        fsGroup.addCounter("FILE_BYTES_READ", 0L);
        fsGroup.addCounter("FILE_BYTES_WRITTEN", 4096L);

        MockCounters counters = new MockCounters();
        counters.addGroup("org.apache.hadoop.mapreduce.TaskCounter", taskGroup);
        counters.addGroup("org.apache.hadoop.mapreduce.FileSystemCounter", fsGroup);

        MockConfiguration config = new MockConfiguration();
        config.set("mapreduce.job.name", "word-count");
        config.set("mapreduce.job.user.name", "testuser");
        config.set("mapreduce.job.queuename", "default");

        return new MockContext(counters, config,
            new MockTaskAttemptID("attempt_123456_m_000001_0"),
            new MockJobID("job_123456"));
    }

    @Test
    void testReadCounters() {
        MockContext context = buildMockContext();
        CounterReader reader = new CounterReader();

        Map<String, Long> counters = reader.readCounters(context);

        assertFalse(counters.isEmpty());
        assertEquals(1000L, counters.get("mr.task.io.map_input_records"));
        assertEquals(950L, counters.get("mr.task.io.map_output_records"));
        assertEquals(102400L, counters.get("mr.task.io.map_output_bytes"));
        assertEquals(2048000L, counters.get("mr.task.io.hdfs_bytes_read"));
        assertEquals(512000L, counters.get("mr.task.io.hdfs_bytes_written"));
        assertEquals(5000L, counters.get("mr.task.cpu_time_ms"));
        assertEquals(200L, counters.get("mr.task.gc_time_ms"));
        assertEquals(4096L, counters.get("mr.task.io.file_bytes_written"));
        assertEquals(10L, counters.get("mr.task.io.spilled_records"));
    }

    @Test
    void testExtractTaskIdentity() {
        MockContext context = buildMockContext();
        CounterReader reader = new CounterReader();

        TaskIdentity identity = reader.extractTaskIdentity(context);

        assertEquals("attempt_123456_m_000001_0", identity.getTaskId());
        assertEquals("job_123456", identity.getJobId());
        assertEquals("word-count", identity.getJobName());
        assertEquals("testuser", identity.getUser());
        assertEquals("default", identity.getQueue());
    }

    @Test
    void testReadCountersNullContext() {
        CounterReader reader = new CounterReader();
        Map<String, Long> counters = reader.readCounters(null);
        assertTrue(counters.isEmpty());
    }

    @Test
    void testExtractTaskIdentityNullContext() {
        CounterReader reader = new CounterReader();
        TaskIdentity identity = reader.extractTaskIdentity(null);
        assertEquals(TaskIdentity.UNKNOWN.getTaskId(), identity.getTaskId());
    }

    @Test
    void testReadCountersWithMissingCounters() {
        // Context with empty counter groups (groups exist but findCounter returns null)
        MockCounterGroup emptyGroup = new MockCounterGroup();
        MockCounters counters = new MockCounters();
        counters.addGroup("org.apache.hadoop.mapreduce.TaskCounter", emptyGroup);
        counters.addGroup("org.apache.hadoop.mapreduce.FileSystemCounter", emptyGroup);

        MockContext context = new MockContext(counters,
            new MockConfiguration(),
            new MockTaskAttemptID("id"),
            new MockJobID("jid"));

        CounterReader reader = new CounterReader();
        Map<String, Long> result = reader.readCounters(context);

        // All counter values should be 0 (findCounter returns null → 0L)
        assertTrue(result.isEmpty() || result.values().stream().allMatch(v -> v == 0L));
    }
}
