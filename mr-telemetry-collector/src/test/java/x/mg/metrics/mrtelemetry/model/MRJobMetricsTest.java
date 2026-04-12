package x.mg.metrics.mrtelemetry.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MRJobMetricsTest {

    @Test
    void testDefaultValues() {
        MRJobMetrics metrics = new MRJobMetrics();

        // Strings default to null
        assertNull(metrics.getJobId());
        assertNull(metrics.getJobName());
        assertNull(metrics.getUser());
        assertNull(metrics.getQueue());
        assertNull(metrics.getState());

        // Numeric fields default to 0
        assertEquals(0L, metrics.getSubmitTime());
        assertEquals(0L, metrics.getStartTime());
        assertEquals(0L, metrics.getFinishTime());
        assertEquals(0L, metrics.getElapsedTime());
        assertEquals(0, metrics.getTotalMaps());
        assertEquals(0, metrics.getTotalReduces());
        assertEquals(0, metrics.getFailedMaps());
        assertEquals(0, metrics.getFailedReduces());

        assertEquals(0L, metrics.getHdfsBytesRead());
        assertEquals(0L, metrics.getHdfsBytesWritten());
        assertEquals(0L, metrics.getFileBytesRead());
        assertEquals(0L, metrics.getFileBytesWritten());

        assertEquals(0L, metrics.getMapInputRecords());
        assertEquals(0L, metrics.getMapOutputRecords());
        assertEquals(0L, metrics.getMapOutputBytes());
        assertEquals(0L, metrics.getReduceInputRecords());
        assertEquals(0L, metrics.getReduceOutputRecords());
        assertEquals(0L, metrics.getReduceShuffleBytes());
        assertEquals(0L, metrics.getSpilledRecords());

        assertEquals(0L, metrics.getCpuMilliseconds());
        assertEquals(0L, metrics.getGcTimeMillis());
        assertEquals(0L, metrics.getPhysicalMemoryBytes());
        assertEquals(0L, metrics.getVirtualMemoryBytes());
        assertEquals(0L, metrics.getCommittedHeapBytes());

        assertEquals(0L, metrics.getMillisMaps());
        assertEquals(0L, metrics.getMillisReduces());
    }

    @Test
    void testSettersAndGetters() {
        MRJobMetrics metrics = new MRJobMetrics();

        metrics.setJobId("job_001");
        metrics.setJobName("wordcount");
        metrics.setUser("testuser");
        metrics.setQueue("default");
        metrics.setState("SUCCEEDED");

        metrics.setSubmitTime(1000L);
        metrics.setStartTime(2000L);
        metrics.setFinishTime(5000L);
        metrics.setElapsedTime(3000L);

        metrics.setTotalMaps(4);
        metrics.setTotalReduces(2);
        metrics.setFailedMaps(1);
        metrics.setFailedReduces(0);

        metrics.setHdfsBytesRead(1024L);
        metrics.setHdfsBytesWritten(2048L);
        metrics.setFileBytesRead(512L);
        metrics.setFileBytesWritten(1024L);

        metrics.setMapInputRecords(100L);
        metrics.setMapOutputRecords(200L);
        metrics.setMapOutputBytes(4096L);
        metrics.setReduceInputRecords(200L);
        metrics.setReduceOutputRecords(50L);
        metrics.setReduceShuffleBytes(8192L);
        metrics.setSpilledRecords(10L);

        metrics.setCpuMilliseconds(5000L);
        metrics.setGcTimeMillis(200L);
        metrics.setPhysicalMemoryBytes(1073741824L);
        metrics.setVirtualMemoryBytes(2147483648L);
        metrics.setCommittedHeapBytes(536870912L);

        metrics.setMillisMaps(3000L);
        metrics.setMillisReduces(2000L);

        assertEquals("job_001", metrics.getJobId());
        assertEquals("wordcount", metrics.getJobName());
        assertEquals("testuser", metrics.getUser());
        assertEquals("default", metrics.getQueue());
        assertEquals("SUCCEEDED", metrics.getState());

        assertEquals(1000L, metrics.getSubmitTime());
        assertEquals(2000L, metrics.getStartTime());
        assertEquals(5000L, metrics.getFinishTime());
        assertEquals(3000L, metrics.getElapsedTime());

        assertEquals(4, metrics.getTotalMaps());
        assertEquals(2, metrics.getTotalReduces());
        assertEquals(1, metrics.getFailedMaps());
        assertEquals(0, metrics.getFailedReduces());

        assertEquals(1024L, metrics.getHdfsBytesRead());
        assertEquals(2048L, metrics.getHdfsBytesWritten());
        assertEquals(512L, metrics.getFileBytesRead());
        assertEquals(1024L, metrics.getFileBytesWritten());

        assertEquals(100L, metrics.getMapInputRecords());
        assertEquals(200L, metrics.getMapOutputRecords());
        assertEquals(4096L, metrics.getMapOutputBytes());
        assertEquals(200L, metrics.getReduceInputRecords());
        assertEquals(50L, metrics.getReduceOutputRecords());
        assertEquals(8192L, metrics.getReduceShuffleBytes());
        assertEquals(10L, metrics.getSpilledRecords());

        assertEquals(5000L, metrics.getCpuMilliseconds());
        assertEquals(200L, metrics.getGcTimeMillis());
        assertEquals(1073741824L, metrics.getPhysicalMemoryBytes());
        assertEquals(2147483648L, metrics.getVirtualMemoryBytes());
        assertEquals(536870912L, metrics.getCommittedHeapBytes());

        assertEquals(3000L, metrics.getMillisMaps());
        assertEquals(2000L, metrics.getMillisReduces());
    }

    @Test
    void testCounterGroups() {
        MRJobMetrics metrics = new MRJobMetrics();

        // No counters initially
        assertNull(metrics.getCounter("FileSystemCounter", "HDFS_BYTES_READ"));

        // Add counters
        metrics.addCounter("org.apache.hadoop.mapreduce.FileSystemCounter", "HDFS_BYTES_READ", 1024L);
        metrics.addCounter("org.apache.hadoop.mapreduce.FileSystemCounter", "HDFS_BYTES_WRITTEN", 2048L);
        metrics.addCounter("org.apache.hadoop.mapreduce.TaskCounter", "CPU_MILLISECONDS", 5000L);

        // Verify retrieval
        assertEquals(1024L, metrics.getCounter("org.apache.hadoop.mapreduce.FileSystemCounter", "HDFS_BYTES_READ").longValue());
        assertEquals(2048L, metrics.getCounter("org.apache.hadoop.mapreduce.FileSystemCounter", "HDFS_BYTES_WRITTEN").longValue());
        assertEquals(5000L, metrics.getCounter("org.apache.hadoop.mapreduce.TaskCounter", "CPU_MILLISECONDS").longValue());

        // Non-existent counter
        assertNull(metrics.getCounter("org.apache.hadoop.mapreduce.FileSystemCounter", "NON_EXISTENT"));

        // Non-existent group
        assertNull(metrics.getCounter("NonExistentGroup", "ANY"));

        // Verify group map size
        assertEquals(2, metrics.getCounterGroups().size());
        assertEquals(2, metrics.getCounterGroups().get("org.apache.hadoop.mapreduce.FileSystemCounter").size());
        assertEquals(1, metrics.getCounterGroups().get("org.apache.hadoop.mapreduce.TaskCounter").size());
    }
}
