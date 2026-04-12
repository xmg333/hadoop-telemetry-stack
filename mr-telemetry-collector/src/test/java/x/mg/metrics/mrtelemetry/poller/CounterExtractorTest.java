package x.mg.metrics.mrtelemetry.poller;

import org.junit.jupiter.api.Test;
import x.mg.metrics.mrtelemetry.model.MRJobMetrics;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CounterExtractorTest {

    private final CounterExtractor extractor = new CounterExtractor();

    private static final String JOBS_JSON = "{\"jobs\":{\"job\":[{\"id\":\"job_1234\",\"name\":\"wordcount\","
            + "\"user\":\"testuser\",\"queue\":\"default\",\"state\":\"SUCCEEDED\","
            + "\"submitTime\":1000,\"startTime\":2000,\"finishTime\":5000,\"elapsedTime\":3000,"
            + "\"mapsTotal\":2,\"reducesTotal\":1,\"mapsCompleted\":2,\"reducesCompleted\":1,"
            + "\"trackingUrl\":\"http://tracker\"}]}}";

    private static final String SINGLE_JOB_JSON = "{\"jobs\":{\"job\":{\"id\":\"job_5678\","
            + "\"name\":\"teragen\",\"user\":\"hadoop\",\"queue\":\"research\",\"state\":\"RUNNING\","
            + "\"submitTime\":100,\"startTime\":200,\"finishTime\":0,\"elapsedTime\":100,"
            + "\"mapsTotal\":4,\"reducesTotal\":0,\"mapsCompleted\":1,\"reducesCompleted\":0,"
            + "\"trackingUrl\":\"http://tracker2\"}}}";

    private static final String EMPTY_JOBS_JSON = "{\"jobs\":{\"job\":[]}}";

    private static final String COUNTERS_JSON = "{\"jobCounters\":{\"counterGroup\":["
            + "{\"counterGroupName\":\"org.apache.hadoop.mapreduce.FileSystemCounter\",\"counter\":["
            + "{\"name\":\"HDFS_BYTES_READ\",\"totalCounterValue\":1024,\"mapCounterValue\":512,\"reduceCounterValue\":512},"
            + "{\"name\":\"HDFS_BYTES_WRITTEN\",\"totalCounterValue\":2048,\"mapCounterValue\":0,\"reduceCounterValue\":2048}"
            + "]},"
            + "{\"counterGroupName\":\"org.apache.hadoop.mapreduce.TaskCounter\",\"counter\":["
            + "{\"name\":\"CPU_MILLISECONDS\",\"totalCounterValue\":5000,\"mapCounterValue\":3000,\"reduceCounterValue\":2000},"
            + "{\"name\":\"GC_TIME_MILLIS\",\"totalCounterValue\":200,\"mapCounterValue\":150,\"reduceCounterValue\":50},"
            + "{\"name\":\"MAP_INPUT_RECORDS\",\"totalCounterValue\":100,\"mapCounterValue\":100,\"reduceCounterValue\":0},"
            + "{\"name\":\"SPILLED_RECORDS\",\"totalCounterValue\":50,\"mapCounterValue\":30,\"reduceCounterValue\":20}"
            + "]},"
            + "{\"counterGroupName\":\"org.apache.hadoop.mapreduce.JobCounter\",\"counter\":["
            + "{\"name\":\"TOTAL_LAUNCHED_MAPS\",\"totalCounterValue\":2,\"mapCounterValue\":0,\"reduceCounterValue\":0},"
            + "{\"name\":\"MILLIS_MAPS\",\"totalCounterValue\":3000,\"mapCounterValue\":0,\"reduceCounterValue\":0}"
            + "]"
            + "}]}}";

    @Test
    void testParseJobs() throws IOException {
        List<CounterExtractor.JobInfo> jobs = extractor.parseJobs(JOBS_JSON);

        assertEquals(1, jobs.size());
        CounterExtractor.JobInfo job = jobs.get(0);
        assertEquals("job_1234", job.id);
        assertEquals("wordcount", job.name);
        assertEquals("testuser", job.user);
        assertEquals("default", job.queue);
        assertEquals("SUCCEEDED", job.state);
        assertEquals(1000L, job.submitTime);
        assertEquals(2000L, job.startTime);
        assertEquals(5000L, job.finishTime);
        assertEquals(3000L, job.elapsedTime);
        assertEquals(2, job.totalMaps);
        assertEquals(1, job.totalReduces);
        assertEquals(2, job.failedMaps);
        assertEquals(1, job.failedReduces);
        assertEquals("http://tracker", job.trackingUrl);
    }

    @Test
    void testParseSingleJob() throws IOException {
        List<CounterExtractor.JobInfo> jobs = extractor.parseJobs(SINGLE_JOB_JSON);

        // Single object (not array) should still produce one job
        assertEquals(1, jobs.size());
        CounterExtractor.JobInfo job = jobs.get(0);
        assertEquals("job_5678", job.id);
        assertEquals("teragen", job.name);
        assertEquals("hadoop", job.user);
        assertEquals("research", job.queue);
        assertEquals("RUNNING", job.state);
        assertEquals(4, job.totalMaps);
        assertEquals(0, job.totalReduces);
    }

    @Test
    void testPopulateCounters() throws IOException {
        MRJobMetrics metrics = new MRJobMetrics();
        extractor.populateCounters(metrics, COUNTERS_JSON);

        // FileSystemCounter fields
        assertEquals(1024L, metrics.getHdfsBytesRead());
        assertEquals(2048L, metrics.getHdfsBytesWritten());

        // TaskCounter fields
        assertEquals(5000L, metrics.getCpuMilliseconds());
        assertEquals(200L, metrics.getGcTimeMillis());
        assertEquals(100L, metrics.getMapInputRecords());
        assertEquals(50L, metrics.getSpilledRecords());

        // JobCounter fields
        assertEquals(2, metrics.getTotalMaps());
        assertEquals(3000L, metrics.getMillisMaps());

        // Verify raw counter groups are populated
        Long hdfsRead = metrics.getCounter(
                "org.apache.hadoop.mapreduce.FileSystemCounter", "HDFS_BYTES_READ");
        assertNotNull(hdfsRead);
        assertEquals(1024L, hdfsRead.longValue());

        Long cpuMillis = metrics.getCounter(
                "org.apache.hadoop.mapreduce.TaskCounter", "CPU_MILLISECONDS");
        assertNotNull(cpuMillis);
        assertEquals(5000L, cpuMillis.longValue());
    }

    @Test
    void testEmptyJobs() throws IOException {
        List<CounterExtractor.JobInfo> jobs = extractor.parseJobs(EMPTY_JOBS_JSON);
        assertNotNull(jobs);
        assertTrue(jobs.isEmpty());
    }
}
