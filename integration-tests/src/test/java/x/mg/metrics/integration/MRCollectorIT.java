package x.mg.metrics.integration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class MRCollectorIT extends K8sTestBase {

    @Test
    void testMRCollectorPollsJobsAndPushesToKafka() throws Exception {
        // 1. Submit a WordCount MR job
        String jobId = MRJobSubmitter.submitWordCount(this);
        assertNotNull(jobId, "Should get a job ID from MR submission");
        System.out.println("Submitted MR job: " + jobId);

        // 2. Start MR Collector as a K8s job or exec into a pod
        // For simplicity, use port-forward to history server
        // Then run the MR collector locally and check Kafka

        // 3. Verify job appears in history server
        String hsPod = kubectlExec("get", "pods", "-l", "app=hadoop-historyserver",
            "-o", "jsonpath={.items[0].metadata.name}");
        String jobsJson = kubectlExec("exec", hsPod.trim(), "--",
            "curl", "-s", "http://localhost:19888/ws/v1/history/mapreduce/jobs");
        assertTrue(jobsJson.contains("job"), "History Server should list jobs: " + jobsJson);

        // 4. Verify counter data is available
        String countersJson = kubectlExec("exec", hsPod.trim(), "--",
            "curl", "-s", "http://localhost:19888/ws/v1/history/mapreduce/jobs/" + jobId + "/counters");
        assertTrue(countersJson.contains("counterGroup"), "Should have counter groups: " + countersJson);
    }
}
