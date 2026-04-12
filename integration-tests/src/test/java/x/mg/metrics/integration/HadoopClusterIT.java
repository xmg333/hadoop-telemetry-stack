package x.mg.metrics.integration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class HadoopClusterIT extends K8sTestBase {

    @Test
    void testHistoryServerApiReturns200() throws Exception {
        // Port-forward to history server and check REST API
        String output = kubectlExec("get", "pods", "-l", "app=hadoop-historyserver",
            "-o", "jsonpath={.items[0].metadata.name}");
        assertNotNull(output);
        assertFalse(output.trim().isEmpty(), "No historyserver pod found");

        // Use kubectl exec to curl the REST API internally
        String result = kubectlExec("exec", output.trim(), "--",
            "curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
            "http://localhost:19888/ws/v1/history/mapreduce/jobs");
        assertTrue(result.contains("200"), "History Server REST API should return 200, got: " + result);
    }

    @Test
    void testYarnResourceManagerUp() throws Exception {
        String output = kubectlExec("get", "pods", "-l", "app=hadoop-resourcemanager",
            "-o", "jsonpath={.items[0].status.phase}");
        assertEquals("Running", output.trim(), "ResourceManager should be running");
    }
}
