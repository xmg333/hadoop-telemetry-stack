package x.mg.metrics.integration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class E2EMetricsFlowTest extends K8sTestBase {

    @Test
    void testHadoopClusterComponentsRunning() throws Exception {
        // Verify all Hadoop components
        String[] components = {"hadoop-namenode", "hadoop-datanode",
            "hadoop-resourcemanager", "hadoop-nodemanager", "hadoop-historyserver"};
        for (String comp : components) {
            String phase = kubectlExec("get", "pods", "-l", "app=" + comp,
                "-o", "jsonpath={.items[0].status.phase}");
            assertEquals("Running", phase.trim(), comp + " should be running");
        }
    }

    @Test
    void testKafkaRunning() throws Exception {
        String phase = kubectlExec("get", "pods", "-l", "app=kafka",
            "-o", "jsonpath={.items[0].status.phase}");
        assertEquals("Running", phase.trim(), "Kafka should be running");
    }

    @Test
    void testOTelCollectorRunning() throws Exception {
        String phase = kubectlExec("get", "pods", "-l", "app=otel-collector",
            "-o", "jsonpath={.items[0].status.phase}");
        assertEquals("Running", phase.trim(), "OTel Collector should be running");
    }
}
