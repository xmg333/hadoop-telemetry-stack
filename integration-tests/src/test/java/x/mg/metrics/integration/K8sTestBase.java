package x.mg.metrics.integration;

import org.junit.jupiter.api.BeforeAll;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Base class for K8s integration tests. Checks kubectl availability.
 */
public abstract class K8sTestBase {

    @BeforeAll
    static void checkKubectl() throws Exception {
        Process p = Runtime.getRuntime().exec(new String[]{"kubectl", "version", "--client"});
        boolean ok = p.waitFor(10, TimeUnit.SECONDS) && p.exitValue() == 0;
        assumeTrue(ok, "kubectl not available, skipping integration tests");
    }

    protected String kubectlExec(String... args) throws Exception {
        String[] cmd = new String[args.length + 1];
        cmd[0] = "kubectl";
        System.arraycopy(args, 0, cmd, 1, args.length);
        Process p = Runtime.getRuntime().exec(cmd);
        p.waitFor(60, TimeUnit.SECONDS);
        String output;
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            output = r.lines().collect(Collectors.joining("\n"));
        }
        if (p.exitValue() != 0) {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getErrorStream()))) {
                throw new RuntimeException("kubectl failed: " + r.lines().collect(Collectors.joining("\n")));
            }
        }
        return output;
    }

    protected void waitForPodReady(String label, int timeoutSecs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutSecs * 1000L;
        while (System.currentTimeMillis() < deadline) {
            String out = kubectlExec("get", "pods", "-l", label, "-o", "jsonpath={.items[0].status.phase}");
            if ("Running".equals(out.trim())) return;
            Thread.sleep(3000);
        }
        throw new RuntimeException("Pod " + label + " not ready within " + timeoutSecs + "s");
    }

    protected int portForward(String namespace, String label, int port) throws Exception {
        // Start port-forward in background
        ProcessBuilder pb = new ProcessBuilder(
            "kubectl", "port-forward", "-n", namespace,
            "svc/" + label, "0:" + port
        );
        pb.redirectErrorStream(true);
        Process pf = pb.start();
        // Give it a moment to start
        Thread.sleep(2000);
        // Read local port from output
        BufferedReader reader = new BufferedReader(new InputStreamReader(pf.getInputStream()));
        String line = reader.readLine();
        if (line != null && line.contains("->")) {
            String localPort = line.split(":")[1].split("-")[0].trim();
            return Integer.parseInt(localPort);
        }
        // Fallback
        return port;
    }
}
