package x.mg.metrics.integration;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Submits a MapReduce WordCount job via kubectl exec on the Hadoop cluster.
 */
class MRJobSubmitter {

    /**
     * Submit a WordCount MR job and return the Job ID.
     */
    static String submitWordCount(K8sTestBase testBase) throws Exception {
        // Find nodemanager pod
        String nmPod = testBase.kubectlExec("get", "pods", "-l", "app=hadoop-nodemanager",
            "-o", "jsonpath={.items[0].metadata.name}");

        // Create input file
        testBase.kubectlExec("exec", nmPod.trim(), "--",
            "bash", "-c",
            "echo 'hello world hello' > /tmp/input.txt && " +
            "hadoop fs -mkdir -p /tmp/wordcount/input && " +
            "hadoop fs -put -f /tmp/input.txt /tmp/wordcount/input/");

        // Submit WordCount
        String output = testBase.kubectlExec("exec", nmPod.trim(), "--",
            "bash", "-c",
            "hadoop jar $(find /opt/hadoop -name 'hadoop-mapreduce-examples-*.jar' | head -1) " +
            "wordcount /tmp/wordcount/input /tmp/wordcount/output 2>&1");

        // Extract job ID from output
        Pattern p = Pattern.compile("job_\\d+_\\d+");
        Matcher m = p.matcher(output);
        if (m.find()) return m.group();

        throw new RuntimeException("Failed to submit MR job. Output: " + output);
    }
}
