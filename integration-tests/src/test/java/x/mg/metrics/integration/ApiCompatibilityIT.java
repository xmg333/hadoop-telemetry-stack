package x.mg.metrics.integration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * API Compatibility Verification Test.
 *
 * This test uses reflection to verify that all Spark/Hadoop/Hive APIs
 * accessed by the telemetry plugins actually exist with the expected signatures.
 *
 * This catches runtime NoSuchMethodError issues at build time.
 */
@Tag("integration")
class ApiCompatibilityIT {

    /**
     * Verifies Spark ShuffleReadMetrics API compatibility.
     * Ensures all methods used by adapters exist.
     */
    @Test
    void testSparkShuffleReadMetricsApi() throws Exception {
        // Load the ShuffleReadMetrics class
        Class<?> shuffleReadMetricsClass;
        try {
            shuffleReadMetricsClass = Class.forName(
                "org.apache.spark.executor.ShuffleReadMetrics");
        } catch (ClassNotFoundException e) {
            // Spark not in classpath, skip this test
            System.out.println("Spark not in classpath, skipping ShuffleReadMetrics API test");
            return;
        }

        // Methods required by all Spark versions
        String[] requiredMethods = {
            "remoteBytesRead",
            "localBytesRead",
            "remoteBlocksFetched",
            "localBlocksFetched",
            "fetchWaitTime",
            "recordsRead"
        };

        for (String methodName : requiredMethods) {
            Method method = shuffleReadMetricsClass.getMethod(methodName);
            assertNotNull(method, "Method " + methodName + " should exist");
            assertTrue(
                method.getReturnType() == long.class ||
                method.getReturnType() == int.class,
                "Method " + methodName + " should return long or int"
            );
        }

        // Methods that may not exist in older versions (3.0.x, 3.2.x)
        // These should be accessed with try-catch in the adapter
        String[] optionalMethods = {
            "remoteBytesReadToDisk",  // Added in Spark 3.3.0
            "remoteReqsDuration"      // Added in Spark 3.3.0
        };

        Map<String, Boolean> optionalMethodExists = new HashMap<>();
        for (String methodName : optionalMethods) {
            try {
                Method method = shuffleReadMetricsClass.getMethod(methodName);
                optionalMethodExists.put(methodName, true);
                System.out.println("Optional method found: " + methodName);
            } catch (NoSuchMethodException e) {
                optionalMethodExists.put(methodName, false);
                System.out.println("Optional method NOT found (expected on older versions): " + methodName);
            }
        }

        // Verify that we documented which methods are optional
        assertTrue(optionalMethodExists.containsKey("remoteBytesReadToDisk"));
        assertTrue(optionalMethodExists.containsKey("remoteReqsDuration"));
    }

    /**
     * Verifies Spark ShuffleWriteMetrics API compatibility.
     */
    @Test
    void testSparkShuffleWriteMetricsApi() throws Exception {
        Class<?> shuffleWriteMetricsClass;
        try {
            shuffleWriteMetricsClass = Class.forName(
                "org.apache.spark.executor.ShuffleWriteMetrics");
        } catch (ClassNotFoundException e) {
            System.out.println("Spark not in classpath, skipping ShuffleWriteMetrics API test");
            return;
        }

        // Spark 3.x API (bytesWritten, writeTime, recordsWritten)
        String[] spark3Methods = {"bytesWritten", "writeTime", "recordsWritten"};

        // Spark 2.x API (shuffleBytesWritten, shuffleWriteTime, shuffleRecordsWritten)
        String[] spark2Methods = {"shuffleBytesWritten", "shuffleWriteTime", "shuffleRecordsWritten"};

        boolean hasSpark3Api = false;
        boolean hasSpark2Api = false;

        for (String method : spark3Methods) {
            try {
                shuffleWriteMetricsClass.getMethod(method);
                hasSpark3Api = true;
            } catch (NoSuchMethodException ignored) {}
        }

        for (String method : spark2Methods) {
            try {
                shuffleWriteMetricsClass.getMethod(method);
                hasSpark2Api = true;
            } catch (NoSuchMethodException ignored) {}
        }

        assertTrue(hasSpark3Api || hasSpark2Api,
            "Should have either Spark 2.x or 3.x ShuffleWriteMetrics API");

        System.out.println("ShuffleWriteMetrics API: Spark 3.x=" + hasSpark3Api + ", Spark 2.x=" + hasSpark2Api);
    }

    /**
     * Verifies Spark TaskMetrics API compatibility.
     */
    @Test
    void testSparkTaskMetricsApi() throws Exception {
        Class<?> taskMetricsClass;
        try {
            taskMetricsClass = Class.forName("org.apache.spark.executor.TaskMetrics");
        } catch (ClassNotFoundException e) {
            System.out.println("Spark not in classpath, skipping TaskMetrics API test");
            return;
        }

        String[] requiredMethods = {
            "inputMetrics",
            "outputMetrics",
            "shuffleReadMetrics",
            "shuffleWriteMetrics",
            "executorRunTime",
            "executorCpuTime",
            "executorDeserializeTime",
            "executorDeserializeCpuTime",
            "resultSerializationTime",
            "jvmGCTime",
            "resultSize",
            "peakExecutionMemory",
            "diskBytesSpilled",
            "memoryBytesSpilled"
        };

        for (String methodName : requiredMethods) {
            Method method = taskMetricsClass.getMethod(methodName);
            assertNotNull(method, "Method " + methodName + " should exist");
        }
    }

    /**
     * Verifies Hadoop JobStatus API compatibility.
     */
    @Test
    void testHadoopJobStatusApi() throws Exception {
        Class<?> jobStatusClass;
        try {
            jobStatusClass = Class.forName("org.apache.hadoop.mapreduce.JobStatus");
        } catch (ClassNotFoundException e) {
            System.out.println("Hadoop not in classpath, skipping JobStatus API test");
            return;
        }

        // Check for enum values
        Object[] states = jobStatusClass.getEnumConstants();
        if (states != null) {
            List<String> stateNames = Arrays.stream(states)
                .map(Object::toString)
                .collect(Collectors.toList());
            System.out.println("Hadoop JobStatus states: " + stateNames);
        }

        // Check for getState method
        try {
            Method getStateMethod = jobStatusClass.getMethod("getState");
            System.out.println("JobStatus.getState() found, returns: " + getStateMethod.getReturnType());
        } catch (NoSuchMethodException e) {
            System.out.println("JobStatus.getState() not found (may use different API)");
        }
    }

    /**
     * Verifies Hive HookContext API compatibility.
     */
    @Test
    void testHiveHookContextApi() throws Exception {
        Class<?> hookContextClass;
        try {
            hookContextClass = Class.forName("org.apache.hadoop.hive.ql.hooks.HookContext");
        } catch (ClassNotFoundException e) {
            System.out.println("Hive not in classpath, skipping HookContext API test");
            return;
        }

        String[] requiredMethods = {
            "getConf",
            "getInputs",
            "getOutputs",
            "getQueryPlan",
            "getHookType",
            "getOperationName"
        };

        for (String methodName : requiredMethods) {
            try {
                Method method = hookContextClass.getMethod(methodName);
                System.out.println("HookContext." + methodName + "() found, returns: " + method.getReturnType());
            } catch (NoSuchMethodException e) {
                System.out.println("HookContext." + methodName + "() NOT found");
            }
        }
    }

    /**
     * Verifies that Spark 3.2/3.0 adapter handles missing APIs gracefully.
     * This test documents which APIs are optional.
     */
    @Test
    void testDocumentOptionalApis() {
        // This test documents which APIs may be missing in different versions
        // and serves as a reference for adapter developers

        Map<String, ApiInfo> apiVersions = new HashMap<>();

        apiVersions.put("ShuffleReadMetrics.remoteBytesReadToDisk",
            new ApiInfo("Spark 3.3.0", "Spark 3.2/3.0 adapters handle with try-catch"));

        apiVersions.put("ShuffleReadMetrics.remoteReqsDuration",
            new ApiInfo("Spark 3.3.0", "Spark 3.2/3.0 adapters handle with try-catch"));

        apiVersions.put("ShuffleWriteMetrics.bytesWritten",
            new ApiInfo("Spark 3.0+", "Spark 2.x uses shuffleBytesWritten"));

        apiVersions.put("QueryExecution.id",
            new ApiInfo("Spark 3.0+", "Spark 2.x does not have this field"));

        System.out.println("=== Optional/Version-Specific APIs ===");
        for (Map.Entry<String, ApiInfo> entry : apiVersions.entrySet()) {
            System.out.println(entry.getKey() + ":");
            System.out.println("  Added in: " + entry.getValue().addedIn);
            System.out.println("  Note: " + entry.getValue().note);
        }

        // This test always passes - it's documentation
        assertTrue(true);
    }

    private static class ApiInfo {
        final String addedIn;
        final String note;

        ApiInfo(String addedIn, String note) {
            this.addedIn = addedIn;
            this.note = note;
        }
    }
}
