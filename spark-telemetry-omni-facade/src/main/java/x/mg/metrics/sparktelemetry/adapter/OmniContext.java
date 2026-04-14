package x.mg.metrics.sparktelemetry.adapter;

/**
 * Detects the running Spark version at class-loading time.
 * Used by the omni facade to select the correct version-specific adapter.
 */
public class OmniContext {

    private static final String DETECTED_VERSION = detectVersion();

    private static String detectVersion() {
        try {
            Class.forName("org.apache.spark.api.plugin.SparkPlugin");
            // Spark 3.x or 4.x — distinguish by Scala version
            // scala.jdk.CollectionConverters was introduced in Scala 2.13
            try {
                Class.forName("scala.jdk.CollectionConverters");
                return "4"; // Scala 2.13 → use v4 adapter (Spark 4.x)
            } catch (ClassNotFoundException e) {
                // Spark 3.x (Scala 2.12) — probe sub-version by API existence
                try {
                    // remoteReqsDuration added in Spark 3.3 (SPARK-37302)
                    Class.forName("org.apache.spark.executor.ShuffleReadMetrics")
                            .getMethod("remoteReqsDuration");
                    return "35"; // Spark 3.3+ → use v35 adapter
                } catch (Exception e2) {
                    // Spark 3.0-3.2
                    try {
                        // PushBasedFetchHelper added in Spark 3.2.0 (push-based shuffle, SPARK-32925)
                        Class.forName("org.apache.spark.storage.PushBasedFetchHelper");
                        return "32"; // Spark 3.2.x → use v32 adapter
                    } catch (ClassNotFoundException e3) {
                        return "30"; // Spark 3.0/3.1 → use v30 adapter
                    }
                }
            }
        } catch (ClassNotFoundException e) {
            return "2"; // No SparkPlugin interface = Spark 2.x
        }
    }

    public static String getVersion() {
        return DETECTED_VERSION;
    }

    public static String getAdapterPackage() {
        return "x.mg.metrics.sparktelemetry.adapter.internal.v" + DETECTED_VERSION;
    }

    public static boolean isSpark2() {
        return "2".equals(DETECTED_VERSION);
    }
}
