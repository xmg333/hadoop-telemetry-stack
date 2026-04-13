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
                return "4"; // Scala 2.13 → use v4 adapter
            } catch (ClassNotFoundException e) {
                return "3"; // Scala 2.12 or earlier → use v3 adapter
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
