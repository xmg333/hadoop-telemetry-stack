package x.mg.metrics.sparktelemetry.adapter;

import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.SparkPlugin;

/**
 * Omnipackage facade for Spark 3.x/4.x plugin loading.
 * <p>
 * Detects the running Spark version and delegates to the version-specific
 * {@code TelemetryDriverPlugin} / {@code TelemetryExecutorPlugin} via reflection.
 * <p>
 * This class is only loaded when {@code spark.plugins} is configured (Spark 3/4).
 * In Spark 2.x, users configure {@code spark.extraListeners} instead, so this class
 * is never referenced and never loaded.
 */
public class SparkTelemetryPlugin implements SparkPlugin {

    private static final ClassLoader SPARK_CLASSLOADER = SparkPlugin.class.getClassLoader();

    @Override
    public DriverPlugin driverPlugin() {
        String pkg = OmniContext.getAdapterPackage();
        try {
            @SuppressWarnings("unchecked")
            Class<? extends DriverPlugin> clazz =
                    (Class<? extends DriverPlugin>) Class.forName(pkg + ".TelemetryDriverPlugin", true, SPARK_CLASSLOADER);
            return clazz.getConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create DriverPlugin for Spark " + OmniContext.getVersion(), e);
        }
    }

    @Override
    public ExecutorPlugin executorPlugin() {
        String pkg = OmniContext.getAdapterPackage();
        try {
            @SuppressWarnings("unchecked")
            Class<? extends ExecutorPlugin> clazz =
                    (Class<? extends ExecutorPlugin>) Class.forName(pkg + ".TelemetryExecutorPlugin", true, SPARK_CLASSLOADER);
            return clazz.getConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create ExecutorPlugin for Spark " + OmniContext.getVersion(), e);
        }
    }
}
