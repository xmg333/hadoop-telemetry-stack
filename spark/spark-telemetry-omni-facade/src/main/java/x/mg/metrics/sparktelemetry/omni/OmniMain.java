package x.mg.metrics.sparktelemetry.omni;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * Omnipackage entry point for standalone execution modes.
 * <p>
 * Deployment modes:
 * <ul>
 *   <li>Spark Plugin: {@code --jars omnipackage.jar} + spark.plugins/spark.extraListeners config</li>
 *   <li>MR Collector: {@code java -jar omnipackage.jar --mr-collector <config-file>}</li>
 *   <li>MR Agent: {@code -javaagent:omnipackage.jar} (Premain-Class in manifest)</li>
 *   <li>Hive Hook: {@code hive.exec.post.hooks=x.mg.metrics.hivetelemetry.HiveTelemetryHook} in hive-site.xml</li>
 * </ul>
 */
public class OmniMain {

    public static void main(String[] args) {
        if (args.length > 0 && "--mr-collector".equals(args[0])) {
            // Delegate to MR collector Main via reflection (no compile-time dependency)
            try {
                Class<?> mainClass = Class.forName("x.mg.metrics.mrtelemetry.Main");
                Method mainMethod = mainClass.getMethod("main", String[].class);
                String[] remainingArgs = Arrays.copyOfRange(args, 1, args.length);
                mainMethod.invoke(null, (Object) remainingArgs);
            } catch (Exception e) {
                System.err.println("Failed to start MR collector: " + e.getMessage());
                e.printStackTrace();
                System.exit(1);
            }
        } else {
            System.out.println("Spark+MR Telemetry Omnipackage v1.0.0");
            System.out.println();
            System.out.println("Usage:");
            System.out.println("  Spark Plugin: --jars <this-jar> + spark.plugins / spark.extraListeners config");
            System.out.println("  MR Collector: java -jar <this-jar> --mr-collector <config-file>");
            System.out.println("  MR Agent:     -javaagent:<this-jar>");
            System.out.println("  Hive Hook:    hive.exec.post.hooks=x.mg.metrics.hivetelemetry.HiveTelemetryHook");
        }
    }
}
