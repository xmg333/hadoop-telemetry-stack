package x.mg.metrics.diagnostic.checks;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Spark Plugin 检查器 — 返回多个检查项
 */
public class SparkPluginChecker {

    public List<CheckItem> check() {
        List<CheckItem> items = new ArrayList<>();
        String sparkHome = System.getenv("SPARK_HOME");

        // 1. 检查 SPARK_HOME
        if (sparkHome == null || sparkHome.isEmpty()) {
            items.add(CheckItem.warn("SPARK_HOME 未设置", "请设置 SPARK_HOME 环境变量"));
            return items;
        }
        items.add(CheckItem.ok("SPARK_HOME=" + sparkHome));

        // 2. 检查 Spark 版本
        String sparkVersion = detectSparkVersion(sparkHome);
        if (sparkVersion != null) {
            items.add(CheckItem.ok("Spark 版本: " + sparkVersion));
        } else {
            items.add(CheckItem.warn("无法检测 Spark 版本", "请检查 " + sparkHome + " 目录结构"));
        }

        // 3. 检查 Plugin JAR 是否在 classpath（--jars 或 jars/ 目录）
        File jarsDir = new File(sparkHome, "jars");
        boolean jarFound = false;
        String foundJar = null;
        if (jarsDir.isDirectory()) {
            File[] files = jarsDir.listFiles((dir, name) ->
                name.contains("spark-telemetry") && name.endsWith(".jar"));
            if (files != null && files.length > 0) {
                jarFound = true;
                foundJar = files[0].getName();
            }
        }
        if (jarFound) {
            items.add(CheckItem.ok("Plugin JAR: " + foundJar));
        } else {
            items.add(CheckItem.warn("Plugin JAR 未在 " + jarsDir.getAbsolutePath() + " 中找到",
                "请确保 spark-telemetry-dist-spark*.jar 已通过 --jars 添加或放入 jars/ 目录"));
        }

        // 4. 检查 spark.plugins 配置
        String sparkPlugins = System.getProperty("spark.plugins");
        if (sparkPlugins != null && sparkPlugins.contains("SparkTelemetryPlugin")) {
            items.add(CheckItem.ok("spark.plugins=" + sparkPlugins));
        } else {
            String hint = "请添加配置：\n" +
                "  --conf spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin";
            if (sparkVersion != null && sparkVersion.startsWith("2.")) {
                hint = "请添加配置：\n" +
                    "  --conf spark.extraListeners=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryListener";
            }
            items.add(CheckItem.fail("spark.plugins 未配置或未包含 SparkTelemetryPlugin", hint));
        }

        // 5. 检查 OTel 端点配置
        String otelEndpoint = System.getProperty("spark.telemetry.otel.exporter.endpoint");
        if (otelEndpoint != null && !otelEndpoint.isEmpty()) {
            items.add(CheckItem.ok("spark.telemetry.otel.exporter.endpoint=" + otelEndpoint));
        } else {
            items.add(CheckItem.fail("spark.telemetry.otel.exporter.endpoint 未配置",
                "请添加配置：\n" +
                "  --conf spark.telemetry.otel.exporter.endpoint=http://otel-collector:4317"));
        }

        // 6. 检查 service.name
        String serviceName = System.getProperty("spark.telemetry.otel.service.name");
        if (serviceName != null && !serviceName.isEmpty()) {
            items.add(CheckItem.ok("spark.telemetry.otel.service.name=" + serviceName));
        } else {
            items.add(CheckItem.warn("spark.telemetry.otel.service.name 未配置（将使用默认值）",
                "建议添加：--conf spark.telemetry.otel.service.name=my-spark-app"));
        }

        // 7. 检查指标类别开关
        checkMetricCategory(items, "spark.telemetry.metrics.task.execution", "true");
        checkMetricCategory(items, "spark.telemetry.metrics.task.shuffle-extended", "true");
        checkMetricCategory(items, "spark.telemetry.metrics.stage.detailed", "false");
        checkMetricCategory(items, "spark.telemetry.metrics.job.lifecycle", "false");
        checkMetricCategory(items, "spark.telemetry.metrics.sql.query-execution", "false");

        return items;
    }

    private String detectSparkVersion(String sparkHome) {
        File releaseFile = new File(sparkHome, "RELEASE");
        if (releaseFile.exists()) {
            try {
                java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(releaseFile));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.contains("version")) {
                        reader.close();
                        return line.replace("Spark ", "").trim();
                    }
                }
                reader.close();
            } catch (Exception ignored) {}
        }
        // 尝试从 jars 目录名推断
        File jarsDir = new File(sparkHome, "jars");
        if (jarsDir.isDirectory()) {
            File[] files = jarsDir.listFiles((dir, name) -> name.startsWith("spark-core_"));
            if (files != null && files.length > 0) {
                String name = files[0].getName();
                // spark-core_2.12-3.5.5.jar
                int dash = name.lastIndexOf('-');
                if (dash > 0) {
                    return name.substring(dash + 1).replace(".jar", "");
                }
            }
        }
        return null;
    }

    private void checkMetricCategory(List<CheckItem> items, String key, String defaultVal) {
        String val = System.getProperty(key);
        if (val != null) {
            items.add(CheckItem.ok(key + "=" + val));
        } else {
            // 使用默认值，不报错
        }
    }
}
