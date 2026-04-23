package x.mg.metrics.diagnostic.checks;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Spark Plugin 检查器 — 返回多个检查项
 */
public class SparkPluginChecker {

    public List<CheckItem> check() {
        List<CheckItem> items = new ArrayList<>();
        String sparkHome = detectSparkHome();

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

        // 4. 解析 spark-defaults.conf
        Map<String, String> conf = loadSparkDefaults(sparkHome);

        // 5. 检查 spark.plugins 配置
        String sparkPlugins = conf.get("spark.plugins");
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

        // 6. 检查 OTel 端点配置
        String otelEndpoint = conf.get("spark.telemetry.otel.exporter.endpoint");
        if (otelEndpoint != null && !otelEndpoint.isEmpty()) {
            items.add(CheckItem.ok("spark.telemetry.otel.exporter.endpoint=" + otelEndpoint));
        } else {
            items.add(CheckItem.fail("spark.telemetry.otel.exporter.endpoint 未配置",
                "请添加配置：\n" +
                "  --conf spark.telemetry.otel.exporter.endpoint=http://otel-collector:4317"));
        }

        // 7. 检查 service.name
        String serviceName = conf.get("spark.telemetry.otel.service.name");
        if (serviceName != null && !serviceName.isEmpty()) {
            items.add(CheckItem.ok("spark.telemetry.otel.service.name=" + serviceName));
        } else {
            items.add(CheckItem.warn("spark.telemetry.otel.service.name 未配置（将使用默认值）",
                "建议添加：--conf spark.telemetry.otel.service.name=my-spark-app"));
        }

        // 8. 检查指标类别开关
        checkMetricCategory(items, conf, "spark.telemetry.metrics.task.execution", "true");
        checkMetricCategory(items, conf, "spark.telemetry.metrics.task.shuffle-extended", "true");
        checkMetricCategory(items, conf, "spark.telemetry.metrics.stage.detailed", "false");
        checkMetricCategory(items, conf, "spark.telemetry.metrics.job.lifecycle", "false");
        checkMetricCategory(items, conf, "spark.telemetry.metrics.sql.query-execution", "false");

        return items;
    }

    private String detectSparkHome() {
        String env = System.getenv("SPARK_HOME");
        if (env != null && !env.isEmpty() && new File(env, "bin/spark-submit").exists()) return env;
        // Probe common paths
        File opt = new File("/opt");
        if (opt.isDirectory()) {
            File[] dirs = opt.listFiles((d, name) ->
                name.startsWith("spark") && new File(d, name + "/bin/spark-submit").exists());
            if (dirs != null && dirs.length > 0) return dirs[0].getAbsolutePath();
        }
        return null;
    }

    private Map<String, String> loadSparkDefaults(String sparkHome) {
        Map<String, String> conf = new HashMap<>();
        File defaultsFile = new File(sparkHome, "conf/spark-defaults.conf");
        if (!defaultsFile.exists()) return conf;
        try (BufferedReader reader = new BufferedReader(new FileReader(defaultsFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                // Split on first whitespace (space or tab)
                int sep = -1;
                for (int i = 0; i < line.length(); i++) {
                    if (Character.isWhitespace(line.charAt(i))) {
                        sep = i;
                        break;
                    }
                }
                if (sep > 0) {
                    String key = line.substring(0, sep).trim();
                    String value = line.substring(sep).trim();
                    // Strip inline comment
                    int commentIdx = value.indexOf('#');
                    if (commentIdx > 0) {
                        value = value.substring(0, commentIdx).trim();
                    }
                    conf.put(key, value);
                }
            }
        } catch (Exception ignored) {
        }
        return conf;
    }

    private String detectSparkVersion(String sparkHome) {
        File releaseFile = new File(sparkHome, "RELEASE");
        if (releaseFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(releaseFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.contains("version")) {
                        return line.replace("Spark ", "").trim();
                    }
                }
            } catch (Exception ignored) {}
        }
        // 尝试从 jars 目录名推断
        File jarsDir = new File(sparkHome, "jars");
        if (jarsDir.isDirectory()) {
            File[] files = jarsDir.listFiles((dir, name) -> name.startsWith("spark-core_"));
            if (files != null && files.length > 0) {
                String name = files[0].getName();
                int dash = name.lastIndexOf('-');
                if (dash > 0) {
                    return name.substring(dash + 1).replace(".jar", "");
                }
            }
        }
        return null;
    }

    private void checkMetricCategory(List<CheckItem> items, Map<String, String> conf,
                                      String key, String defaultVal) {
        String val = conf.get(key);
        if (val != null) {
            items.add(CheckItem.ok(key + "=" + val));
        }
    }
}
