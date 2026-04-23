package x.mg.metrics.diagnostic.checks;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Hive Hook 检查器 — 返回多个检查项
 */
public class HiveHookChecker {

    public List<CheckItem> check() {
        List<CheckItem> items = new ArrayList<>();
        String hiveHome = detectHiveHome();

        if (hiveHome == null || hiveHome.isEmpty()) {
            items.add(CheckItem.skip("HIVE_HOME 未设置，跳过 Hive Hook 检查"));
            return items;
        }
        items.add(CheckItem.ok("HIVE_HOME=" + hiveHome));

        // 1. 检查 auxlib 目录中的 Hook JAR
        File auxlib = new File(hiveHome, "auxlib");
        boolean jarFound = false;
        String foundJar = null;
        if (auxlib.isDirectory()) {
            File[] files = auxlib.listFiles((dir, name) ->
                (name.contains("hive-telemetry") || name.contains("spark-telemetry")) && name.endsWith(".jar"));
            if (files != null && files.length > 0) {
                jarFound = true;
                foundJar = files[0].getName();
            }
        }
        if (jarFound) {
            items.add(CheckItem.ok("Hook JAR: auxlib/" + foundJar));
        } else {
            items.add(CheckItem.fail("Hook JAR 未在 " + auxlib.getAbsolutePath() + " 中找到",
                "请将 spark-telemetry-omni.jar 或 hive-telemetry-hook-dist-*.jar 复制到 HiveServer2 的 auxlib 目录"));
        }

        // 2. 检查 hive-site.xml
        File hiveSite = new File(hiveHome, "conf/hive-site.xml");
        if (hiveSite.exists()) {
            items.add(CheckItem.ok("hive-site.xml: " + hiveSite.getAbsolutePath()));
        } else {
            items.add(CheckItem.warn("hive-site.xml 未在 " + hiveSite.getAbsolutePath() + " 找到",
                "请检查 Hive 配置目录"));
            return items;
        }

        // 3. 从 hive-site.xml 解析配置
        String hooksValue = getXmlPropertyValue(hiveSite, "hive.exec.post.hooks");
        String otelEndpoint = getXmlPropertyValue(hiveSite, "hive.telemetry.otel.exporter.endpoint");

        // 4. 检查 hive.exec.post.hooks
        if (hooksValue != null && hooksValue.contains("HiveTelemetryHook")) {
            items.add(CheckItem.ok("hive.exec.post.hooks=" + hooksValue));
        } else {
            items.add(CheckItem.fail("hive.exec.post.hooks 未配置 HiveTelemetryHook",
                "请在 hive-site.xml 中添加：\n" +
                "  <property><name>hive.exec.post.hooks</name>\n" +
                "  <value>x.mg.metrics.hivetelemetry.HiveTelemetryHook</value></property>"));
        }

        // 5. 检查 OTel 端点
        if (otelEndpoint != null && !otelEndpoint.isEmpty()) {
            items.add(CheckItem.ok("hive.telemetry.otel.exporter.endpoint=" + otelEndpoint));
        } else {
            items.add(CheckItem.fail("hive.telemetry.otel.exporter.endpoint 未配置",
                "请在 hive-site.xml 中添加：\n" +
                "  <property><name>hive.telemetry.otel.exporter.endpoint</name>\n" +
                "  <value>http://otel-collector:4317</value></property>"));
        }

        return items;
    }

    private String detectHiveHome() {
        String env = System.getenv("HIVE_HOME");
        if (env != null && !env.isEmpty() && new File(env, "bin/hive").exists()) return env;
        String[] probePaths = {"/opt/hive", "/opt/apache-hive-2.3.9-bin", "/opt/apache-hive-2.3.10-bin",
            "/opt/apache-hive-3.1.3-bin"};
        for (String path : probePaths) {
            if (new File(path, "bin/hive").exists()) return path;
        }
        File opt = new File("/opt");
        if (opt.isDirectory()) {
            File[] dirs = opt.listFiles((d, name) ->
                name.startsWith("apache-hive") && new File(d, name + "/bin/hive").exists());
            if (dirs != null && dirs.length > 0) return dirs[0].getAbsolutePath();
        }
        return null;
    }

    private String getXmlPropertyValue(File xmlFile, String propertyName) {
        try (BufferedReader reader = new BufferedReader(new FileReader(xmlFile))) {
            String line;
            boolean foundName = false;
            while ((line = reader.readLine()) != null) {
                if (foundName) {
                    int start = line.indexOf("<value>");
                    if (start >= 0) {
                        int end = line.indexOf("</value>");
                        if (end >= 0) {
                            return line.substring(start + 7, end).trim();
                        }
                    }
                    foundName = false;
                }
                if (line.contains("<name>") && line.contains(propertyName) && line.contains("</name>")) {
                    foundName = true;
                }
            }
        } catch (Exception ignored) {
        }
        return null;
    }
}
