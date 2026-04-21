package x.mg.metrics.diagnostic.checks;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Hive Hook 检查器 — 返回多个检查项
 */
public class HiveHookChecker {

    public List<CheckItem> check() {
        List<CheckItem> items = new ArrayList<>();
        String hiveHome = System.getenv("HIVE_HOME");

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
                name.contains("hive-telemetry") && name.endsWith(".jar"));
            if (files != null && files.length > 0) {
                jarFound = true;
                foundJar = files[0].getName();
            }
        }
        if (jarFound) {
            items.add(CheckItem.ok("Hook JAR: auxlib/" + foundJar));
        } else {
            items.add(CheckItem.fail("Hook JAR 未在 " + auxlib.getAbsolutePath() + " 中找到",
                "请将 hive-telemetry-hook-dist-*.jar 复制到 HiveServer2 的 auxlib 目录"));
        }

        // 2. 检查 hive-site.xml
        File hiveSite = new File(hiveHome, "conf/hive-site.xml");
        if (hiveSite.exists()) {
            items.add(CheckItem.ok("hive-site.xml: " + hiveSite.getAbsolutePath()));
        } else {
            items.add(CheckItem.warn("hive-site.xml 未在 " + hiveSite.getAbsolutePath() + " 找到",
                "请检查 Hive 配置目录"));
        }

        // 3. 检查 hive.exec.post.hooks
        String hooks = System.getProperty("hive.exec.post.hooks");
        if (hooks != null && hooks.contains("HiveTelemetryHook")) {
            items.add(CheckItem.ok("hive.exec.post.hooks=" + hooks));
        } else {
            items.add(CheckItem.fail("hive.exec.post.hooks 未配置 HiveTelemetryHook",
                "请在 hive-site.xml 中添加：\n" +
                "  <property><name>hive.exec.post.hooks</name>\n" +
                "  <value>x.mg.metrics.hivetelemetry.HiveTelemetryHook</value></property>"));
        }

        // 4. 检查 OTel 端点
        String otelEndpoint = System.getProperty("hive.telemetry.otel.exporter.endpoint");
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
}
