package x.mg.metrics.diagnostic.checks;

import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * OTel Collector 检查器 — 返回多个检查项
 */
public class OtelCollectorChecker {

    private final String endpoint;
    private final int healthCheckPort;
    private final int timeoutMs;

    public OtelCollectorChecker(String endpoint, int healthCheckPort, int timeoutMs) {
        this.endpoint = endpoint;
        this.healthCheckPort = healthCheckPort;
        this.timeoutMs = timeoutMs;
    }

    public List<CheckItem> check() {
        List<CheckItem> items = new ArrayList<>();
        String host = extractHost(endpoint);
        items.add(CheckItem.ok("目标端点: " + endpoint));

        // 1. 健康检查
        String healthUrl = "http://" + host + ":" + healthCheckPort + "/";
        boolean healthOk = checkHttp(healthUrl);
        if (healthOk) {
            items.add(CheckItem.ok("健康检查: HTTP 200 (" + healthUrl + ")"));
        } else {
            items.add(CheckItem.warn("健康检查失败: " + healthUrl,
                "检查 OTel Collector 进程：kubectl get pods -l app=otel-collector"));
        }

        // 2. gRPC 端点 (4317)
        boolean grpcOk = checkTcp(host, 4317);
        if (grpcOk) {
            items.add(CheckItem.ok("gRPC 端点: " + host + ":4317 可连接"));
        } else {
            items.add(CheckItem.fail("gRPC 端点: " + host + ":4317 不可连接",
                "检查 OTel Collector 是否监听 4317 端口"));
        }

        // 3. HTTP 端点 (4318)
        boolean httpOk = checkTcp(host, 4318);
        if (httpOk) {
            items.add(CheckItem.ok("HTTP 端点: " + host + ":4318 可连接"));
        } else {
            items.add(CheckItem.warn("HTTP 端点: " + host + ":4318 不可连接",
                "若需要接收 traces，请确认 receivers.otlp.http 已配置"));
        }

        return items;
    }

    private String extractHost(String endpoint) {
        try {
            URI uri = new URI(endpoint);
            String host = uri.getHost();
            return host != null ? host : "localhost";
        } catch (Exception e) {
            return "localhost";
        }
    }

    private boolean checkHttp(String url) {
        try {
            HttpURLConnection conn = (HttpURLConnection) new java.net.URL(url).openConnection();
            conn.setConnectTimeout(timeoutMs);
            conn.setReadTimeout(timeoutMs);
            conn.setRequestMethod("GET");
            return conn.getResponseCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean checkTcp(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMs);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
