# 基础设施部署

本文档介绍 OTel Collector、Kafka、MySQL/ClickHouse 等基础设施的部署配置。

## OTel Collector

### 最小配置

创建 `config.yaml`：

```yaml
extensions:
  health_check:
    endpoint: 0.0.0.0:13133

receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

exporters:
  debug:
    verbosity: detailed
  kafka:
    topic: telemetry-metrics
    encoding: otlp_proto
    brokers:
      - kafka:9092
    producer:
      compression: snappy
      max_message_bytes: 1000000

service:
  extensions: [health_check]
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [debug, kafka]
```

### 运行

```bash
docker run -d --name otel-collector \
  -v $(pwd)/config.yaml:/etc/otelcol-contrib/config.yaml \
  -p 4317:4317 -p 4318:4318 -p 13133:13133 \
  otel/opentelemetry-collector-contrib:0.96.0 \
  --config=/etc/otelcol-contrib/config.yaml
```

> **重要**: 必须使用 `otel/opentelemetry-collector-contrib` 镜像（非 core 版本），因核心镜像不含 Kafka exporter。配置中必须包含 `health_check` 扩展，否则带探针的 K8s 部署会 CrashLoopBackOff。

---

## Kafka

### KRaft 模式（无需 ZooKeeper）

```bash
docker run -d --name kafka --network host \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$(hostname):9092 \
  apache/kafka:3.7.0
```

### 创建 Topic

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic telemetry-metrics --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

---

## MySQL

```bash
docker run -d --name mysql --network host \
  -e MYSQL_ROOT_PASSWORD=root123 \
  -e MYSQL_DATABASE=metrics_db \
  mysql:8.0
```

---

## ClickHouse

```bash
docker run -d --name clickhouse --network host \
  clickhouse/clickhouse-server:23.8
```

---

## Docker Compose 完整示例

```yaml
version: '3.8'

services:
  otel-collector:
    image: otel/opentelemetry-collector-contrib:0.96.0
    command: --config=/etc/otelcol-contrib/config.yaml
    volumes:
      - ./otel-collector-config.yaml:/etc/otelcol-contrib/config.yaml
    ports:
      - "4317:4317"
      - "4318:4318"
      - "13133:13133"
    networks:
      - telemetry

  kafka:
    image: apache/kafka:3.7.0
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
    ports:
      - "9092:9092"
      - "9093:9093"
    networks:
      - telemetry

  mysql:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: root123
      MYSQL_DATABASE: metrics_db
    ports:
      - "3306:3306"
    networks:
      - telemetry

  # 可选：ClickHouse
  clickhouse:
    image: clickhouse/clickhouse-server:23.8
    ports:
      - "8123:8123"
      - "9000:9000"
    networks:
      - telemetry

networks:
  telemetry:
    driver: bridge
```

---

## 验证

### 检查 OTel Collector 是否正常运行

```bash
curl http://localhost:13133/health
# 预期输出：{"status":"server_available"}
```

### 检查 Kafka Topic 是否创建

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe \
  --topic telemetry-metrics --bootstrap-server localhost:9092
```

### 检查 MySQL 是否可连接

```bash
docker exec mysql mysql -u root -proot123 metrics_db -e "SHOW TABLES;"
```
