# Infrastructure Deployment

This document describes the deployment and configuration of infrastructure components including OTel Collector, Kafka, MySQL/ClickHouse.

## OTel Collector

### Minimal Configuration

Create `config.yaml`:

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

### Running

```bash
docker run -d --name otel-collector \
  -v $(pwd)/config.yaml:/etc/otelcol-contrib/config.yaml \
  -p 4317:4317 -p 4318:4318 -p 13133:13133 \
  otel/opentelemetry-collector-contrib:0.96.0 \
  --config=/etc/otelcol-contrib/config.yaml
```

> **Important**: You must use the `otel/opentelemetry-collector-contrib` image (not the core version), as the core image does not include the Kafka exporter. The configuration must include the `health_check` extension, otherwise K8s deployments with probes will enter CrashLoopBackOff.

---

## Kafka

### KRaft Mode (No ZooKeeper Required)

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

### Create Topic

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

## Docker Compose Full Example

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

  # Optional: ClickHouse
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

## Verification

### Check OTel Collector Health

```bash
curl http://localhost:13133/health
# Expected output: {"status":"server_available"}
```

### Check Kafka Topic

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe \
  --topic telemetry-metrics --bootstrap-server localhost:9092
```

### Check MySQL Connectivity

```bash
docker exec mysql mysql -u root -proot123 metrics_db -e "SHOW TABLES;"
```
