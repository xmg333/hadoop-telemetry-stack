#!/bin/bash
#
# Release packaging script for YARN Telemetry.
# Builds all artifacts and assembles a user-friendly tar.gz distribution.
#
# Usage:
#   ./release.sh [--skip-build] [--skip-docs]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# ── Parse version from pom.xml ──────────────────────────────
VERSION=$(grep -m1 '<version>' pom.xml | sed 's/.*<version>\(.*\)<\/version>.*/\1/')
PKG_NAME="yarn-telemetry-${VERSION}"
DIST_DIR="dist/${PKG_NAME}"

# ── Parse options ───────────────────────────────────────────
SKIP_BUILD=false
SKIP_DOCS=false
for arg in "$@"; do
    case "$arg" in
        --skip-build)  SKIP_BUILD=true ;;
        --skip-docs)   SKIP_DOCS=true ;;
        -h|--help)
            echo "Usage: $0 [--skip-build] [--skip-docs]"
            echo ""
            echo "  --skip-build   Skip Maven build (use existing JARs)"
            echo "  --skip-docs    Skip docs/ copying"
            exit 0 ;;
        *) echo "Unknown option: $arg"; exit 1 ;;
    esac
done

echo "=== YARN Telemetry Release ==="
echo "Version: ${VERSION}"
echo "Package: ${PKG_NAME}.tar.gz"
echo ""

# ── Step 1: Build ───────────────────────────────────────────
if [ "$SKIP_BUILD" = false ]; then
    echo ">>> [1/6] Building omnipackage..."
    chmod +x build-omni.sh && ./build-omni.sh

    echo ""
    echo ">>> [2/6] Building Flink consumer..."
    mvn clean package -pl flink/metrics-flink-consumer,flink/metrics-flink-consumer-dist -am -DskipTests

    echo ""
    echo ">>> [3/6] Building diagnostic tool..."
    mvn clean package -pl diagnostic/diagnostic-core -am -DskipTests
else
    echo ">>> [1-3/6] Skipping build (--skip-build)"
fi

# ── Step 2: Clean and create directory structure ────────────
echo ""
echo ">>> [4/6] Assembling package..."

rm -rf "dist"
mkdir -p "${DIST_DIR}"/{lib,conf/{spark,flink},grafana,otel-collector,sql,deploy,docs}

# ── Step 3: Copy JARs with short names ──────────────────────
OMNI_JAR=$(ls spark/spark-telemetry-dist-omni/target/spark-telemetry-dist-omni-*.jar 2>/dev/null | head -1)
FLINK_JAR=$(ls flink/metrics-flink-consumer-dist/target/metrics-flink-consumer-dist-*.jar 2>/dev/null | head -1)
DIAG_JAR=$(ls diagnostic/diagnostic-core/target/diagnostic-core-*.jar 2>/dev/null | head -1)

if [ -z "$OMNI_JAR" ]; then echo "ERROR: Omnipackage JAR not found"; exit 1; fi
if [ -z "$FLINK_JAR" ]; then echo "ERROR: Flink consumer JAR not found"; exit 1; fi
if [ -z "$DIAG_JAR" ]; then echo "ERROR: Diagnostic JAR not found"; exit 1; fi

cp "$OMNI_JAR" "${DIST_DIR}/lib/spark-telemetry-omni.jar"
cp "$FLINK_JAR" "${DIST_DIR}/lib/flink-consumer.jar"
cp "$DIAG_JAR"  "${DIST_DIR}/lib/diagnostic.jar"

echo "  lib/spark-telemetry-omni.jar  ($(du -h "$OMNI_JAR" | cut -f1))"
echo "  lib/flink-consumer.jar        ($(du -h "$FLINK_JAR" | cut -f1))"
echo "  lib/diagnostic.jar            ($(du -h "$DIAG_JAR" | cut -f1))"

# ── Step 4: Copy configs ───────────────────────────────────
SPARK_PRESET="conf/spark32-hadoop3-hive2.3.9"

cp "${SPARK_PRESET}/basic/telemetry.conf" "${DIST_DIR}/conf/spark/basic.conf"
cp "${SPARK_PRESET}/full/telemetry.conf"  "${DIST_DIR}/conf/spark/full.conf"
cp "${SPARK_PRESET}/submit.sh"            "${DIST_DIR}/conf/spark/submit.sh"
cp "${SPARK_PRESET}/hive-hook.conf"       "${DIST_DIR}/conf/hive-hook.conf"
cp "${SPARK_PRESET}/mr-collector.conf"    "${DIST_DIR}/conf/mr-collector.conf"

cp conf/flink/flink-consumer-mysql.conf       "${DIST_DIR}/conf/flink/"
cp conf/flink/flink-consumer-clickhouse.conf  "${DIST_DIR}/conf/flink/"

cp diagnostic/diagnostic-core/src/main/resources/diagnostic.conf.example "${DIST_DIR}/conf/diagnostic.conf"

echo "  conf/ (Spark/Flink/MR/Hive/Diagnostic configs)"

# ── Step 5: Copy deployment resources ──────────────────────
cp deploy/grafana/*.json "${DIST_DIR}/grafana/"
cp deploy/otel-collector/config.yaml "${DIST_DIR}/otel-collector/"
cp deploy/sql/*.sql "${DIST_DIR}/sql/"
cp deploy/install-omni.sh "${DIST_DIR}/deploy/"
cp deploy/deploy-grafana.sh "${DIST_DIR}/deploy/"
chmod +x "${DIST_DIR}/deploy/"*.sh

echo "  grafana/ ($(ls deploy/grafana/*.json | wc -l) dashboards)"
echo "  otel-collector/"
echo "  sql/ ($(ls deploy/sql/*.sql | wc -l) migrations)"
echo "  deploy/ (install-omni.sh, deploy-grafana.sh)"

# ── Step 6: Copy docs ──────────────────────────────────────
if [ "$SKIP_DOCS" = false ] && [ -d docs ]; then
    cp docs/*.md "${DIST_DIR}/docs/"
    echo "  docs/ ($(ls docs/*.md | wc -l) pages)"
fi

# ── Step 7: Generate README.md ─────────────────────────────
echo ""
echo ">>> [5/6] Generating README..."

cat > "${DIST_DIR}/README.md" << 'README_EOF'
# YARN Telemetry

Spark / MapReduce / Hive 指标透明采集方案，基于 OpenTelemetry + Kafka + Flink + MySQL/ClickHouse。

## 目录结构

```
lib/                 JAR 文件
  spark-telemetry-omni.jar    Omnipackage（Spark 2/3/4 + MR Agent/Collector + Hive Hook）
  flink-consumer.jar          Flink 消费者（Kafka → MySQL/ClickHouse）
  diagnostic.jar              诊断工具（端到端健康检查）

conf/                配置文件
  spark/             Spark 插件预设（basic.conf / full.conf / submit.sh）
  flink/             Flink 消费者配置（mysql / clickhouse）
  mr-collector.conf  MR History Server 采集配置
  hive-hook.conf     Hive Hook 配置
  diagnostic.conf    诊断工具配置

grafana/             Grafana 仪表盘（13 个 JSON，直接导入）
otel-collector/      OTel Collector 配置（Kafka exporter）
sql/                 数据库迁移脚本
deploy/              部署脚本
  install-omni.sh            自动安装 Omnipackage 到 Spark/Hive/MR
  deploy-grafana.sh          批量导入 Grafana 仪表盘
docs/                文档（快速开始、架构、配置参考等）
```

## 快速部署

### 1. 基础设施

```bash
# OTel Collector（必须用 contrib 版本，core 不含 Kafka exporter）
docker run -d --name otel-collector --network host \
  -v $(pwd)/otel-collector/config.yaml:/etc/otelcol-contrib/config.yaml \
  otel/opentelemetry-collector-contrib:0.96.0 \
  --config=/etc/otelcol-contrib/config.yaml

# Kafka
docker run -d --name kafka --network host \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$(hostname):9092 \
  apache/kafka:3.7.0

# 创建 topic
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic telemetry-metrics --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

# MySQL
docker run -d --name mysql --network host \
  -e MYSQL_ROOT_PASSWORD=root123 \
  -e MYSQL_DATABASE=telemetry \
  mysql:8.0

# Grafana
docker run -d --name grafana --network host \
  -e GF_SECURITY_ADMIN_PASSWORD=admin123 \
  grafana/grafana:latest
```

### 2. Flink Consumer（Kafka → MySQL）

```bash
# 修改配置文件中的连接信息
vim conf/flink/flink-consumer-mysql.conf

# 启动（自动建表，无需手动初始化 schema）
java -jar lib/flink-consumer.jar conf/flink/flink-consumer-mysql.conf
```

### 3. Spark Plugin

```bash
# 自动部署（推荐）
./deploy/install-omni.sh --spark-home=$SPARK_HOME --otel-endpoint=http://localhost:4317

# 或手动 spark-submit
spark-submit --master yarn \
  --jars lib/spark-telemetry-omni.jar \
  --conf spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin \
  --conf spark.telemetry.otel.exporter.endpoint=http://localhost:4317 \
  --conf spark.telemetry.otel.service.name=my-spark-app \
  your-app.jar

# 使用预设配置
spark-submit --master yarn \
  --jars lib/spark-telemetry-omni.jar \
  --conf spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin \
  --conf spark.telemetry.config.path=conf/spark/full.conf \
  --conf spark.telemetry.otel.exporter.endpoint=http://localhost:4317 \
  your-app.jar
```

### 4. Hive Hook

```bash
# 将 JAR 放到 HiveServer2 auxlib（自动加载）
cp lib/spark-telemetry-omni.jar $HIVE_HOME/auxlib/

# 配置 hive-site.xml
# <property>
#   <name>hive.exec.post.hooks</name>
#   <value>x.mg.metrics.hivetelemetry.HiveTelemetryHook</value>
# </property>
# <property>
#   <name>hive.telemetry.otel.exporter.endpoint</name>
#   <value>http://localhost:4317</value>
# </property>
```

### 5. MR Collector

```bash
# 修改配置
vim conf/mr-collector.conf

# 启动
nohup java -jar lib/spark-telemetry-omni.jar --mr-collector conf/mr-collector.conf &
```

### 6. Grafana 仪表盘

```bash
# 批量导入 13 个仪表盘
./deploy/deploy-grafana.sh --grafana-url=http://localhost:3000 --password=admin123
```

### 7. 诊断工具

```bash
# 修改配置后运行
vim conf/diagnostic.conf
java -jar lib/diagnostic.jar --config conf/diagnostic.conf
```

## 更多文档

完整文档见 `docs/` 目录：
- `quickstart.md` — 快速开始
- `architecture.md` — 架构设计
- `configuration-reference.md` — 配置参考
- `deployment-guide.md` — 完整部署指南
- `troubleshooting.md` — 故障排查

README_EOF

# ── Step 8: Fix paths in conf/spark/submit.sh ──────────────
sed -i 's|/path/to/spark-telemetry-dist-omni-1.0.0-SNAPSHOT.jar|lib/spark-telemetry-omni.jar|g' \
    "${DIST_DIR}/conf/spark/submit.sh"
sed -i 's|/path/to/basic/telemetry.conf|conf/spark/basic.conf|g' \
    "${DIST_DIR}/conf/spark/submit.sh"
sed -i 's|/path/to/full/telemetry.conf|conf/spark/full.conf|g' \
    "${DIST_DIR}/conf/spark/submit.sh"
sed -i 's|/path/to/mr-collector.conf|conf/mr-collector.conf|g' \
    "${DIST_DIR}/conf/spark/submit.sh"

# ── Step 9: Create tar.gz ──────────────────────────────────
echo ""
echo ">>> [6/6] Creating tar.gz..."

tar czf "dist/${PKG_NAME}.tar.gz" -C dist "${PKG_NAME}"

TAR_SIZE=$(du -h "dist/${PKG_NAME}.tar.gz" | cut -f1)

echo ""
echo "=== Release Complete ==="
echo ""
echo "  Package: dist/${PKG_NAME}.tar.gz (${TAR_SIZE})"
echo ""
echo "  Contents:"
echo "    lib/spark-telemetry-omni.jar  — Omnipackage (Spark 2/3/4 + MR + Hive)"
echo "    lib/flink-consumer.jar        — Kafka → MySQL/ClickHouse"
echo "    lib/diagnostic.jar            — Diagnostic tool"
echo "    conf/                         — Configuration files"
echo "    grafana/                      — 13 Grafana dashboards"
echo "    otel-collector/               — OTel Collector config"
echo "    sql/                          — Database migrations"
echo "    deploy/                       — Deployment scripts"
echo "    docs/                         — Documentation"
echo ""
echo "  Verify:  tar tzf dist/${PKG_NAME}.tar.gz | head -20"
echo "  Extract: tar xzf dist/${PKG_NAME}.tar.gz"
