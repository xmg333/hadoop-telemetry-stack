# 故障排查指南

本文档提供系统化的故障排查步骤和常见问题解决方案。

## 问题排查决策树

```
开始
  │
  ├─ 无指标数据？
  │   ├─ 检查 Spark/MR/Hive 配置 → 未生效 → 检查 JAR 路径和配置项
  │   ├─ OTel Collector 无日志 → 连接失败 → 检查端点地址
  │   └─ Kafka 无消息 → 导出失败 → 检查 Kafka 连接
  │
  ├─ 指标不完整？
  │   ├─ 缺少某些指标 → 配置开关未开启 → 检查 metrics.* 配置项
  │   └─ SQL 指标为 NULL → AQE 问题 → 检查 Spark 版本
  │
  └─ 性能问题？
      ├─ 作业变慢 → 指标开销大 → 减小 export.interval
      └─ 内存不足 → OTel 缓冲过大 → 调整 batch size
```

---

## 常见问题

### Q1: Spark 插件不生效，没有指标输出

**症状**: Spark 作业运行后，OTel Collector 无日志，Kafka 无消息

**排查步骤**:

1. 确认 JAR 路径正确且可访问
   ```bash
   ls -la /path/to/spark-telemetry-dist-omni-*.jar
   ```

2. 检查 `spark.plugins`（Spark 3/4）或 `spark.extraListeners`（Spark 2）配置
   ```bash
   # Spark 3.x
   --conf spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin

   # Spark 2.x
   --conf spark.extraListeners=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryListener
   ```

3. 检查 Driver/Executor 日志中是否有 `TelemetryLifecycle initialized`
   ```bash
   yarn logs -applicationId <app_id> -log_files stdout | grep -i telemetry
   ```

4. 确认 OTel Collector 地址可达
   ```bash
   nc -zv otel-collector 4317
   ```

5. 检查配置键是否包含 `.otel.` 段（常见错误）
   ```bash
   # 正确
   --conf spark.telemetry.otel.exporter.endpoint=http://otel-collector:4317

   # 错误（缺少 .otel.）
   --conf spark.telemetry.exporter.endpoint=http://otel-collector:4317
   ```

---

### Q2: 短时作业指标丢失

**症状**: 运行时间短于 `export.interval` 的作业，指标未导出

**解决方案**:

1. 插件在 `onJobEnd` 时自动触发 `flushAsync()` 非阻塞刷新，关闭时同步 `forceFlush()`
2. 如仍有丢失，减小导出间隔：
   ```bash
   --conf spark.telemetry.otel.export.interval.ms=5000
   ```

---

### Q3: MR Collector 连接 History Server 超时

**症状**: MR Collector 日志显示 `Connection timed out`

**排查步骤**:

1. 确认 URL 和端口正确（History Server 端口均为 19888，Hadoop 2.x 和 3.x 相同）
   ```hocon
   mr-telemetry.history-server.url = "http://hadoop-historyserver:19888"
   ```

2. 增大超时配置：
   ```hocon
   mr-telemetry.history-server.connect.timeout.secs = 10
   mr-telemetry.history-server.read.timeout.secs = 30
   ```

3. 检查网络连通性：
   ```bash
   curl -v http://hadoop-historyserver:19888/ws/v1/jobs
   ```

---

### Q4: OTel Collector 启动失败

**症状**: Docker 容器立即退出，日志显示错误

**解决方案**:

1. 使用 `otel/opentelemetry-collector-contrib`（非核心镜像）
   ```bash
   docker run ... otel/opentelemetry-collector-contrib:0.96.0
   ```

2. 配置中必须包含 `health_check` 扩展
   ```yaml
   extensions:
     health_check:
       endpoint: 0.0.0.0:13133
   ```

3. 通过 `--config` 参数指定配置文件
   ```bash
   docker run ... --config=/etc/otelcol-contrib/config.yaml
   ```

---

### Q5: Kafka 中看不到指标数据

**症状**: OTel Collector 正常运行，但 Kafka 无消息

**排查步骤**:

1. 检查 OTel Collector 日志：
   ```bash
   docker logs otel-collector --tail=100 | grep -E "kafka|export"
   ```

2. 确认 Kafka exporter 配置（broker 地址、topic）
   ```yaml
   exporters:
     kafka:
       topic: telemetry-metrics
       brokers:
         - kafka:9092
   ```

3. 使用 `kafka-dump-log.sh` 验证消息存在：
   ```bash
   docker exec kafka /opt/kafka/bin/kafka-dump-log.sh \
     --files /tmp/kafka-logs/telemetry-metrics-0/00000000000000000000.log
   ```

4. 注意：`kafka-console-consumer.sh` 在单节点 KRaft 模式下可能超时

---

### Q6: Omnipackage 版本检测错误

**症状**: 日志显示错误的 Spark 版本，加载错误的适配器

**排查步骤**:

1. 检查 Driver/Executor 日志中 `OmniContext` 检测到的版本号
   ```
   OmniContext detected: Spark 3.5.0, Scala 2.12.15
   ```

2. 确认 classpath 上没有冲突的 `scala-library` JAR
   ```bash
   spark-submit --help | grep classpath
   ```

3. 如使用自定义 classpath，确保 `scala-library` 与 Spark 版本匹配

---

### Q7: SQL shuffle_bytes 为 NULL

**症状**: `sql_query_metrics` 表中 `shuffle_bytes` 字段为 NULL

**原因**: AQE `inputPlan` 返回初始计划

**解决方案**: 使用最新版本，已修复此问题。确保使用最新 Omnipackage JAR。

---

## 日志分析指南

### 检查 Spark 插件初始化

```bash
# Driver 日志
yarn logs -applicationId <app_id> -log_files stdout | grep -i "telemetry\|plugin"

# 预期输出
TelemetryLifecycle initialized
SparkTelemetryPlugin started
```

### 检查 OTel Collector 状态

```bash
# 健康检查
curl http://otel-collector:13133/health

# 日志
docker logs otel-collector --tail=100 | grep -E "error|warn|kafka"
```

### 检查 Kafka 消息

```bash
# 查看 Topic 消息数
docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe \
  --topic telemetry-metrics --bootstrap-server localhost:9092

# 消费测试消息
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic telemetry-metrics --bootstrap-server localhost:9092 \
  --from-beginning --max-messages=5
```

### 检查 MySQL 数据

```bash
# 连接 MySQL
docker exec -it mysql mysql -u root -proot123 metrics_db

# 检查表数据
SELECT COUNT(*) FROM task_metrics;
SELECT COUNT(*) FROM stage_metrics;
SELECT COUNT(*) FROM sql_query_metrics;

# 检查最新数据
SELECT * FROM task_metrics ORDER BY timestamp DESC LIMIT 5;
```

---

## 指标验证脚本

### 验证端到端数据流

```bash
#!/bin/bash
# verify-telemetry.sh

echo "=== 检查 OTel Collector ==="
curl -s http://localhost:13133/health | jq .

echo "=== 检查 Kafka Topic ==="
docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe \
  --topic telemetry-metrics --bootstrap-server localhost:9092

echo "=== 检查 MySQL 数据 ==="
docker exec mysql mysql -u root -proot123 metrics_db -e \
  "SELECT 'task' as type, COUNT(*) as cnt FROM task_metrics
   UNION ALL SELECT 'stage', COUNT(*) FROM stage_metrics
   UNION ALL SELECT 'sql', COUNT(*) FROM sql_query_metrics;"
```

---

## 性能调优建议

### 减少指标开销

1. 关闭不需要的指标类别：
   ```bash
   --conf spark.telemetry.metrics.stage.detailed=false
   --conf spark.telemetry.metrics.job.lifecycle=false
   ```

2. 增大导出间隔（适用于短时间作业）：
   ```bash
   --conf spark.telemetry.otel.export.interval.ms=30000
   ```

3. 减少 OTel 批处理大小：
   ```yaml
   # OTel Collector config
   exporters:
     kafka:
       producer:
         max_message_bytes: 500000  # 默认 1MB
   ```

---

## 联系支持

如遇到未覆盖的问题，请提供以下信息：

1. Spark/Hadoop/Hive 版本
2. Omnipackage JAR 版本
3. 相关日志片段（Driver/Executor/OTel Collector）
4. 配置文件内容（脱敏）
5. 问题复现步骤
