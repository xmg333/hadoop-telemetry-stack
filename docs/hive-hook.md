# Hive Telemetry Hook — 部署与指标参考

通过 Hive `ExecuteWithHookContext` 机制采集 HiveServer2 查询指标，导出到 OTel Collector。支持 Hive 2.x 和 3.x，兼容 MR / Spark / Tez 执行引擎。

## 部署

### 分发 JAR

```bash
# 方式 A：独立 Hook JAR
cp hive/hive-telemetry-hook-dist/target/*.jar $HIVE_HOME/lib/

# 方式 B：Omnipackage（包含 Spark/MR/Hive 全部组件）
cp spark/spark-telemetry-dist-omni/target/*.jar $HIVE_HOME/lib/
```

### 配置 hive-site.xml

```xml
<property>
  <name>hive.exec.post.hooks</name>
  <value>x.mg.metrics.hivetelemetry.HiveTelemetryHook</value>
</property>
<property>
  <name>hive.telemetry.otel.exporter.endpoint</name>
  <value>http://otel-collector:4317</value>
</property>
<property>
  <name>hive.telemetry.sql.max-length</name>
  <value>4096</value>
</property>
```

### 配置方式（三选一）

| 方式 | 示例 |
|------|------|
| HiveConf 覆盖 | `hive-site.xml` 中 `hive.telemetry.*` 配置项 |
| HOCON 配置文件 | `hive.telemetry.config.path=/etc/hive-telemetry.conf` |
| 混合（HiveConf 覆盖文件） | 同时使用，HiveConf 优先级最高 |

### 关键配置项

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `hive.telemetry.otel.exporter.endpoint` | `http://localhost:4317` | OTel Collector gRPC 端点 |
| `hive.telemetry.otel.service.name` | `hive-server2` | OTel 服务名 |
| `hive.telemetry.otel.export.interval.ms` | `10000` | 导出间隔（ms） |
| `hive.telemetry.metrics.enabled` | `true` | 总开关 |
| `hive.telemetry.metrics.query.duration` | `true` | 查询时长指标 |
| `hive.telemetry.metrics.query.io` | `true` | IO 字节/行数指标 |
| `hive.telemetry.metrics.query.tables` | `true` | 表级 IO 指标 |
| `hive.telemetry.sql.max-length` | `4096` | SQL 文本最大截断长度（字符） |
| `hive.telemetry.filter.user.include` | `[".*"]` | 用户白名单（正则） |
| `hive.telemetry.filter.user.exclude` | `[]` | 用户黑名单（正则） |
| `hive.telemetry.filter.operation.include` | `[".*"]` | 操作白名单（正则） |
| `hive.telemetry.filter.operation.exclude` | `[]` | 操作黑名单（正则） |

> **注意**: HiveConf key 必须以 `hive.telemetry.` 开头，内部映射为 `hive-telemetry.*`。

### HOCON 配置文件示例

```hocon
# conf/examples/hive-telemetry.conf.example
hive-telemetry {
  otel {
    exporter.endpoint = "http://localhost:4317"
    service.name = "hive-server2"
    export.interval.ms = 10000
  }
  metrics {
    enabled = true
    query.duration = true
    query.io = true
    query.tables = true
    sql.max-length = 4096
  }
  filter {
    user.include = [".*"]
    user.exclude = []
    operation.include = [".*"]
    operation.exclude = []
  }
}
```

---

## OTel 指标

### 指标列表

| 指标名 | 类型 | 单位 | 说明 | 条件 |
|--------|------|------|------|------|
| `hive.query.duration_ms` | Histogram | ms | 查询执行时长 | `query.duration=true` |
| `hive.query.success` | Counter | - | 成功查询计数 | 始终记录 |
| `hive.query.failure` | Counter | - | 失败查询计数 | 始终记录 |
| `hive.query.input_bytes` | Counter | By | 输入字节数 | `query.io=true` |
| `hive.query.output_bytes` | Counter | By | 输出字节数 | `query.io=true` |
| `hive.query.input_rows` | Counter | {rows} | 输入行数 | `query.io=true` |
| `hive.query.output_rows` | Counter | {rows} | 输出行数 | `query.io=true` |
| `hive.query.input_tables` | Counter | - | 输入表访问计数 | `query.tables=true` |
| `hive.query.output_tables` | Counter | - | 输出表访问计数 | `query.tables=true` |

### 指标属性

所有指标共享以下基础属性：

| 属性 | 说明 |
|------|------|
| `hive.query.id` | Hive 查询 ID |
| `hive.query.operation` | 操作类型（QUERY / CREATETABLE / INSERT 等） |
| `hive.query.user` | 执行用户 |
| `hive.query.success` | 是否成功（"true" / "false"） |
| `hive.query.execution_engine` | 执行引擎（mr / spark / tez） |
| `hive.query.sql_text` | SQL 查询文本（截断后） |

表级指标额外属性：

| 属性 | 说明 |
|------|------|
| `hive.query.input_table` | 输入表名（用于 `input_tables`） |
| `hive.query.output_table` | 输出表名（用于 `output_tables`） |

---

## 数据流

```
Hive Query → HiveTelemetryHook (POST_EXEC) → HiveHookContext (singleton)
  → HiveMetricRecorder → OTel SDK → OTLP gRPC → OTel Collector
  → Kafka → Flink Consumer → MySQL / ClickHouse
```

写入的数据库表：
- `hive_query_metrics` — 查询级指标（duration / success / IO / query_text）
- `hive_table_io_metrics` — 表级 IO 指标（per-table bytes / rows）
- `metric_events` — 统一宽表（跨引擎聚合用）

---

## 设计要点

### 错误隔离

Hook 的 `run()` 方法整体包裹在 try/catch 中，**任何异常不会传播到 HiveServer2**，不影响查询执行。

### 懒初始化

`HiveHookContext` 单例在首次查询时通过双重检查锁初始化，OTel SDK 只创建一次。注册了 JVM shutdown hook 保证退出时 flush。

### 短生命周期进程

Hive CLI 是短生命周期 JVM 进程。Hook 在每次查询后主动调用 `flush()`，确保指标在 JVM 退出前导出。

### IO 指标精度

IO 字节数和行数基于 Hive Metastore 表统计信息（`totalSize`、`numRows`），为估算值而非实际扫描量。DML 后需执行 `ANALYZE TABLE` 更新统计。

---

## 兼容性

| Hive 版本 | 编译版本 | 执行引擎 | 状态 |
|-----------|---------|---------|------|
| Hive 2.3.x | Hive 2.3.9 | MR / Spark | 已验证 |
| Hive 3.1.x | Hive 2.3.9 | MR / Spark / Tez | 已验证 |

## 故障排查

| 问题 | 原因 | 解决 |
|------|------|------|
| 无指标输出 | Hook 未注册 | 检查 `hive.exec.post.hooks` 配置 |
| 无指标输出 | JAR 不在 classpath | 确认 JAR 在 `$HIVE_HOME/lib/` |
| IO 字节数为 0 | 表缺少统计信息 | 执行 `ANALYZE TABLE ... COMPUTE STATISTICS` |
| 指标被过滤 | 用户/操作匹配 exclude 规则 | 检查 `filter.user.*` 和 `filter.operation.*` |
| Hook 初始化失败 | OTel Collector 不可达 | 检查 endpoint 和网络连通性 |
