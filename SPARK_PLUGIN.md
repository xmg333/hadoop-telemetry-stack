# Spark Telemetry Plugin — 部署与指标参考

## 部署

### Spark 3.x / 4.x（SparkPlugin API）

#### 分发 JAR

```bash
# 方式 A：HDFS
hdfs dfs -put spark-telemetry-dist-spark3/target/spark-telemetry-plugin.jar /spark/libs/

# 方式 B：本地路径
scp spark-telemetry-dist-spark3/target/spark-telemetry-plugin.jar node:/opt/spark/libs/
```

#### 配置

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --jars /opt/spark/libs/spark-telemetry-plugin.jar \
  --conf spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin \
  --conf spark.telemetry.otel.exporter.endpoint=http://otel-collector:4317 \
  --conf spark.telemetry.otel.service.name=my-spark-app \
  --conf spark.telemetry.otel.export.interval.ms=10000 \
  your-app.jar
```

`spark-defaults.conf` 方式：

```properties
spark.plugins              x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin
spark.telemetry.otel.exporter.endpoint    http://otel-collector:4317
spark.telemetry.otel.service.name         my-spark-app
spark.telemetry.otel.export.interval.ms   10000
```

### Spark 2.x（spark.extraListeners）

Spark 2.x 没有 `SparkPlugin` API：

```bash
spark-submit \
  --master yarn \
  --jars /opt/spark/libs/spark-telemetry-plugin.jar \
  --conf spark.extraListeners=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryListener \
  --conf spark.telemetry.otel.exporter.endpoint=http://otel-collector:4317 \
  --conf spark.telemetry.otel.service.name=my-spark2-app \
  your-app.jar
```

**Spark 2.x 注意事项**：
- 在第一个事件到达时通过 `ensureInit()` 懒初始化
- Shuffle Write API 使用 `shuffleBytesWritten` / `shuffleWriteTime` / `shuffleRecordsWritten`（与 3.x 不同）

### Omnipackage 统一部署

Omnipackage 将 Spark 2/3/4 + MR Collector + MR Agent 合并为单个 JAR，运行时自动检测 Spark 版本。

配置方式与版本专用 JAR **完全一致**，直接替换 JAR 即可：

```bash
# Spark 3/4
spark-submit --jars /opt/omnipackage.jar \
  --conf spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin \
  --conf spark.telemetry.otel.exporter.endpoint=http://otel-collector:4317 \
  --conf spark.telemetry.otel.service.name=my-app \
  your-app.jar

# Spark 2
spark-submit --jars /opt/omnipackage.jar \
  --conf spark.extraListeners=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryListener \
  --conf spark.telemetry.otel.exporter.endpoint=http://otel-collector:4317 \
  --conf spark.telemetry.otel.service.name=my-app \
  your-app.jar

# MR Collector 模式
java -jar omnipackage.jar --mr-collector /path/to/mr-collector.conf

# MR Agent 模式
-javaagent:/opt/omnipackage.jar -Dmr.telemetry.agent.otel.exporter.endpoint=http://otel-collector:4317
```

#### Omnipackage vs 版本专用 JAR

| 特性 | 版本专用 JAR | Omnipackage |
|------|-------------|-------------|
| 文件数量 | 3 个（Spark 2/3/4 各一） | 1 个 |
| 运维复杂度 | 需按 Spark 版本分发 | 统一分发 |
| 配置差异 | 需注意版本对应的入口类 | 同一入口类，自动检测 |
| JAR 体积 | ~30MB 各 | ~50-60MB |
| MR 支持 | 需额外 JAR | 内含 MR Collector + Agent |

### HOCON 配置文件（可选）

除 Spark Conf 外，还可通过 HOCON 配置文件进行详细配置：

```bash
cp telemetry.conf.example telemetry.conf
spark-submit --files telemetry.conf ...
```

配置优先级：**Spark Conf 覆盖 > HOCON 文件 > 内置默认值**

### 验证

```bash
# 检查 Driver/Executor 日志
# 应看到：INFO TelemetryLifecycle: Telemetry initialized, endpoint=http://collector:4317

# 检查 OTel Collector
kubectl logs -l app=otel-collector --tail=100 | grep "spark\."
```

> **注意**：短时 Spark 作业（< 10 秒）可能在 OTel SDK 首次导出前完成。建议使用较长作业测试，或减小 `spark.telemetry.otel.export.interval.ms`。

---

## 配置参数

### 最小配置（必填）

| 参数 | 说明 | 示例 |
|------|------|------|
| `spark.plugins` | Spark 插件类名 | `x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin` |
| `spark.telemetry.otel.exporter.endpoint` | OTel Collector gRPC 地址 | `http://collector:4317` |

### 可选配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `spark.telemetry.otel.service.name` | `spark-application` | OTel 服务名 |
| `spark.telemetry.otel.export.interval.ms` | `10000` | 指标导出间隔（毫秒） |
| `spark.telemetry.otel.exporter.protocol` | `grpc` | 导出协议 (`grpc` / `http`) |
| `spark.telemetry.config.path` | (classpath) | HOCON 配置文件路径 |
| `spark.telemetry.metrics.task.execution` | `true` | Category 1: 任务执行指标 |
| `spark.telemetry.metrics.task.shuffle-extended` | `true` | Category 2: 扩展 Shuffle 指标 |
| `spark.telemetry.metrics.task.info` | `true` | Category 3: 任务信息属性 |
| `spark.telemetry.metrics.stage.detailed` | `false` | Category 4: 阶段详细指标 |
| `spark.telemetry.metrics.job.lifecycle` | `false` | Category 5: 作业生命周期 |

> **重要**：Spark 配置键必须包含完整内部路径，包括 `.otel.` 段。映射规则为 `spark.telemetry.X` → `spark-telemetry.X`：
> - 正确：`spark.telemetry.otel.exporter.endpoint=http://host:4317`
> - 错误：`spark.telemetry.exporter.endpoint=http://host:4317`

### HOCON 配置完整参考（telemetry.conf）

```hocon
spark-telemetry {
  otel {
    exporter.endpoint = "http://localhost:4317"
    exporter.protocol = "grpc"
    service.name = "spark-application"
    export.interval.ms = 10000
    resource.attributes = {
      "deployment.environment" = "production"
    }
  }

  metrics {
    listener {
      enabled = true
      capture.task-end = true
      capture.stage-complete = true
      capture.job-end = false
    }

    system {
      enabled = true
      capture.jvm-memory = true
      capture.jvm-gc = true
      capture.buffer-pools = true
      capture.executor-memory = true
    }

    task.execution = true             # Category 1
    task.shuffle-extended = true      # Category 2
    task.info = true                  # Category 3
    stage.detailed = false            # Category 4
    job.lifecycle = false             # Category 5
  }

  filter {
    app.name.include = [".*"]
    app.name.exclude = []
  }
}
```

---

## 指标参考

### 核心 IO 指标（始终采集）

| 指标名 | 类型 | 单位 | 说明 |
|--------|------|------|------|
| `spark.task.io.bytes_read` | Counter | By | 任务读取字节数 |
| `spark.task.io.bytes_written` | Counter | By | 任务写入字节数 |
| `spark.task.io.records_read` | Counter | {records} | 任务读取记录数 |
| `spark.task.io.records_written` | Counter | {records} | 任务写入记录数 |
| `spark.task.shuffle.bytes_read` | Counter | By | Shuffle 读取字节数 |
| `spark.task.shuffle.bytes_written` | Counter | By | Shuffle 写入字节数 |
| `spark.task.shuffle.fetch_wait_time_ms` | Counter | ms | Shuffle 等待时间 |
| `spark.task.disk_bytes_spilled` | Counter | By | 磁盘溢写字节数 |
| `spark.task.memory_bytes_spilled` | Counter | By | 内存溢写字节数 |
| `spark.task.duration_ms` | Histogram | ms | 任务执行时长 |

### 任务执行指标（Category 1）

| 指标名 | 类型 | 单位 | 说明 |
|--------|------|------|------|
| `spark.task.executor.run_time_ms` | Histogram | ms | Executor 运行时间 |
| `spark.task.executor.cpu_time_ns` | Counter | ns | Executor CPU 时间 |
| `spark.task.deserialize_time_ms` | Histogram | ms | 反序列化时间 |
| `spark.task.deserialize_cpu_time_ns` | Counter | ns | 反序列化 CPU 时间 |
| `spark.task.result_serialization_time_ms` | Histogram | ms | 结果序列化时间 |
| `spark.task.jvm_gc_time_ms` | Histogram | ms | 任务 JVM GC 时间 |
| `spark.task.scheduler_delay_ms` | Histogram | ms | 调度延迟 |
| `spark.task.result_size_bytes` | Counter | By | 任务结果大小 |
| `spark.task.peak_execution_memory_bytes` | Counter | By | 峰值执行内存 |

### 扩展 Shuffle 指标（Category 2）

| 指标名 | 类型 | 单位 | 说明 |
|--------|------|------|------|
| `spark.task.shuffle.local_blocks_fetched` | Counter | {blocks} | 本地 Shuffle 块数 |
| `spark.task.shuffle.records_read` | Counter | {records} | Shuffle 读取记录数 |
| `spark.task.shuffle.remote_bytes_read_to_disk` | Counter | By | 远程 Shuffle 写盘字节数 |
| `spark.task.shuffle.remote_reqs_duration_ms` | Counter | ms | 远程 Shuffle 请求时长 |

### 阶段详细指标（Category 4）

| 指标名 | 类型 | 单位 | 说明 |
|--------|------|------|------|
| `spark.stage.duration_ms` | Histogram | ms | 阶段时长 |
| `spark.stage.num_tasks` | Counter | {tasks} | 阶段任务数 |
| `spark.stage.executor.run_time_ms` | Counter | ms | 阶段总运行时间 |
| `spark.stage.executor.cpu_time_ns` | Counter | ns | 阶段总 CPU 时间 |
| `spark.stage.jvm_gc_time_ms` | Counter | ms | 阶段总 GC 时间 |
| `spark.stage.peak_execution_memory_bytes` | Counter | By | 阶段峰值内存 |
| `spark.stage.io.bytes_read` | Counter | By | 阶段读取字节数（独立于 task 级） |
| `spark.stage.io.bytes_written` | Counter | By | 阶段写入字节数（独立于 task 级） |

### 作业生命周期指标（Category 5）

| 指标名 | 类型 | 单位 | 说明 |
|--------|------|------|------|
| `spark.job.duration_ms` | Histogram | ms | 作业时长 |
| `spark.job.num_stages` | Counter | {stages} | 作业阶段数 |

### JVM 系统指标

| 指标名 | 类型 | 单位 | 说明 |
|--------|------|------|------|
| `spark.jvm.memory.heap_used` | Gauge | By | JVM 堆内存使用量 |
| `spark.jvm.memory.non_heap_used` | Gauge | By | JVM 非堆内存使用量 |
| `spark.jvm.gc.count` | Counter | {count} | GC 次数（按 gc_name 区分） |
| `spark.jvm.gc.time_ms` | Counter | ms | GC 时间 |

### 指标标签（Attributes）

| 标签名 | 适用范围 | 说明 |
|--------|---------|------|
| `spark.app.id` | 所有 | 应用 ID |
| `spark.executor.id` | 任务/系统 | Executor ID |
| `spark.stage.id` | 任务/阶段 | Stage ID |
| `spark.task.id` | 任务 | Task ID |
| `spark.task.success` | 任务 | 任务是否成功 |
| `spark.task.host` | 任务 | 任务执行主机（Category 3） |
| `spark.task.locality` | 任务 | 数据本地性（Category 3） |
| `spark.task.speculative` | 任务 | 是否推测执行（Category 3） |
| `spark.job.id` | 作业 | Job ID |
| `spark.job.success` | 作业 | 作业是否成功 |
| `gc_name` | GC | GC 收集器名称 |
