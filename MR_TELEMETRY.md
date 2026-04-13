# MR Telemetry — 部署与指标参考

## MR Telemetry Collector

独立 Java 应用，定时轮询 Hadoop YARN History Server REST API，获取已完成 MR 作业计数器并通过 OTel 导出。

### 配置

创建 `mr-collector.conf`：

```hocon
mr-telemetry {
  history-server {
    url = "http://history-server:19888"
    poll.interval.secs = 30
    connect.timeout.secs = 10
    read.timeout.secs = 30
  }

  otel {
    exporter.endpoint = "http://otel-collector:4317"
    exporter.protocol = "grpc"
    service.name = "mr-telemetry-collector"
    export.interval.ms = 10000
  }

  state {
    # 持久化文件（记录上次轮询时间，重启后不重复采集）
    file = "/var/lib/mr-telemetry/state.json"
  }

  filter {
    user.include = [".*"]
    user.exclude = []
    job.name.include = [".*"]
    job.name.exclude = []
  }

  collection {
    job.counters = true
    task.counters = false    # 任务级粒度（数据量可能较大）
    job.details = true
  }
}
```

### 运行

```bash
# 前台运行
java -jar mr-telemetry-dist.jar mr-collector.conf

# 后台运行
nohup java -jar mr-telemetry-dist.jar mr-collector.conf > mr-collector.log 2>&1 &

# systemd
cat > /etc/systemd/system/mr-telemetry-collector.service <<'EOF'
[Unit]
Description=MR Telemetry Collector
After=network.target

[Service]
Type=simple
User=hadoop
ExecStart=/usr/bin/java -jar /opt/mr-telemetry/mr-telemetry-dist.jar /opt/mr-telemetry/mr-collector.conf
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

systemctl enable --now mr-telemetry-collector
```

`state.file` 记录上次轮询时间戳，重启后只采集新增作业。首次运行采集所有已完成作业。

### 使用 Omnipackage 运行

```bash
java -jar omnipackage.jar --mr-collector /path/to/mr-collector.conf
```

---

## MR Telemetry Agent

Java Agent，通过 ByteBuddy 字节码增强拦截 `Mapper.run()` 和 `Reducer.run()`，在任务执行期间实时采样计数器。

### 配置（JVM 系统属性）

| 系统属性 | 默认值 | 说明 |
|---------|--------|------|
| `mr.telemetry.agent.enabled` | `true` | 是否启用 Agent |
| `mr.telemetry.agent.otel.exporter.endpoint` | `http://localhost:4317` | OTel Collector 地址 |
| `mr.telemetry.agent.otel.service.name` | `mr-telemetry-agent` | OTel 服务名 |
| `mr.telemetry.agent.otel.export.interval.ms` | `10000` | 导出间隔（毫秒） |
| `mr.telemetry.agent.sampling.interval.secs` | `5` | 计数器采样间隔（秒） |

### 部署

在 `mapred-site.xml` 中配置：

```xml
<property>
  <name>mapreduce.map.java.opts</name>
  <value>-javaagent:/opt/mr-telemetry-agent.jar
    -Dmr.telemetry.agent.otel.exporter.endpoint=http://otel-collector:4317
    -Dmr.telemetry.agent.otel.service.name=my-mr-job</value>
</property>

<property>
  <name>mapreduce.reduce.java.opts</name>
  <value>-javaagent:/opt/mr-telemetry-agent.jar
    -Dmr.telemetry.agent.otel.exporter.endpoint=http://otel-collector:4317
    -Dmr.telemetry.agent.otel.service.name=my-mr-job</value>
</property>
```

或者命令行指定：

```bash
hadoop jar my-job.jar \
  -Dmapreduce.map.java.opts="-javaagent:/opt/mr-telemetry-agent.jar -Dmr.telemetry.agent.otel.exporter.endpoint=http://collector:4317" \
  -Dmapreduce.reduce.java.opts="-javaagent:/opt/mr-telemetry-agent.jar -Dmr.telemetry.agent.otel.exporter.endpoint=http://collector:4317"
```

> **注意**：Agent JAR 必须在所有 NodeManager 节点的本地路径可访问。

### 使用 Omnipackage

直接用 omnipackage.jar 替换 `mr-telemetry-agent.jar` 路径即可，无需其他改动。

### Collector vs Agent

| 特性 | MR Collector | MR Agent |
|------|-------------|----------|
| 部署方式 | 独立进程 | Java Agent（嵌入 MR 任务） |
| 指标粒度 | Job 级别 + Task 级别（可选） | Task 级别（实时采样） |
| 数据时效 | 作业完成后采集 | 任务执行中实时采集 |
| 运行依赖 | History Server | 无外部依赖 |
| 对任务影响 | 无侵入 | 轻微运行时开销 |

**推荐**：两者可同时使用。Collector 用于作业级汇总，Agent 用于任务级实时监控。

---

## 指标参考

### MR Collector 作业级指标

| 指标名 | 类型 | 单位 | 说明 |
|--------|------|------|------|
| `mr.job.io.hdfs_bytes_read` | Counter | By | HDFS 读取字节数 |
| `mr.job.io.hdfs_bytes_written` | Counter | By | HDFS 写入字节数 |
| `mr.job.cpu_time_ms` | Counter | ms | CPU 时间 |
| `mr.job.gc_time_ms` | Counter | ms | GC 时间 |
| `mr.job.spilled_records` | Counter | {records} | 溢出记录数 |
| `mr.job.map_input_records` | Counter | {records} | Map 输入记录数 |
| `mr.job.map_output_records` | Counter | {records} | Map 输出记录数 |
| `mr.job.reduce_input_records` | Counter | {records} | Reduce 输入记录数 |
| `mr.job.reduce_output_records` | Counter | {records} | Reduce 输出记录数 |
| `mr.job.maps_duration_ms` | Counter | ms | Map 总时长 |
| `mr.job.reduces_duration_ms` | Counter | ms | Reduce 总时长 |
| `mr.job.physical_memory_bytes` | Counter | By | 物理内存 |
| `mr.job.virtual_memory_bytes` | Counter | By | 虚拟内存 |
| `mr.job.io.file_bytes_read` | Counter | By | 本地文件读取字节 |
| `mr.job.io.file_bytes_written` | Counter | By | 本地文件写入字节 |
| `mr.job.reduce_shuffle_bytes` | Counter | By | Shuffle 字节 |
| `mr.job.map_output_bytes` | Counter | By | Map 输出字节 |
| `mr.job.launched_maps` | Counter | {tasks} | Map 任务数 |
| `mr.job.launched_reduces` | Counter | {tasks} | Reduce 任务数 |
| `mr.job.elapsed_time_ms` | Counter | ms | 作业运行时长 |

#### 作业级标签

`mr.job.id`、`mr.job.name`、`mr.job.user`、`mr.job.state`、`mr.job.queue`

### MR Agent 任务级指标

| 指标名 | 类型 | 单位 | 说明 |
|--------|------|------|------|
| `mr.task.io.map_input_records` | Counter | {records} | Map 输入记录 |
| `mr.task.io.map_output_records` | Counter | {records} | Map 输出记录 |
| `mr.task.io.map_output_bytes` | Counter | By | Map 输出字节 |
| `mr.task.io.reduce_input_records` | Counter | {records} | Reduce 输入记录 |
| `mr.task.io.reduce_output_records` | Counter | {records} | Reduce 输出记录 |
| `mr.task.io.reduce_shuffle_bytes` | Counter | By | Reduce Shuffle 字节 |
| `mr.task.io.spilled_records` | Counter | {records} | 溢出记录 |
| `mr.task.cpu_time_ms` | Counter | ms | CPU 时间 |
| `mr.task.gc_time_ms` | Counter | ms | GC 时间 |
| `mr.task.io.hdfs_bytes_read` | Counter | By | HDFS 读取字节 |
| `mr.task.io.hdfs_bytes_written` | Counter | By | HDFS 写入字节 |
| `mr.task.io.file_bytes_read` | Counter | By | 本地文件读取字节 |
| `mr.task.io.file_bytes_written` | Counter | By | 本地文件写入字节 |

#### 任务级标签

`mr.task.id`、`mr.task.type`（map/reduce）、`mr.job.id`、`mr.job.name`
