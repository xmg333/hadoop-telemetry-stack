# MR Telemetry -- Deployment & Metrics Reference

## MR Telemetry Collector

A standalone Java application that periodically polls the Hadoop YARN History Server REST API for completed MR job counters and exports them via OTel.

### Configuration

Create `mr-collector.conf`:

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
    # Persistence file (records last poll timestamp to avoid re-collecting after restart)
    file = "/tmp/mr-telemetry-state.json"
  }

  filter {
    user.include = [".*"]
    user.exclude = []
    job.name.include = [".*"]
    job.name.exclude = []
  }

  collection {
    job.counters = true
    task.counters = true    # Task-level granularity (can produce large volumes)
    job.details = true
  }
}
```

### Running

```bash
# Foreground
java -jar mr-telemetry-dist.jar mr-collector.conf

# Background
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

`state.file` records the last poll timestamp; after restart, only newly completed jobs are collected. First run collects all completed jobs.

### Running with Omnipackage

```bash
java -jar omnipackage.jar --mr-collector /path/to/mr-collector.conf
```

---

## MR Telemetry Agent

A Java Agent that intercepts `Mapper.run()` and `Reducer.run()` via ByteBuddy bytecode instrumentation, sampling counters in real-time during task execution.

### Configuration (JVM System Properties)

| System Property | Default | Description |
|-----------------|---------|-------------|
| `mr.telemetry.agent.enabled` | `true` | Enable the Agent |
| `mr.telemetry.agent.otel.exporter.endpoint` | `http://localhost:4317` | OTel Collector endpoint |
| `mr.telemetry.agent.otel.service.name` | `mr-telemetry-agent` | OTel service name |
| `mr.telemetry.agent.otel.export.interval.ms` | `10000` | Export interval (ms) |
| `mr.telemetry.agent.sampling.interval.secs` | `5` | Counter sampling interval (seconds) |

### Deployment

Configure in `mapred-site.xml`:

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

Or specify via command line:

```bash
hadoop jar my-job.jar \
  -Dmapreduce.map.java.opts="-javaagent:/opt/mr-telemetry-agent.jar -Dmr.telemetry.agent.otel.exporter.endpoint=http://collector:4317" \
  -Dmapreduce.reduce.java.opts="-javaagent:/opt/mr-telemetry-agent.jar -Dmr.telemetry.agent.otel.exporter.endpoint=http://collector:4317"
```

> **Note**: The Agent JAR must be accessible on the local filesystem of all NodeManager nodes.

### Using Omnipackage

Simply replace the `mr-telemetry-agent.jar` path with `omnipackage.jar` -- no other changes needed.

### Collector vs Agent

| Feature | MR Collector | MR Agent |
|---------|-------------|----------|
| Deployment | Standalone process | Java Agent (embedded in MR tasks) |
| Metric granularity | Job-level + Task-level (optional) | Task-level (real-time sampling) |
| Data freshness | Collected after job completion | Real-time during task execution |
| Runtime dependency | History Server | No external dependencies |
| Impact on tasks | Non-intrusive | Slight runtime overhead |

**Recommendation**: Both can be used together. The Collector provides job-level aggregation, while the Agent provides task-level real-time monitoring.

---

## Metrics Reference

### MR Collector Job-Level Metrics

| Metric Name | Type | Unit | Description |
|-------------|------|------|-------------|
| `mr.job.io.hdfs_bytes_read` | Counter | By | HDFS bytes read |
| `mr.job.io.hdfs_bytes_written` | Counter | By | HDFS bytes written |
| `mr.job.cpu_time_ms` | Counter | ms | CPU time |
| `mr.job.gc_time_ms` | Counter | ms | GC time |
| `mr.job.spilled_records` | Counter | {records} | Spilled records |
| `mr.job.map_input_records` | Counter | {records} | Map input records |
| `mr.job.map_output_records` | Counter | {records} | Map output records |
| `mr.job.reduce_input_records` | Counter | {records} | Reduce input records |
| `mr.job.reduce_output_records` | Counter | {records} | Reduce output records |
| `mr.job.maps_duration_ms` | Counter | ms | Total map duration |
| `mr.job.reduces_duration_ms` | Counter | ms | Total reduce duration |
| `mr.job.physical_memory_bytes` | Counter | By | Physical memory |
| `mr.job.virtual_memory_bytes` | Counter | By | Virtual memory |
| `mr.job.io.file_bytes_read` | Counter | By | Local file bytes read |
| `mr.job.io.file_bytes_written` | Counter | By | Local file bytes written |
| `mr.job.reduce_shuffle_bytes` | Counter | By | Shuffle bytes |
| `mr.job.map_output_bytes` | Counter | By | Map output bytes |
| `mr.job.launched_maps` | Counter | {tasks} | Launched map tasks |
| `mr.job.launched_reduces` | Counter | {tasks} | Launched reduce tasks |
| `mr.job.elapsed_time_ms` | Counter | ms | Job elapsed time |
| `mr.job.committed_heap_bytes` | Counter | By | Committed heap memory bytes |

#### Job-Level Attributes

`mr.job.id`, `mr.job.name`, `mr.job.user`, `mr.job.state`, `mr.job.queue`, `mr.job.finish_time_ms`, `mr.job.start_time_ms`

### MR Agent Task-Level Metrics

| Metric Name | Type | Unit | Description |
|-------------|------|------|-------------|
| `mr.task.io.map_input_records` | Counter | {records} | Map input records |
| `mr.task.io.map_output_records` | Counter | {records} | Map output records |
| `mr.task.io.map_output_bytes` | Counter | By | Map output bytes |
| `mr.task.io.reduce_input_records` | Counter | {records} | Reduce input records |
| `mr.task.io.reduce_output_records` | Counter | {records} | Reduce output records |
| `mr.task.io.reduce_shuffle_bytes` | Counter | By | Reduce shuffle bytes |
| `mr.task.io.spilled_records` | Counter | {records} | Spilled records |
| `mr.task.cpu_time_ms` | Counter | ms | CPU time |
| `mr.task.gc_time_ms` | Counter | ms | GC time |
| `mr.task.io.hdfs_bytes_read` | Counter | By | HDFS bytes read |
| `mr.task.io.hdfs_bytes_written` | Counter | By | HDFS bytes written |
| `mr.task.io.file_bytes_read` | Counter | By | Local file bytes read |
| `mr.task.io.file_bytes_written` | Counter | By | Local file bytes written |
| `mr.task.io.hdfs_read_ops` | Counter | {ops} | HDFS read operations count |
| `mr.task.io.hdfs_write_ops` | Counter | {ops} | HDFS write operations count |
| `mr.task.io.hdfs_large_read_ops` | Counter | {ops} | HDFS large read operations count |
| `mr.task.duration_ms` | Histogram | ms | Task execution duration |
| `mr.task.success` | Counter | {tasks} | Successful tasks count |
| `mr.task.failure` | Counter | {tasks} | Failed tasks count |

#### Task-Level Attributes

`mr.task.id`, `mr.task.type` (map/reduce), `mr.job.id`, `mr.job.name`, `mr.task.state`, `mr.job.user`, `mr.job.queue`
