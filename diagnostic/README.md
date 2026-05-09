# Diagnostic 模块

遥测诊断工具，用于检查 OTel Collector、Kafka、MySQL 等后端组件的健康状态，以及 Spark Plugin、Hive Hook 等应用配置的正确性。

## 功能

- **应用诊断**：检查 Spark Plugin、Hive Hook、MR Collector 配置
- **后端诊断**：检查 OTel Collector、Kafka、MySQL 健康状态
- **数据流验证**：端到端数据流检查
- **Grafana 面板检查**：扫描 Dashboard JSON 文件，提取 SQL 查询并在 MySQL 中执行验证
- **中文输出**：诊断结果以中文显示

## 构建

```bash
# 构建 diagnostic 模块
mvn clean package -pl diagnostic/diagnostic-core -am -DskipTests

# 构建 omnipackage（推荐，单一 JAR 支持 Spark 2/3/4）
chmod +x build-omni.sh && ./build-omni.sh
```

## 环境重置（清理 Kafka + MySQL + OTel Collector）

每次运行诊断前建议重置环境，确保测试数据不被旧数据污染：

```bash
# 一键重置脚本 — 在测试机上执行
SSH="ssh -i ~/.ssh/id_rsa root@<HOST>"

# 1. 清理 Kafka topic
$SSH 'docker exec kafka /opt/kafka/bin/kafka-topics.sh --delete --topic telemetry-metrics --bootstrap-server localhost:9092 2>/dev/null; sleep 1; docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic telemetry-metrics --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1'

# 2. 清理 MySQL 所有指标表
$SSH 'docker exec mysql mysql -u root -proot123 telemetry -e "
SET FOREIGN_KEY_CHECKS=0;
TRUNCATE TABLE task_metrics;
TRUNCATE TABLE stage_metrics;
TRUNCATE TABLE job_metrics;
TRUNCATE TABLE jvm_memory_metrics;
TRUNCATE TABLE jvm_gc_metrics;
TRUNCATE TABLE task_histogram_buckets;
TRUNCATE TABLE stage_histogram_buckets;
TRUNCATE TABLE job_histogram_buckets;
TRUNCATE TABLE stage_governance;
TRUNCATE TABLE sql_query_metrics;
TRUNCATE TABLE sql_query_table_metrics;
TRUNCATE TABLE hive_query_metrics;
TRUNCATE TABLE hive_table_io_metrics;
TRUNCATE TABLE mr_job_metrics;
TRUNCATE TABLE mr_task_metrics;
SET FOREIGN_KEY_CHECKS=1;"'

# 3. 重启 OTel Collector
$SSH 'docker restart otel-collector'
```

## 运行

```bash
# 上传 diagnostic JAR 和配置到测试机
scp -i ~/.ssh/id_rsa diagnostic/diagnostic-core/target/diagnostic-core-1.0.0-SNAPSHOT.jar root@<HOST>:/tmp/diagnostic.jar
scp -i ~/.ssh/id_rsa diagnostic.conf root@<HOST>:/tmp/diagnostic.conf

# 运行（需设置所有环境变量以启用完整检查）
ssh -i ~/.ssh/id_rsa root@<HOST> '
export JAVA_HOME=/opt/jdk8u482-b08
export PATH=$JAVA_HOME/bin:$PATH
export SPARK_HOME=/opt/spark-3.2.0-bin-hadoop2.7
export HADOOP_HOME=/opt/hadoop-2.7.0
export HADOOP_CONF_DIR=/opt/hadoop-2.7.0/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop-2.7.0/etc/hadoop
export HIVE_HOME=/opt/apache-hive-2.3.9-bin
java -jar /tmp/diagnostic.jar --config /tmp/diagnostic.conf
'
```

**关键环境变量：**

| 变量 | 用途 | 未设置影响 |
|------|------|-----------|
| `SPARK_HOME` | Spark 测试（RDD + SQL） | 跳过所有 Spark 测试，5 个指标表为空 |
| `HADOOP_HOME` | MR 测试 + MR Collector | 跳过 MR 测试 |
| `HADOOP_CONF_DIR` | Spark 提交到 YARN | Spark 作业无法提交到 YARN |
| `HIVE_HOME` | Hive 测试 | 跳过 Hive 测试 |
| `JAVA_HOME` | 所有 Java 进程 | 使用系统默认 Java（可能与 Spark 不兼容） |

**部署 Omnipackage 到 Spark：**

```bash
# 清理旧 JAR，部署 omnipackage（必须在 spark-submit 前完成）
ssh -i ~/.ssh/id_rsa root@<HOST> '
rm -f $SPARK_HOME/jars/spark-telemetry*.jar $SPARK_HOME/jars/omnipackage*.jar
cp /opt/spark-telemetry-omni.jar $SPARK_HOME/jars/
'
```

## 配置文件

配置文件位于 `src/main/resources/diagnostic.conf`，需根据实际环境调整 MySQL 用户名/密码/数据库名：

```hocon
diagnostic {
  otel-collector {
    endpoint = "http://localhost:4317"
    health-check-port = 13133
    timeout-ms = 5000
  }
  kafka {
    bootstrap-servers = "localhost:9092"
    metrics-topic = "telemetry-metrics"
    traces-topic = "telemetry-traces"
    timeout-ms = 5000
  }
  mysql {
    host = "localhost"
    port = 3306
    database = "telemetry"       # 实际数据库名
    username = "CHANGE_ME"            # 实际用户名
    password = "CHANGE_ME"         # 实际密码
    timeout-ms = 5000
  }
}
```

## 诊断流程

```
初始化 (INIT)
  ↓
加载配置 (LOAD_CONFIG)
  ↓
检查 Spark Plugin (CHECK_SPARK_PLUGIN)
  ↓
检查 Hive Hook (CHECK_HIVE_HOOK)
  ↓
检查 MR Collector (CHECK_MR_COLLECTOR)
  ↓
检查 OTel Collector (CHECK_OTEL_COLLECTOR)
  ↓
检查 Kafka (CHECK_KAFKA)
  ↓
检查 MySQL (CHECK_MYSQL)
  ↓
检查 Grafana 面板 (CHECK_GRAFANA)
  ↓
数据流验证 (DATA_FLOW_CHECK)
  ↓
生成报告 (GENERATE_REPORT)
```

## 输出示例

```
╔══════════════════════════════════════════════════════╗
║     遥测诊断工具                                           ║
║     Telemetry Diagnostic Tool                             ║
╚══════════════════════════════════════════════════════╝

▶ INIT
▶ LOAD_CONFIG
▶ CHECK_SPARK_PLUGIN
  ✓ JAR 文件存在
  ✗ spark.plugins 未配置
  修复：请添加配置 spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin

▶ SPARK_ERROR
  诊断异常终止：SPARK_ERROR
```

## 模块结构

```
diagnostic/
├── diagnostic-core/
│   ├── src/main/java/x/mg/metrics/diagnostic/
│   │   ├── DiagnosticApp.java              # 主入口
│   │   ├── config/
│   │   │   └── DiagnosticConfig.java       # 配置加载
│   │   ├── state/
│   │   │   ├── DiagnosticState.java        # 状态枚举
│   │   │   ├── DiagnosticStateMachine.java # 状态机
│   │   │   ├── DiagnosticContext.java      # 诊断上下文
│   │   │   ├── StateHandler.java           # 状态处理器接口
│   │   │   └── handlers/                   # 状态处理器实现
│   │   │       ├── InitHandler.java
│   │   │       ├── LoadConfigHandler.java
│   │   │       ├── CheckSparkPluginHandler.java
│   │   │       ├── CheckHiveHookHandler.java
│   │   │       ├── CheckMrCollectorHandler.java
│   │   │       ├── CheckOtelAppHandler.java
│   │   │       ├── CheckOtelCollectorHandler.java
│   │   │       ├── CheckKafkaHandler.java
│   │   │       ├── CheckMySqlHandler.java
│   │   │       ├── GrafanaSqlCheckHandler.java  # Grafana 面板 SQL 有效性检查
│   │   │       ├── DataFlowCheckHandler.java
│   │   │       ├── GenerateReportHandler.java
│   │   │       └── ErrorHandlers.java      # 错误状态处理器
│   │   ├── checks/                         # 检查器
│   │   │   ├── Checker.java                # 检查器接口
│   │   │   ├── CheckResult.java            # 检查结果
│   │   │   ├── SparkPluginChecker.java
│   │   │   ├── HiveHookChecker.java
│   │   │   ├── OtelCollectorChecker.java
│   │   │   ├── KafkaChecker.java
│   │   │   └── MySQLChecker.java
│   │   ├── report/
│   │   │   └── DiagnosticReport.java       # 诊断报告
│   │   ├── ui/
│   │   │   ├── AnsiColors.java             # ANSI 颜色定义
│   │   │   └── CheckPrinter.java           # 检查结果格式化输出
│   └── src/main/resources/
│       └── diagnostic.conf                 # 配置文件
└── pom.xml
```

## 状态机设计

状态机采用状态模式设计，每个状态对应一个状态处理器：

```
状态机流程：
1. 初始化状态机
2. 循环执行当前状态的处理
3. 获取下一个状态的处理器
4. 执行处理，获取下一个状态
5. 直到达到终止状态（EXIT_SUCCESS 或 EXIT_FAILURE）

错误处理：
- 每个状态处理异常后，会跳转到对应的错误状态
- 错误状态会记录错误信息，然后跳转到报告生成
```

## 检查器设计

检查器采用接口设计，每个检查器实现 `Checker` 接口：

```java
public interface Checker {
    CheckResult check();
    String getName();
    String getDescription();
}
```

检查结果包含：
- 成功/失败状态
- 消息
- 修复建议

## 扩展指南

### 添加新的状态处理器

1. 在 `handlers` 包中创建新的处理器类
2. 实现 `StateHandler` 接口
3. 在 `DiagnosticStateMachine.getStateHandler()` 中添加状态映射

### 添加新的检查器

1. 在 `checks` 包中创建新的检查器类
2. 实现 `Checker` 接口
3. 在对应的状态处理器中使用检查器

## 故障排查

### 终端无法创建

```
WARNING: Unable to create a system terminal, creating a dumb terminal
```

这是 JLine 的警告，不影响功能。可以使用哑终端运行。

### Spark Plugin 检查失败

如果环境中没有 Spark，Spark Plugin 检查会失败。这是预期的行为，诊断工具会继续执行后续检查。

### Kafka 连接失败

确保 Kafka broker 正在运行，并且 `bootstrap.servers` 配置正确。

### MySQL 连接失败

确保 MySQL 服务正在运行，并且数据库、用户名、密码配置正确。
