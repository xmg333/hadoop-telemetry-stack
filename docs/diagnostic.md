# 遥测诊断工具

交互式诊断工具，用于检查 OTel Collector、Kafka、MySQL、Grafana 面板等后端组件的健康状态，以及 Spark Plugin、Hive Hook、MR Collector 等应用配置的正确性。

## 功能

- **应用诊断**：检查 Spark Plugin、Hive Hook、MR Collector 配置
- **后端诊断**：检查 OTel Collector、Kafka、MySQL 健康状态
- **Grafana 面板检查**：扫描 Dashboard JSON 文件，提取 SQL 查询并在 MySQL 中执行验证
- **数据流验证**：端到端数据流检查
- **中文输出**：诊断结果以中文显示

## 构建

```bash
mvn clean package -pl diagnostic/diagnostic-core -am -DskipTests
```

## 运行

```bash
# 交互式 CLI（JLine 终端）
java -jar diagnostic/diagnostic-core/target/diagnostic-core-1.0.0-SNAPSHOT.jar

# 指定配置文件
java -jar diagnostic/diagnostic-core/target/diagnostic-core-1.0.0-SNAPSHOT.jar \
  --config /path/to/diagnostic.conf
```

## 配置文件

配置文件位于 `diagnostic/diagnostic-core/src/main/resources/diagnostic.conf`：

```hocon
diagnostic {
  # OTel Collector 配置
  otel-collector {
    endpoint = "http://localhost:4317"
    health-check-port = 13133
    timeout-ms = 5000
  }

  # Kafka 配置
  kafka {
    bootstrap-servers = "localhost:9092"
    metrics-topic = "telemetry-metrics"
    traces-topic = "telemetry-traces"
    timeout-ms = 5000
  }

  # MySQL 配置
  mysql {
    host = "localhost"
    port = 3306
    database = "metrics_db"
    username = "metrics"
    password = "metrics"
    timeout-ms = 5000
  }

  # 启用/禁用特定检查
  spark {
    plugins-config.enabled = true
  }

  hive {
    hook-config.enabled = true
  }

  mr-collector {
    enabled = true
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

## 检查项说明

| 检查项 | 说明 |
|--------|------|
| Spark Plugin | 检查 SPARK_HOME、JAR 存在性、`spark.plugins` 配置 |
| Hive Hook | 检查 HIVE_HOME、JAR 存在性、`hive.exec.post.hooks` 配置 |
| MR Collector | 检查配置文件、History Server 连通性 |
| OTel Collector | HTTP Health Check（端口 13133）、gRPC 端口连通性（端口 4317） |
| Kafka | Kafka AdminClient 连通性、Topic `telemetry-metrics` 存在性 |
| MySQL | JDBC 连通性、15 张分类表 + `metric_events` 宽表存在性、列 schema 验证、行数统计 |
| Grafana 面板 | 扫描 `deploy/grafana/*.json`，提取 `rawSql` 查询在 MySQL 中执行，报告返回 0 行或全 NULL 列的面板 |
| 数据流 | 提交 Spark/MR/Hive 测试作业，验证 Kafka offset 变化、MySQL 行数增长 |

## 输出示例

```
╔══════════════════════════════════════════════════════╗
║     遥测诊断工具                                     ║
║     Telemetry Diagnostic Tool                        ║
╚══════════════════════════════════════════════════════╝

▶ 初始化
▶ 加载配置
▶ 检查 Spark Plugin
  ✓ JAR 文件存在
  ✓ spark.plugins 已配置
▶ 检查 OTel Collector
  ✓ Health Check 通过 (200)
  ✓ gRPC 端口 4317 可达
▶ 检查 Kafka
  ✓ Broker 连接成功
  ✓ Topic telemetry-metrics 存在
▶ 检查 MySQL
  ✓ 连接成功 (metrics@localhost:3306/metrics_db)
  ✓ 16/16 表存在
  ✓ 全部列 schema 验证通过
▶ 检查 Grafana 面板
  ✓ 13 个 Dashboard JSON 扫描完成
  ✓ 全部 SQL 查询执行成功
▶ 数据流验证
  ✓ Spark 测试作业提交成功
  ✓ Kafka offset 增长验证通过
  ✓ MySQL 行数增长验证通过
▶ 生成报告
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
│   │   │       ├── CheckOtelCollectorHandler.java
│   │   │       ├── CheckKafkaHandler.java
│   │   │       ├── CheckMySqlHandler.java
│   │   │       ├── GrafanaSqlCheckHandler.java
│   │   │       ├── DataFlowCheckHandler.java
│   │   │       └── GenerateReportHandler.java
│   │   ├── checks/                         # 检查器
│   │   │   ├── CheckItem.java              # 检查结果
│   │   │   ├── SparkPluginChecker.java
│   │   │   ├── HiveHookChecker.java
│   │   │   ├── OtelCollectorChecker.java
│   │   │   ├── KafkaChecker.java
│   │   │   └── MySQLChecker.java
│   │   ├── report/
│   │   │   └── DiagnosticReport.java       # 诊断报告
│   │   └── ui/
│   │       ├── AnsiColors.java             # ANSI 颜色定义
│   │       └── CheckPrinter.java           # 检查结果格式化输出
│   └── src/main/resources/
│       └── diagnostic.conf                 # 配置文件
└── pom.xml
```

## 状态机设计

状态机采用状态模式设计，每个状态对应一个状态处理器：

- 每个检查无论成败都继续下一个，不中断整个诊断流程
- 异常状态记录错误信息后跳转到下一个正常状态
- 只有 `EXIT_SUCCESS` 和 `EXIT_FAILURE` 为终止状态

## 扩展指南

### 添加新的状态处理器

1. 在 `DiagnosticState` 枚举中添加新状态
2. 在 `handlers` 包中创建新的处理器类，实现 `StateHandler` 接口
3. 在 `DiagnosticStateMachine.getHandler()` 中添加状态映射
4. 在 `DiagnosticStateMachine.nextAfter()` 中定义异常后的下一个状态

### 添加新的检查器

1. 在 `checks` 包中创建新的检查器类
2. 在对应的状态处理器中使用检查器

## 故障排查

| 问题 | 原因 | 解决 |
|------|------|------|
| `Unable to create a system terminal` | JLine 警告 | 不影响功能，可使用哑终端运行 |
| Spark Plugin 检查失败 | 环境中没有 Spark | 预期行为，工具继续执行后续检查 |
| Kafka 连接失败 | Broker 未运行或地址错误 | 确保 Kafka 运行且 `bootstrap.servers` 正确 |
| MySQL 连接失败 | 服务未运行或凭据错误 | 确保 MySQL 运行且数据库/用户名/密码正确 |
| Grafana 面板检查全失败 | MySQL 中无数据 | 先运行 Spark/MR/Hive 作业产生数据，再执行诊断 |
