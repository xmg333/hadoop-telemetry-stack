# Telemetry Diagnostic Tool

An interactive diagnostic tool for checking the health of backend components (OTel Collector, Kafka, MySQL, Grafana dashboards, etc.) and verifying the correctness of application configurations (Spark Plugin, Hive Hook, MR Collector, etc.).

## Features

- **Application diagnostics**: Check Spark Plugin, Hive Hook, MR Collector configuration
- **Backend diagnostics**: Check OTel Collector, Kafka, MySQL health status
- **Grafana dashboard check**: Scan Dashboard JSON files, extract SQL queries and execute validation against MySQL
- **Data flow validation**: End-to-end data flow checks
- **Chinese output**: Diagnostic results are displayed in Chinese

## Build

```bash
mvn clean package -pl diagnostic/diagnostic-core -am -DskipTests
```

## Run

```bash
# Interactive CLI (JLine terminal)
java -jar diagnostic/diagnostic-core/target/diagnostic-core-1.0.0-SNAPSHOT.jar

# Specify config file
java -jar diagnostic/diagnostic-core/target/diagnostic-core-1.0.0-SNAPSHOT.jar \
  --config /path/to/diagnostic.conf
```

## Configuration File

The config file is located at `diagnostic/diagnostic-core/src/main/resources/diagnostic.conf`:

```hocon
diagnostic {
  # OTel Collector configuration
  otel-collector {
    endpoint = "http://localhost:4317"
    health-check-port = 13133
    timeout-ms = 5000
  }

  # Kafka configuration
  kafka {
    bootstrap-servers = "localhost:9092"
    metrics-topic = "telemetry-metrics"
    traces-topic = "telemetry-traces"
    timeout-ms = 5000
  }

  # MySQL configuration
  mysql {
    host = "localhost"
    port = 3306
    database = "metrics_db"
    username = "metrics"
    password = "metrics"
    timeout-ms = 5000
  }

  # Enable/disable specific checks
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

## Diagnostic Flow

```
Initialization (INIT)
  |
Load Configuration (LOAD_CONFIG)
  |
Check Spark Plugin (CHECK_SPARK_PLUGIN)
  |
Check Hive Hook (CHECK_HIVE_HOOK)
  |
Check MR Collector (CHECK_MR_COLLECTOR)
  |
Check OTel Collector (CHECK_OTEL_COLLECTOR)
  |
Check Kafka (CHECK_KAFKA)
  |
Check MySQL (CHECK_MYSQL)
  |
Check Grafana Dashboards (CHECK_GRAFANA)
  |
Data Flow Validation (DATA_FLOW_CHECK)
  |
Generate Report (GENERATE_REPORT)
```

## Check Descriptions

| Check Item | Description |
|------------|-------------|
| Spark Plugin | Checks SPARK_HOME, JAR existence, `spark.plugins` configuration |
| Hive Hook | Checks HIVE_HOME, JAR existence, `hive.exec.post.hooks` configuration |
| MR Collector | Checks configuration file, History Server connectivity |
| OTel Collector | HTTP Health Check (port 13133), gRPC port connectivity (port 4317) |
| Kafka | Kafka AdminClient connectivity, topic `telemetry-metrics` existence |
| MySQL | JDBC connectivity, existence of 15 category tables + `metric_events` wide table, column schema validation, row count statistics |
| Grafana Dashboards | Scan `deploy/grafana/*.json`, extract `rawSql` queries and execute against MySQL, report panels returning 0 rows or all-NULL columns |
| Data Flow | Submit Spark/MR/Hive test jobs, verify Kafka offset changes, MySQL row count growth |

## Output Example

```
+----------------------------------------------------+
|      Telemetry Diagnostic Tool                      |
|      Telemetry Diagnostic Tool                      |
+----------------------------------------------------+

> Initializing
> Loading configuration
> Checking Spark Plugin
  / JAR file exists
  / spark.plugins configured
> Checking OTel Collector
  / Health Check passed (200)
  / gRPC port 4317 reachable
> Checking Kafka
  / Broker connected successfully
  / Topic telemetry-metrics exists
> Checking MySQL
  / Connected successfully (metrics@localhost:3306/metrics_db)
  / 16/16 tables exist
  / All column schema validations passed
> Checking Grafana Dashboards
  / 13 Dashboard JSON files scanned
  / All SQL queries executed successfully
> Data Flow Validation
  / Spark test job submitted successfully
  / Kafka offset growth verified
  / MySQL row count growth verified
> Generating report
```

## Module Structure

```
diagnostic/
+-- diagnostic-core/
|   +-- src/main/java/x/mg/metrics/diagnostic/
|   |   +-- DiagnosticApp.java              # Main entry point
|   |   +-- config/
|   |   |   +-- DiagnosticConfig.java       # Config loading
|   |   +-- state/
|   |   |   +-- DiagnosticState.java        # State enum
|   |   |   +-- DiagnosticStateMachine.java # State machine
|   |   |   +-- DiagnosticContext.java      # Diagnostic context
|   |   |   +-- StateHandler.java           # State handler interface
|   |   |   +-- handlers/                   # State handler implementations
|   |   |       +-- InitHandler.java
|   |   |       +-- LoadConfigHandler.java
|   |   |       +-- CheckSparkPluginHandler.java
|   |   |       +-- CheckHiveHookHandler.java
|   |   |       +-- CheckMrCollectorHandler.java
|   |   |       +-- CheckOtelCollectorHandler.java
|   |   |       +-- CheckKafkaHandler.java
|   |   |       +-- CheckMySqlHandler.java
|   |   |       +-- GrafanaSqlCheckHandler.java
|   |   |       +-- DataFlowCheckHandler.java
|   |   |       +-- GenerateReportHandler.java
|   |   +-- checks/                         # Checkers
|   |   |   +-- CheckItem.java              # Check result
|   |   |   +-- SparkPluginChecker.java
|   |   |   +-- HiveHookChecker.java
|   |   |   +-- OtelCollectorChecker.java
|   |   |   +-- KafkaChecker.java
|   |   |   +-- MySQLChecker.java
|   |   +-- report/
|   |   |   +-- DiagnosticReport.java       # Diagnostic report
|   |   +-- ui/
|   |       +-- AnsiColors.java             # ANSI color definitions
|   |       +-- CheckPrinter.java           # Check result formatting output
|   +-- src/main/resources/
|       +-- diagnostic.conf                 # Configuration file
+-- pom.xml
```

## State Machine Design

The state machine uses the State pattern -- each state corresponds to a state handler:

- Each check continues to the next regardless of success or failure, without interrupting the entire diagnostic flow
- Error states record the error message then transition to the next normal state
- Only `EXIT_SUCCESS` and `EXIT_FAILURE` are terminal states

## Extension Guide

### Adding a New State Handler

1. Add a new state to the `DiagnosticState` enum
2. Create a new handler class in the `handlers` package implementing the `StateHandler` interface
3. Add the state mapping in `DiagnosticStateMachine.getHandler()`
4. Define the next state after an error in `DiagnosticStateMachine.nextAfter()`

### Adding a New Checker

1. Create a new checker class in the `checks` package
2. Use the checker in the corresponding state handler

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| `Unable to create a system terminal` | JLine warning | Does not affect functionality; can run with a dumb terminal |
| Spark Plugin check fails | No Spark in environment | Expected behavior; the tool continues to subsequent checks |
| Kafka connection failed | Broker not running or incorrect address | Ensure Kafka is running and `bootstrap.servers` is correct |
| MySQL connection failed | Service not running or wrong credentials | Ensure MySQL is running and database/user/password are correct |
| Grafana dashboard check all failed | No data in MySQL | Run Spark/MR/Hive jobs to generate data first, then execute diagnostics |
