#!/usr/bin/env bash
#
# benchmark.sh — End-to-end stress test for the Spark/MR/Hive telemetry pipeline
#
# Spark/MR/Hive → OTel SDK → OTel Collector → Kafka → Flink Consumer → MySQL/ClickHouse
#
# Usage:
#   ./k8s/benchmark.sh                           # MySQL backend, 10 min
#   SINK_TYPE=clickhouse ./k8s/benchmark.sh       # ClickHouse backend
#   BENCHMARK_DURATION=120 ./k8s/benchmark.sh     # Quick 2-min test
#
set -euo pipefail

##############################################################################
# Configuration
##############################################################################
BENCHMARK_DURATION="${BENCHMARK_DURATION:-600}"       # seconds
SPARK_JOB_INTERVAL="${SPARK_JOB_INTERVAL:-30}"        # seconds between Spark jobs
MR_JOB_INTERVAL="${MR_JOB_INTERVAL:-30}"              # seconds between MR jobs
HIVE_JOB_INTERVAL="${HIVE_JOB_INTERVAL:-45}"            # seconds between Hive jobs
SPARK_PI_ITERATIONS="${SPARK_PI_ITERATIONS:-5000}"    # default SparkPi iterations
SINK_TYPE="${SINK_TYPE:-mysql}"                       # mysql | clickhouse
FLINK_PARALLELISM="${FLINK_PARALLELISM:-2}"
KEEP_SERVICES="${KEEP_SERVICES:-false}"               # true = don't stop Flink/MR collector on exit
DRAIN_SECONDS="${DRAIN_SECONDS:-60}"
MAX_HDFS_GB="${MAX_HDFS_GB:-10}"                      # max HDFS usage in GB

# Counters
SPARK_SUBMITTED=0
SPARK_SUCCEEDED=0
SPARK_FAILED=0
MR_SUBMITTED=0
MR_SUCCEEDED=0
MR_FAILED=0
HIVE_SUBMITTED=0
HIVE_SUCCEEDED=0
HIVE_FAILED=0
HEALTH_ERRORS=0

##############################################################################
# Helpers
##############################################################################
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log()  { echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $*"; }
ok()   { echo -e "${GREEN}[OK]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; }

# Get current HDFS used space in GB
hdfs_used_gb() {
    kubectl exec "$HADOOP3_POD" -- bash -c '
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
$HADOOP_HOME/bin/hdfs dfs -df -h / 2>/dev/null | tail -1 | awk "{val=\$4; unit=\$5; if(unit==\"T\") printf \"%.2f\",val*1024; else if(unit==\"G\") printf \"%.2f\",val; else if(unit==\"M\") printf \"%.4f\",val/1024; else if(unit==\"K\") printf \"%.6f\",val/1048576; else printf \"%.2f\",val}"
' 2>/dev/null || echo "0"
}

# Assert HDFS usage is under limit
check_hdfs_limit() {
    local used
    used=$(hdfs_used_gb)
    # Handle decimal comparison via awk
    local over
    over=$(awk "BEGIN { print ($used > $MAX_HDFS_GB) ? 1 : 0 }")
    if [[ "$over" == "1" ]]; then
        warn "HDFS usage ${used}GB exceeds limit ${MAX_HDFS_GB}GB — cleaning outputs..."
        kubectl exec "$HADOOP3_POD" -- bash -c '
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash /tmp/benchmark/output 2>/dev/null || true
$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash /tmp/benchmark/terainput 2>/dev/null || true
' || true
    fi
}

# Resolve pod name from label selector (handles Deployment-generated names)
resolve_pod() {
    local label=$1
    local name
    name=$(kubectl get pods -l "app=${label}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    if [[ -z "$name" ]]; then
        # Fallback: try direct pod name
        if kubectl get pod "$label" &>/dev/null; then
            name="$label"
        fi
    fi
    echo "$name"
}

##############################################################################
# Phase 1: Preflight
##############################################################################
phase1_preflight() {
    log "========== Phase 1: Preflight Checks =========="

    # Resolve dynamic pod names (Deployments generate random suffixes)
    KAFKA_POD=$(resolve_pod "kafka")
    OTEL_POD=$(resolve_pod "otel-collector")
    MYSQL_POD=$(resolve_pod "mysql")
    CLICKHOUSE_POD=$(resolve_pod "clickhouse")
    SPARK3_POD="spark3"
    HADOOP3_POD="hadoop3"

    local pods=("$SPARK3_POD" "$HADOOP3_POD" "$KAFKA_POD" "$OTEL_POD" "$MYSQL_POD" "$CLICKHOUSE_POD")
    for pod in "${pods[@]}"; do
        if [[ -z "$pod" ]]; then
            fail "Could not resolve a required pod"
            exit 1
        fi
        if ! kubectl get pod "$pod" &>/dev/null; then
            fail "Pod '$pod' not found. Deploy K8s environment first: ./k8s/deploy.sh"
            exit 1
        fi
        local phase
        phase=$(kubectl get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
        if [[ "$phase" != "Running" ]]; then
            warn "Pod '$pod' is $phase (not Running)"
        fi
    done
    ok "All pods present"

    # Get IPs
    SPARK3_IP=$(kubectl get pod "$SPARK3_POD" -o jsonpath='{.status.podIP}')
    HADOOP3_IP=$(kubectl get pod "$HADOOP3_POD" -o jsonpath='{.status.podIP}')
    KAFKA_IP=$(kubectl get pod "$KAFKA_POD" -o jsonpath='{.status.podIP}')
    OTEL_IP=$(kubectl get pod "$OTEL_POD" -o jsonpath='{.status.podIP}')
    MYSQL_IP=$(kubectl get pod "$MYSQL_POD" -o jsonpath='{.status.podIP}')
    CLICKHOUSE_IP=$(kubectl get pod "$CLICKHOUSE_POD" -o jsonpath='{.status.podIP}')

    log "Pods: spark3=$SPARK3_POD hadoop3=$HADOOP3_POD kafka=$KAFKA_POD otel=$OTEL_POD mysql=$MYSQL_POD ch=$CLICKHOUSE_POD"
    log "IPs:  spark3=$SPARK3_IP hadoop3=$HADOOP3_IP kafka=$KAFKA_IP otel=$OTEL_IP mysql=$MYSQL_IP ch=$CLICKHOUSE_IP"

    # Check JARs exist
    local spark_jar
    spark_jar=$(ls /home/xmg333/spark-telemetry-listener/spark-telemetry-dist-omni/target/spark-telemetry-dist-omni-*-SNAPSHOT.jar 2>/dev/null || true)
    if [[ -z "$spark_jar" ]]; then
        fail "Spark omnipackage JAR not found. Run: ./build-omni.sh"
        exit 1
    fi

    local mr_jar
    mr_jar=$(ls /home/xmg333/spark-telemetry-listener/mr-telemetry-dist/target/mr-telemetry-dist-*-SNAPSHOT.jar 2>/dev/null || true)
    if [[ -z "$mr_jar" ]]; then
        fail "MR collector JAR not found. Run: mvn clean package -Pspark-3 -DskipTests"
        exit 1
    fi

    local flink_jar
    flink_jar=$(ls /home/xmg333/spark-telemetry-listener/metrics-flink-consumer-dist/target/metrics-flink-consumer-dist-*-SNAPSHOT.jar 2>/dev/null || true)
    if [[ -z "$flink_jar" ]]; then
        fail "Flink consumer JAR not found. Run: mvn clean package -Pspark-3 -DskipTests"
        exit 1
    fi

    local hive_jar
    hive_jar=$(ls /home/xmg333/spark-telemetry-listener/hive-telemetry-hook-dist/target/hive-telemetry-hook-dist-*-SNAPSHOT.jar 2>/dev/null || true)
    if [[ -z "$hive_jar" ]]; then
        warn "Hive hook JAR not found. Hive benchmark will be skipped. Run: mvn clean package -Pspark-3 -DskipTests"
    fi

    # Check if Hive is installed on hadoop3
    HIVE_AVAILABLE=false
    if kubectl exec "$HADOOP3_POD" -- test -d /opt/hive 2>/dev/null; then
        HIVE_AVAILABLE=true
        ok "Hive installation found on hadoop3"
    else
        warn "Hive not installed on hadoop3. Hive benchmark will be skipped."
    fi

    ok "All JARs found"
}

##############################################################################
# Phase 2: Environment Preparation
##############################################################################
phase2_environment() {
    log "========== Phase 2: Environment Preparation =========="

    # Deploy Spark omnipackage JAR
    log "Deploying Spark omnipackage JAR to spark3..."
    kubectl cp spark-telemetry-dist-omni/target/spark-telemetry-dist-omni-*-SNAPSHOT.jar \
        "$SPARK3_POD":/opt/spark-telemetry-plugin.jar

    # Deploy Hadoop config to spark3 for YARN submission
    log "Deploying Hadoop config to spark3 for YARN..."
    for f in core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml; do
        kubectl exec "$HADOOP3_POD" -- cat /opt/hadoop/etc/hadoop/$f > /tmp/$f
    done
    # Replace localhost with hadoop3 IP in config files
    sed -i "s|localhost:9000|${HADOOP3_IP}:9000|g" /tmp/core-site.xml
    sed -i "s|0\.0\.0\.0|${HADOOP3_IP}|g" /tmp/yarn-site.xml
    for f in core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml; do
        kubectl cp /tmp/$f "$SPARK3_POD":/opt/spark/conf/$f
    done
    rm -f /tmp/core-site.xml /tmp/hdfs-site.xml /tmp/yarn-site.xml /tmp/mapred-site.xml
    ok "Hadoop config deployed to spark3"

    # Upload Spark examples JAR to HDFS for YARN cluster mode
    log "Uploading Spark examples JAR to HDFS..."
    local examples_jar_path
    examples_jar_path=$(kubectl exec "$SPARK3_POD" -- bash -c 'ls /opt/spark/examples/jars/spark-examples_2.13-*.jar 2>/dev/null' | head -1 || true)
    if [[ -n "$examples_jar_path" ]]; then
        local examples_jar_name
        examples_jar_name=$(basename "$examples_jar_path")
        # Copy to hadoop3 first, then hdfs dfs -put from there
        kubectl exec "$SPARK3_POD" -- cat "$examples_jar_path" > /tmp/spark-examples.jar
        kubectl cp /tmp/spark-examples.jar "$HADOOP3_POD":/tmp/spark-examples.jar
        rm -f /tmp/spark-examples.jar
        kubectl exec "$HADOOP3_POD" -- bash -c "
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
\$HADOOP_HOME/bin/hdfs dfs -mkdir -p /tmp/spark-libs 2>/dev/null || true
\$HADOOP_HOME/bin/hdfs dfs -put -f /tmp/spark-examples.jar /tmp/spark-libs/ 2>/dev/null || true
rm -f /tmp/spark-examples.jar
"
        SPARK_EXAMPLES_HDFS="hdfs://${HADOOP3_IP}:9000/tmp/spark-libs/spark-examples.jar"
        ok "Spark examples JAR uploaded to HDFS"
    else
        warn "Spark examples JAR not found on spark3"
        SPARK_EXAMPLES_HDFS=""
    fi

    # Deploy Flink consumer JAR to spark3 (we reuse spark3 pod for Flink)
    log "Deploying Flink consumer JAR to spark3..."
    kubectl cp metrics-flink-consumer-dist/target/metrics-flink-consumer-dist-*-SNAPSHOT.jar \
        "$SPARK3_POD":/tmp/flink-consumer.jar

    # Deploy MR collector JAR to hadoop3
    log "Deploying MR collector JAR to hadoop3..."
    kubectl cp mr-telemetry-dist/target/mr-telemetry-dist-*-SNAPSHOT.jar \
        "$HADOOP3_POD":/tmp/mr-collector.jar

    # Write Flink consumer config on spark3
    log "Writing Flink consumer config (sink=$SINK_TYPE)..."
    local flink_conf_file
    flink_conf_file=$(mktemp)
    if [[ "$SINK_TYPE" == "clickhouse" ]]; then
        cat > "$flink_conf_file" <<EOF
flink-consumer {
  kafka {
    bootstrap.servers = "kafka:9092"
    topic = "telemetry-metrics"
    group.id = "benchmark-flink-consumer"
    startup.mode = "earliest-offset"
  }
  sink {
    type = "clickhouse"
    clickhouse {
      url = "jdbc:clickhouse://${CLICKHOUSE_IP}:8123/metrics_db"
      user = "default"
      password = ""
      batch.size = 5000
      flush.interval.ms = 3000
    }
  }
  filter {
    metric.name.include = ["spark.", "mr.", "hive."]
  }
  processing {
    parallelism = ${FLINK_PARALLELISM}
  }
}
EOF
    else
        cat > "$flink_conf_file" <<EOF
flink-consumer {
  kafka {
    bootstrap.servers = "kafka:9092"
    topic = "telemetry-metrics"
    group.id = "benchmark-flink-consumer"
    startup.mode = "earliest-offset"
  }
  sink {
    type = "mysql"
    mysql {
      url = "jdbc:mysql://${MYSQL_IP}:3306/metrics_db"
      user = "metrics"
      password = "metrics"
      batch.size = 1000
      flush.interval.ms = 5000
    }
  }
  filter {
    metric.name.include = ["spark.", "mr.", "hive."]
  }
  processing {
    parallelism = ${FLINK_PARALLELISM}
  }
}
EOF
    fi
    kubectl exec "$SPARK3_POD" -- rm -f /tmp/flink-consumer.conf
    kubectl exec "$SPARK3_POD" -- rm -f /tmp/flink-consumer-checkpoint.txt
    kubectl cp "$flink_conf_file" "$SPARK3_POD":/tmp/flink-consumer.conf
    rm -f "$flink_conf_file"
    ok "Flink config written"

    # Write MR collector config on hadoop3
    log "Writing MR collector config..."
    local mr_conf_file
    mr_conf_file=$(mktemp)
    cat > "$mr_conf_file" <<EOF
mr-telemetry {
  history-server {
    url = "http://localhost:19888"
    poll.interval.secs = 10
  }
  otel {
    exporter.endpoint = "http://${OTEL_IP}:4317"
    exporter.protocol = "grpc"
    service.name = "benchmark-mr-collector"
    export.interval.ms = 5000
  }
}
EOF
    kubectl exec "$HADOOP3_POD" -- rm -f /tmp/mr-collector.conf
    kubectl cp "$mr_conf_file" "$HADOOP3_POD":/tmp/mr-collector.conf
    rm -f "$mr_conf_file"
    ok "MR collector config written"

    # Ensure Kafka __consumer_offsets topic exists (KRaft mode does NOT auto-create it)
    log "Ensuring Kafka __consumer_offsets topic exists..."
    kubectl exec "$KAFKA_POD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
        --create --if-not-exists --topic __consumer_offsets --partitions 50 \
        --replication-factor 1 --config cleanup.policy=compact --config segment.bytes=10485760 \
        2>/dev/null || warn "Could not create __consumer_offsets (may already exist)"

    # Ensure telemetry-metrics topic exists
    log "Ensuring Kafka telemetry-metrics topic exists..."
    kubectl exec "$KAFKA_POD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
        --create --if-not-exists --topic telemetry-metrics --partitions 3 \
        --replication-factor 1 2>/dev/null || warn "Could not create telemetry-metrics (may already exist)"

    # Clean up any previous data for a fresh benchmark
    log "Cleaning previous benchmark data..."
    if [[ "$SINK_TYPE" == "clickhouse" ]]; then
        kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
            "DROP TABLE IF EXISTS task_metrics; DROP TABLE IF EXISTS stage_metrics; DROP TABLE IF EXISTS job_metrics; DROP TABLE IF EXISTS jvm_memory_metrics; DROP TABLE IF EXISTS jvm_gc_metrics; DROP TABLE IF EXISTS task_histogram_buckets; DROP TABLE IF EXISTS stage_histogram_buckets; DROP TABLE IF EXISTS job_histogram_buckets; DROP TABLE IF EXISTS stage_governance; DROP TABLE IF EXISTS sql_query_metrics; DROP TABLE IF EXISTS sql_query_table_metrics; DROP TABLE IF EXISTS hive_query_metrics; DROP TABLE IF EXISTS hive_table_io_metrics;" 2>/dev/null || \
            warn "Could not clean ClickHouse tables (may not exist yet)"
    else
        kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -e \
            "DROP TABLE IF EXISTS task_metrics, stage_metrics, job_metrics, jvm_memory_metrics, jvm_gc_metrics, task_histogram_buckets, stage_histogram_buckets, job_histogram_buckets, stage_governance, sql_query_metrics, sql_query_table_metrics, hive_query_metrics, hive_table_io_metrics;" 2>/dev/null || \
            warn "Could not clean MySQL tables (may not exist yet)"
    fi

    # Prepare HDFS input for MR jobs (replica=1, compact sizes to stay under limit)
    log "Preparing HDFS input on hadoop3 (replica=1, max ${MAX_HDFS_GB}GB)..."
    kubectl exec "$HADOOP3_POD" -- bash -c '
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export PATH=$HADOOP_HOME/bin:$JAVA_HOME/bin:$PATH

# Clean any previous benchmark data
$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash /tmp/benchmark 2>/dev/null || true

$HADOOP_HOME/bin/hdfs dfs -mkdir -p /tmp/benchmark/input 2>/dev/null || true

# Compact sizes: small=1K, medium=10K, large=50K lines (~5MB max input)
for size in small medium large; do
    case $size in
        small)  lines=1000 ;;
        medium) lines=10000 ;;
        large)  lines=50000 ;;
    esac
    cat /dev/urandom | tr -dc "a-zA-Z0-9 " | fold -w 100 | head -n $lines > /tmp/input_${size}.txt
    $HADOOP_HOME/bin/hdfs dfs -put -f /tmp/input_${size}.txt /tmp/benchmark/input/input_${size}.txt 2>/dev/null || true
    rm -f /tmp/input_${size}.txt
done
# Set replication to 1 to save space
$HADOOP_HOME/bin/hdfs dfs -setrep -R 1 /tmp/benchmark 2>/dev/null || true
echo "HDFS input ready"
'

    # Prepare local input files on spark3 for IO-heavy Spark jobs
    log "Preparing local IO input files on spark3..."
    kubectl exec "$SPARK3_POD" -- bash -c '
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
# ~10MB text file for Spark wordcount
cat /dev/urandom | tr -dc "a-zA-Z0-9 " | fold -w 100 | head -n 100000 > /tmp/spark-io-input.txt
echo "Local IO file ready ($(wc -c < /tmp/spark-io-input.txt) bytes)"
'

    # Write PySpark SQL benchmark script to spark3
    log "Writing PySpark SQL benchmark script to spark3..."
    kubectl exec "$SPARK3_POD" -- bash -c 'cat > /tmp/spark-sql-bench.py << "PYEOF"
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
mode = sys.argv[1] if len(sys.argv) > 1 else "scan"

if mode == "scan":
    # SCAN: create Parquet, read back with filter -> FileSourceScanExec
    df = spark.range(10000).withColumn("v", F.rand() * 1000)
    df.write.mode("overwrite").parquet("/tmp/bench-scan.parquet")
    spark.read.parquet("/tmp/bench-scan.parquet").filter("v > 500").count()

elif mode == "join":
    # JOIN: two DataFrames -> BroadcastHashJoin or SortMergeJoin
    df1 = spark.range(10000).withColumn("key", F.col("id") % 100)
    df2 = spark.range(5000).withColumn("key", F.col("id") % 100)
    df1.join(df2, "key").count()

elif mode == "groupby":
    # GROUP BY + aggregation -> shuffle
    df = spark.range(50000).withColumn("grp", F.col("id") % 50) \
                           .withColumn("val", F.rand() * 1000)
    df.groupBy("grp").agg(F.sum("val"), F.count("*"), F.avg("val")).collect()

elif mode == "write":
    # SCAN + WRITE -> FileSourceScanExec + DataWritingCommandExec
    df = spark.range(10000).withColumn("name", F.lit("hello"))
    df.write.mode("overwrite").parquet("/tmp/bench-write.parquet")
    result = spark.read.parquet("/tmp/bench-write.parquet")
    result.write.mode("overwrite").csv("/tmp/bench-write-csv")

elif mode == "shuffle_sort":
    # SHUFFLE-HEAVY: global sort on 500K rows -> full data shuffle across partitions
    df = spark.range(500000).withColumn("rand_key", F.rand()) \
                             .withColumn("bucket", F.col("id") % 20) \
                             .withColumn("value", F.rand() * 10000)
    df.repartition(8).sort("rand_key").write.mode("overwrite").parquet("/tmp/bench-sort-out.parquet")

elif mode == "shuffle_join":
    # SHUFFLE-HEAVY: 3-way join on large DataFrames -> multiple shuffle stages
    orders = spark.range(200000).withColumn("customer_id", F.col("id") % 5000) \
                                 .withColumn("product_id", F.col("id") % 1000) \
                                 .withColumn("amount", F.rand() * 500)
    customers = spark.range(5000).withColumn("region", F.when(F.col("id") % 3 == 0, "east") \
                                                  .when(F.col("id") % 3 == 1, "west") \
                                                  .otherwise("north"))
    products = spark.range(1000).withColumn("category", F.when(F.col("id") % 5 == 0, "A") \
                                                  .when(F.col("id") % 5 == 1, "B") \
                                                  .otherwise("C"))
    joined = orders.join(customers, orders.customer_id == customers.id) \
                   .join(products, orders.product_id == products.id)
    joined.groupBy("region", "category").agg(F.sum("amount"), F.count("*")).collect()

elif mode == "shuffle_agg":
    # SHUFFLE-HEAVY: wide aggregation + distinct + window -> multiple shuffle rounds
    df = spark.range(500000).withColumn("grp1", F.col("id") % 200) \
                             .withColumn("grp2", (F.col("id") / 1000).cast("int")) \
                             .withColumn("val1", F.rand() * 1000) \
                             .withColumn("val2", F.rand() * 500)
    # Round 1: groupBy shuffle
    agg1 = df.groupBy("grp1").agg(F.sum("val1").alias("s1"), F.avg("val2").alias("a2"))
    # Round 2: join back shuffle
    enriched = df.join(agg1, "grp1")
    # Round 3: distinct shuffle
    enriched.select("grp2").distinct().count()

spark.stop()
PYEOF'
    ok "PySpark SQL benchmark script written"

    # Deploy Hive hook JAR and prepare Hive benchmark tables (if Hive available)
    if [[ "$HIVE_AVAILABLE" == "true" ]]; then
        local hive_jar
        hive_jar=$(ls /home/xmg333/spark-telemetry-listener/hive-telemetry-hook-dist/target/hive-telemetry-hook-dist-*-SNAPSHOT.jar 2>/dev/null || true)
        if [[ -n "$hive_jar" ]]; then
            log "Deploying Hive hook JAR to hadoop3..."
            kubectl cp "$hive_jar" "$HADOOP3_POD":/opt/hive/lib/hive-telemetry-hook.jar

            # Ensure hook config in hive-site.xml
            log "Configuring Hive telemetry hook..."
            kubectl exec "$HADOOP3_POD" -- bash -c '
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HIVE_HOME=/opt/hive

# Add hook config to hive-site.xml if not already present
if ! grep -q "hive.telemetry.otel.exporter.endpoint" $HIVE_HOME/conf/hive-site.xml 2>/dev/null; then
    # Insert before </configuration>
    sed -i "s|</configuration>|<property><name>hive.exec.post.hooks</name><value>x.mg.metrics.hivetelemetry.HiveTelemetryHook</value></property>\n<property><name>hive.telemetry.otel.exporter.endpoint</name><value>http://'"$OTEL_IP"':4317</value></property>\n<property><name>hive.telemetry.otel.service.name</name><value>benchmark-hive</value></property>\n<property><name>hive.telemetry.otel.export.interval.ms</name><value>5000</value></property>\n</configuration>|" $HIVE_HOME/conf/hive-site.xml
fi

# Restart HiveServer2 to pick up hook
# Kill existing process and wait for it to exit (avoid exit code 143 from SIGTERM)
for pid in $(jps 2>/dev/null | grep HiveServer2 | awk "{print \$1}"); do
    kill $pid 2>/dev/null || true
done
sleep 3
# Ensure process is really gone
for pid in $(jps 2>/dev/null | grep HiveServer2 | awk "{print \$1}"); do
    kill -9 $pid 2>/dev/null || true
done

export HADOOP_HOME=/opt/hadoop
nohup $HIVE_HOME/bin/hiveserver2 > /tmp/hiveserver2.log 2>&1 &
echo $! > /tmp/hiveserver2.pid
echo "HiveServer2 restarted with PID $(cat /tmp/hiveserver2.pid)"
' || warn "Failed to configure Hive hook"

            sleep 5

            # Prepare benchmark tables
            log "Preparing Hive benchmark tables..."
            kubectl exec "$HADOOP3_POD" -- bash -c '
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HIVE_HOME=/opt/hive
export HADOOP_HOME=/opt/hadoop

$HIVE_HOME/bin/beeline -u "jdbc:hive2://localhost:10000" -e "
CREATE DATABASE IF NOT EXISTS bench_db;
USE bench_db;
CREATE TABLE IF NOT EXISTS orders (id INT, amount DOUBLE, customer STRING, region STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY \",\";
CREATE TABLE IF NOT EXISTS products (id INT, name STRING, category STRING, price DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY \",\";
TRUNCATE TABLE orders;
TRUNCATE TABLE products;
INSERT INTO TABLE orders VALUES (1, 100.0, \"alice\", \"east\"), (2, 200.0, \"bob\", \"west\"), (3, 150.0, \"charlie\", \"east\"), (4, 300.0, \"alice\", \"north\"), (5, 250.0, \"bob\", \"south\");
INSERT INTO TABLE products VALUES (1, \"widget\", \"hardware\", 10.0), (2, \"gadget\", \"electronics\", 25.0), (3, \"thingamajig\", \"hardware\", 15.0), (4, \"doohickey\", \"electronics\", 30.0), (5, \"whatchamacallit\", \"misc\", 5.0);
" 2>/dev/null || warn "Hive benchmark table setup failed (tables may already exist)"
' || warn "Hive benchmark table preparation skipped"
            ok "Hive benchmark environment prepared"
        else
            warn "Hive hook JAR not found — Hive benchmark disabled"
            HIVE_AVAILABLE=false
        fi
    fi

    ok "Environment prepared"
}

##############################################################################
# Phase 3: Start Flink Consumer
##############################################################################
phase3_start_flink() {
    log "========== Phase 3: Start Flink Consumer =========="

    # Kill any existing Flink consumer
    kubectl exec "$SPARK3_POD" -- bash -c 'pkill -f flink-consumer.jar 2>/dev/null; exit 0' || true

    log "Starting Flink consumer on spark3..."
    kubectl exec "$SPARK3_POD" -- bash -c '
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
nohup $JAVA_HOME/bin/java -jar /tmp/flink-consumer.jar /tmp/flink-consumer.conf \
    > /tmp/flink-consumer.log 2>&1 &
echo $! > /tmp/flink-consumer.pid
echo "Flink consumer PID: $(cat /tmp/flink-consumer.pid)"
' || true
    sleep 8

    # Verify it started
    local pid
    pid=$(kubectl exec "$SPARK3_POD" -- cat /tmp/flink-consumer.pid 2>/dev/null || echo "")
    if [[ -n "$pid" ]]; then
        local alive
        alive=$(kubectl exec "$SPARK3_POD" -- kill -0 "$pid" 2>&1 || echo "dead")
        if [[ "$alive" != *"dead"* ]]; then
            ok "Flink consumer running (PID $pid)"
        else
            fail "Flink consumer process died. Log:"
            kubectl exec "$SPARK3_POD" -- tail -30 /tmp/flink-consumer.log || true
            exit 1
        fi
    else
        fail "Could not start Flink consumer"
        exit 1
    fi
}

##############################################################################
# Phase 4: Start MR Collector
##############################################################################
phase4_start_mr_collector() {
    log "========== Phase 4: Start MR Collector =========="

    # Kill any existing MR collector
    kubectl exec "$HADOOP3_POD" -- bash -c 'pkill -f mr-collector.jar 2>/dev/null; exit 0' || true

    log "Starting MR collector on hadoop3..."
    kubectl exec "$HADOOP3_POD" -- bash -c '
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
nohup $JAVA_HOME/bin/java -jar /tmp/mr-collector.jar /tmp/mr-collector.conf \
    > /tmp/mr-collector.log 2>&1 &
echo $! > /tmp/mr-collector.pid
echo "MR collector PID: $(cat /tmp/mr-collector.pid)"
' || true
    sleep 3

    local pid
    pid=$(kubectl exec "$HADOOP3_POD" -- cat /tmp/mr-collector.pid 2>/dev/null || echo "")
    if [[ -n "$pid" ]]; then
        local alive
        alive=$(kubectl exec "$HADOOP3_POD" -- kill -0 "$pid" 2>&1 || echo "dead")
        if [[ "$alive" != *"dead"* ]]; then
            ok "MR collector running (PID $pid)"
        else
            fail "MR collector process died. Log:"
            kubectl exec "$HADOOP3_POD" -- tail -30 /tmp/mr-collector.log
            exit 1
        fi
    else
        fail "Could not start MR collector"
        exit 1
    fi
}

##############################################################################
# Phase 5: Main Benchmark Loop
##############################################################################
submit_spark_job() {
    local job_type=$1
    local job_name="spark-${job_type}-$(date +%s)"
    ((SPARK_SUBMITTED++)) || true

    log "Submitting Spark job: $job_name (type=$job_type)"

    case "$job_type" in
        pi|groupby|wordcount|kmeans)
            submit_scala_job "$job_type" "$job_name"
            ;;
        sql_scan|sql_join|sql_groupby|sql_write|sql_shuffle_sort|sql_shuffle_join|sql_shuffle_agg)
            submit_sql_job "$job_type" "$job_name"
            ;;
        *)
            fail "Unknown Spark job type: $job_type"
            return
            ;;
    esac
}

# Submit Scala-based Spark example jobs (pi, groupby, wordcount, kmeans)
submit_scala_job() {
    local job_type=$1
    local job_name=$2

    local spark_class=""
    local spark_args=""

    case "$job_type" in
        pi)
            spark_class="org.apache.spark.examples.SparkPi"
            local iters=$SPARK_PI_ITERATIONS
            spark_args="$iters"
            ;;
        groupby)
            # Shuffle-heavy: generates data in memory, does groupBy
            spark_class="org.apache.spark.examples.GroupByTest"
            spark_args="1000000 8"
            ;;
        wordcount)
            # IO-heavy: read local file, count words (produces bytes_read + shuffle)
            spark_class="org.apache.spark.examples.JavaWordCount"
            spark_args="file:///tmp/spark-io-input.txt"
            ;;
        kmeans)
            # CPU + data: ML algorithm with in-memory data generation
            spark_class="org.apache.spark.examples.SparkKMeans"
            spark_args="3 5 10 4"
            ;;
    esac

    (
        kubectl exec "$SPARK3_POD" -- bash -c "
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export SPARK_HOME=/opt/spark
export HADOOP_CONF_DIR=/opt/spark/conf
export YARN_CONF_DIR=/opt/spark/conf
\$SPARK_HOME/bin/spark-submit --master yarn \
    --deploy-mode cluster \
    --class ${spark_class} \
    --jars /opt/spark-telemetry-plugin.jar \
    --conf spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin \
    --conf spark.telemetry.otel.exporter.endpoint=http://${OTEL_IP}:4317 \
    --conf spark.telemetry.otel.service.name=${job_name} \
    --conf spark.telemetry.otel.export.interval.ms=5000 \
    --conf spark.telemetry.metrics.stage.detailed=true \
    --conf spark.telemetry.metrics.job.lifecycle=true \
    --conf spark.executor.instances=1 \
    --conf spark.executor.memory=512m \
    --conf spark.executor.cores=1 \
    ${SPARK_EXAMPLES_HDFS} ${spark_args}
" > /tmp/spark-${job_name}.log 2>&1
        echo $? > /tmp/spark-${job_name}.rc
    ) &

    SPARK_PIDS["$job_name"]=$!
}

# Submit PySpark SQL benchmark jobs (sql_scan, sql_join, sql_groupby, sql_write)
submit_sql_job() {
    local job_type=$1
    local job_name=$2
    local sql_mode="${job_type#sql_}"

    (
        kubectl exec "$SPARK3_POD" -- bash -c "
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export SPARK_HOME=/opt/spark
export HADOOP_CONF_DIR=/opt/spark/conf
export YARN_CONF_DIR=/opt/spark/conf
export PYSPARK_PYTHON=python3
\$SPARK_HOME/bin/spark-submit --master yarn \
    --deploy-mode cluster \
    --jars /opt/spark-telemetry-plugin.jar \
    --conf spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin \
    --conf spark.telemetry.otel.exporter.endpoint=http://${OTEL_IP}:4317 \
    --conf spark.telemetry.otel.service.name=${job_name} \
    --conf spark.telemetry.otel.export.interval.ms=5000 \
    --conf spark.telemetry.metrics.sql.query-execution=true \
    --conf spark.telemetry.metrics.stage.detailed=true \
    --conf spark.telemetry.metrics.job.lifecycle=true \
    --conf spark.executor.instances=1 \
    --conf spark.executor.memory=512m \
    --conf spark.executor.cores=1 \
    /tmp/spark-sql-bench.py ${sql_mode}
" > /tmp/spark-${job_name}.log 2>&1
        echo $? > /tmp/spark-${job_name}.rc
    ) &

    SPARK_PIDS["$job_name"]=$!
}

submit_mr_job() {
    local job_type=$1
    local job_name="mr-${job_type}-$(date +%s)"
    local output_dir="/tmp/benchmark/output/${job_name}"
    ((MR_SUBMITTED++)) || true

    log "Submitting MR job: $job_name (type=$job_type)"

    local mr_class=""
    local mr_args=""

    case "$job_type" in
        wordcount)
            # IO-heavy: read HDFS, count words, write output
            local sizes=("small" "medium" "large")
            local size_idx=$((RANDOM % 3))
            local size=${sizes[$size_idx]}
            mr_class="wordcount"
            mr_args="/tmp/benchmark/input/input_${size}.txt ${output_dir}"
            ;;
        randomtextwriter)
            # Write-heavy: generate random text data to HDFS (1MB per run)
            mr_class="randomtextwriter"
            mr_args="-D mapreduce.randomtextwriter.totalbytes=1048576 ${output_dir}"
            ;;
        sort)
            # Shuffle-heavy: sort HDFS data (use medium input to save space)
            mr_class="sort"
            mr_args="/tmp/benchmark/input/input_medium.txt ${output_dir}"
            ;;
        terasort)
            # Heavy: TeraGen + TeraSort (small, 10K rows ~1MB)
            mr_class="terasort"
            mr_args="${output_dir}"
            ;;
        *)
            fail "Unknown MR job type: $job_type"
            return
            ;;
    esac

    (
        if [[ "$mr_class" == "terasort" ]]; then
            # Special handling: TeraGen then TeraSort
            kubectl exec "$HADOOP3_POD" -- bash -c "
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export PATH=\$HADOOP_HOME/bin:\$JAVA_HOME/bin:\$PATH
EXAMPLES_JAR=\$(find -L \$HADOOP_HOME -name 'hadoop-mapreduce-examples-*.jar' ! -name '*sources*' ! -name '*test*' | head -1)
\$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash /tmp/benchmark/terainput 2>/dev/null || true
\$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash ${output_dir} 2>/dev/null || true
hadoop jar \$EXAMPLES_JAR teragen 10000 /tmp/benchmark/terainput && \
hadoop jar \$EXAMPLES_JAR terasort /tmp/benchmark/terainput ${output_dir} && \
\$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash /tmp/benchmark/terainput 2>/dev/null || true
\$HADOOP_HOME/bin/hdfs dfs -setrep -R 1 ${output_dir} 2>/dev/null || true
" > /tmp/mr-${job_name}.log 2>&1
        else
            kubectl exec "$HADOOP3_POD" -- bash -c "
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export PATH=\$HADOOP_HOME/bin:\$JAVA_HOME/bin:\$PATH
EXAMPLES_JAR=\$(find -L \$HADOOP_HOME -name 'hadoop-mapreduce-examples-*.jar' ! -name '*sources*' ! -name '*test*' | head -1)
\$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash ${output_dir} 2>/dev/null || true
hadoop jar \$EXAMPLES_JAR ${mr_class} ${mr_args}
\$HADOOP_HOME/bin/hdfs dfs -setrep -R 1 ${output_dir} 2>/dev/null || true
" > /tmp/mr-${job_name}.log 2>&1
        fi
        echo $? > /tmp/mr-${job_name}.rc
    ) &

    MR_PIDS["$job_name"]=$!
}

submit_hive_job() {
    local job_type=$1
    local job_name="hive-${job_type}-$(date +%s)"
    ((HIVE_SUBMITTED++)) || true

    log "Submitting Hive job: $job_name (type=$job_type)"

    local hive_query=""
    case "$job_type" in
        select)
            # Simple SELECT with filter
            hive_query="USE bench_db; SELECT * FROM orders WHERE region = 'east';"
            ;;
        join)
            # JOIN two tables
            hive_query="USE bench_db; SELECT o.id, o.amount, p.name, p.category FROM orders o JOIN products p ON o.id = p.id;"
            ;;
        groupby)
            # GROUP BY with aggregation
            hive_query="USE bench_db; SELECT region, COUNT(*), SUM(amount), AVG(amount) FROM orders GROUP BY region;"
            ;;
        ctas)
            # CREATE TABLE AS SELECT — exercises write path
            hive_query="USE bench_db; DROP TABLE IF EXISTS bench_agg; CREATE TABLE bench_agg AS SELECT region, COUNT(*) as cnt, SUM(amount) as total FROM orders GROUP BY region;"
            ;;
        orderby)
            # ORDER BY — exercises shuffle/sort
            hive_query="USE bench_db; SELECT * FROM orders ORDER BY amount DESC;"
            ;;
        *)
            fail "Unknown Hive job type: $job_type"
            return
            ;;
    esac

    (
        kubectl exec "$HADOOP3_POD" -- bash -c "
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HIVE_HOME=/opt/hive
\$HIVE_HOME/bin/beeline -u 'jdbc:hive2://localhost:10000' -e \"${hive_query}\"
" > /tmp/hive-${job_name}.log 2>&1
        echo $? > /tmp/hive-${job_name}.rc
    ) &

    HIVE_PIDS["$job_name"]=$!
}

check_health() {
    local errors=0

    # Check Flink consumer — auto-restart if dead
    local flink_pid
    flink_pid=$(kubectl exec "$SPARK3_POD" -- cat /tmp/flink-consumer.pid 2>/dev/null || echo "")
    if [[ -n "$flink_pid" ]]; then
        local alive
        alive=$(kubectl exec "$SPARK3_POD" -- kill -0 "$flink_pid" 2>&1 || echo "dead")
        if [[ "$alive" == *"dead"* ]]; then
            warn "Flink consumer process is DEAD — restarting..."
            kubectl exec "$SPARK3_POD" -- bash -c '
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
pkill -f flink-consumer.jar 2>/dev/null; exit 0
' || true
            sleep 2
            kubectl exec "$SPARK3_POD" -- bash -c "
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
nohup \$JAVA_HOME/bin/java -jar /tmp/flink-consumer.jar /tmp/flink-consumer.conf > /tmp/flink-consumer.log 2>&1 &
echo \$! > /tmp/flink-consumer.pid
"
            sleep 5
            local new_pid
            new_pid=$(kubectl exec "$SPARK3_POD" -- cat /tmp/flink-consumer.pid 2>/dev/null || echo "")
            if [[ -n "$new_pid" ]]; then
                ok "Flink consumer restarted (PID $new_pid)"
            else
                fail "Flink consumer restart failed"
                ((errors++)) || true
            fi
        fi
    else
        fail "Flink consumer PID not found"
        ((errors++)) || true
    fi

    # Check MR collector — auto-restart if dead
    local mr_pid
    mr_pid=$(kubectl exec "$HADOOP3_POD" -- cat /tmp/mr-collector.pid 2>/dev/null || echo "")
    if [[ -n "$mr_pid" ]]; then
        local alive
        alive=$(kubectl exec "$HADOOP3_POD" -- kill -0 "$mr_pid" 2>&1 || echo "dead")
        if [[ "$alive" == *"dead"* ]]; then
            warn "MR collector process is DEAD — restarting..."
            kubectl exec "$HADOOP3_POD" -- bash -c 'pkill -f mr-collector.jar 2>/dev/null; exit 0' || true
            sleep 2
            kubectl exec "$HADOOP3_POD" -- bash -c "
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
nohup \$JAVA_HOME/bin/java -jar /tmp/mr-collector.jar /tmp/mr-collector.conf > /tmp/mr-collector.log 2>&1 &
echo \$! > /tmp/mr-collector.pid
"
            sleep 3
            local new_pid
            new_pid=$(kubectl exec "$HADOOP3_POD" -- cat /tmp/mr-collector.pid 2>/dev/null || echo "")
            if [[ -n "$new_pid" ]]; then
                ok "MR collector restarted (PID $new_pid)"
            else
                fail "MR collector restart failed"
                ((errors++)) || true
            fi
        fi
    else
        fail "MR collector PID not found"
        ((errors++)) || true
    fi

    # Check DB row count growth
    local rows
    if [[ "$SINK_TYPE" == "clickhouse" ]]; then
        rows=$(kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
            "SELECT count() FROM task_metrics" 2>/dev/null || echo "0")
    else
        rows=$(kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -N -e \
            "SELECT count(*) FROM task_metrics" 2>/dev/null || echo "0")
    fi
    log "Health check: task_metrics rows=$rows"

    # Clean up old MR output dirs to keep HDFS under limit
    kubectl exec "$HADOOP3_POD" -- bash -c '
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export PATH=$HADOOP_HOME/bin:$JAVA_HOME/bin:$PATH
# Remove all but the last 2 output dirs
$HADOOP_HOME/bin/hdfs dfs -ls -t /tmp/benchmark/output/ 2>/dev/null | tail -n +2 | awk "{print \$NF}" | tail -n +3 | xargs -r -n1 $HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash 2>/dev/null || true
# Also clean terasort temp input
$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash /tmp/benchmark/terainput 2>/dev/null || true
' || true

    check_hdfs_limit

    if [[ "$errors" -gt 0 ]]; then
        HEALTH_ERRORS=$((HEALTH_ERRORS + errors))
    fi

    return $errors
}

collect_finished_jobs() {
    # Check Spark jobs
    for job_name in "${!SPARK_PIDS[@]}"; do
        local pid=${SPARK_PIDS[$job_name]}
        if ! kill -0 "$pid" 2>/dev/null; then
            wait "$pid" 2>/dev/null || true
            local rc_file="/tmp/spark-${job_name}.rc"
            if [[ -f "$rc_file" ]]; then
                local rc
                rc=$(cat "$rc_file")
                if [[ "$rc" == "0" ]]; then
                    ((SPARK_SUCCEEDED++)) || true
                else
                    ((SPARK_FAILED++)) || true
                    warn "Spark job $job_name failed (rc=$rc)"
                fi
            fi
            rm -f "$rc_file"
            unset 'SPARK_PIDS[$job_name]'
        fi
    done

    # Check MR jobs
    for job_name in "${!MR_PIDS[@]}"; do
        local pid=${MR_PIDS[$job_name]}
        if ! kill -0 "$pid" 2>/dev/null; then
            wait "$pid" 2>/dev/null || true
            local rc_file="/tmp/mr-${job_name}.rc"
            if [[ -f "$rc_file" ]]; then
                local rc
                rc=$(cat "$rc_file")
                if [[ "$rc" == "0" ]]; then
                    ((MR_SUCCEEDED++)) || true
                else
                    ((MR_FAILED++)) || true
                    warn "MR job $job_name failed (rc=$rc)"
                fi
            fi
            rm -f "$rc_file"
            unset 'MR_PIDS[$job_name]'
        fi
    done

    # Check Hive jobs
    for job_name in "${!HIVE_PIDS[@]}"; do
        local pid=${HIVE_PIDS[$job_name]}
        if ! kill -0 "$pid" 2>/dev/null; then
            wait "$pid" 2>/dev/null || true
            local rc_file="/tmp/hive-${job_name}.rc"
            if [[ -f "$rc_file" ]]; then
                local rc
                rc=$(cat "$rc_file")
                if [[ "$rc" == "0" ]]; then
                    ((HIVE_SUCCEEDED++)) || true
                else
                    ((HIVE_FAILED++)) || true
                    warn "Hive job $job_name failed (rc=$rc)"
                fi
            fi
            rm -f "$rc_file"
            unset 'HIVE_PIDS[$job_name]'
        fi
    done
}

phase5_main_loop() {
    log "========== Phase 5: Main Benchmark Loop (${BENCHMARK_DURATION}s) =========="

    declare -A SPARK_PIDS
    declare -A MR_PIDS
    declare -A HIVE_PIDS

    local start_time
    start_time=$(date +%s)
    local last_spark=0
    local last_mr=0
    local last_hive=0
    local last_health=0
    local elapsed=0
    local iteration=0

    # Job type rotation
    local spark_types=("pi" "groupby" "wordcount" "kmeans" "sql_scan" "sql_join" "sql_groupby" "sql_write" "sql_shuffle_sort" "sql_shuffle_join" "sql_shuffle_agg")
    local mr_types=("wordcount" "randomtextwriter" "sort" "terasort")
    local hive_types=("select" "join" "groupby" "ctas" "orderby")

    while true; do
        local now
        now=$(date +%s)
        elapsed=$(( now - start_time ))

        if [[ "$elapsed" -ge "$BENCHMARK_DURATION" ]]; then
            log "Benchmark duration reached ($elapsed >= $BENCHMARK_DURATION)"
            break
        fi

        iteration=$((iteration + 1))

        # Submit Spark job if interval elapsed — rotate through types
        local since_spark=$(( now - last_spark ))
        if [[ "$since_spark" -ge "$SPARK_JOB_INTERVAL" ]]; then
            local type_idx=$(( (iteration - 1) % ${#spark_types[@]} ))
            submit_spark_job "${spark_types[$type_idx]}"
            last_spark=$now
        fi

        # Submit MR job if interval elapsed — rotate through types
        # Skip if HDFS is over limit
        local since_mr=$(( now - last_mr ))
        if [[ "$since_mr" -ge "$MR_JOB_INTERVAL" ]]; then
            local used
            used=$(hdfs_used_gb)
            local over
            over=$(awk "BEGIN { print ($used > $MAX_HDFS_GB) ? 1 : 0 }")
            if [[ "$over" == "1" ]]; then
                warn "HDFS at ${used}GB, skipping MR job (limit ${MAX_HDFS_GB}GB)"
            else
                local type_idx=$(( (iteration - 1) % ${#mr_types[@]} ))
                submit_mr_job "${mr_types[$type_idx]}"
                last_mr=$now
            fi
        fi

        # Submit Hive job if interval elapsed — only if Hive is available
        if [[ "$HIVE_AVAILABLE" == "true" ]]; then
            local since_hive=$(( now - last_hive ))
            if [[ "$since_hive" -ge "$HIVE_JOB_INTERVAL" ]]; then
                local type_idx=$(( (iteration - 1) % ${#hive_types[@]} ))
                submit_hive_job "${hive_types[$type_idx]}"
                last_hive=$now
            fi
        fi

        # Health check every 30 seconds
        local since_health=$(( now - last_health ))
        if [[ "$since_health" -ge 30 ]]; then
            check_health || true
            last_health=$now
        fi

        # Collect finished jobs
        collect_finished_jobs

        sleep 5
    done

    # Wait for all remaining background jobs to finish (with timeout)
    log "Waiting for remaining jobs to finish (timeout 120s)..."
    local wait_start
    wait_start=$(date +%s)
    while true; do
        collect_finished_jobs
        local remaining=$(( ${#SPARK_PIDS[@]} + ${#MR_PIDS[@]} + ${#HIVE_PIDS[@]} ))
        if [[ "$remaining" -eq 0 ]]; then
            break
        fi
        local waited
        waited=$(( $(date +%s) - wait_start ))
        if [[ "$waited" -ge 120 ]]; then
            warn "Timeout waiting for $remaining remaining jobs"
            # Count timed-out jobs as failed
            SPARK_FAILED=$((SPARK_FAILED + ${#SPARK_PIDS[@]}))
            MR_FAILED=$((MR_FAILED + ${#MR_PIDS[@]}))
            HIVE_FAILED=$((HIVE_FAILED + ${#HIVE_PIDS[@]}))
            break
        fi
        sleep 5
    done

    log "Main loop complete. Spark: ${SPARK_SUBMITTED} submitted, ${SPARK_SUCCEEDED} ok, ${SPARK_FAILED} fail | MR: ${MR_SUBMITTED} submitted, ${MR_SUCCEEDED} ok, ${MR_FAILED} fail | Hive: ${HIVE_SUBMITTED} submitted, ${HIVE_SUCCEEDED} ok, ${HIVE_FAILED} fail"
}

##############################################################################
# Phase 6: Drain
##############################################################################
phase6_drain() {
    log "========== Phase 6: Drain (${DRAIN_SECONDS}s) =========="
    log "Waiting for pipeline to flush..."

    local start_time
    start_time=$(date +%s)
    while true; do
        local now
        now=$(date +%s)
        local elapsed=$(( now - start_time ))
        if [[ "$elapsed" -ge "$DRAIN_SECONDS" ]]; then
            break
        fi
        echo -n "."
        sleep 10
    done
    echo ""
    ok "Drain complete"
}

##############################################################################
# Phase 7: Verify and Report
##############################################################################
phase7_verify_report() {
    log "========== Phase 7: Verify & Report =========="

    local pass=true

    # --- Stop Flink consumer and MR collector (unless KEEP_SERVICES) ---
    local flink_errors=0 mr_errors=0
    if [[ "$KEEP_SERVICES" != "true" ]]; then
        log "Stopping Flink consumer..."
        kubectl exec "$SPARK3_POD" -- bash -c 'kill $(cat /tmp/flink-consumer.pid) 2>/dev/null || true'
        sleep 2
        local flink_log_lines
        flink_log_lines=$(kubectl exec "$SPARK3_POD" -- wc -l < /tmp/flink-consumer.log 2>/dev/null || echo "0")
        flink_errors=$(kubectl exec "$SPARK3_POD" -- grep -ci "error\|exception" /tmp/flink-consumer.log 2>/dev/null || echo "0")

        log "Stopping MR collector..."
        kubectl exec "$HADOOP3_POD" -- bash -c 'kill $(cat /tmp/mr-collector.pid) 2>/dev/null || true'
        sleep 1
        mr_errors=$(kubectl exec "$HADOOP3_POD" -- grep -ci "error\|exception" /tmp/mr-collector.log 2>/dev/null || echo "0")
    else
        log "KEEP_SERVICES=true — leaving Flink consumer and MR collector running"
        flink_errors=$(kubectl exec "$SPARK3_POD" -- grep -ci "error\|exception" /tmp/flink-consumer.log 2>/dev/null || echo "0")
        mr_errors=$(kubectl exec "$HADOOP3_POD" -- grep -ci "error\|exception" /tmp/mr-collector.log 2>/dev/null || echo "0")
    fi

    # --- Data verification ---
    log "Checking data in $SINK_TYPE..."

    local task_rows=0 stage_rows=0 job_rows=0 gov_rows=0 mem_rows=0 gc_rows=0 bucket_rows=0
    local sql_query_rows=0 sql_table_rows=0
    local hive_query_rows=0 hive_table_rows=0
    local tables_found=""

    if [[ "$SINK_TYPE" == "clickhouse" ]]; then
        task_rows=$(kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
            "SELECT count() FROM task_metrics" 2>/dev/null || echo "0")
        stage_rows=$(kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
            "SELECT count() FROM stage_metrics" 2>/dev/null || echo "0")
        job_rows=$(kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
            "SELECT count() FROM job_metrics" 2>/dev/null || echo "0")
        gov_rows=$(kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
            "SELECT count() FROM stage_governance" 2>/dev/null || echo "0")
        mem_rows=$(kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
            "SELECT count() FROM jvm_memory_metrics" 2>/dev/null || echo "0")
        gc_rows=$(kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
            "SELECT count() FROM jvm_gc_metrics" 2>/dev/null || echo "0")
        bucket_rows=$(kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
            "SELECT count() FROM task_histogram_buckets" 2>/dev/null || echo "0")
        sql_query_rows=$(kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
            "SELECT count() FROM sql_query_metrics" 2>/dev/null || echo "0")
        sql_table_rows=$(kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
            "SELECT count() FROM sql_query_table_metrics" 2>/dev/null || echo "0")
        hive_query_rows=$(kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
            "SELECT count() FROM hive_query_metrics" 2>/dev/null || echo "0")
        hive_table_rows=$(kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
            "SELECT count() FROM hive_table_io_metrics" 2>/dev/null || echo "0")
        tables_found=$(kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
            "SHOW TABLES" 2>/dev/null || echo "")
    else
        task_rows=$(kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -N -e \
            "SELECT count(*) FROM task_metrics" 2>/dev/null || echo "0")
        stage_rows=$(kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -N -e \
            "SELECT count(*) FROM stage_metrics" 2>/dev/null || echo "0")
        job_rows=$(kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -N -e \
            "SELECT count(*) FROM job_metrics" 2>/dev/null || echo "0")
        gov_rows=$(kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -N -e \
            "SELECT count(*) FROM stage_governance" 2>/dev/null || echo "0")
        mem_rows=$(kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -N -e \
            "SELECT count(*) FROM jvm_memory_metrics" 2>/dev/null || echo "0")
        gc_rows=$(kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -N -e \
            "SELECT count(*) FROM jvm_gc_metrics" 2>/dev/null || echo "0")
        bucket_rows=$(kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -N -e \
            "SELECT count(*) FROM task_histogram_buckets" 2>/dev/null || echo "0")
        sql_query_rows=$(kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -N -e \
            "SELECT count(*) FROM sql_query_metrics" 2>/dev/null || echo "0")
        sql_table_rows=$(kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -N -e \
            "SELECT count(*) FROM sql_query_table_metrics" 2>/dev/null || echo "0")
        hive_query_rows=$(kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -N -e \
            "SELECT count(*) FROM hive_query_metrics" 2>/dev/null || echo "0")
        hive_table_rows=$(kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -N -e \
            "SELECT count(*) FROM hive_table_io_metrics" 2>/dev/null || echo "0")
        tables_found=$(kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -N -e \
            "SHOW TABLES" 2>/dev/null || echo "")
    fi

    log "Tables: $tables_found"
    log "task_metrics: $task_rows | stage_metrics: $stage_rows | job_metrics: $job_rows"
    log "stage_governance: $gov_rows | jvm_memory: $mem_rows | jvm_gc: $gc_rows"
    log "histogram_buckets: $bucket_rows"
    log "sql_query_metrics: $sql_query_rows | sql_query_table_metrics: $sql_table_rows"
    log "hive_query_metrics: $hive_query_rows | hive_table_io_metrics: $hive_table_rows"

    # Sample data from task_metrics
    log "Sample task_metrics data:"
    if [[ "$SINK_TYPE" == "clickhouse" ]]; then
        kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
            "SELECT app_id, stage_id, task_id, duration_ms, io_bytes_read, executor_run_time_ms FROM task_metrics LIMIT 3" 2>/dev/null || true
    else
        kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -e \
            "SELECT app_id, stage_id, task_id, duration_ms, io_bytes_read, executor_run_time_ms FROM task_metrics LIMIT 3;" 2>/dev/null || true
    fi

    # Sample data from stage_governance
    if [[ "$gov_rows" -gt 0 ]]; then
        log "Sample stage_governance data:"
        if [[ "$SINK_TYPE" == "clickhouse" ]]; then
            kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
                "SELECT app_id, stage_id, task_count, avg_task_duration_ms, duration_skew_ratio, cpu_efficiency, gc_overhead_ratio FROM stage_governance LIMIT 3" 2>/dev/null || true
        else
            kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -e \
                "SELECT app_id, stage_id, task_count, avg_task_duration_ms, duration_skew_ratio, cpu_efficiency, gc_overhead_ratio FROM stage_governance LIMIT 3;" 2>/dev/null || true
        fi
    fi

    # Sample data from SQL tables
    if [[ "$sql_query_rows" -gt 0 ]]; then
        log "Sample sql_query_metrics data:"
        if [[ "$SINK_TYPE" == "clickhouse" ]]; then
            kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
                "SELECT app_id, execution_id, duration_ms, join_count FROM sql_query_metrics LIMIT 3" 2>/dev/null || true
        else
            kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -e \
                "SELECT app_id, execution_id, duration_ms, join_count FROM sql_query_metrics LIMIT 3;" 2>/dev/null || true
        fi
    fi
    if [[ "$sql_table_rows" -gt 0 ]]; then
        log "Sample sql_query_table_metrics data:"
        if [[ "$SINK_TYPE" == "clickhouse" ]]; then
            kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
                "SELECT app_id, execution_id, table_name, operation, bytes, \`rows\` FROM sql_query_table_metrics LIMIT 3" 2>/dev/null || true
        else
            kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -e \
                "SELECT app_id, execution_id, table_name, operation, bytes, \`rows\` FROM sql_query_table_metrics LIMIT 3;" 2>/dev/null || true
        fi
    fi

    # --- Data volume checks ---
    if [[ "$task_rows" -eq 0 ]]; then
        fail "No task_metrics data found!"
        pass=false
    fi
    if [[ "$stage_rows" -eq 0 ]]; then
        warn "No stage_metrics data found"
    fi
    if [[ "$gov_rows" -eq 0 ]]; then
        warn "No stage_governance data found"
    fi
    if [[ "$sql_query_rows" -eq 0 ]]; then
        warn "No sql_query_metrics data found (SQL jobs may not have completed yet)"
    fi

    # Sample data from Hive tables
    if [[ "$hive_query_rows" -gt 0 ]]; then
        log "Sample hive_query_metrics data:"
        if [[ "$SINK_TYPE" == "clickhouse" ]]; then
            kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
                "SELECT query_id, operation, user_name, success, duration_ms FROM hive_query_metrics LIMIT 3" 2>/dev/null || true
        else
            kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -e \
                "SELECT query_id, operation, user_name, success, duration_ms FROM hive_query_metrics LIMIT 3;" 2>/dev/null || true
        fi
    fi
    if [[ "$hive_table_rows" -gt 0 ]]; then
        log "Sample hive_table_io_metrics data:"
        if [[ "$SINK_TYPE" == "clickhouse" ]]; then
            kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
                "SELECT query_id, table_name, table_type, operation FROM hive_table_io_metrics LIMIT 3" 2>/dev/null || true
        else
            kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -e \
                "SELECT query_id, table_name, table_type, operation FROM hive_table_io_metrics LIMIT 3;" 2>/dev/null || true
        fi
    fi

    # --- Data volume checks for Hive ---
    if [[ "$HIVE_AVAILABLE" == "true" ]]; then
        if [[ "$hive_query_rows" -eq 0 ]]; then
            warn "No hive_query_metrics data found (Hive queries may not have completed yet)"
        fi
    fi

    # --- Data freshness check ---
    local latest_ts=""
    if [[ "$SINK_TYPE" == "clickhouse" ]]; then
        latest_ts=$(kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client -q \
            "SELECT max(timestamp_ms) FROM task_metrics" 2>/dev/null || echo "")
    else
        latest_ts=$(kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -N -e \
            "SELECT FROM_UNIXTIME(MAX(timestamp_ms)/1000) FROM task_metrics" 2>/dev/null || echo "")
    fi
    log "Latest data timestamp: $latest_ts"

    # --- Generate report ---
    local total_rows=$((task_rows + stage_rows + job_rows + gov_rows + mem_rows + gc_rows + bucket_rows + sql_query_rows + sql_table_rows + hive_query_rows + hive_table_rows))
    echo ""
    echo "============================================"
    echo "  Benchmark Report"
    echo "============================================"
    echo "Duration:           ${BENCHMARK_DURATION}s (+ ${DRAIN_SECONDS}s drain)"
    echo "Sink type:          $SINK_TYPE"
    echo ""
    echo "Spark jobs:         $SPARK_SUBMITTED submitted, $SPARK_SUCCEEDED succeeded, $SPARK_FAILED failed"
    echo "MR jobs:            $MR_SUBMITTED submitted, $MR_SUCCEEDED succeeded, $MR_FAILED failed"
    echo "Hive jobs:          $HIVE_SUBMITTED submitted, $HIVE_SUCCEEDED succeeded, $HIVE_FAILED failed"
    echo ""
    echo "Pipeline (13 tables, total $total_rows rows):"
    echo "  task_metrics:     $task_rows rows"
    echo "  stage_metrics:    $stage_rows rows"
    echo "  job_metrics:      $job_rows rows"
    echo "  stage_governance: $gov_rows rows"
    echo "  jvm_memory:       $mem_rows rows"
    echo "  jvm_gc:           $gc_rows rows"
    echo "  hist_buckets:     $bucket_rows rows"
    echo "  sql_query:        $sql_query_rows rows"
    echo "  sql_table_io:     $sql_table_rows rows"
    echo "  hive_query:       $hive_query_rows rows"
    echo "  hive_table_io:    $hive_table_rows rows"
    echo "  Latest data:      $latest_ts"
    echo ""
    echo "Errors:"
    echo "  Flink consumer:   $flink_errors error lines in log"
    echo "  MR collector:     $mr_errors error lines in log"
    echo "  Health checks:    $HEALTH_ERRORS failures"
    echo ""

    if $pass; then
        echo "Result:             ${GREEN}PASS${NC}"
    else
        echo "Result:             ${RED}FAIL${NC}"
    fi
    echo "============================================"

    # Save report to file
    local report_file="/tmp/benchmark-report-$(date +%Y%m%d-%H%M%S).txt"
    {
        echo "Benchmark Report - $(date)"
        echo "Duration: ${BENCHMARK_DURATION}s, Sink: $SINK_TYPE"
        echo "Spark: $SPARK_SUBMITTED/$SPARK_SUCCEEDED/$SPARK_FAILED"
        echo "MR: $MR_SUBMITTED/$MR_SUCCEEDED/$MR_FAILED"
        echo "Hive: $HIVE_SUBMITTED/$HIVE_SUCCEEDED/$HIVE_FAILED"
        echo "task_metrics: $task_rows, stage_metrics: $stage_rows, job_metrics: $job_rows"
        echo "stage_governance: $gov_rows, jvm_memory: $mem_rows, jvm_gc: $gc_rows, hist_buckets: $bucket_rows"
        echo "sql_query_metrics: $sql_query_rows, sql_query_table_metrics: $sql_table_rows"
        echo "hive_query_metrics: $hive_query_rows, hive_table_io_metrics: $hive_table_rows"
        echo "Flink errors: $flink_errors, MR errors: $mr_errors, Health failures: $HEALTH_ERRORS"
    } > "$report_file"
    log "Report saved to $report_file"

    if ! $pass; then
        return 1
    fi
}

##############################################################################
# Cleanup on exit
##############################################################################
cleanup() {
    log "Cleaning up..."
    if [[ "$KEEP_SERVICES" != "true" ]]; then
        kubectl exec "$SPARK3_POD" -- bash -c 'pkill -f flink-consumer.jar 2>/dev/null; exit 0' || true
        kubectl exec "$HADOOP3_POD" -- bash -c 'pkill -f mr-collector.jar 2>/dev/null; exit 0' || true
    else
        log "KEEP_SERVICES=true — skipping service cleanup"
    fi
}
trap cleanup EXIT

##############################################################################
# Main
##############################################################################
cd /home/xmg333/spark-telemetry-listener

log "Starting benchmark (duration=${BENCHMARK_DURATION}s, sink=${SINK_TYPE})"
echo ""

phase1_preflight
echo ""
phase2_environment
echo ""
phase3_start_flink
echo ""
phase4_start_mr_collector
echo ""
phase5_main_loop
echo ""
phase6_drain
echo ""
phase7_verify_report

log "Benchmark complete."
