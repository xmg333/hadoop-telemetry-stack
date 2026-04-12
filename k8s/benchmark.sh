#!/usr/bin/env bash
#
# benchmark.sh — End-to-end stress test for the Spark/MR telemetry pipeline
#
# Spark/MR → OTel SDK → OTel Collector → Kafka → Flink Consumer → MySQL/ClickHouse
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
    spark_jar=$(ls /home/xmg333/spark-telemetry-listener/spark-telemetry-dist-spark3/target/spark-telemetry-dist-spark3-*-SNAPSHOT.jar 2>/dev/null || true)
    if [[ -z "$spark_jar" ]]; then
        fail "Spark plugin JAR not found. Run: mvn clean package -Pspark-3 -DskipTests"
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

    ok "All JARs found"
}

##############################################################################
# Phase 2: Environment Preparation
##############################################################################
phase2_environment() {
    log "========== Phase 2: Environment Preparation =========="

    # Deploy Spark plugin JAR
    log "Deploying Spark plugin JAR to spark3..."
    kubectl cp spark-telemetry-dist-spark3/target/spark-telemetry-dist-spark3-*-SNAPSHOT.jar \
        "$SPARK3_POD":/opt/spark-telemetry-plugin.jar

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
    metric.name.include = ["spark.", "mr."]
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
    metric.name.include = ["spark.", "mr."]
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
            "DROP TABLE IF EXISTS task_metrics; DROP TABLE IF EXISTS stage_metrics; DROP TABLE IF EXISTS job_metrics; DROP TABLE IF EXISTS jvm_memory_metrics; DROP TABLE IF EXISTS jvm_gc_metrics; DROP TABLE IF EXISTS task_histogram_buckets; DROP TABLE IF EXISTS stage_histogram_buckets; DROP TABLE IF EXISTS job_histogram_buckets; DROP TABLE IF EXISTS stage_governance;" 2>/dev/null || \
            warn "Could not clean ClickHouse tables (may not exist yet)"
    else
        kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -e \
            "DROP TABLE IF EXISTS task_metrics, stage_metrics, job_metrics, jvm_memory_metrics, jvm_gc_metrics, task_histogram_buckets, stage_histogram_buckets, job_histogram_buckets, stage_governance;" 2>/dev/null || \
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
        *)
            fail "Unknown Spark job type: $job_type"
            return
            ;;
    esac

    (
        kubectl exec "$SPARK3_POD" -- bash -c "
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export SPARK_HOME=/opt/spark
\$SPARK_HOME/bin/spark-submit --master 'local[2]' \
    --class ${spark_class} \
    --jars /opt/spark-telemetry-plugin.jar \
    --conf spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin \
    --conf spark.telemetry.otel.exporter.endpoint=http://${OTEL_IP}:4317 \
    --conf spark.telemetry.otel.service.name=${job_name} \
    --conf spark.telemetry.otel.export.interval.ms=5000 \
    \$SPARK_HOME/examples/jars/spark-examples_2.12-*.jar ${spark_args}
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
}

phase5_main_loop() {
    log "========== Phase 5: Main Benchmark Loop (${BENCHMARK_DURATION}s) =========="

    declare -A SPARK_PIDS
    declare -A MR_PIDS

    local start_time
    start_time=$(date +%s)
    local last_spark=0
    local last_mr=0
    local last_health=0
    local elapsed=0
    local iteration=0

    # Job type rotation
    local spark_types=("pi" "groupby" "wordcount" "kmeans")
    local mr_types=("wordcount" "randomtextwriter" "sort" "terasort")

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
        local remaining=$(( ${#SPARK_PIDS[@]} + ${#MR_PIDS[@]} ))
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
            break
        fi
        sleep 5
    done

    log "Main loop complete. Spark: ${SPARK_SUBMITTED} submitted, ${SPARK_SUCCEEDED} ok, ${SPARK_FAILED} fail | MR: ${MR_SUBMITTED} submitted, ${MR_SUCCEEDED} ok, ${MR_FAILED} fail"
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
        tables_found=$(kubectl exec "$MYSQL_POD" -- mysql -umetrics -pmetrics metrics_db -N -e \
            "SHOW TABLES" 2>/dev/null || echo "")
    fi

    log "Tables: $tables_found"
    log "task_metrics: $task_rows | stage_metrics: $stage_rows | job_metrics: $job_rows"
    log "stage_governance: $gov_rows | jvm_memory: $mem_rows | jvm_gc: $gc_rows"
    log "histogram_buckets: $bucket_rows"

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
    local total_rows=$((task_rows + stage_rows + job_rows + gov_rows + mem_rows + gc_rows + bucket_rows))
    echo ""
    echo "============================================"
    echo "  Benchmark Report"
    echo "============================================"
    echo "Duration:           ${BENCHMARK_DURATION}s (+ ${DRAIN_SECONDS}s drain)"
    echo "Sink type:          $SINK_TYPE"
    echo ""
    echo "Spark jobs:         $SPARK_SUBMITTED submitted, $SPARK_SUCCEEDED succeeded, $SPARK_FAILED failed"
    echo "MR jobs:            $MR_SUBMITTED submitted, $MR_SUCCEEDED succeeded, $MR_FAILED failed"
    echo ""
    echo "Pipeline (9 tables, total $total_rows rows):"
    echo "  task_metrics:     $task_rows rows"
    echo "  stage_metrics:    $stage_rows rows"
    echo "  job_metrics:      $job_rows rows"
    echo "  stage_governance: $gov_rows rows"
    echo "  jvm_memory:       $mem_rows rows"
    echo "  jvm_gc:           $gc_rows rows"
    echo "  hist_buckets:     $bucket_rows rows"
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
        echo "task_metrics: $task_rows, stage_metrics: $stage_rows, job_metrics: $job_rows"
        echo "stage_governance: $gov_rows, jvm_memory: $mem_rows, jvm_gc: $gc_rows, hist_buckets: $bucket_rows"
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
