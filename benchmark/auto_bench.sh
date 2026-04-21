#!/bin/bash
# Auto benchmark: Spark/MR/Hive telemetry plugin vs baseline
# Runs on 192.168.10.65 via SSH
set -uo pipefail

SSH="ssh -i $HOME/.ssh/id_rsa -o StrictHostKeyChecking=no"
REMOTE="root@192.168.10.65"
RUN_ID="bench-$(date +%Y%m%d-%H%M%S)"
RESULT_DIR="/root/benchmark-results/$RUN_ID"

# Remote environment
JAVA_HOME="/opt/jdk8u482-b08"
HADOOP32_HOME="/opt/hadoop-3.2.0"
HADOOP27_HOME="/opt/hadoop-2.7.0"
SPARK32_HOME="/opt/spark-3.2.0-bin-hadoop3.2"
SPARK24_HOME="/opt/spark-2.4.4-bin-hadoop2.7"
OMNI_JAR="/opt/spark-telemetry-omnipackage.jar"
MR_AGENT_JAR="/opt/mr-telemetry-agent.jar"
HIVE_HOOK_JAR="/opt/hive-telemetry-hook.jar"
HIBENCH_HOME="/root/hibench"
HDFS_PORT="9000"
OTEL_EP="http://127.0.0.1:4317"
DRAIN_WAIT=30
SCALE="small"

# SSH helper
run() { $SSH $REMOTE "export JAVA_HOME=$JAVA_HOME PATH=\$JAVA_HOME/bin:\$PATH && $*"; }

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
warn() { echo "[$(date '+%H:%M:%S')] WARN: $*" >&2; }
err()  { echo "[$(date '+%H:%M:%S')] ERROR: $*" >&2; }

# ============================================================
# Setup functions
# ============================================================
setup_hadoop_env() {
    echo "export JAVA_HOME=$JAVA_HOME PATH=\$JAVA_HOME/bin:\$PATH \
          HDFS_NAMENODE_USER=root HDFS_DATANODE_USER=root HDFS_SECONDARYNAMENODE_USER=root \
          YARN_RESOURCEMANAGER_USER=root YARN_NODEMANAGER_USER=root"
}

start_hadoop32() {
    log "Starting Hadoop 3.2..."
    run "$(setup_hadoop_env) && $HADOOP32_HOME/sbin/start-dfs.sh && $HADOOP32_HOME/sbin/start-yarn.sh \
         && $HADOOP32_HOME/bin/mapred --daemon start historyserver && $HADOOP32_HOME/bin/hdfs dfsadmin -safemode wait"
}

stop_hadoop32() {
    log "Stopping Hadoop 3.2..."
    run "$(setup_hadoop_env) && $HADOOP32_HOME/bin/mapred --daemon stop historyserver 2>/dev/null; \
         $HADOOP32_HOME/sbin/stop-yarn.sh 2>/dev/null; $HADOOP32_HOME/sbin/stop-dfs.sh 2>/dev/null; sleep 3"
}

start_hadoop27() {
    log "Starting Hadoop 2.7..."
    run "$(setup_hadoop_env) && $HADOOP27_HOME/sbin/hadoop-daemon.sh start namenode \
         && $HADOOP27_HOME/sbin/hadoop-daemon.sh start datanode \
         && $HADOOP27_HOME/sbin/yarn-daemon.sh start resourcemanager \
         && $HADOOP27_HOME/sbin/yarn-daemon.sh start nodemanager \
         && $HADOOP27_HOME/sbin/mr-jobhistory-daemon.sh start historyserver \
         && sleep 5 && $HADOOP27_HOME/bin/hdfs dfsadmin -safemode wait"
}

stop_hadoop27() {
    log "Stopping Hadoop 2.7..."
    run "$(setup_hadoop_env) && $HADOOP27_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver 2>/dev/null; \
         $HADOOP27_HOME/sbin/yarn-daemon.sh stop nodemanager 2>/dev/null; \
         $HADOOP27_HOME/sbin/yarn-daemon.sh stop resourcemanager 2>/dev/null; \
         $HADOOP27_HOME/sbin/hadoop-daemon.sh stop datanode 2>/dev/null; \
         $HADOOP27_HOME/sbin/hadoop-daemon.sh stop namenode 2>/dev/null; sleep 3"
}

configure_hibench() {
    local hadoop_home="$1" spark_home="$2"
    run "cat > $HIBENCH_HOME/conf/hadoop.conf << 'EOF'
hibench.hadoop.home     $hadoop_home
hibench.hadoop.executable     $hadoop_home/bin/hadoop
hibench.hadoop.configure.dir  $hadoop_home/etc/hadoop
hibench.hdfs.master       hdfs://localhost:$HDFS_PORT
hibench.hadoop.release    apache
EOF
cat > $HIBENCH_HOME/conf/spark.conf << 'EOF'
hibench.spark.home      $spark_home
hibench.spark.master    local[2]
hibench.yarn.executor.num     2
hibench.yarn.executor.cores   2
spark.executor.memory  2g
spark.driver.memory    2g
spark.default.parallelism     8
spark.sql.shuffle.partitions  8
EOF
sed -i 's/hibench.scale.profile.*/hibench.scale.profile                $SCALE/' $HIBENCH_HOME/conf/hibench.conf"
}

# Install spark-submit wrapper
install_wrapper() {
    local spark_home="$1"
    local plugin_key="$2"
    local plugin_class="$3"
    local service_name="$4"

    run "test -f $spark_home/bin/spark-submit.real || cp $spark_home/bin/spark-submit $spark_home/bin/spark-submit.real"
    run "cat > $spark_home/bin/spark-submit << 'WRAPPER'
#!/bin/bash
if [ \"\$BENCHMARK_TELEMETRY\" = \"1\" ]; then
  $spark_home/bin/spark-submit.real \\
    --jars $OMNI_JAR \\
    --conf $plugin_key=$plugin_class \\
    --conf spark.telemetry.otel.exporter.endpoint=$OTEL_EP \\
    --conf spark.telemetry.otel.service.name=$service_name \\
    --conf spark.telemetry.otel.export.interval.ms=5000 \\
    --conf spark.telemetry.metrics.task.execution=true \\
    --conf spark.telemetry.metrics.task.shuffle-extended=true \\
    --conf spark.telemetry.metrics.task.info=true \\
    --conf spark.telemetry.metrics.stage.detailed=true \\
    --conf spark.telemetry.metrics.job.lifecycle=true \\
    --conf spark.telemetry.metrics.sql.query-execution=true \\
    \"\$@\"
else
  $spark_home/bin/spark-submit.real \"\$@\"
fi
WRAPPER
chmod +x $spark_home/bin/spark-submit"
}

restore_spark() {
    local spark_home="$1"
    run "if [ -f $spark_home/bin/spark-submit.real ]; then mv $spark_home/bin/spark-submit.real $spark_home/bin/spark-submit; fi"
}

inject_mr_agent() {
    local hadoop_home="$1"
    local mapred_site="$hadoop_home/etc/hadoop/mapred-site.xml"
    run "cp $mapred_site ${mapred_site}.bak"
    run "sed -i 's|</configuration>|<property><name>mapreduce.map.java.opts</name><value>-javaagent:${MR_AGENT_JAR} -Dmr.telemetry.agent.otel.exporter.endpoint=${OTEL_EP} -Dmr.telemetry.agent.otel.service.name=mr-agent-bench</value></property><property><name>mapreduce.reduce.java.opts</name><value>-javaagent:${MR_AGENT_JAR} -Dmr.telemetry.agent.otel.exporter.endpoint=${OTEL_EP} -Dmr.telemetry.agent.otel.service.name=mr-agent-bench</value></property></configuration>|' $mapred_site"
}

restore_mr_agent() {
    local hadoop_home="$1"
    run "mapred_site=$hadoop_home/etc/hadoop/mapred-site.xml; [ -f \${mapred_site}.bak ] && mv \${mapred_site}.bak \$mapred_site"
}

inject_hive_hook() {
    local hive_home="$1"
    run "cp $HIVE_HOOK_JAR $hive_home/lib/ 2>/dev/null; cp $hive_home/conf/hive-site.xml $hive_home/conf/hive-site.xml.bak"
    run "sed -i 's|</configuration>|<property><name>hive.exec.post.hooks</name><value>x.mg.metrics.hivetelemetry.HiveTelemetryHook</value></property><property><name>hive.telemetry.otel.exporter.endpoint</name><value>${OTEL_EP}</value></property></configuration>|' $hive_home/conf/hive-site.xml"
}

restore_hive_hook() {
    local hive_home="$1"
    run "[ -f $hive_home/conf/hive-site.xml.bak ] && mv $hive_home/conf/hive-site.xml.bak $hive_home/conf/hive-site.xml"
}

# ============================================================
# Benchmark runner
# ============================================================
CSV_HEADER="tag,framework,workload,mode,duration_ms,exit_code,metrics_found"

record() { run "echo '$1' >> $RESULT_DIR/timing.csv"; }

run_bench() {
    local label="$1"
    shift
    local cmd="$*"
    local start_ms=$(date +%s%3N)
    log "$label: starting..."
    run "$cmd"
    local rc=$?
    local end_ms=$(date +%s%3N)
    local duration=$(( end_ms - start_ms ))
    log "$label: ${duration}ms (rc=$rc)"
    echo "$duration"
    return $rc
}

check_metrics() {
    local table="$1"
    local count
    count=$(run "docker exec mysql mysql -u root -proot123 telemetry -N -e \"SELECT COUNT(*) FROM $table WHERE timestamp_ms > (UNIX_TIMESTAMP(NOW()) - 120) * 1000\"" 2>/dev/null | tr -d '[:space:]')
    echo "${count:-0}"
}

# ============================================================
# Main
# ============================================================
log "=========================================="
log "  TELEMETRY BENCHMARK  Run: $RUN_ID"
log "=========================================="

# Create result dir
run "mkdir -p $RESULT_DIR"
run "echo '$CSV_HEADER' > $RESULT_DIR/timing.csv"

# Preflight
log "--- Preflight ---"
run "echo 'SSH OK'" || { err "SSH failed"; exit 1; }
run "test -d $JAVA_HOME" || { err "Java missing"; exit 1; }
run "test -d $HADOOP32_HOME" || warn "Hadoop 3.2 missing"
run "test -d $HADOOP27_HOME" || warn "Hadoop 2.7 missing"
run "test -d $SPARK32_HOME" || warn "Spark 3.2 missing"
run "test -d $SPARK24_HOME" || warn "Spark 2.4 missing"
run "test -f $OMNI_JAR" || warn "Omnipackage JAR missing"
run "docker exec mysql mysql -u root -proot123 telemetry -e 'SELECT 1'" >/dev/null 2>&1 || warn "MySQL unreachable"
run "pgrep -f flink-consumer" >/dev/null 2>&1 || warn "Flink consumer not running"

# Fix HiBench Python3 compat (idempotent)
run "sed -i '1s|python2|python3|' $HIBENCH_HOME/bin/functions/load_config.py 2>/dev/null; \
     sed -i 's|^import urllib$|import urllib.request|' $HIBENCH_HOME/bin/functions/load_config.py 2>/dev/null; \
     sed -i 's|urllib.urlopen|urllib.request.urlopen|' $HIBENCH_HOME/bin/functions/load_config.py 2>/dev/null"

# Spark workloads
SPARK_WORKLOADS="micro/wordcount micro/sort micro/terasort micro/repartition sql/aggregation sql/join sql/scan ml/kmeans ml/lr websearch/pagerank"
MR_WORKLOADS="micro/wordcount micro/sort micro/terasort"
HIVE_WORKLOADS="sql/aggregation sql/join sql/scan"

# ============================================================
# Phase 1: Hadoop 3.2 + Spark 3.2
# ============================================================
log "========== Phase 1: Hadoop 3.2 + Spark 3.2 =========="

start_hadoop32

# Configure HiBench
configure_hibench "$HADOOP32_HOME" "$SPARK32_HOME"

# Prepare all data
log "--- Preparing Spark/MR data on Hadoop 3.2 ---"
for wl in $SPARK_WORKLOADS $MR_WORKLOADS; do
    log "Preparing: $wl"
    run "$(setup_hadoop_env) && export HADOOP_HOME=$HADOOP32_HOME HADOOP_CONF_DIR=$HADOOP32_HOME/etc/hadoop SPARK_HOME=$SPARK32_HOME && cd $HIBENCH_HOME && bin/workloads/${wl}/prepare/prepare.sh" 2>&1 | tail -1 || warn "Prepare failed: $wl"
done

# --- Spark 3.2 benchmarks ---
log "--- Running Spark 3.2 benchmarks ---"
TAG="spark32"

for wl in $SPARK_WORKLOADS; do
    log ""
    log "--- $TAG / $wl ---"

    # Baseline
    dur=$(run_bench "$TAG-$wl-baseline" \
        "$(setup_hadoop_env) && export HADOOP_HOME=$HADOOP32_HOME HADOOP_CONF_DIR=$HADOOP32_HOME/etc/hadoop SPARK_HOME=$SPARK32_HOME && cd $HIBENCH_HOME && bin/workloads/${wl}/spark/run.sh") || true
    record "$TAG,spark,$wl,baseline,${dur},$?,"

    # Telemetry
    install_wrapper "$SPARK32_HOME" "spark.plugins" "x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin" "bench-$TAG-${wl//\//-}"
    dur=$(run_bench "$TAG-$wl-telemetry" \
        "$(setup_hadoop_env) && export HADOOP_HOME=$HADOOP32_HOME HADOOP_CONF_DIR=$HADOOP32_HOME/etc/hadoop SPARK_HOME=$SPARK32_HOME BENCHMARK_TELEMETRY=1 && cd $HIBENCH_HOME && bin/workloads/${wl}/spark/run.sh") || true
    trc=$?
    restore_spark "$SPARK32_HOME"
    sleep "$DRAIN_WAIT"
    metrics=$(check_metrics "task_metrics")
    [ "$metrics" -gt 0 ] 2>/dev/null && mf="YES" || mf="NO"
    record "$TAG,spark,$wl,telemetry,${dur},${trc},$mf"
    log "$TAG/$wl: metrics=$mf ($metrics rows)"
done

# --- MR benchmarks on Hadoop 3.2 ---
log "--- Running MR benchmarks on Hadoop 3.2 ---"
TAG="mr-hadoop32"

for wl in $MR_WORKLOADS; do
    log ""
    log "--- $TAG / $wl ---"

    dur=$(run_bench "$TAG-$wl-baseline" \
        "$(setup_hadoop_env) && export HADOOP_HOME=$HADOOP32_HOME HADOOP_CONF_DIR=$HADOOP32_HOME/etc/hadoop && cd $HIBENCH_HOME && bin/workloads/${wl}/hadoop/run.sh") || true
    record "$TAG,mr,$wl,baseline,${dur},$?,"

    inject_mr_agent "$HADOOP32_HOME"
    dur=$(run_bench "$TAG-$wl-telemetry" \
        "$(setup_hadoop_env) && export HADOOP_HOME=$HADOOP32_HOME HADOOP_CONF_DIR=$HADOOP32_HOME/etc/hadoop && cd $HIBENCH_HOME && bin/workloads/${wl}/hadoop/run.sh") || true
    trc=$?
    restore_mr_agent "$HADOOP32_HOME"
    sleep "$DRAIN_WAIT"
    metrics=$(check_metrics "mr_task_metrics")
    [ "$metrics" -gt 0 ] 2>/dev/null && mf="YES" || mf="NO"
    record "$TAG,mr,$wl,telemetry,${dur},${trc},$mf"
    log "$TAG/$wl: metrics=$mf ($metrics rows)"
done

# --- Hive benchmarks on Hadoop 3.2 ---
log "--- Running Hive benchmarks on Hadoop 3.2 ---"
TAG="hive313"

for wl in $HIVE_WORKLOADS; do
    log ""
    log "--- $TAG / $wl ---"

    dur=$(run_bench "$TAG-$wl-baseline" \
        "$(setup_hadoop_env) && export HADOOP_HOME=$HADOOP32_HOME HADOOP_CONF_DIR=$HADOOP32_HOME/etc/hadoop HIVE_HOME=/opt/apache-hive-3.1.3-bin && cd $HIBENCH_HOME && HIVE_RELEASE=apache-hive-3.1.3-bin bin/workloads/${wl}/hadoop/run.sh") || true
    record "$TAG,hive,$wl,baseline,${dur},$?,"

    inject_hive_hook "/opt/apache-hive-3.1.3-bin"
    dur=$(run_bench "$TAG-$wl-telemetry" \
        "$(setup_hadoop_env) && export HADOOP_HOME=$HADOOP32_HOME HADOOP_CONF_DIR=$HADOOP32_HOME/etc/hadoop HIVE_HOME=/opt/apache-hive-3.1.3-bin && cd $HIBENCH_HOME && HIVE_RELEASE=apache-hive-3.1.3-bin bin/workloads/${wl}/hadoop/run.sh") || true
    trc=$?
    restore_hive_hook "/opt/apache-hive-3.1.3-bin"
    sleep "$DRAIN_WAIT"
    metrics=$(check_metrics "hive_query_metrics")
    [ "$metrics" -gt 0 ] 2>/dev/null && mf="YES" || mf="NO"
    record "$TAG,hive,$wl,telemetry,${dur},${trc},$mf"
    log "$TAG/$wl: metrics=$mf ($metrics rows)"
done

stop_hadoop32

# ============================================================
# Phase 2: Hadoop 2.7 + Spark 2.4
# ============================================================
log "========== Phase 2: Hadoop 2.7 + Spark 2.4 =========="

# Rebuild HiBench for Spark 2.4
log "--- Rebuilding HiBench for Spark 2.4 ---"
(cd /home/xmg333/spark-telemetry-listener/hibench && mvn -Dspark=2.4 -Dscala=2.11 -Phadoopbench -Psparkbench clean package -DskipTests 2>&1 | tail -5)
rsync -az -e "$SSH" /home/xmg333/spark-telemetry-listener/hibench/ $REMOTE:$HIBENCH_HOME/ 2>/dev/null
# Re-fix Python3
run "sed -i '1s|python2|python3|' $HIBENCH_HOME/bin/functions/load_config.py"

start_hadoop27
configure_hibench "$HADOOP27_HOME" "$SPARK24_HOME"

# Prepare data
log "--- Preparing Spark/MR data on Hadoop 2.7 ---"
for wl in $SPARK_WORKLOADS $MR_WORKLOADS; do
    log "Preparing: $wl"
    run "$(setup_hadoop_env) && export HADOOP_HOME=$HADOOP27_HOME HADOOP_CONF_DIR=$HADOOP27_HOME/etc/hadoop SPARK_HOME=$SPARK24_HOME && cd $HIBENCH_HOME && bin/workloads/${wl}/prepare/prepare.sh" 2>&1 | tail -1 || warn "Prepare failed: $wl"
done

# --- Spark 2.4 benchmarks ---
log "--- Running Spark 2.4 benchmarks ---"
TAG="spark24"

for wl in $SPARK_WORKLOADS; do
    log ""
    log "--- $TAG / $wl ---"

    dur=$(run_bench "$TAG-$wl-baseline" \
        "$(setup_hadoop_env) && export HADOOP_HOME=$HADOOP27_HOME HADOOP_CONF_DIR=$HADOOP27_HOME/etc/hadoop SPARK_HOME=$SPARK24_HOME && cd $HIBENCH_HOME && bin/workloads/${wl}/spark/run.sh") || true
    record "$TAG,spark,$wl,baseline,${dur},$?,"

    install_wrapper "$SPARK24_HOME" "spark.extraListeners" "x.mg.metrics.sparktelemetry.adapter.SparkTelemetryListener" "bench-$TAG-${wl//\//-}"
    dur=$(run_bench "$TAG-$wl-telemetry" \
        "$(setup_hadoop_env) && export HADOOP_HOME=$HADOOP27_HOME HADOOP_CONF_DIR=$HADOOP27_HOME/etc/hadoop SPARK_HOME=$SPARK24_HOME BENCHMARK_TELEMETRY=1 && cd $HIBENCH_HOME && bin/workloads/${wl}/spark/run.sh") || true
    trc=$?
    restore_spark "$SPARK24_HOME"
    sleep "$DRAIN_WAIT"
    metrics=$(check_metrics "task_metrics")
    [ "$metrics" -gt 0 ] 2>/dev/null && mf="YES" || mf="NO"
    record "$TAG,spark,$wl,telemetry,${dur},${trc},$mf"
    log "$TAG/$wl: metrics=$mf ($metrics rows)"
done

# --- MR benchmarks on Hadoop 2.7 ---
log "--- Running MR benchmarks on Hadoop 2.7 ---"
TAG="mr-hadoop27"

for wl in $MR_WORKLOADS; do
    log ""
    log "--- $TAG / $wl ---"

    dur=$(run_bench "$TAG-$wl-baseline" \
        "$(setup_hadoop_env) && export HADOOP_HOME=$HADOOP27_HOME HADOOP_CONF_DIR=$HADOOP27_HOME/etc/hadoop && cd $HIBENCH_HOME && bin/workloads/${wl}/hadoop/run.sh") || true
    record "$TAG,mr,$wl,baseline,${dur},$?,"

    inject_mr_agent "$HADOOP27_HOME"
    dur=$(run_bench "$TAG-$wl-telemetry" \
        "$(setup_hadoop_env) && export HADOOP_HOME=$HADOOP27_HOME HADOOP_CONF_DIR=$HADOOP27_HOME/etc/hadoop && cd $HIBENCH_HOME && bin/workloads/${wl}/hadoop/run.sh") || true
    trc=$?
    restore_mr_agent "$HADOOP27_HOME"
    sleep "$DRAIN_WAIT"
    metrics=$(check_metrics "mr_task_metrics")
    [ "$metrics" -gt 0 ] 2>/dev/null && mf="YES" || mf="NO"
    record "$TAG,mr,$wl,telemetry,${dur},${trc},$mf"
    log "$TAG/$wl: metrics=$mf ($metrics rows)"
done

# --- Hive 2.3.9 on Hadoop 2.7 ---
log "--- Running Hive 2.3.9 benchmarks ---"
TAG="hive239"

for wl in $HIVE_WORKLOADS; do
    log ""
    log "--- $TAG / $wl ---"

    dur=$(run_bench "$TAG-$wl-baseline" \
        "$(setup_hadoop_env) && export HADOOP_HOME=$HADOOP27_HOME HADOOP_CONF_DIR=$HADOOP27_HOME/etc/hadoop HIVE_HOME=/opt/apache-hive-2.3.9-bin && cd $HIBENCH_HOME && HIVE_RELEASE=apache-hive-2.3.9-bin bin/workloads/${wl}/hadoop/run.sh") || true
    record "$TAG,hive,$wl,baseline,${dur},$?,"

    inject_hive_hook "/opt/apache-hive-2.3.9-bin"
    dur=$(run_bench "$TAG-$wl-telemetry" \
        "$(setup_hadoop_env) && export HADOOP_HOME=$HADOOP27_HOME HADOOP_CONF_DIR=$HADOOP27_HOME/etc/hadoop HIVE_HOME=/opt/apache-hive-2.3.9-bin && cd $HIBENCH_HOME && HIVE_RELEASE=apache-hive-2.3.9-bin bin/workloads/${wl}/hadoop/run.sh") || true
    trc=$?
    restore_hive_hook "/opt/apache-hive-2.3.9-bin"
    sleep "$DRAIN_WAIT"
    metrics=$(check_metrics "hive_query_metrics")
    [ "$metrics" -gt 0 ] 2>/dev/null && mf="YES" || mf="NO"
    record "$TAG,hive,$wl,telemetry,${dur},${trc},$mf"
    log "$TAG/$wl: metrics=$mf ($metrics rows)"
done

stop_hadoop27

# ============================================================
# Generate report
# ============================================================
log "========== Generating Report =========="

$SSH $REMOTE "cat $RESULT_DIR/timing.csv" > /tmp/bench-results-$RUN_ID.csv 2>/dev/null

cat /tmp/bench-results-$RUN_ID.csv

echo ""
echo "============================================================"
echo "  BENCHMARK COMPLETE - $RUN_ID"
echo "============================================================"
echo "Results: /tmp/bench-results-$RUN_ID.csv"
echo "Remote:  $RESULT_DIR/timing.csv"
