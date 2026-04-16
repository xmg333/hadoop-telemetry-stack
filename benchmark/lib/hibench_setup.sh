#!/bin/bash
# HiBench build, configuration, and data preparation functions

source "$(lib_dir)/helpers.sh"

# ============================================================
# Build HiBench for a specific Spark/Scala version
# Usage: build_hibench "3.0" "2.12"
# ============================================================
build_hibench() {
  local spark_ver="$1"
  local scala_ver="$2"

  local profile="${HIBENCH_SPARK_PROFILES[$scala_ver]}"
  if [ -z "$profile" ]; then
    log_error "Unknown Scala version: $scala_ver (no HiBench Maven profile)"
    return 1
  fi

  log_info "Building HiBench: -Dspark=$spark_ver -Dscala=$scala_ver -P$profile"
  ssh_run "cd $HIBENCH_HOME && mvn -Dspark=$spark_ver -Dscala=$scala_ver -Phadoopbench -Psparkbench clean package -DskipTests" 2>&1
  local rc=$?

  if [ $rc -ne 0 ]; then
    log_error "HiBench build FAILED for spark=$spark_ver scala=$scala_ver"
    return 1
  fi
  log_info "HiBench build succeeded for spark=$spark_ver scala=$scala_ver"
  return 0
}

# ============================================================
# Configure HiBench for a specific version combo
# Usage: configure_hibench hadoop_home spark_home
# ============================================================
configure_hibench() {
  local hadoop_home="$1"
  local spark_home="$2"

  log_info "Configuring HiBench: HADOOP=$hadoop_home SPARK=$spark_home"

  # Generate hadoop.conf
  ssh_run "cat > $HIBENCH_HOME/conf/hadoop.conf << 'CONF'
hibench.hadoop.home     $hadoop_home
hibench.hadoop.executable     $hadoop_home/bin/hadoop
hibench.hadoop.configure.dir  $hadoop_home/etc/hadoop
hibench.hdfs.master       hdfs://localhost:8020
hibench.hadoop.release    apache
CONF"

  # Generate spark.conf
  ssh_run "cat > $HIBENCH_HOME/conf/spark.conf << 'CONF'
hibench.spark.home      $spark_home
hibench.spark.master    $SPARK_MASTER
hibench.yarn.executor.num     $YARN_NUM_EXECUTORS
hibench.yarn.executor.cores   2
spark.executor.memory  2g
spark.driver.memory    2g
spark.default.parallelism     8
spark.sql.shuffle.partitions  8
CONF"

  # Set scale profile in hibench.conf
  ssh_run "cd $HIBENCH_HOME && sed -i 's/hibench.scale.profile.*/hibench.scale.profile                $HIBENCH_SCALE/' conf/hibench.conf"

  log_info "HiBench configuration updated"
}

# ============================================================
# Prepare data for a specific workload
# Usage: prepare_data "micro/wordcount" "spark"
# ============================================================
prepare_data() {
  local workload="$1"
  local framework="${2:-spark}"  # spark or hadoop

  log_info "Preparing data for $workload ($framework)"

  local prepare_script="$HIBENCH_HOME/bin/workloads/${workload}/prepare/prepare.sh"
  if ! ssh_run "test -f $prepare_script" >/dev/null 2>&1; then
    log_warn "Prepare script not found: $prepare_script"
    return 1
  fi

  ssh_run "cd $HIBENCH_HOME && $prepare_script" 2>&1
  local rc=$?

  if [ $rc -ne 0 ]; then
    log_error "Data preparation FAILED for $workload"
    return 1
  fi
  log_info "Data preparation succeeded for $workload"
  return 0
}

# ============================================================
# Prepare all data for a given Hadoop version
# Usage: prepare_all_data "workloads.lst" "spark"
# ============================================================
prepare_all_data() {
  local workload_list="$1"
  local framework="${2:-spark}"
  local failures=0

  while IFS= read -r workload || [ -n "$workload" ]; do
    # Skip empty lines and comments
    [[ -z "$workload" || "$workload" =~ ^# ]] && continue
    if ! prepare_data "$workload" "$framework"; then
      ((failures++))
    fi
  done < "$workload_list"

  if [ $failures -gt 0 ]; then
    log_warn "$failures workload(s) failed data preparation"
  fi
  return $failures
}

# ============================================================
# Hadoop cluster management
# ============================================================
start_hadoop() {
  local hadoop_home="$1"
  local hadoop_ver="${hadoop_home##*hadoop-}"  # extract version like 2.7.0, 3.2.0

  log_info "Starting Hadoop $hadoop_ver from $hadoop_home"

  if [[ "$hadoop_ver" == 2.* ]]; then
    # Hadoop 2.x uses legacy daemon scripts
    ssh_run "$hadoop_home/sbin/hadoop-daemon.sh start namenode"
    ssh_run "$hadoop_home/sbin/hadoop-daemon.sh start datanode"
    ssh_run "$hadoop_home/sbin/yarn-daemon.sh start resourcemanager"
    ssh_run "$hadoop_home/sbin/yarn-daemon.sh start nodemanager"
    ssh_run "$hadoop_home/sbin/mr-jobhistory-daemon.sh start historyserver"
  else
    # Hadoop 3.x uses --daemon flag
    ssh_run "$hadoop_home/sbin/start-dfs.sh"
    ssh_run "$hadoop_home/sbin/start-yarn.sh"
    ssh_run "$hadoop_home/bin/mapred --daemon start historyserver"
  fi

  # Wait for safe mode
  log_info "Waiting for HDFS to leave safe mode..."
  ssh_run "$hadoop_home/bin/hdfs dfsadmin -safemode wait" 2>/dev/null || true
  sleep 5
  log_info "Hadoop $hadoop_ver started"
}

stop_hadoop() {
  local hadoop_home="$1"
  local hadoop_ver="${hadoop_home##*hadoop-}"

  log_info "Stopping Hadoop $hadoop_ver from $hadoop_home"

  if [[ "$hadoop_ver" == 2.* ]]; then
    ssh_run "$hadoop_home/sbin/mr-jobhistory-daemon.sh stop historyserver" 2>/dev/null || true
    ssh_run "$hadoop_home/sbin/yarn-daemon.sh stop nodemanager" 2>/dev/null || true
    ssh_run "$hadoop_home/sbin/yarn-daemon.sh stop resourcemanager" 2>/dev/null || true
    ssh_run "$hadoop_home/sbin/hadoop-daemon.sh stop datanode" 2>/dev/null || true
    ssh_run "$hadoop_home/sbin/hadoop-daemon.sh stop namenode" 2>/dev/null || true
  else
    ssh_run "$hadoop_home/bin/mapred --daemon stop historyserver" 2>/dev/null || true
    ssh_run "$hadoop_home/sbin/stop-yarn.sh" 2>/dev/null || true
    ssh_run "$hadoop_home/sbin/stop-dfs.sh" 2>/dev/null || true
  fi

  sleep 3
  log_info "Hadoop $hadoop_ver stopped"
}

# Check if Hadoop is running
check_hadoop_running() {
  local hadoop_home="$1"
  ssh_run "$hadoop_home/bin/hdfs dfsadmin -report" >/dev/null 2>&1
}
