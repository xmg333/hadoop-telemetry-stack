#!/bin/bash
#
# Telemetry Omnipackage Installer
# Deploys spark-telemetry-dist-omni JAR to Spark, MapReduce, and Hive classpaths
# and generates ready-to-use configuration files.
#
# Usage:
#   ./deploy/install-omni.sh [OPTIONS]
#
# Options:
#   --spark-home=DIR        Spark installation directory (default: $SPARK_HOME)
#   --hadoop-home=DIR       Hadoop installation directory (default: $HADOOP_HOME)
#   --hive-home=DIR         Hive installation directory (default: $HIVE_HOME)
#   --otel-endpoint=URL     OTel Collector gRPC endpoint (default: http://localhost:4317)
#   --spark-service=NAME    Spark OTel service name (default: spark-application)
#   --hive-service=NAME     Hive OTel service name (default: hive-server2)
#   --mr-service=NAME       MR Collector OTel service name (default: mr-telemetry-collector)
#   --mr-history-url=URL    MR History Server URL (default: http://localhost:19888)
#   --config-dir=DIR        Generated config output directory (default: ./telemetry-configs)
#   --skip-spark            Skip Spark installation
#   --skip-mr               Skip MR Collector setup
#   --skip-hive             Skip Hive installation
#   --spark2                Configure for Spark 2.x (uses extraListeners)
#   --backup                Backup existing files before overwriting
#   --dry-run               Show what would be done without executing
#   -y, --yes               Skip confirmation prompt
#   -h, --help              Show this help message
#
set -euo pipefail

# ── Colors ────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }
step()  { echo -e "${BLUE}==>${NC} $*"; }

# ── Defaults ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

SPARK_HOME="${SPARK_HOME:-}"
HADOOP_HOME="${HADOOP_HOME:-}"
HIVE_HOME="${HIVE_HOME:-}"
OTEL_ENDPOINT="http://localhost:4317"
SPARK_SERVICE="spark-application"
HIVE_SERVICE="hive-server2"
MR_SERVICE="mr-telemetry-collector"
MR_HISTORY_URL="http://localhost:19888"
CONFIG_DIR="$PROJECT_ROOT/telemetry-configs"
SKIP_SPARK=false
SKIP_MR=false
SKIP_HIVE=false
SPARK2_MODE=false
DO_BACKUP=false
DRY_RUN=false
AUTO_YES=false

# ── Find Omnipackage JAR ─────────────────────────────────────────
find_omni_jar() {
  # Release package layout: lib/spark-telemetry-omni.jar
  local release_jar="$PROJECT_ROOT/lib/spark-telemetry-omni.jar"
  if [ -f "$release_jar" ]; then
    echo "$release_jar"
    return
  fi
  # Repo layout: spark/spark-telemetry-dist-omni/target/
  local jar_dir="$PROJECT_ROOT/spark/spark-telemetry-dist-omni/target"
  if [ ! -d "$jar_dir" ]; then
    error "Omnipackage not built. Run ./build-omni.sh first."
    exit 1
  fi
  local jar_file
  jar_file=$(ls "$jar_dir"/spark-telemetry-dist-omni-*.jar 2>/dev/null | head -1)
  if [ -z "$jar_file" ]; then
    error "No omnipackage JAR found in $jar_dir"
    error "Run ./build-omni.sh first."
    exit 1
  fi
  echo "$jar_file"
}

OMNI_JAR=""
JAR_NAME=""

# ── Parse Arguments ───────────────────────────────────────────────
parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --spark-home=*)    SPARK_HOME="${1#*=}" ;;
      --hadoop-home=*)   HADOOP_HOME="${1#*=}" ;;
      --hive-home=*)     HIVE_HOME="${1#*=}" ;;
      --otel-endpoint=*) OTEL_ENDPOINT="${1#*=}" ;;
      --spark-service=*) SPARK_SERVICE="${1#*=}" ;;
      --hive-service=*)  HIVE_SERVICE="${1#*=}" ;;
      --mr-service=*)    MR_SERVICE="${1#*=}" ;;
      --mr-history-url=*)MR_HISTORY_URL="${1#*=}" ;;
      --config-dir=*)    CONFIG_DIR="${1#*=}" ;;
      --skip-spark)      SKIP_SPARK=true ;;
      --skip-mr)         SKIP_MR=true ;;
      --skip-hive)       SKIP_HIVE=true ;;
      --spark2)          SPARK2_MODE=true ;;
      --backup)          DO_BACKUP=true ;;
      --dry-run)         DRY_RUN=true ;;
      -y|--yes)          AUTO_YES=true ;;
      -h|--help)
        head -30 "$0" | grep '^#' | sed 's/^# \?//'
        exit 0
        ;;
      *)
        error "Unknown option: $1"
        exit 1
        ;;
    esac
    shift
  done
}

# ── Helpers ───────────────────────────────────────────────────────
backup_file() {
  local file="$1"
  if [ "$DO_BACKUP" = true ] && [ -f "$file" ]; then
    local bak="${file}.bak.$(date +%Y%m%d%H%M%S)"
    run_cmd cp "$file" "$bak"
    info "Backed up $file -> $bak"
  fi
}

run_cmd() {
  if [ "$DRY_RUN" = true ]; then
    echo -e "  ${YELLOW}[DRY-RUN]${NC} $*"
  else
    "$@"
  fi
}

check_dir() {
  local name="$1"
  local dir="$2"
  if [ -z "$dir" ]; then
    warn "$name is not set. Skipping $name installation."
    return 1
  fi
  if [ ! -d "$dir" ]; then
    warn "$name directory does not exist: $dir. Skipping."
    return 1
  fi
  return 0
}

# ── Print Banner ──────────────────────────────────────────────────
print_banner() {
  echo ""
  echo "============================================================"
  echo "  Telemetry Omnipackage Installer"
  echo "============================================================"
  echo ""
  echo "  JAR: $OMNI_JAR"
  echo "  OTel Endpoint: $OTEL_ENDPOINT"
  echo ""
  if [ "$SKIP_SPARK" = false ]; then
    echo "  [Spark]   ${SPARK_HOME:-<not set>}"
  else
    echo "  [Spark]   SKIP"
  fi
  if [ "$SKIP_HIVE" = false ]; then
    echo "  [Hive]    ${HIVE_HOME:-<not set>}"
  else
    echo "  [Hive]    SKIP"
  fi
  if [ "$SKIP_MR" = false ]; then
    echo "  [MR]      ${HADOOP_HOME:-<not set>}"
  else
    echo "  [MR]      SKIP"
  fi
  echo ""
  echo "  Config output: $CONFIG_DIR"
  echo "  Spark2 mode:   $SPARK2_MODE"
  echo "  Dry run:       $DRY_RUN"
  echo "============================================================"
  echo ""
}

confirm() {
  if [ "$AUTO_YES" = true ]; then
    return 0
  fi
  echo -n "Proceed with installation? [y/N] "
  read -r answer
  case "$answer" in
    [yY][eE][sS]|[yY]) return 0 ;;
    *) echo "Aborted."; exit 0 ;;
  esac
}

# ── Install to Spark ──────────────────────────────────────────────
install_spark() {
  if [ "$SKIP_SPARK" = true ]; then
    info "Skipping Spark installation (--skip-spark)"
    return
  fi

  check_dir "SPARK_HOME" "$SPARK_HOME" || return

  step "Installing telemetry plugin to Spark: $SPARK_HOME"

  # Copy JAR to Spark jars directory
  local jars_dir="$SPARK_HOME/jars"
  if [ ! -d "$jars_dir" ]; then
    warn "Spark jars directory not found: $jars_dir"
    warn "Attempting to create it..."
    run_cmd mkdir -p "$jars_dir"
  fi

  local dest="$jars_dir/spark-telemetry-omni.jar"

  # Remove old versions (matching any spark-telemetry-omni*.jar pattern)
  local old_jars
  old_jars=$(find "$jars_dir" -name 'spark-telemetry-omni*.jar' 2>/dev/null || true)
  if [ -n "$old_jars" ]; then
    for old in $old_jars; do
      if [ "$(readlink -f "$old" 2>/dev/null || echo "$old")" != "$(readlink -f "$OMNI_JAR" 2>/dev/null || echo "$OMNI_JAR")" ]; then
        backup_file "$old"
        run_cmd rm -f "$old"
        info "Removed old JAR: $(basename "$old")"
      fi
    done
  fi

  run_cmd cp "$OMNI_JAR" "$dest"
  info "Copied $JAR_NAME -> $dest"

  # Generate spark-defaults.conf snippet
  local defaults_conf="$SPARK_HOME/conf/spark-defaults.conf"
  local snippet_file="$CONFIG_DIR/spark-telemetry.conf.snippet"

  cat > "$snippet_file" <<SNIPPET
# ── Spark Telemetry Plugin ──────────────────────────────────
# Add these lines to $defaults_conf
SNIPPET

  if [ "$SPARK2_MODE" = true ]; then
    cat >> "$snippet_file" <<'EOF'
spark.extraListeners            x.mg.metrics.sparktelemetry.adapter.SparkTelemetryListener
EOF
  else
    cat >> "$snippet_file" <<'EOF'
spark.plugins                   x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin
EOF
  fi

  cat >> "$snippet_file" <<EOF
spark.telemetry.otel.exporter.endpoint  $OTEL_ENDPOINT
spark.telemetry.otel.service.name       $SPARK_SERVICE
spark.telemetry.otel.export.interval.ms 10000
spark.telemetry.config.path             $CONFIG_DIR/spark-telemetry.conf
EOF

  info "Generated Spark config snippet: $snippet_file"
  echo ""
  echo "  Add the following to $defaults_conf:"
  echo ""
  cat "$snippet_file" | sed 's/^/    /'
  echo ""
}

# ── Install to Hive ───────────────────────────────────────────────
install_hive() {
  if [ "$SKIP_HIVE" = true ]; then
    info "Skipping Hive installation (--skip-hive)"
    return
  fi

  check_dir "HIVE_HOME" "$HIVE_HOME" || return

  step "Installing telemetry hook to Hive: $HIVE_HOME"

  # Copy JAR to Hive lib
  local lib_dir="$HIVE_HOME/lib"

  local dest="$lib_dir/spark-telemetry-omni.jar"

  # Remove old versions (matching any spark-telemetry-omni*.jar pattern)
  local old_jars
  old_jars=$(find "$lib_dir" -name 'spark-telemetry-omni*.jar' 2>/dev/null || true)
  if [ -n "$old_jars" ]; then
    for old in $old_jars; do
      if [ "$(readlink -f "$old" 2>/dev/null || echo "$old")" != "$(readlink -f "$OMNI_JAR" 2>/dev/null || echo "$OMNI_JAR")" ]; then
        backup_file "$old"
        run_cmd rm -f "$old"
        info "Removed old JAR: $(basename "$old")"
      fi
    done
  fi

  run_cmd cp "$OMNI_JAR" "$dest"
  info "Copied $JAR_NAME -> $dest"

  # Generate hive-telemetry.conf
  local hive_conf_file="$CONFIG_DIR/hive-telemetry.conf"
  cat > "$hive_conf_file" <<EOF
# Hive Telemetry Hook Configuration
# Generated by install-omni.sh on $(date)
hive-telemetry {
  otel {
    exporter.endpoint = "$OTEL_ENDPOINT"
    exporter.protocol = "grpc"
    service.name = "$HIVE_SERVICE"
    export.interval.ms = 10000
  }
  metrics {
    enabled = true
    query.duration = true
    query.io = true
    query.tables = true
  }
  filter {
    user.include = [".*"]
    user.exclude = []
    operation.include = [".*"]
    operation.exclude = []
  }
}
EOF
  info "Generated Hive config: $hive_conf_file"

  # Generate hive-site.xml snippet
  local snippet_file="$CONFIG_DIR/hive-telemetry-site.xml.snippet"
  cat > "$snippet_file" <<'EOF'
<!-- ── Hive Telemetry Hook ──────────────────────────────── -->
<!-- Add these properties to hive-site.xml                    -->
<property>
  <name>hive.exec.post.hooks</name>
  <value>x.mg.metrics.hivetelemetry.HiveTelemetryHook</value>
</property>
<property>
  <name>hive.telemetry.otel.exporter.endpoint</name>
EOF
  echo "  <value>$OTEL_ENDPOINT</value>" >> "$snippet_file"
  cat >> "$snippet_file" <<'EOF'
</property>
<property>
  <name>hive.telemetry.otel.service.name</name>
EOF
  echo "  <value>$HIVE_SERVICE</value>" >> "$snippet_file"
  cat >> "$snippet_file" <<'EOF'
</property>
<property>
  <name>hive.telemetry.config.path</name>
EOF
  echo "  <value>CONFIG_DIR_PLACEHOLDER/hive-telemetry.conf</value>" >> "$snippet_file"
  sed -i "s|CONFIG_DIR_PLACEHOLDER|$CONFIG_DIR|g" "$snippet_file"
  cat >> "$snippet_file" <<'EOF'
</property>
EOF

  info "Generated hive-site.xml snippet: $snippet_file"
  echo ""
  echo "  Add the following to $HIVE_HOME/conf/hive-site.xml:"
  echo ""
  cat "$snippet_file" | sed 's/^/    /'
  echo ""
  echo "  And copy the config file to HiveServer2 classpath:"
  echo "    cp $hive_conf_file $HIVE_HOME/conf/"
  echo ""
}

# ── Install MR Collector ─────────────────────────────────────────
install_mr() {
  if [ "$SKIP_MR" = true ]; then
    info "Skipping MR Collector setup (--skip-mr)"
    return
  fi

  check_dir "HADOOP_HOME" "$HADOOP_HOME" || return

  step "Setting up MR Telemetry Collector in: $HADOOP_HOME"

  # Copy JAR to Hadoop share directory
  local lib_dir="$HADOOP_HOME/share/hadoop/mapreduce-telemetry"
  local dest="$lib_dir/spark-telemetry-omni.jar"
  run_cmd mkdir -p "$lib_dir"

  # Remove old versions
  local old_jars
  old_jars=$(find "$lib_dir" -name 'spark-telemetry-omni*.jar' 2>/dev/null || true)
  if [ -n "$old_jars" ]; then
    for old in $old_jars; do
      if [ "$(readlink -f "$old" 2>/dev/null || echo "$old")" != "$(readlink -f "$OMNI_JAR" 2>/dev/null || echo "$OMNI_JAR")" ]; then
        backup_file "$old"
        run_cmd rm -f "$old"
        info "Removed old JAR: $(basename "$old")"
      fi
    done
  fi

  run_cmd cp "$OMNI_JAR" "$dest"
  info "Copied $JAR_NAME -> $dest"

  # Generate mr-collector.conf
  local mr_conf_file="$CONFIG_DIR/mr-collector.conf"
  cat > "$mr_conf_file" <<EOF
# MR Telemetry Collector Configuration
# Generated by install-omni.sh on $(date)
mr-telemetry {
  history-server {
    url = "$MR_HISTORY_URL"
    poll.interval.secs = 30
    connect.timeout.secs = 10
    read.timeout.secs = 30
  }
  otel {
    exporter.endpoint = "$OTEL_ENDPOINT"
    exporter.protocol = "grpc"
    service.name = "$MR_SERVICE"
    export.interval.ms = 10000
  }
  state {
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
    task.counters = false
    job.details = true
  }
}
EOF
  info "Generated MR Collector config: $mr_conf_file"

  # Generate systemd service file (optional)
  local service_file="$CONFIG_DIR/mr-telemetry-collector.service"
  cat > "$service_file" <<EOF
[Unit]
Description=MR Telemetry Collector
After=network.target

[Service]
Type=simple
User=hadoop
ExecStart=/usr/bin/java -jar $lib_dir/spark-telemetry-omni.jar --mr-collector $mr_conf_file
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
  info "Generated systemd service: $service_file"

  # Generate start script
  local start_script="$CONFIG_DIR/start-mr-collector.sh"
  cat > "$start_script" <<EOF
#!/bin/bash
# Start MR Telemetry Collector
# Generated by install-omni.sh on $(date)
export JAVA_HOME=\${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}
exec \$JAVA_HOME/bin/java -jar $lib_dir/spark-telemetry-omni.jar --mr-collector $mr_conf_file
EOF
  chmod +x "$start_script"
  info "Generated start script: $start_script"

  echo ""
  echo "  Start the MR Collector:"
  echo "    $start_script"
  echo ""
  echo "  Or install as systemd service:"
  echo "    sudo cp $service_file /etc/systemd/system/"
  echo "    sudo systemctl daemon-reload"
  echo "    sudo systemctl enable --now mr-telemetry-collector"
  echo ""
}

# ── Generate Spark telemetry.conf ─────────────────────────────────
generate_spark_conf() {
  step "Generating Spark telemetry config"
  local conf_file="$CONFIG_DIR/spark-telemetry.conf"

  cat > "$conf_file" <<EOF
# Spark Telemetry Configuration
# Generated by install-omni.sh on $(date)
spark-telemetry {
  otel {
    exporter.endpoint = "$OTEL_ENDPOINT"
    exporter.protocol = "grpc"
    service.name = "$SPARK_SERVICE"
    export.interval.ms = 10000
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
  }
  filter {
    app.name.include = [".*"]
    app.name.exclude = []
  }
}
EOF
  info "Generated: $conf_file"
}

# ── Generate Summary ──────────────────────────────────────────────
generate_summary() {
  echo ""
  echo "============================================================"
  echo "  Installation Summary"
  echo "============================================================"

  local summary_file="$CONFIG_DIR/INSTALL_SUMMARY.txt"
  cat > "$summary_file" <<EOF
Telemetry Omnipackage Installation Summary
Date: $(date)
JAR: $OMNI_JAR
OTel Endpoint: $OTEL_ENDPOINT

Configuration files generated in: $CONFIG_DIR/
EOF

  if [ "$SKIP_SPARK" = false ] && [ -n "$SPARK_HOME" ] && [ -d "$SPARK_HOME" ]; then
    cat >> "$summary_file" <<EOF

[Spark]
  JAR installed to: $SPARK_HOME/jars/spark-telemetry-omni.jar
  Config snippet:   $CONFIG_DIR/spark-telemetry.conf.snippet
  Telemetry config: $CONFIG_DIR/spark-telemetry.conf
EOF
    echo "  [Spark]   JAR installed to $SPARK_HOME/jars/spark-telemetry-omni.jar"
  fi

  if [ "$SKIP_HIVE" = false ] && [ -n "$HIVE_HOME" ] && [ -d "$HIVE_HOME" ]; then
    cat >> "$summary_file" <<EOF

[Hive]
  JAR installed to: $HIVE_HOME/lib/spark-telemetry-omni.jar
  Config file:      $CONFIG_DIR/hive-telemetry.conf
  hive-site.xml:    $CONFIG_DIR/hive-telemetry-site.xml.snippet
EOF
    echo "  [Hive]    JAR installed to $HIVE_HOME/lib/spark-telemetry-omni.jar"
  fi

  if [ "$SKIP_MR" = false ] && [ -n "$HADOOP_HOME" ] && [ -d "$HADOOP_HOME" ]; then
    cat >> "$summary_file" <<EOF

[MR Collector]
  JAR installed to: $HADOOP_HOME/share/hadoop/mapreduce-telemetry/spark-telemetry-omni.jar
  Config file:      $CONFIG_DIR/mr-collector.conf
  Start script:     $CONFIG_DIR/start-mr-collector.sh
  Systemd service:  $CONFIG_DIR/mr-telemetry-collector.service
EOF
    echo "  [MR]      JAR installed to $HADOOP_HOME/share/hadoop/mapreduce-telemetry/spark-telemetry-omni.jar"
  fi

  echo ""
  info "Full summary saved to: $summary_file"
  echo ""
  echo "============================================================"
}

# ── Main ──────────────────────────────────────────────────────────
main() {
  parse_args "$@"

  OMNI_JAR=$(find_omni_jar)
  JAR_NAME=$(basename "$OMNI_JAR")

  print_banner
  confirm

  # Create config output directory (always, even in dry-run — config generation needs it)
  mkdir -p "$CONFIG_DIR"

  # Generate Spark config first (needed by install_spark)
  generate_spark_conf

  # Install to each component
  install_spark
  install_hive
  install_mr

  generate_summary

  if [ "$DRY_RUN" = true ]; then
    warn "This was a dry run. No changes were made."
  else
    info "Installation complete!"
  fi
}

main "$@"
