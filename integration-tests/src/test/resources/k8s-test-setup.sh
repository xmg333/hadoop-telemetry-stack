#!/bin/bash
# K8s Test Environment Setup Script
#
# This script sets up the K8s test environment for integration tests.
# It applies manifests, waits for pods, and optionally runs tests.
#
# Usage: ./k8s-test-setup.sh [command]
# Commands:
#   setup    - Deploy K8s resources and wait for readiness
#   test     - Run integration tests
#   cleanup  - Remove K8s resources
#   all      - Setup, test, and cleanup (default)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
NAMESPACE="${TEST_NAMESPACE:-default}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl not found. Please install kubectl."
        exit 1
    fi

    if ! kubectl cluster-info &> /dev/null; then
        log_error "No K8s cluster found. Please configure kubectl."
        exit 1
    fi

    if ! command -v mvn &> /dev/null; then
        log_error "Maven not found. Please install Maven."
        exit 1
    fi

    log_info "Prerequisites check passed"
}

# Apply K8s manifests
setup_environment() {
    log_info "Setting up K8s test environment..."

    cd "$PROJECT_ROOT/deploy/k8s"

    # Apply manifests in order
    log_info "Applying Kafka manifest..."
    kubectl apply -f kafka/kafka-pod.yaml

    log_info "Applying MySQL manifest..."
    kubectl apply -f mysql/mysql-pod.yaml

    log_info "Applying OTel Collector manifest..."
    kubectl apply -f otel-collector/otel-collector-config.yaml
    kubectl apply -f otel-collector/otel-collector-deployment.yaml

    log_info "Applying Hadoop 3 manifest..."
    kubectl apply -f hadoop/hadoop3-pod.yaml

    log_info "Applying Hadoop 2 manifest..."
    kubectl apply -f hadoop/hadoop2-pod.yaml

    # Wait for pods to be ready
    log_info "Waiting for pods to be ready..."

    wait_for_pod "kafka" 120
    wait_for_pod "mysql" 120
    wait_for_pod "otel-collector" 60
    wait_for_pod "hadoop3" 180
    wait_for_pod "hadoop2" 180

    log_info "All pods are ready"

    # Port-forward services for local access
    log_info "Setting up port-forwards..."
    setup_port_forwards
}

# Wait for a pod to be ready
wait_for_pod() {
    local pod_name=$1
    local timeout=${2:-120}

    log_info "Waiting for pod $pod_name (timeout: ${timeout}s)..."

    if ! kubectl wait --for=condition=ready pod/$pod_name --timeout=${timeout}s 2>/dev/null; then
        log_warn "Pod $pod_name not ready within ${timeout}s, checking status..."
        kubectl get pod $pod_name -o wide
        kubectl describe pod $pod_name
        return 1
    fi

    log_info "Pod $pod_name is ready"
}

# Setup port-forwards for services
setup_port_forwards() {
    # Kill existing port-forwards
    pkill -f "kubectl port-forward.*kafka" 2>/dev/null || true
    pkill -f "kubectl port-forward.*mysql" 2>/dev/null || true
    pkill -f "kubectl port-forward.*otel-collector" 2>/dev/null || true

    # Start new port-forwards in background
    log_info "Starting port-forward for Kafka (9092)..."
    kubectl port-forward svc/kafka 9092:9092 &
    KAFKA_PF_PID=$!

    log_info "Starting port-forward for MySQL (3306)..."
    kubectl port-forward svc/mysql 3306:3306 &
    MYSQL_PF_PID=$!

    log_info "Starting port-forward for OTel Collector (4317)..."
    kubectl port-forward svc/otel-collector 4317:4317 &
    OTEL_PF_PID=$!

    # Wait for port-forwards to be ready
    sleep 3

    # Export environment variables for tests
    export OTEL_ENDPOINT="http://localhost:4317"
    export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
    export MYSQL_HOST="localhost"
    export MYSQL_PORT="3306"

    log_info "Port-forwards established"
}

# Run integration tests
run_tests() {
    log_info "Running integration tests..."

    cd "$PROJECT_ROOT"

    # Build test JARs
    log_info "Building test artifacts..."
    mvn clean package -Pspark-3 -DskipTests -q

    # Run tests
    log_info "Executing integration tests..."
    mvn verify -Pspark-3 \
        -Dtest=none \
        -DfailIfNoTests=false \
        -DskipK8sTests=false

    log_info "Tests completed"
}

# Cleanup K8s resources
cleanup() {
    log_info "Cleaning up K8s resources..."

    cd "$PROJECT_ROOT/deploy/k8s"

    # Kill port-forwards
    pkill -f "kubectl port-forward" 2>/dev/null || true

    # Delete resources
    kubectl delete -f kafka/kafka-pod.yaml --ignore-not-found=true
    kubectl delete -f mysql/mysql-pod.yaml --ignore-not-found=true
    kubectl delete -f otel-collector/otel-collector-deployment.yaml --ignore-not-found=true
    kubectl delete -f otel-collector/otel-collector-config.yaml --ignore-not-found=true
    kubectl delete -f hadoop/hadoop3-pod.yaml --ignore-not-found=true
    kubectl delete -f hadoop/hadoop2-pod.yaml --ignore-not-found=true

    log_info "Cleanup completed"
}

# Main command handler
case "${1:-all}" in
    setup)
        check_prerequisites
        setup_environment
        ;;
    test)
        run_tests
        ;;
    cleanup)
        cleanup
        ;;
    all)
        check_prerequisites
        setup_environment
        run_tests
        cleanup
        ;;
    *)
        echo "Usage: $0 [setup|test|cleanup|all]"
        echo ""
        echo "Commands:"
        echo "  setup    - Deploy K8s resources and wait for readiness"
        echo "  test     - Run integration tests (requires setup)"
        echo "  cleanup  - Remove K8s resources"
        echo "  all      - Setup, test, and cleanup (default)"
        exit 1
        ;;
esac
