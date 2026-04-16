#!/bin/bash
# Build script for the Spark+MR Telemetry Omnipackage.
# Compiles all three Spark adapters (Scala 2.11/2.12/2.13) and assembles
# them into a single unified JAR that auto-detects Spark version at runtime.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Phase 0: Install root POM ==="
mvn install -N -DskipTests

echo ""
echo "=== Phase 1: Install common module (Java, profile-independent) ==="
mvn install -pl spark/spark-telemetry-common -DskipTests

echo ""
echo "=== Phase 2: Install Spark 2.x adapter (Scala 2.11) ==="
mvn install -pl spark/spark-telemetry-adapter-spark2 -Pspark-2 -DskipTests

echo ""
echo "=== Phase 3: Install Spark 3.0.x adapter (Scala 2.12, Spark 3.0.3) ==="
mvn install -pl spark/spark-telemetry-adapter-spark30 -Pspark-30 -DskipTests

echo ""
echo "=== Phase 3a: Install Spark 3.2.x adapter (Scala 2.12, Spark 3.2.4) ==="
mvn install -pl spark/spark-telemetry-adapter-spark32 -Pspark-32 -DskipTests

echo ""
echo "=== Phase 3b: Install Spark 3.5.x adapter (Scala 2.12, Spark 3.5.5) ==="
mvn install -pl spark/spark-telemetry-adapter-spark3 -DskipTests

echo ""
echo "=== Phase 4: Install Spark 4.x adapter (Scala 2.13, Spark 4.0.0) ==="
mvn install -pl spark/spark-telemetry-adapter-spark4 -Pspark-4 -DskipTests

echo ""
echo "=== Phase 5: Install MR + Hive modules (Java) ==="
mvn install -pl mapreduce-collector/mr-telemetry-collector,mapreduce-agent/mr-telemetry-agent,hive/hive-telemetry-hook -DskipTests

echo ""
echo "=== Phase 6: Relocate adapters to version-specific packages ==="
mvn install -pl spark/spark-telemetry-adapters-relocated -Pomni -DskipTests

echo ""
echo "=== Phase 7: Build facade + assemble omnipackage ==="
mvn install -pl spark/spark-telemetry-omni-facade -Pomni -DskipTests
mvn clean package -pl spark/spark-telemetry-dist-omni -Pomni -DskipTests

echo ""
echo "=== Build Complete ==="
JAR=$(ls spark/spark-telemetry-dist-omni/target/spark-telemetry-dist-omni-*.jar 2>/dev/null | head -1)
if [ -n "$JAR" ]; then
    SIZE=$(ls -lh "$JAR" | awk '{print $5}')
    echo "Omnipackage: $JAR ($SIZE)"
    echo ""
    echo "Verify relocated adapters:"
    jar tf "$JAR" | grep "adapter/internal" | head -10
    echo ""
    echo "Verify no unshaded OTel classes:"
    UNLOCKED=$(jar tf "$JAR" | grep "^io/opentelemetry/" | head -3)
    if [ -z "$UNLOCKED" ]; then
        echo "  OK - no unshaded io.opentelemetry classes"
    else
        echo "  WARNING: found unshaded classes:"
        echo "  $UNLOCKED"
    fi
    echo ""
    echo "Verify manifest:"
    unzip -p "$JAR" META-INF/MANIFEST.MF | head -10

    echo ""
    echo "=== API Compliance ==="

    echo "Check v30 adapter does NOT reference remoteReqsDuration..."
    V30_CLASSES=$(jar tf "$JAR" | grep "adapter/internal/v30/.*\\.class$" | head -20)
    FOUND_REMOTE_V30=false
    for cls in $V30_CLASSES; do
        if unzip -p "$JAR" "$cls" | grep -q "remoteReqsDuration"; then
            FOUND_REMOTE_V30=true
            echo "  FAIL: $cls references remoteReqsDuration"
        fi
    done
    if [ "$FOUND_REMOTE_V30" = "false" ]; then
        echo "  OK - v30 adapter has no remoteReqsDuration references"
    fi

    echo "Check v32 adapter does NOT reference remoteReqsDuration..."
    V32_CLASSES=$(jar tf "$JAR" | grep "adapter/internal/v32/.*\\.class$" | head -20)
    FOUND_REMOTE_V32=false
    for cls in $V32_CLASSES; do
        if unzip -p "$JAR" "$cls" | grep -q "remoteReqsDuration"; then
            FOUND_REMOTE_V32=true
            echo "  FAIL: $cls references remoteReqsDuration"
        fi
    done
    if [ "$FOUND_REMOTE_V32" = "false" ]; then
        echo "  OK - v32 adapter has no remoteReqsDuration references"
    fi

    echo "Check v35 adapter DOES reference remoteReqsDuration..."
    V35_LISTENER=$(jar tf "$JAR" | grep "adapter/internal/v35/SparkTelemetryListener\\.class$")
    if [ -n "$V35_LISTENER" ] && unzip -p "$JAR" "$V35_LISTENER" | grep -q "remoteReqsDuration"; then
        echo "  OK - v35 adapter correctly uses remoteReqsDuration"
    else
        echo "  WARNING: v35 adapter does not reference remoteReqsDuration"
    fi
else
    echo "ERROR: Omnipackage JAR not found!"
    exit 1
fi
