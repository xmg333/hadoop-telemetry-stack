#!/bin/bash
# Build script for the Spark+MR Telemetry Omnipackage.
# Compiles all three Spark adapters (Scala 2.11/2.12/2.13) and assembles
# them into a single unified JAR that auto-detects Spark version at runtime.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Phase 1: Install common module (Java, profile-independent) ==="
mvn install -pl spark-telemetry-common -DskipTests

echo ""
echo "=== Phase 2: Install Spark 2.x adapter (Scala 2.11) ==="
mvn install -pl spark-telemetry-adapter-spark2 -Pspark-2 -DskipTests

echo ""
echo "=== Phase 3: Install Spark 3.x adapter (Scala 2.12) ==="
mvn install -pl spark-telemetry-adapter-spark3 -DskipTests

echo ""
echo "=== Phase 4: Install Spark 4.x adapter (Scala 2.13) ==="
mvn install -pl spark-telemetry-adapter-spark4 -Pspark-4 -DskipTests

echo ""
echo "=== Phase 5: Install MR + Hive modules (Java) ==="
mvn install -pl mr-telemetry-collector,mr-telemetry-agent,hive-telemetry-hook -DskipTests

echo ""
echo "=== Phase 6: Relocate adapters to version-specific packages ==="
mvn install -pl spark-telemetry-adapters-relocated -Pomni -DskipTests

echo ""
echo "=== Phase 7: Build facade + assemble omnipackage ==="
mvn install -pl spark-telemetry-omni-facade -Pomni -DskipTests
mvn clean package -pl spark-telemetry-dist-omni -Pomni -DskipTests

echo ""
echo "=== Build Complete ==="
JAR=$(ls spark-telemetry-dist-omni/target/spark-telemetry-dist-omni-*.jar 2>/dev/null | head -1)
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
else
    echo "ERROR: Omnipackage JAR not found!"
    exit 1
fi
