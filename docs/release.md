# Packaging and Release Guide

## Version Management

The version is defined in the root `pom.xml`, and all submodules inherit the same version. The release process follows Semantic Versioning (SemVer).

```bash
# View current version
grep '<version>' pom.xml | head -1

# Update version (all submodules)
find . -name 'pom.xml' -not -path '*/target/*' \
  -exec sed -i 's|<version>OLD_VERSION</version>|<version>NEW_VERSION</version>|g' {} +
```

## One-Click Release (release.sh)

`release.sh` automates the build, packaging, and generation of the distribution tarball:

```bash
# Full build + packaging
./release.sh

# Skip build (use existing JARs)
./release.sh --skip-build

# Skip documentation copy
./release.sh --skip-docs
```

### release.sh Execution Steps

| Step | Action | Artifact |
|------|--------|----------|
| 1/6 | `./build-omni.sh` -- Build Omnipackage | `spark-telemetry-dist-omni-*.jar` |
| 2/6 | Build Flink Consumer | `metrics-flink-consumer-dist-*.jar` |
| 3/6 | Build Diagnostic Tool | `diagnostic-core-*.jar` |
| 4/6 | Assemble distribution directory | `dist/yarn-telemetry-VERSION/` |
| 5/6 | Generate README | `dist/yarn-telemetry-VERSION/README.md` |
| 6/6 | Package tar.gz | `dist/yarn-telemetry-VERSION.tar.gz` |

### Distribution Package Structure

```
yarn-telemetry-VERSION/
  lib/
    spark-telemetry-omni.jar    Omnipackage (Spark 2/3/4 + MR Agent/Collector + Hive Hook)
    flink-consumer.jar          Flink Consumer (Kafka -> MySQL/ClickHouse)
    diagnostic.jar              Diagnostic Tool
  conf/
    spark/                      Spark Plugin presets (basic.conf / full.conf / submit.sh)
    flink/                      Flink Consumer config (mysql / clickhouse)
    mr-collector.conf           MR Collector config
    hive-hook.conf              Hive Hook config
    diagnostic.conf             Diagnostic Tool config
  grafana/                      13 Grafana dashboard JSON files
  otel-collector/               OTel Collector config
  sql/                          Database migration scripts
  deploy/                       Deployment scripts (install-omni.sh, deploy-grafana.sh)
  docs/                         Documentation
```

## Manual Build

### Building Individual Spark Versions

```bash
# Spark 3.x (default profile)
mvn clean package -DskipTests

# Spark 2.x
mvn clean package -Pspark-2 -DskipTests

# Spark 4.x
mvn clean package -Pspark-4 -DskipTests

# Omnipackage (7-stage build: common -> adapters -> MR -> relocate -> facade -> dist)
chmod +x build-omni.sh && ./build-omni.sh
```

### Building Individual Components

```bash
# Flink Consumer
mvn clean package -pl flink/metrics-flink-consumer,flink/metrics-flink-consumer-dist -am -DskipTests

# MR Collector
mvn clean package -pl mapreduce-collector/mr-telemetry-collector,mapreduce-collector/mr-telemetry-dist -am -DskipTests

# MR Agent
mvn clean package -pl mapreduce-agent/mr-telemetry-agent,mapreduce-agent/mr-telemetry-agent-dist -am -DskipTests

# Hive Hook
mvn clean package -pl hive/hive-telemetry-hook,hive/hive-telemetry-hook-dist -am -DskipTests

# Diagnostic Tool
mvn clean package -pl diagnostic/diagnostic-core -am -DskipTests
```

## Publishing to Maven Repository

The Maven repository is configured in `~/.m2/settings.xml`, with the `rdc` profile specifying the Alibaba Cloud Packages repository URL and authentication information.

### Publishing All Modules

```bash
# First release (includes parent POM) -- choose one profile, the parent POM only needs to be pushed once
mvn deploy -Pspark-3 -DskipTests -Prdc

# Other Spark versions (skip already-existing parent POM)
mvn deploy -Pspark-2 -DskipTests -Prdc -pl spark/spark-telemetry-adapter-spark2,spark/spark-telemetry-dist-spark2
mvn deploy -Pspark-4 -DskipTests -Prdc -pl spark/spark-telemetry-adapter-spark4,spark/spark-telemetry-dist-spark4
mvn deploy -Pspark-32 -DskipTests -Prdc -pl spark/spark-telemetry-adapter-spark32,spark/spark-telemetry-dist-spark32
mvn deploy -Pspark-30 -DskipTests -Prdc -pl spark/spark-telemetry-adapter-spark30,spark/spark-telemetry-dist-spark3

# Omnipackage
mvn deploy -Pomni -DskipTests -Prdc -pl spark/spark-telemetry-omni-facade,spark/spark-telemetry-dist-omni
```

### Uploading tar.gz Distribution Package

Upload the complete distribution package as a generic artifact to the Maven repository:

```bash
mvn deploy:deploy-file -Prdc \
  -DgroupId=x.mg.metrics \
  -DartifactId=yarn-telemetry \
  -Dversion=VERSION \
  -Dpackaging=tar.gz \
  -Dfile=dist/yarn-telemetry-VERSION.tar.gz \
  -DrepositoryId=YOUR_REPO_ID \
  -Durl=https://packages.example.com/YOUR_NAMESPACE/maven/YOUR_REPO_ID
```

### Notes

- The parent POM can only be pushed once per version (409 Conflict). Subsequent pushes must use `-pl` to specify submodules
- The `release` repository does not allow overwriting existing artifacts; to republish you must increment the version number
- Shaded dist modules are large (flink-consumer 81M, diagnostic 25M), uploads may take considerable time

## Git Tag

```bash
# Commit version changes
git add -A
git commit -m "release: vVERSION"

# Create tag
git tag -a vVERSION -m "Release vVERSION"

# Push
git push origin master
git push origin vVERSION
```

## Full Release Process

```bash
# 1. Update version number
find . -name 'pom.xml' -not -path '*/target/*' \
  -exec sed -i 's|<version>OLD</version>|<version>NEW</version>|g' {} +

# 2. Build distribution package
./release.sh

# 3. Commit + tag
git add -A && git commit -m "release: vNEW"
git tag -a vNEW -m "Release vNEW"
git push origin master && git push origin vNEW

# 4. Push to Maven repository
mvn deploy -Pspark-3 -DskipTests -Prdc
mvn deploy -Pspark-2 -DskipTests -Prdc -pl spark/spark-telemetry-adapter-spark2,spark/spark-telemetry-dist-spark2
mvn deploy -Pspark-4 -DskipTests -Prdc -pl spark/spark-telemetry-adapter-spark4,spark/spark-telemetry-dist-spark4
mvn deploy -Pomni -DskipTests -Prdc -pl spark/spark-telemetry-omni-facade,spark/spark-telemetry-dist-omni

# 5. Upload tar.gz
mvn deploy:deploy-file -Prdc \
  -DgroupId=x.mg.metrics -DartifactId=yarn-telemetry \
  -Dversion=NEW -Dpackaging=tar.gz \
  -Dfile=dist/yarn-telemetry-NEW.tar.gz \
  -DrepositoryId=YOUR_REPO_ID \
  -Durl=https://packages.example.com/YOUR_NAMESPACE/maven/YOUR_REPO_ID
```
