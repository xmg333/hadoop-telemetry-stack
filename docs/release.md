# 打包发布指南

## 版本管理

版本号定义在根 `pom.xml` 中，所有子模块继承同一版本。发布流程使用语义化版本（SemVer）。

```bash
# 查看当前版本
grep '<version>' pom.xml | head -1

# 更新版本（所有子模块）
find . -name 'pom.xml' -not -path '*/target/*' \
  -exec sed -i 's|<version>OLD_VERSION</version>|<version>NEW_VERSION</version>|g' {} +
```

## 一键发布（release.sh）

`release.sh` 自动完成构建、打包、生成发行包：

```bash
# 完整构建 + 打包
./release.sh

# 跳过构建（使用已有 JAR）
./release.sh --skip-build

# 跳过文档拷贝
./release.sh --skip-docs
```

### release.sh 执行步骤

| 步骤 | 操作 | 产物 |
|------|------|------|
| 1/6 | `./build-omni.sh` — 构建 Omnipackage | `spark-telemetry-dist-omni-*.jar` |
| 2/6 | 构建 Flink Consumer | `metrics-flink-consumer-dist-*.jar` |
| 3/6 | 构建诊断工具 | `diagnostic-core-*.jar` |
| 4/6 | 组装发行目录 | `dist/yarn-telemetry-VERSION/` |
| 5/6 | 生成 README | `dist/yarn-telemetry-VERSION/README.md` |
| 6/6 | 打包 tar.gz | `dist/yarn-telemetry-VERSION.tar.gz` |

### 发行包结构

```
yarn-telemetry-VERSION/
  lib/
    spark-telemetry-omni.jar    Omnipackage（Spark 2/3/4 + MR Agent/Collector + Hive Hook）
    flink-consumer.jar          Flink 消费者（Kafka → MySQL/ClickHouse）
    diagnostic.jar              诊断工具
  conf/
    spark/                      Spark 插件预设（basic.conf / full.conf / submit.sh）
    flink/                      Flink 消费者配置（mysql / clickhouse）
    mr-collector.conf           MR Collector 配置
    hive-hook.conf              Hive Hook 配置
    diagnostic.conf             诊断工具配置
  grafana/                      13 个 Grafana 仪表盘 JSON
  otel-collector/               OTel Collector 配置
  sql/                          数据库迁移脚本
  deploy/                       部署脚本（install-omni.sh, deploy-grafana.sh）
  docs/                         文档
```

## 手动构建

### 单独构建各 Spark 版本

```bash
# Spark 3.x（默认 profile）
mvn clean package -DskipTests

# Spark 2.x
mvn clean package -Pspark-2 -DskipTests

# Spark 4.x
mvn clean package -Pspark-4 -DskipTests

# Omnipackage（7 阶段构建：common → adapters → MR → relocate → facade → dist）
chmod +x build-omni.sh && ./build-omni.sh
```

### 单独构建其他组件

```bash
# Flink Consumer
mvn clean package -pl flink/metrics-flink-consumer,flink/metrics-flink-consumer-dist -am -DskipTests

# MR Collector
mvn clean package -pl mapreduce-collector/mr-telemetry-collector,mapreduce-collector/mr-telemetry-dist -am -DskipTests

# MR Agent
mvn clean package -pl mapreduce-agent/mr-telemetry-agent,mapreduce-agent/mr-telemetry-agent-dist -am -DskipTests

# Hive Hook
mvn clean package -pl hive/hive-telemetry-hook,hive/hive-telemetry-hook-dist -am -DskipTests

# 诊断工具
mvn clean package -pl diagnostic/diagnostic-core -am -DskipTests
```

## 发布到 Maven 仓库

Maven 仓库配置在 `~/.m2/settings.xml` 中，profile `rdc` 配置了阿里云 Packages 仓库地址和认证信息。

### 发布全部模块

```bash
# 首次发布（含父 POM）— 选择一个 profile 即可，父 POM 只需推一次
mvn deploy -Pspark-3 -DskipTests -Prdc

# 其他 Spark 版本（跳过已存在的父 POM）
mvn deploy -Pspark-2 -DskipTests -Prdc -pl spark/spark-telemetry-adapter-spark2,spark/spark-telemetry-dist-spark2
mvn deploy -Pspark-4 -DskipTests -Prdc -pl spark/spark-telemetry-adapter-spark4,spark/spark-telemetry-dist-spark4
mvn deploy -Pspark-32 -DskipTests -Prdc -pl spark/spark-telemetry-adapter-spark32,spark/spark-telemetry-dist-spark32
mvn deploy -Pspark-30 -DskipTests -Prdc -pl spark/spark-telemetry-adapter-spark30,spark/spark-telemetry-dist-spark3

# Omnipackage
mvn deploy -Pomni -DskipTests -Prdc -pl spark/spark-telemetry-omni-facade,spark/spark-telemetry-dist-omni
```

### 上传 tar.gz 发行包

将完整发行包作为 generic artifact 上传到 Maven 仓库：

```bash
mvn deploy:deploy-file -Prdc \
  -DgroupId=x.mg.metrics \
  -DartifactId=yarn-telemetry \
  -Dversion=VERSION \
  -Dpackaging=tar.gz \
  -Dfile=dist/yarn-telemetry-VERSION.tar.gz \
  -DrepositoryId=2587999-release-4bK5py \
  -Durl=https://packages.aliyun.com/68c25bfda72fbe62724140bd/maven/2587999-release-4bk5py
```

### 注意事项

- 父 POM 同一版本只能推一次（409 Conflict），后续 push 需用 `-pl` 指定子模块
- `release` 仓库不可覆盖已有 artifact，如需重新发布需升级版本号
- Shaded dist 模块较大（flink-consumer 81M，diagnostic 25M），上传耗时较长

## Git Tag

```bash
# 提交版本变更
git add -A
git commit -m "release: vVERSION"

# 创建 tag
git tag -a vVERSION -m "Release vVERSION"

# 推送
git push origin master
git push origin vVERSION
```

## 完整发布流程

```bash
# 1. 更新版本号
find . -name 'pom.xml' -not -path '*/target/*' \
  -exec sed -i 's|<version>OLD</version>|<version>NEW</version>|g' {} +

# 2. 构建发行包
./release.sh

# 3. 提交 + 打 tag
git add -A && git commit -m "release: vNEW"
git tag -a vNEW -m "Release vNEW"
git push origin master && git push origin vNEW

# 4. 推送到 Maven 仓库
mvn deploy -Pspark-3 -DskipTests -Prdc
mvn deploy -Pspark-2 -DskipTests -Prdc -pl spark/spark-telemetry-adapter-spark2,spark/spark-telemetry-dist-spark2
mvn deploy -Pspark-4 -DskipTests -Prdc -pl spark/spark-telemetry-adapter-spark4,spark/spark-telemetry-dist-spark4
mvn deploy -Pomni -DskipTests -Prdc -pl spark/spark-telemetry-omni-facade,spark/spark-telemetry-dist-omni

# 5. 上传 tar.gz
mvn deploy:deploy-file -Prdc \
  -DgroupId=x.mg.metrics -DartifactId=yarn-telemetry \
  -Dversion=NEW -Dpackaging=tar.gz \
  -Dfile=dist/yarn-telemetry-NEW.tar.gz \
  -DrepositoryId=2587999-release-4bK5py \
  -Durl=https://packages.aliyun.com/68c25bfda72fbe62724140bd/maven/2587999-release-4bk5py
```
