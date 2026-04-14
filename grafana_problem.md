# Grafana 仪表板视觉检查报告

**检查时间**: 2026-04-14 12:40 UTC
**检查范围**: http://100.125.162.254:3000/dashboards

## 检查概述

共检查 6 个仪表板，发现以下问题：

| 仪表板 | 状态 | 问题 |
|--------|------|------|
| Hive on MR Telemetry | ❌ 部分无数据 | 4 个面板无数据 |
| Hive on Spark Telemetry | ❌ 部分无数据 | 6 个面板无数据 |
| MapReduce Telemetry | ❌ 部分无数据 | 5 个面板无数据 |
| Platform Telemetry Overview | ⚠️ 部分无数据 | 1 个面板无数据 |
| Spark / MR / Hive Telemetry Dashboard | ❌ 不存在 | 404 Not Found |
| Spark Telemetry | ✅ 正常 | 有数据 |

---

## 详细问题

### 1. Hive on MR Telemetry
**URL**: `/d/hive-mr-telemetry/hive-on-mr-telemetry`

**问题面板**:
- Operation Distribution - **No data**
- Duration by Operation - **No data**
- Query Detail - **No data**
- Table IO Detail - **No data**

**统计值**:
- Total Hive-MR Queries: 0

---

### 2. Hive on Spark Telemetry
**URL**: `/d/hive-spark-telemetry/hive-on-spark-telemetry`

**问题面板**:
- Operation Distribution - **No data**
- Duration by Operation - **No data**
- Query Detail - **No data**
- Table IO Detail - **No data**

**统计值**:
- Total Hive-Spark Queries: 0
- Hive-Spark Table Events: 0

---

### 3. MapReduce Telemetry
**URL**: `/d/mr-telemetry/mapreduce-telemetry`

**问题面板**:
- Job IO Bytes - **No data**
- Map/Reduce Task Counts - **No data**
- CPU/GC Time - **No data**
- Task IO Records - **No data**

**统计值**:
- Total MR Jobs: 0

---

### 4. Platform Telemetry Overview
**URL**: `/d/telemetry-overview/platform-telemetry-overview`

**问题面板**:
- Recent Failed Tasks - **No data**

**统计值**:
- Total MR Jobs: 0
- Total Hive Queries: 0
- MR Job Success Rate: 无值

**正常**:
- Total Spark Apps: 43 (有数据)
- Success Rates by Engine: Spark 有数据，MR/Hive 为 0

---

### 5. Spark / MR / Hive Telemetry Dashboard
**URL**: `/d/spark-mr-hive-telemetry/spark-mr-hive-telemetry-dashboard`

**问题**: **404 Not Found** - 仪表板不存在

错误信息:
```
Dashboard not found
We're looking but can't seem to find this dashboard.
```

---

### 6. Spark Telemetry
**URL**: `/d/spark-telemetry/spark-telemetry`

**状态**: ✅ **正常**

**有数据的面板**:
- Total Tasks: 2432
- Total Stages: 53
- Skewed Stages: 12
- Avg CPU Efficiency: 61.4%
- Task I/O Bytes: 有数据
- Task Duration: 有数据
- JVM Memory: 有数据
- JVM GC: 有数据

---

## 根本原因分析

1. **Hive 相关仪表板无数据**: Hive Hook 可能未部署到 HiveServer2，或配置不正确
2. **MR 相关仪表板无数据**: MR Collector 可能未运行，或 Hadoop 集群没有 MR 任务执行
3. **Spark / MR / Hive 综合仪表板丢失**: 仪表板未导入或被误删除
4. **Spark Telemetry 正常**: Spark 插件工作正常，数据已成功上报

## 建议修复

1. **导入丢失的仪表板**: 重新导入 `Spark / MR / Hive Telemetry Dashboard`
2. **检查 Hive Hook 部署**: 确认 HiveServer2 已配置 `hive.exec.post.hooks`
3. **启动 MR Collector**: 在 Hadoop 集群上运行 MR Collector
4. **验证数据源**: 确认 Kafka/MySQL 数据源连接正常
