# Grafana 仪表板视觉检查报告

**检查时间**: 2026-04-16 00:12 UTC
**检查范围**: http://192.168.10.65:3000/dashboards

## 检查概述

共检查 6 个仪表板，发现以下问题：

| 仪表板 | 状态 | 问题 |
|--------|------|------|
| Spark Telemetry | ⚠️ 部分无数据 | 1 个面板无数据 |
| MapReduce Telemetry | ⚠️ 部分无数据 | 5 个面板无数据 |
| Hive on Spark Telemetry | ❌ 无数据 | 6 个面板无数据 |
| Spark / MR / Hive Telemetry Dashboard | ⚠️ 部分无数据 | 1 个面板无数据 |
| Hive on MR Telemetry | ✅ 正常 | 有数据 |
| Platform Telemetry Overview | ✅ 正常 | 有数据 |

---

## 详细问题

### 1. Spark Telemetry
**URL**: `/d/spark-telemetry/spark-telemetry`

**问题面板**:
- SQL Table IO Detail - **No data**

**统计值**:
- Total Tasks: 4389
- Total Stages: 4
- Skewed Stages: 1
- Avg CPU Efficiency: 88.4%

---

### 2. MapReduce Telemetry
**URL**: `/d/mr-telemetry/mapreduce-telemetry`

**问题面板 (Task Level Metrics 区域)**:
- Avg Task Duration - **无数据**
- Task Success Rate - **无数据**
- Total Map Output Bytes - **无数据**
- File Operations - **No data**
- Task Duration - **No data**

**统计值**:
- Total MR Jobs: 15
- Job Success Rate: 100%
- Total Tasks: 11
- Reduce Tasks: 6
- Total Shuffle Bytes: 4.96 MiB

---

### 3. Hive on Spark Telemetry
**URL**: `/d/hive-spark-telemetry/hive-on-spark-telemetry`

**问题面板**:
- Total Hive-Spark Queries - **值为 0**
- Hive-Spark Table Events - **值为 0**
- Operation Distribution - **No data**
- Duration by Operation - **No data**
- Query Detail - **No data**
- Table IO Detail - **No data**

**统计值**:
- Total Hive-Spark Queries: 0
- Hive-Spark Table Events: 0

---

### 4. Spark / MR / Hive Telemetry Dashboard
**URL**: `/d/spark-mr-telemetry/spark-mr-hive-telemetry-dashboard`

**问题面板**:
- Hive IO Throughput - **No data**

**统计值**:
- Total Tasks: 4511
- Total Stages: 14
- Skewed Stages: 2
- Avg CPU Efficiency: 88.4%

---

## 正常工作的仪表板

### Hive on MR Telemetry
**URL**: `/d/hive-mr-telemetry/hive-on-mr-telemetry`

**状态**: ✅ **正常**

**有数据的面板**:
- Total Hive-MR Queries: 2
- Avg Hive-MR Duration: 3.02 s
- Hive-MR IO Bytes: 有数据
- Hive-MR Table Events: 1
- Operation Distribution: 有数据
- Duration by Operation: 有数据
- Query Detail: 有数据（2 条查询记录）
- Table IO Detail: 有数据（1 条记录）

### Platform Telemetry Overview
**URL**: `/d/telemetry-overview/platform-telemetry-overview`

**状态**: ✅ **正常**

**有数据的面板**:
- Total Spark Apps: 5
- Total MR Jobs: 15
- Total Hive Queries: 2
- MR Job Success Rate: 100%
- Success Rates by Engine: 有数据（MR: 52%, Spark: 41%, Hive: 7%）
- IO Throughput by Engine: 有数据
- Job Duration Trends: 有数据
- Recent Failed Tasks: 有数据

### Spark / MR / Hive Telemetry Dashboard
**URL**: `/d/spark-mr-telemetry/spark-mr-hive-telemetry-dashboard`

**状态**: ✅ **正常（大部分面板）**

**有数据的面板**:
- Total Tasks: 4511
- Total Stages: 14
- Skewed Stages: 2
- Avg CPU Efficiency: 88.4%
- Task I/O Bytes: 有数据
- Task Duration: 有数据
- JVM Memory: 有数据
- JVM GC: 有数据
- Stage Duration & Task Count: 有数据
- Job Overview: 有数据
- Hive Operations Distribution: 有数据
- Hive Duration by Operation: 有数据
- Hive Operation Count: 有数据
- Hive Query Detail: 有数据（2 条记录）
- Hive Table IO Detail: 有数据（1 条记录）

---

## 根本原因分析

1. **Spark Telemetry - SQL Table IO Detail 无数据**: SQL 表 IO 数据未被正确采集或写入
2. **MapReduce Telemetry - Task Level 部分面板无数据**: MR Agent 的 Task 级别指标中，File Operations 和 Task Duration 数据缺失
3. **Hive on Spark 无数据**: Hive on Spark 的 Hook 可能未部署到 HiveServer2，或配置不正确，或者没有 Hive on Spark 查询执行
4. **综合仪表板 Hive IO Throughput 无数据**: Hive IO 吞吐量数据缺失

## 建议修复

1. **检查 Hive on Spark Hook 部署**: 确认 HiveServer2 已配置 `hive.exec.post.hooks` 用于 Spark 执行引擎
2. **检查 MR Agent 指标**: 确认 MR Agent 是否正确上报 File Operations 和 Task Duration 指标
3. **检查 Spark SQL 指标**: 确认 Spark SQL 表 IO 数据是否正确采集
4. **检查 Hive IO 指标**: 确认 Hive IO 吞吐量数据是否正确上报
