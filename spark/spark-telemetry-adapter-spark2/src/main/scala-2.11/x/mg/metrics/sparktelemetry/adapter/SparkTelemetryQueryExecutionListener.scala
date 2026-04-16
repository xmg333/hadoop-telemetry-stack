package x.mg.metrics.sparktelemetry.adapter

import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.util.QueryExecutionListener
import x.mg.metrics.sparktelemetry.lifecycle.TelemetryLifecycle
import x.mg.metrics.sparktelemetry.model.{SparkMetricEvent, SqlExecutionMetrics, SqlTableIOMetrics}

/**
 * Spark 2.x QueryExecutionListener adapter.
 *
 * Key differences from Spark 3.x:
 *   - No BatchScanExec, ShuffledHashJoinExec (Spark 3.x+ only)
 *   - No-arg constructor: registered via spark.sql.queryExecutionListeners config
 *   - Lazy initialization via ensureInit() (same pattern as SparkTelemetryListener)
 *   - DataSourceScanExec.metadata is protected; use FileSourceScanExec.relation instead
 *   - InsertIntoHadoopFsRelationCommand is a logical node, matched via ExecutedCommandExec wrapper
 *
 * Registration: spark.sql.queryExecutionListeners=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryQueryExecutionListener
 */
class SparkTelemetryQueryExecutionListener extends QueryExecutionListener {

  private var initialized = false
  private var lifecycle: TelemetryLifecycle = _
  private var config: x.mg.metrics.sparktelemetry.config.TelemetryConfig = _

  private def ensureInit(): Unit = {
    if (!initialized) {
      synchronized {
        if (!initialized) {
          val confMap = new java.util.HashMap[String, String]()
          try {
            val env = org.apache.spark.SparkEnv.get
            if (env != null && env.conf != null) {
              env.conf.getAll.foreach { case (k, v) => confMap.put(k, v) }
            }
          } catch {
            case _: Exception =>
          }
          TelemetryLifecycle.init(confMap)
          lifecycle = TelemetryLifecycle.getInstance
          config = lifecycle.getConfig
          initialized = true
        }
      }
    }
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    ensureInit()
    if (!config.isCaptureSqlQueryExecution) return
    val (queryMetrics, tableMetrics) = extractMetrics(qe, durationNs, success = true, null)
    emitEvent(funcName, qe, queryMetrics, tableMetrics)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    ensureInit()
    if (!config.isCaptureSqlQueryExecution) return
    val (queryMetrics, tableMetrics) = extractMetrics(qe, 0L, success = false, exception)
    emitEvent(funcName, qe, queryMetrics, tableMetrics)
  }

  private def extractMetrics(qe: QueryExecution, durationNs: Long, success: Boolean, ex: Exception): (SqlExecutionMetrics, java.util.List[SqlTableIOMetrics]) = {
    val queryMetrics = new SqlExecutionMetrics
    queryMetrics.setSuccess(success)
    if (ex != null) queryMetrics.setErrorMessage(if (ex.getMessage != null) ex.getMessage else ex.getClass.getName)
    queryMetrics.setDurationMs(durationNs / 1000000)

    val tableMetrics = new java.util.ArrayList[SqlTableIOMetrics]()
    collectPlanMetrics(qe.executedPlan, queryMetrics, tableMetrics)

    (queryMetrics, tableMetrics)
  }

  private def collectPlanMetrics(plan: SparkPlan, qm: SqlExecutionMetrics, tm: java.util.List[SqlTableIOMetrics]): Unit = {
    plan foreach {
      case s: FileSourceScanExec =>
        // Spark 2.x: FileSourceScanExt has public relation field
        val tableMetric = new SqlTableIOMetrics
        tableMetric.setOperation("scan")
        tableMetric.setTableName(
          try {
            s.relation.location.rootPaths.map(_.toString).mkString(",")
          } catch {
            case _: Exception => "unknown"
          }
        )
        tableMetric.setBytes(metricValue(s, "numBytesRead"))
        tableMetric.setRows(metricValue(s, "numOutputRows"))
        tableMetric.setFilesRead(metricValue(s, "numFilesRead"))
        tableMetric.setTimeMs(metricValue(s, "scanTime"))
        tm.add(tableMetric)

      case j: BroadcastHashJoinExec =>
        qm.setJoinCount(qm.getJoinCount + 1)
        qm.setJoinTypes(appendJoinType(qm.getJoinTypes, "broadcast"))

      case j: SortMergeJoinExec =>
        qm.setJoinCount(qm.getJoinCount + 1)
        qm.setJoinTypes(appendJoinType(qm.getJoinTypes, "sort_merge"))

      case w: DataWritingCommandExec =>
        w.cmd match {
          case i: InsertIntoHadoopFsRelationCommand =>
            val tableMetric = new SqlTableIOMetrics
            tableMetric.setOperation("write")
            tableMetric.setTableName(i.options.getOrElse("path", "unknown"))
            tableMetric.setRows(metricValue(w, "numOutputRows"))
            tableMetric.setBytes(metricValue(w, "numOutputBytes"))
            tm.add(tableMetric)
          case _ =>
        }

      case _ =>
    }
  }

  private def metricValue(plan: SparkPlan, name: String): Long = {
    plan.metrics.get(name).map(_.value).getOrElse(0L)
  }

  private def appendJoinType(current: String, joinType: String): String = {
    if (current == null || current.isEmpty) joinType else current + "," + joinType
  }

  private def emitEvent(funcName: String, qe: QueryExecution,
                        queryMetrics: SqlExecutionMetrics, tableMetrics: java.util.List[SqlTableIOMetrics]): Unit = {
    val event = new SparkMetricEvent
    event.setEventType(SparkMetricEvent.EventType.SQL_EXECUTION)
    event.setTimestamp(System.currentTimeMillis())
    event.setSqlExecutionMetrics(queryMetrics)
    event.setSqlTableIOMetrics(tableMetrics)

    try {
      val env = org.apache.spark.SparkEnv.get
      if (env != null) {
        if (env.conf != null) {
          val appId = env.conf.getAppId
          event.setApplicationId(if (appId != null && appId.nonEmpty) appId else env.conf.get("spark.app.name", "unknown"))
          event.setApplicationName(env.conf.get("spark.app.name", ""))
        }
      }
    } catch {
      case _: Exception =>
    }

    lifecycle.accept(event)
  }
}
