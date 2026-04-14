package x.mg.metrics.sparktelemetry.adapter

import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.util.QueryExecutionListener
import x.mg.metrics.sparktelemetry.lifecycle.TelemetryLifecycle
import x.mg.metrics.sparktelemetry.model.{SparkMetricEvent, SqlExecutionMetrics, SqlTableIOMetrics}

import scala.collection.JavaConverters._

/**
 * Spark 3.x QueryExecutionListener adapter.
 * Captures SQL query-level and table-level metrics from executedPlan.
 *
 * Registration: via TelemetryDriverPlugin.init() using session.listenerManager.register()
 */
class SparkTelemetryQueryExecutionListener(confMap: Map[String, String]) extends QueryExecutionListener {

  private val lifecycle = TelemetryLifecycle.getInstance
  private val config = lifecycle.getConfig

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    if (!config.isCaptureSqlQueryExecution) return
    val (queryMetrics, tableMetrics) = extractMetrics(qe, durationNs, success = true, null)
    emitEvent(funcName, qe, queryMetrics, tableMetrics)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
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
        val tableMetric = new SqlTableIOMetrics
        tableMetric.setOperation("scan")
        tableMetric.setTableName(s.tableIdentifier.map(_.unquotedString).getOrElse("unknown"))
        tableMetric.setBytes(metricValue(s, "numBytesRead"))
        tableMetric.setRows(metricValue(s, "numOutputRows"))
        tableMetric.setFilesRead(metricValue(s, "numFilesRead"))
        tableMetric.setTimeMs(metricValue(s, "scanTime"))
        tm.add(tableMetric)

      case s: BatchScanExec =>
        val tableMetric = new SqlTableIOMetrics
        tableMetric.setOperation("scan")
        tableMetric.setTableName("unknown") // BatchScanExec.table not available in Spark 3.0
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

      case j: ShuffledHashJoinExec =>
        qm.setJoinCount(qm.getJoinCount + 1)
        qm.setJoinTypes(appendJoinType(qm.getJoinTypes, "hash"))

      case e: ShuffleExchangeExec =>
        val dataSize = metricValue(e, "dataSize")
        if (dataSize > 0) {
          qm.setShuffleBytesWritten(qm.getShuffleBytesWritten + dataSize)
          qm.setShuffleBytesRead(qm.getShuffleBytesRead + dataSize)
        }

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

    // AppId fallback
    try {
      val env = org.apache.spark.SparkEnv.get
      if (env != null && env.conf != null) {
        val appId = env.conf.getAppId
        event.setApplicationId(if (appId != null && appId.nonEmpty) appId else env.conf.get("spark.app.name", "unknown"))
        event.setApplicationName(env.conf.get("spark.app.name", ""))
      }
    } catch {
      case _: Exception =>
    }

    lifecycle.accept(event)
  }
}
