package x.mg.metrics.sparktelemetry.adapter

import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.util.QueryExecutionListener
import x.mg.metrics.sparktelemetry.lifecycle.TelemetryLifecycle
import x.mg.metrics.sparktelemetry.model.{SparkMetricEvent, SqlExecutionMetrics, SqlTableIOMetrics}

import scala.jdk.CollectionConverters._

/**
 * Spark 4.x QueryExecutionListener adapter.
 * Same interface as Spark 3.x but compiled against Spark 4.x + Scala 2.13.
 */
class SparkTelemetryQueryExecutionListener(confMap: Map[String, String]) extends QueryExecutionListener {

  private val lifecycle = TelemetryLifecycle.getInstance
  private val config = lifecycle.getConfig

  private lazy val hiveTableScanClass: Option[Class[_]] = try {
    Some(Class.forName("org.apache.spark.sql.hive.execution.HiveTableScanExec"))
  } catch {
    case _: ClassNotFoundException => None
  }

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

    // Fix executionId (was hardcoded 0)
    queryMetrics.setExecutionId(qe.id)

    // Look up SQL text from shared cache (populated by SparkTelemetryListener.onOtherEvent)
    var sqlText: String = lifecycle.getAndRemoveSqlText(qe.id)
    if (sqlText == null || sqlText.isEmpty) {
      sqlText = extractSqlText(qe)
    }
    if (sqlText != null && sqlText.nonEmpty) {
      val maxLen = config.getSqlMaxLength
      queryMetrics.setQueryText(if (sqlText.length > maxLen) sqlText.substring(0, maxLen) else sqlText)
    }

    if (ex != null) queryMetrics.setErrorMessage(if (ex.getMessage != null) ex.getMessage else ex.getClass.getName)
    queryMetrics.setDurationMs(durationNs / 1000000)

    val tableMetrics = new java.util.ArrayList[SqlTableIOMetrics]()
    collectPlanMetrics(qe.executedPlan, queryMetrics, tableMetrics)

    // Propagate executionId to table metrics
    tableMetrics.asScala.foreach(_.setExecutionId(qe.id))

    (queryMetrics, tableMetrics)
  }

  /**
   * Traverse the physical plan tree, unwrapping AQE nodes.
   *
   * Spark 3.x/4.x AQE: AdaptiveSparkPlanExec and QueryStageExec extend LeafExecNode (children=Nil),
   * which makes the default `plan foreach` skip all inner plan nodes.
   * We must extract their inner plans manually to reach joins, scans, and exchanges.
   */
  private def collectPlanMetrics(plan: SparkPlan, qm: SqlExecutionMetrics, tm: java.util.List[SqlTableIOMetrics]): Unit = {
    visit(plan, qm, tm)
  }

  private def visit(plan: SparkPlan, qm: SqlExecutionMetrics, tm: java.util.List[SqlTableIOMetrics]): Unit = {
    // Unwrap AQE wrappers that hide inner plans
    plan match {
      case a: AdaptiveSparkPlanExec =>
        // AdaptiveSparkPlanExec extends LeafExecNode, children=Nil
        // inputPlan() returns the INITIAL plan (before AQE optimizations).
        // currentPhysicalPlan (private[sql]) holds the FINAL plan with
        // ShuffleQueryStageExec nodes that contain actual shuffle stats.
        try {
          val method = a.getClass.getDeclaredMethod("currentPhysicalPlan")
          method.setAccessible(true)
          val finalPlan = method.invoke(a).asInstanceOf[SparkPlan]
          if (finalPlan != null) visit(finalPlan, qm, tm)
          else visit(a.inputPlan, qm, tm)
        } catch {
          case _: Exception => visit(a.inputPlan, qm, tm)
        }
        return
      case sqs: ShuffleQueryStageExec =>
        // ShuffleQueryStageExec holds shuffle stats (bytesWritten) that are
        // NOT available on the inner ShuffleExchangeExec node in AQE mode.
        // mapOutputStatistics is private[spark], so use reflection.
        try {
          val method = sqs.getClass.getDeclaredMethod("mapOutputStatistics")
          method.setAccessible(true)
          val stats = method.invoke(sqs)
          if (stats != null) {
            val bytesField = stats.getClass.getDeclaredField("bytesWritten")
            bytesField.setAccessible(true)
            val bytes = bytesField.getLong(stats)
            if (bytes > 0) {
              qm.setShuffleBytesWritten(qm.getShuffleBytesWritten + bytes)
              qm.setShuffleBytesRead(qm.getShuffleBytesRead + bytes)
            }
          }
        } catch { case _: Exception => }
        visit(sqs.plan, qm, tm)
        return
      case q: QueryStageExec =>
        // QueryStageExec extends LeafExecNode, children=Nil
        // Use public plan() method to get the inner plan
        visit(q.plan, qm, tm)
        return
      case _ =>
    }

    // Handle the current node
    plan match {
      case s: FileSourceScanExec =>
        val tableMetric = new SqlTableIOMetrics
        tableMetric.setOperation("scan")
        tableMetric.setTableName(s.tableIdentifier.map(_.unquotedString).getOrElse(extractFileScanTableName(s)))
        tableMetric.setBytes(metricValue(s, "numBytesRead"))
        tableMetric.setRows(metricValue(s, "numOutputRows"))
        tableMetric.setFilesRead(metricValue(s, "numFilesRead"))
        tableMetric.setTimeMs(metricValue(s, "scanTime"))
        tm.add(tableMetric)

      case s: BatchScanExec =>
        val tableMetric = new SqlTableIOMetrics
        tableMetric.setOperation("scan")
        tableMetric.setTableName(extractBatchScanTableName(s))
        tableMetric.setBytes(metricValue(s, "numBytesRead"))
        tableMetric.setRows(metricValue(s, "numOutputRows"))
        tableMetric.setFilesRead(metricValue(s, "numFilesRead"))
        tableMetric.setTimeMs(metricValue(s, "scanTime"))
        tm.add(tableMetric)

      case s if hiveTableScanClass.exists(_.isInstance(s)) =>
        val tableMetric = new SqlTableIOMetrics
        tableMetric.setOperation("scan")
        tableMetric.setTableName(extractHiveScanTableName(s))
        tableMetric.setRows(metricValue(s, "numOutputRows"))
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
        val tableMetric = new SqlTableIOMetrics
        tableMetric.setOperation("write")
        tableMetric.setTableName(resolveWriteTableName(w.cmd))
        tableMetric.setRows(metricValue(w, "numOutputRows"))
        tableMetric.setBytes(metricValue(w, "numOutputBytes"))
        tm.add(tableMetric)

      case _ =>
    }

    // Recurse into children
    plan.children.foreach(c => visit(c, qm, tm))
  }

  private def metricValue(plan: SparkPlan, name: String): Long = {
    plan.metrics.get(name).map(_.value).getOrElse(0L)
  }

  private def extractFileScanTableName(scan: FileSourceScanExec): String = {
    try {
      val roots = scan.relation.location.rootPaths
      if (roots != null && roots.nonEmpty) roots.map(_.toString).mkString(",")
      else "unknown"
    } catch {
      case _: Exception => "unknown"
    }
  }

  private def resolveWriteTableName(cmd: org.apache.spark.sql.execution.command.DataWritingCommand): String = {
    cmd match {
      case i: InsertIntoHadoopFsRelationCommand =>
        i.options.getOrElse("path", "unknown")
      case _ =>
        try {
          val tableField = cmd.getClass.getDeclaredField("table")
          tableField.setAccessible(true)
          catalogTableToName(tableField.get(cmd))
        } catch {
          case _: Exception =>
            try {
              val tdField = cmd.getClass.getDeclaredField("tableDesc")
              tdField.setAccessible(true)
              catalogTableToName(tdField.get(cmd))
            } catch {
              case _: Exception => "unknown"
            }
        }
    }
  }

  private def catalogTableToName(obj: Any): String = {
    val ident = obj.getClass.getMethod("identifier").invoke(obj)
    ident.getClass.getMethod("unquotedString").invoke(ident).asInstanceOf[String]
  }

  private def extractHiveScanTableName(scan: SparkPlan): String = {
    try {
      val relation = scan.getClass.getMethod("relation").invoke(scan)
      val tableMeta = relation.getClass.getMethod("tableMeta").invoke(relation)
      catalogTableToName(tableMeta)
    } catch {
      case _: Exception => "unknown"
    }
  }

  private def extractBatchScanTableName(scan: BatchScanExec): String = {
    try {
      val table = scan.table
      if (table != null && table.name != null) table.name
      else if (table != null && table.properties != null) table.properties.asScala.getOrElse("path", "unknown")
      else "unknown"
    } catch {
      case _: Exception => "unknown"
    }
  }

  private def appendJoinType(current: String, joinType: String): String = {
    if (current == null || current.isEmpty) joinType else current + "," + joinType
  }

  private def extractSqlText(qe: QueryExecution): String = {
    try {
      val desc = qe.logical.toString
      if (desc != null && desc.nonEmpty && desc.length < config.getSqlMaxLength * 2) desc
      else null
    } catch {
      case _: Exception => null
    }
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
      if (env != null && env.conf != null) {
        val appId = env.conf.getAppId
        event.setApplicationId(if (appId != null && appId.nonEmpty) appId else env.conf.get("spark.app.name", "unknown"))
        event.setApplicationName(env.conf.get("spark.app.name", ""))
      }
    } catch {
      case _: Exception =>
    }

    event.setUser(lifecycle.getUser)
    event.setQueue(lifecycle.getQueue)

    lifecycle.accept(event)
  }
}
