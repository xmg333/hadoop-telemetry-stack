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

  private lazy val hiveTableScanClass: Option[Class[_]] = try {
    Some(Class.forName("org.apache.spark.sql.hive.execution.HiveTableScanExec"))
  } catch {
    case _: ClassNotFoundException => None
  }

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

    // Deduplicate table metrics: same (tableName, operation) should appear once
    val deduped = new java.util.ArrayList[SqlTableIOMetrics]()
    val seen = new java.util.HashSet[String]()
    for (i <- 0 until tableMetrics.size()) {
      val m = tableMetrics.get(i)
      val key = m.getTableName + "|" + m.getOperation
      if (seen.add(key)) {
        deduped.add(m)
      }
    }

    (queryMetrics, deduped)
  }

  private def collectPlanMetrics(plan: SparkPlan, qm: SqlExecutionMetrics, tm: java.util.List[SqlTableIOMetrics]): Unit = {
    plan foreach {
      case s: FileSourceScanExec =>
        // Spark 2.x: FileSourceScanExt has public relation field
        val tableMetric = new SqlTableIOMetrics
        tableMetric.setOperation("scan")
        tableMetric.setTableName(extractFileScanTableName(s))
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

      case w: DataWritingCommandExec =>
        val tableMetric = new SqlTableIOMetrics
        tableMetric.setOperation("write")
        tableMetric.setTableName(resolveWriteTableName(w.cmd))
        tableMetric.setRows(metricValue(w, "numOutputRows"))
        tableMetric.setBytes(metricValue(w, "numOutputBytes"))
        tm.add(tableMetric)

      case _ =>
    }
  }

  private def metricValue(plan: SparkPlan, name: String): Long = {
    plan.metrics.get(name).map(_.value).getOrElse(0L)
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

  private def extractFileScanTableName(scan: FileSourceScanExec): String = {
    try {
      val roots = scan.relation.location.rootPaths
      if (roots != null && roots.nonEmpty) roots.map(_.toString).mkString(",")
      else "unknown"
    } catch {
      case _: Exception => "unknown"
    }
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

  private def catalogTableToName(obj: Any): String = {
    val ident = obj.getClass.getMethod("identifier").invoke(obj)
    ident.getClass.getMethod("unquotedString").invoke(ident).asInstanceOf[String]
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

    event.setUser(lifecycle.getUser)
    event.setQueue(lifecycle.getQueue)

    lifecycle.accept(event)
  }
}
