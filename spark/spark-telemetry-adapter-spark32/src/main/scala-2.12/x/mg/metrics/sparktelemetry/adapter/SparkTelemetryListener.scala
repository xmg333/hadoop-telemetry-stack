package x.mg.metrics.sparktelemetry.adapter

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerTaskEnd}
import x.mg.metrics.sparktelemetry.lifecycle.TelemetryLifecycle
import x.mg.metrics.sparktelemetry.model.{IOMetrics, SparkMetricEvent, TaskExecutionMetrics}

import scala.collection.JavaConverters._

/**
 * Spark 3.2.x Listener adapter.
 * Extracts TaskMetrics from SparkListenerTaskEnd and converts to SparkMetricEvent.
 *
 * For Spark 3.2.x: uses ShuffleWriteMetrics.bytesWritten (renamed from shuffleBytesWritten in 2.x)
 * Note: remoteReqsDuration is not available in Spark 3.2.x (added in 3.3.0)
 */
class SparkTelemetryListener(confMap: Map[String, String]) extends SparkListener {

  private val lifecycle = TelemetryLifecycle.getInstance
  private val config = lifecycle.getConfig

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    if (!config.isCaptureTaskEnd) return

    val taskMetrics = taskEnd.taskMetrics
    if (taskMetrics == null) return

    val event = new SparkMetricEvent
    event.setEventType(SparkMetricEvent.EventType.TASK_END)
    event.setTimestamp(System.currentTimeMillis())
    event.setStageId(taskEnd.stageId)
    event.setTaskSuccessful(taskEnd.reason.getClass.getSimpleName == "Success$" || taskEnd.reason.getClass.getName.contains("Success"))
    event.setTaskDurationMs(taskEnd.taskInfo.duration)
    event.setTaskId(taskEnd.taskInfo.taskId)
    event.setTaskAttemptNumber(taskEnd.taskInfo.attemptNumber)

    // Application info from SparkEnv
    try {
      val env = org.apache.spark.SparkEnv.get
      if (env != null) {
        event.setExecutorId(env.executorId)
        if (env.conf != null) {
          val appId = env.conf.getAppId
          event.setApplicationId(if (appId != null && appId.nonEmpty) appId else env.conf.get("spark.app.name", "unknown"))
          event.setApplicationName(env.conf.get("spark.app.name", ""))
          event.setUser(lifecycle.getUser)
          event.setQueue(lifecycle.getQueue)
        }
      }
    } catch {
      case _: Exception => // SparkEnv may not be available in some contexts
    }

    // Extract IO metrics
    val io = new IOMetrics

    // Input metrics
    if (taskMetrics.inputMetrics != null) {
      io.setBytesRead(taskMetrics.inputMetrics.bytesRead)
      io.setRecordsRead(taskMetrics.inputMetrics.recordsRead)
    }

    // Output metrics
    if (taskMetrics.outputMetrics != null) {
      io.setBytesWritten(taskMetrics.outputMetrics.bytesWritten)
      io.setRecordsWritten(taskMetrics.outputMetrics.recordsWritten)
    }

    // Shuffle read metrics
    if (taskMetrics.shuffleReadMetrics != null) {
      io.setShuffleRemoteBytesRead(taskMetrics.shuffleReadMetrics.remoteBytesRead)
      io.setShuffleLocalBytesRead(taskMetrics.shuffleReadMetrics.localBytesRead)
      io.setShuffleRemoteBlocksFetched(taskMetrics.shuffleReadMetrics.remoteBlocksFetched)
      io.setShuffleFetchWaitTime(taskMetrics.shuffleReadMetrics.fetchWaitTime)

      // Category 2: Extended shuffle
      // Note: remoteReqsDuration not available in Spark 3.2.x (added in 3.3.0)
      if (config.isCaptureTaskShuffleExtended) {
        io.setShuffleLocalBlocksFetched(taskMetrics.shuffleReadMetrics.localBlocksFetched)
        io.setShuffleRecordsRead(taskMetrics.shuffleReadMetrics.recordsRead)
        io.setShuffleRemoteBytesReadToDisk(taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk)
      }
    }

    // Shuffle write metrics (Spark 3.x API: bytesWritten, writeTime)
    if (taskMetrics.shuffleWriteMetrics != null) {
      io.setShuffleBytesWritten(taskMetrics.shuffleWriteMetrics.bytesWritten)
      io.setShuffleWriteTime(taskMetrics.shuffleWriteMetrics.writeTime)
      io.setShuffleRecordsWritten(taskMetrics.shuffleWriteMetrics.recordsWritten)
    }

    // Spill metrics
    io.setDiskBytesSpilled(taskMetrics.diskBytesSpilled)
    io.setMemoryBytesSpilled(taskMetrics.memoryBytesSpilled)

    event.setIoMetrics(io)

    // Category 1: Task execution metrics
    if (config.isCaptureTaskExecution) {
      val exec = new TaskExecutionMetrics
      exec.setExecutorRunTime(taskMetrics.executorRunTime)
      exec.setExecutorCpuTime(taskMetrics.executorCpuTime)
      exec.setExecutorDeserializeTime(taskMetrics.executorDeserializeTime)
      exec.setExecutorDeserializeCpuTime(taskMetrics.executorDeserializeCpuTime)
      exec.setResultSerializationTime(taskMetrics.resultSerializationTime)
      exec.setJvmGcTime(taskMetrics.jvmGCTime)
      val schedulerDelay = event.getTaskDurationMs -
        (taskMetrics.executorRunTime + taskMetrics.executorDeserializeTime +
         taskMetrics.resultSerializationTime + taskMetrics.jvmGCTime)
      exec.setSchedulerDelay(Math.max(0L, schedulerDelay))
      exec.setResultSize(taskMetrics.resultSize)
      exec.setPeakExecutionMemory(taskMetrics.peakExecutionMemory)
      event.setTaskExecutionMetrics(exec)
    }

    // Category 3: Task info attributes
    if (config.isCaptureTaskInfo) {
      event.setTaskHost(taskEnd.taskInfo.host)
      event.setTaskLocality(taskEnd.taskInfo.taskLocality.toString)
      event.setTaskSpeculative(taskEnd.taskInfo.speculative)
    }

    // Check filter
    if (config.shouldAcceptApp(event.getApplicationName)) {
      lifecycle.accept(event)
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    if (!config.isCaptureStageComplete && !config.isCaptureStageDetailed) return

    val stageInfo = stageCompleted.stageInfo
    val taskMetrics = stageInfo.taskMetrics
    if (taskMetrics == null) return

    val event = new SparkMetricEvent
    event.setEventType(SparkMetricEvent.EventType.STAGE_COMPLETE)
    event.setTimestamp(System.currentTimeMillis())
    event.setStageId(stageInfo.stageId)
    event.setStageAttemptNumber(stageInfo.attemptNumber())

    // Application info
    try {
      val env = org.apache.spark.SparkEnv.get
      if (env != null) {
        event.setExecutorId(env.executorId)
        if (env.conf != null) {
          event.setApplicationId(env.conf.getAppId)
          event.setApplicationName(env.conf.get("spark.app.name", ""))
          event.setUser(lifecycle.getUser)
          event.setQueue(lifecycle.getQueue)
        }
      }
    } catch {
      case _: Exception =>
    }

    if (config.isCaptureStageComplete) {
      val io = new IOMetrics
      if (taskMetrics.inputMetrics != null) {
        io.setBytesRead(taskMetrics.inputMetrics.bytesRead)
        io.setRecordsRead(taskMetrics.inputMetrics.recordsRead)
      }
      if (taskMetrics.outputMetrics != null) {
        io.setBytesWritten(taskMetrics.outputMetrics.bytesWritten)
        io.setRecordsWritten(taskMetrics.outputMetrics.recordsWritten)
      }
      if (taskMetrics.shuffleReadMetrics != null) {
        io.setShuffleRemoteBytesRead(taskMetrics.shuffleReadMetrics.remoteBytesRead)
        io.setShuffleLocalBytesRead(taskMetrics.shuffleReadMetrics.localBytesRead)
      }
      if (taskMetrics.shuffleWriteMetrics != null) {
        io.setShuffleBytesWritten(taskMetrics.shuffleWriteMetrics.bytesWritten)
        io.setShuffleWriteTime(taskMetrics.shuffleWriteMetrics.writeTime)
        io.setShuffleRecordsWritten(taskMetrics.shuffleWriteMetrics.recordsWritten)
      }
      io.setDiskBytesSpilled(taskMetrics.diskBytesSpilled)
      io.setMemoryBytesSpilled(taskMetrics.memoryBytesSpilled)
      event.setIoMetrics(io)
    }

    // Category 4: Stage detailed metrics
    if (config.isCaptureStageDetailed) {
      event.setStageNumTasks(stageInfo.numTasks)
      val sub = stageInfo.submissionTime.getOrElse(0L)
      val comp = stageInfo.completionTime.getOrElse(0L)
      if (sub > 0 && comp > 0) {
        event.setStageDurationMs(comp - sub)
      }
      val exec = new TaskExecutionMetrics
      exec.setExecutorRunTime(taskMetrics.executorRunTime)
      exec.setExecutorCpuTime(taskMetrics.executorCpuTime)
      exec.setJvmGcTime(taskMetrics.jvmGCTime)
      exec.setPeakExecutionMemory(taskMetrics.peakExecutionMemory)
      event.setTaskExecutionMetrics(exec)
    }

    if (config.shouldAcceptApp(event.getApplicationName)) {
      lifecycle.accept(event)
    }
  }

  // Category 5: Job lifecycle
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    if (!config.isCaptureJobLifecycle) return

    val event = new SparkMetricEvent
    event.setEventType(SparkMetricEvent.EventType.JOB_START)
    event.setTimestamp(System.currentTimeMillis())
    event.setJobId(jobStart.jobId)
    event.setJobNumStages(jobStart.stageIds.size)

    try {
      val env = org.apache.spark.SparkEnv.get
      if (env != null) {
        event.setExecutorId(env.executorId)
        if (env.conf != null) {
          event.setApplicationId(env.conf.getAppId)
          event.setApplicationName(env.conf.get("spark.app.name", ""))
          event.setUser(lifecycle.getUser)
          event.setQueue(lifecycle.getQueue)
        }
      }
    } catch {
      case _: Exception =>
    }

    if (config.shouldAcceptApp(event.getApplicationName)) {
      lifecycle.accept(event)
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    if (config.isCaptureJobLifecycle) {
      val event = new SparkMetricEvent
      event.setEventType(SparkMetricEvent.EventType.JOB_END)
      event.setTimestamp(System.currentTimeMillis())
      event.setJobId(jobEnd.jobId)
      event.setJobSuccessful(jobEnd.jobResult.getClass.getSimpleName == "JobSucceeded$" ||
        jobEnd.jobResult.getClass.getName.contains("JobSucceeded"))

      try {
        val env = org.apache.spark.SparkEnv.get
        if (env != null) {
          event.setExecutorId(env.executorId)
          if (env.conf != null) {
            event.setApplicationId(env.conf.getAppId)
            event.setApplicationName(env.conf.get("spark.app.name", ""))
            event.setUser(lifecycle.getUser)
            event.setQueue(lifecycle.getQueue)
          }
        }
      } catch {
        case _: Exception =>
      }

      if (config.shouldAcceptApp(event.getApplicationName)) {
        lifecycle.accept(event)
      }
    }

    // Flush metrics after job end to ensure short-lived jobs don't lose data.
    // Must run regardless of isCaptureJobLifecycle — task/stage metrics also need flushing.
    lifecycle.flushAsync()
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case sqlStart: org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart =>
        if (sqlStart.description != null && sqlStart.description.nonEmpty) {
          lifecycle.putSqlText(sqlStart.executionId, sqlStart.description)
        }
      case _ =>
    }
  }
}
