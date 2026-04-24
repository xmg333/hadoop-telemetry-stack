package x.mg.metrics.sparktelemetry.adapter

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerTaskEnd}
import x.mg.metrics.sparktelemetry.lifecycle.TelemetryLifecycle
import x.mg.metrics.sparktelemetry.model.{IOMetrics, SparkMetricEvent, TaskExecutionMetrics}

import scala.collection.JavaConverters._

class SparkTelemetryListener(confMap: Map[String, String]) extends SparkListener {

  private val lifecycle = TelemetryLifecycle.getInstance
  private val config = lifecycle.getConfig

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = try {
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
      case _: Exception =>
    }

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
      io.setShuffleRemoteBlocksFetched(taskMetrics.shuffleReadMetrics.remoteBlocksFetched)
      io.setShuffleFetchWaitTime(taskMetrics.shuffleReadMetrics.fetchWaitTime)
      if (config.isCaptureTaskShuffleExtended) {
        io.setShuffleLocalBlocksFetched(taskMetrics.shuffleReadMetrics.localBlocksFetched)
        io.setShuffleRecordsRead(taskMetrics.shuffleReadMetrics.recordsRead)
        io.setShuffleRemoteBytesReadToDisk(taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk)
        io.setShuffleRemoteReqsDuration(taskMetrics.shuffleReadMetrics.remoteReqsDuration)
      }
    }
    if (taskMetrics.shuffleWriteMetrics != null) {
      io.setShuffleBytesWritten(taskMetrics.shuffleWriteMetrics.bytesWritten)
      io.setShuffleWriteTime(taskMetrics.shuffleWriteMetrics.writeTime)
      io.setShuffleRecordsWritten(taskMetrics.shuffleWriteMetrics.recordsWritten)
    }
    io.setDiskBytesSpilled(taskMetrics.diskBytesSpilled)
    io.setMemoryBytesSpilled(taskMetrics.memoryBytesSpilled)
    event.setIoMetrics(io)

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

    if (config.isCaptureTaskInfo) {
      event.setTaskHost(taskEnd.taskInfo.host)
      event.setTaskLocality(taskEnd.taskInfo.taskLocality.toString)
      event.setTaskSpeculative(taskEnd.taskInfo.speculative)
    }

    if (config.shouldAcceptApp(event.getApplicationName)) {
      lifecycle.accept(event)
    }
  } catch {
    case e: VirtualMachineError => throw e
    case _: Throwable =>
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = try {
    if (!config.isCaptureStageComplete && !config.isCaptureStageDetailed) return

    val stageInfo = stageCompleted.stageInfo
    val taskMetrics = stageInfo.taskMetrics
    if (taskMetrics == null) return

    val event = new SparkMetricEvent
    event.setEventType(SparkMetricEvent.EventType.STAGE_COMPLETE)
    event.setTimestamp(System.currentTimeMillis())
    event.setStageId(stageInfo.stageId)
    event.setStageAttemptNumber(stageInfo.attemptNumber())

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
  } catch {
    case e: VirtualMachineError => throw e
    case _: Throwable =>
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = try {
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
          val appId = env.conf.getAppId
          event.setApplicationId(if (appId != null && appId.nonEmpty) appId else env.conf.get("spark.app.name", "unknown"))
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
  } catch {
    case e: VirtualMachineError => throw e
    case _: Throwable =>
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = try {
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
            val appId = env.conf.getAppId
            event.setApplicationId(if (appId != null && appId.nonEmpty) appId else env.conf.get("spark.app.name", "unknown"))
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

    lifecycle.flushAsync()
  } catch {
    case e: VirtualMachineError => throw e
    case _: Throwable =>
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = try {
    event match {
      case sqlStart: org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart =>
        if (sqlStart.description != null && sqlStart.description.nonEmpty) {
          lifecycle.putSqlText(sqlStart.executionId, sqlStart.description)
        }
      case _ =>
    }
  } catch {
    case e: VirtualMachineError => throw e
    case _: Throwable =>
  }
}
