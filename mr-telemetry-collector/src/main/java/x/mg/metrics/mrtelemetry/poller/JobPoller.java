package x.mg.metrics.mrtelemetry.poller;

import x.mg.metrics.mrtelemetry.config.MRCollectorConfig;
import x.mg.metrics.mrtelemetry.model.MRJobMetrics;
import x.mg.metrics.mrtelemetry.otel.MRMetricRecorder;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Periodically polls MR History Server for completed jobs and records metrics.
 */
public class JobPoller {
    private static final Logger LOG = Logger.getLogger(JobPoller.class.getName());

    private final MRCollectorConfig config;
    private final HistoryServerClient client;
    private final CounterExtractor extractor;
    private final MRMetricRecorder recorder;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private long lastPollTime = 0;

    public JobPoller(MRCollectorConfig config, HistoryServerClient client,
                     CounterExtractor extractor, MRMetricRecorder recorder) {
        this.config = config;
        this.client = client;
        this.extractor = extractor;
        this.recorder = recorder;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "mr-job-poller");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            loadState();
            int interval = config.getPollIntervalSecs();
            scheduler.scheduleAtFixedRate(this::poll, 0, interval, TimeUnit.SECONDS);
            LOG.info("JobPoller started: interval=" + interval + "s, historyServer=" + config.getHistoryServerUrl());
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            LOG.info("JobPoller stopped");
        }
    }

    private void poll() {
        if (!running.get()) return;
        try {
            String json = client.getJobs();
            List<CounterExtractor.JobInfo> jobs = extractor.parseJobs(json);
            long newMaxFinishTime = lastPollTime;

            for (CounterExtractor.JobInfo job : jobs) {
                if (job.finishTime <= lastPollTime) continue;
                if (!config.shouldAcceptJob(job.name, job.user)) continue;

                LOG.info("Processing job: " + job.id + " (" + job.name + ") state=" + job.state);

                MRJobMetrics metrics = new MRJobMetrics();
                metrics.setJobId(job.id);
                metrics.setJobName(job.name);
                metrics.setUser(job.user);
                metrics.setQueue(job.queue);
                metrics.setState(job.state);
                metrics.setSubmitTime(job.submitTime);
                metrics.setStartTime(job.startTime);
                metrics.setFinishTime(job.finishTime);
                metrics.setElapsedTime(job.elapsedTime);
                metrics.setTotalMaps(job.totalMaps);
                metrics.setTotalReduces(job.totalReduces);

                if (config.isJobCounters()) {
                    try {
                        String countersJson = client.getJobCounters(job.id);
                        extractor.populateCounters(metrics, countersJson);
                    } catch (Exception e) {
                        LOG.log(Level.WARNING, "Failed to get counters for job " + job.id + ": " + e.getMessage());
                    }
                }

                recorder.record(metrics);

                if (config.isTaskCounters()) {
                    try {
                        String tasksJson = client.getTasks(job.id);
                        List<CounterExtractor.TaskInfo> tasks = extractor.parseTasks(tasksJson);
                        for (CounterExtractor.TaskInfo task : tasks) {
                            try {
                                String taskCountersJson = client.getTaskCounters(job.id, task.id);
                                Map<String, Long> counters = extractor.extractTaskCounters(taskCountersJson);
                                recorder.recordTask(task.id, task.type, job.id, job.name,
                                        job.user, job.state, job.finishTime, counters);
                            } catch (Exception e) {
                                LOG.log(Level.FINE, "Failed to get counters for task " + task.id
                                        + " in job " + job.id + ": " + e.getMessage());
                            }
                        }
                    } catch (Exception e) {
                        LOG.log(Level.WARNING, "Failed to get tasks for job " + job.id + ": " + e.getMessage());
                    }
                }

                if (job.finishTime > newMaxFinishTime) {
                    newMaxFinishTime = job.finishTime;
                }
            }

            if (newMaxFinishTime > lastPollTime) {
                lastPollTime = newMaxFinishTime;
                saveState();
            }
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Poll failed: " + e.getMessage(), e);
        }
    }

    private void loadState() {
        try {
            Path path = Paths.get(config.getStateFile());
            if (Files.exists(path)) {
                String content = new String(Files.readAllBytes(path), "UTF-8");
                lastPollTime = Long.parseLong(content.trim());
                LOG.info("Loaded state: lastPollTime=" + lastPollTime);
            }
        } catch (Exception e) {
            LOG.log(Level.FINE, "No state file found, starting fresh");
            lastPollTime = System.currentTimeMillis();
        }
    }

    private void saveState() {
        try {
            Path path = Paths.get(config.getStateFile());
            Files.write(path, String.valueOf(lastPollTime).getBytes("UTF-8"));
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Failed to save state: " + e.getMessage());
        }
    }
}
