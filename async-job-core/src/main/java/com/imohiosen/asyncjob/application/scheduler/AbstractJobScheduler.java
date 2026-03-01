package com.imohiosen.asyncjob.application.scheduler;

import com.imohiosen.asyncjob.domain.BackoffPolicy;
import com.imohiosen.asyncjob.domain.Job;
import com.imohiosen.asyncjob.domain.JobStatus;
import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskStatus;
import com.imohiosen.asyncjob.port.messaging.JobMessage;
import com.imohiosen.asyncjob.port.messaging.JobMessageProducer;
import com.imohiosen.asyncjob.port.repository.JobRepository;
import com.imohiosen.asyncjob.port.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Abstract base for all job schedulers in the async job library.
 *
 * <h2>How to extend</h2>
 * <pre>{@code
 * @Component
 * public class InvoiceJobScheduler extends AbstractJobScheduler {
 *
 *     public InvoiceJobScheduler(JobRepository r, TaskRepository t, JobMessageProducer p) {
 *         super(r, t, p);
 *     }
 *
 *     @Override protected String getJobName()     { return "invoice-job"; }
 *     @Override protected long   getDeadlineMs()  { return 3_600_000L; }
 *     @Override protected String getDestination() { return "invoice-tasks"; }
 *     @Override protected String getTaskType()    { return "INVOICE_GENERATION"; }
 *
 *     @Override
 *     protected List<String> buildTaskPayloads(JobTriggerContext ctx) {
 *         return invoiceService.getPendingIds().stream()
 *             .map(id -> "{\"invoiceId\":\"" + id + "\"}")
 *             .toList();
 *     }
 *
 *     @Scheduled(cron = "${invoice.job.cron:0 0 2 * * *}")
 *     @SchedulerLock(name = "invoice-job", lockAtMostFor = "PT1H", lockAtLeastFor = "PT30S")
 *     @Override
 *     public void trigger() { super.trigger(); }
 * }
 * }</pre>
 */
public abstract class AbstractJobScheduler {

    private static final Logger log = LoggerFactory.getLogger(AbstractJobScheduler.class);

    private final JobRepository        jobRepository;
    private final TaskRepository       taskRepository;
    private final JobMessageProducer   messageProducer;

    protected AbstractJobScheduler(JobRepository jobRepository,
                                   TaskRepository taskRepository,
                                   JobMessageProducer messageProducer) {
        this.jobRepository   = jobRepository;
        this.taskRepository  = taskRepository;
        this.messageProducer = messageProducer;
    }

    // ── Contract ──────────────────────────────────────────────────────────────

    /** Unique logical name used for logging and job row identification. */
    protected abstract String getJobName();

    /** Wall-clock duration (ms) before the job is considered stuck and flagged by the deadline guard. */
    protected abstract long getDeadlineMs();

    /** Destination for task messages (e.g. Kafka topic, RabbitMQ queue, Redis stream key). */
    protected abstract String getDestination();

    /** Task type discriminator written to each job_tasks row. */
    protected abstract String getTaskType();

    /**
     * Builds the list of JSON payloads to fan out as individual tasks.
     * Called once per trigger. Each returned string becomes one message
     * and one {@code job_tasks} row.
     *
     * @param ctx context carrying the jobId and trigger timestamp
     * @return list of JSON payload strings (must not be null or empty)
     */
    protected abstract List<String> buildTaskPayloads(JobTriggerContext ctx);

    /**
     * Override to customise the backoff policy applied to all tasks produced by this scheduler.
     * Defaults to {@link BackoffPolicy#DEFAULT}.
     */
    protected BackoffPolicy backoffPolicy() {
        return BackoffPolicy.DEFAULT;
    }

    /**
     * Override to customise per-task deadline relative to the job's trigger time.
     * Defaults to {@link #getDeadlineMs()} (same as the job deadline).
     */
    protected long getTaskDeadlineMs() {
        return getDeadlineMs();
    }

    // ── Trigger ───────────────────────────────────────────────────────────────

    /**
     * The concrete subclass annotates its override with {@code @Scheduled} and
     * {@code @SchedulerLock}. ShedLock ensures exactly-once execution across nodes.
     */
    public void trigger() {
        UUID jobId       = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime deadline = now.plusNanos(getDeadlineMs() * 1_000_000L);

        log.info("Triggering job={} name={} deadline={}", jobId, getJobName(), deadline);

        // 1. Insert the parent job row
        Job job = new Job(jobId, getJobName(), null, JobStatus.PENDING,
                now, now, null, null, deadline, false,
                0, 0, 0, 0, 0, 0, null);
        jobRepository.insert(job);

        // 2. Build payloads
        JobTriggerContext ctx = new JobTriggerContext(jobId, now);
        List<String> payloads = buildTaskPayloads(ctx);

        if (payloads == null || payloads.isEmpty()) {
            log.info("Job={} produced no tasks — marking COMPLETED immediately", jobId);
            jobRepository.updateStatus(jobId, JobStatus.COMPLETED);
            return;
        }

        BackoffPolicy policy = backoffPolicy();
        OffsetDateTime taskDeadline = now.plusNanos(getTaskDeadlineMs() * 1_000_000L);
        String destination = getDestination();

        // 3. Fan out: insert task rows + publish messages
        int produced = 0;
        for (String payload : payloads) {
            UUID taskId = UUID.randomUUID();

            // Insert job_task row
            JobTask task = new JobTask(
                    taskId, jobId, getTaskType(), destination,
                    null, null, TaskStatus.PENDING, now, now,
                    null, null, taskDeadline, false, 0,
                    null, null,
                    policy.baseIntervalMs(), policy.multiplier(), policy.maxDelayMs(),
                    null, null, null, null, null, payload, null);
            taskRepository.insert(task);

            // Publish to messaging system
            messageProducer.publish(destination,
                    new JobMessage(taskId, jobId, getTaskType(), payload));
            produced++;
        }

        // 4. Update job counters and mark IN_PROGRESS
        jobRepository.updateCounters(jobId);
        jobRepository.markStarted(jobId);

        log.info("Job={} started with {} tasks", jobId, produced);
    }
}
