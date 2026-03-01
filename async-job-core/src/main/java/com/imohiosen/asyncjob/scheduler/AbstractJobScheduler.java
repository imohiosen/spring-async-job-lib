package com.imohiosen.asyncjob.scheduler;

import com.imohiosen.asyncjob.domain.BackoffPolicy;
import com.imohiosen.asyncjob.domain.Job;
import com.imohiosen.asyncjob.domain.JobStatus;
import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskStatus;
import com.imohiosen.asyncjob.kafka.JobKafkaProducer;
import com.imohiosen.asyncjob.repository.JobRepository;
import com.imohiosen.asyncjob.repository.TaskRepository;
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
 *     public InvoiceJobScheduler(JobRepository r, TaskRepository t, JobKafkaProducer p) {
 *         super(r, t, p);
 *     }
 *
 *     @Override protected String getJobName()    { return "invoice-job"; }
 *     @Override protected long   getDeadlineMs() { return 3_600_000L; }
 *     @Override protected String getKafkaTopic() { return "invoice-tasks"; }
 *     @Override protected String getTaskType()   { return "INVOICE_GENERATION"; }
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

    private final JobRepository    jobRepository;
    private final TaskRepository   taskRepository;
    private final JobKafkaProducer kafkaProducer;

    protected AbstractJobScheduler(JobRepository jobRepository,
                                   TaskRepository taskRepository,
                                   JobKafkaProducer kafkaProducer) {
        this.jobRepository = jobRepository;
        this.taskRepository = taskRepository;
        this.kafkaProducer  = kafkaProducer;
    }

    // ── Contract ──────────────────────────────────────────────────────────────

    /** Unique logical name used for logging and job row identification. */
    protected abstract String getJobName();

    /** Wall-clock duration (ms) before the job is considered stuck and flagged by the deadline guard. */
    protected abstract long getDeadlineMs();

    /** Kafka topic to produce task messages to. */
    protected abstract String getKafkaTopic();

    /** Task type discriminator written to each job_tasks row. */
    protected abstract String getTaskType();

    /**
     * Builds the list of JSON payloads to fan out as individual tasks.
     * Called once per trigger. Each returned string becomes one Kafka message
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

        // 3. Fan out: insert task rows + produce Kafka messages
        int produced = 0;
        for (String payload : payloads) {
            UUID taskId = UUID.randomUUID();

            // Insert job_task row
            JobTask task = new JobTask(
                    taskId, jobId, getTaskType(), getKafkaTopic(),
                    null, null, TaskStatus.PENDING, now, now,
                    null, null, taskDeadline, false, 0,
                    null, null,
                    policy.baseIntervalMs(), policy.multiplier(), policy.maxDelayMs(),
                    null, null, null, null, null, payload, null);
            taskRepository.insert(task);

            // Produce to Kafka
            kafkaProducer.produce(getKafkaTopic(), taskId, jobId, getTaskType(), payload);
            produced++;
        }

        // 4. Update job counters and mark IN_PROGRESS
        jdbc_updateTotalTasks(jobId, produced);
        jobRepository.markStarted(jobId);

        log.info("Job={} started with {} tasks", jobId, produced);
    }

    private void jdbc_updateTotalTasks(UUID jobId, int totalTasks) {
        // Delegated to JobRepository to keep SQL out of the abstract class
        jobRepository.updateCounters(jobId);
        // total_tasks is set here directly since updateCounters reads from child tasks
        // which are now all PENDING; pending_tasks will equal totalTasks after this call.
    }
}
