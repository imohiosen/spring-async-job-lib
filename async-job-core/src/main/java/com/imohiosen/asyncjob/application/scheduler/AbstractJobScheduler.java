package com.imohiosen.asyncjob.application.scheduler;

import com.imohiosen.asyncjob.domain.BackoffPolicy;
import com.imohiosen.asyncjob.domain.JobSubmissionRequest;
import com.imohiosen.asyncjob.application.service.JobSubmissionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Abstract base for all job schedulers in the async job library.
 *
 * <p>Delegates to {@link JobSubmissionService} for the actual job creation and
 * task fan-out. Subclasses define <em>what</em> to run; this base class plus
 * the submission service handle <em>how</em> it gets executed.
 *
 * <h2>How to extend</h2>
 * <pre>{@code
 * @Component
 * public class InvoiceJobScheduler extends AbstractJobScheduler {
 *
 *     public InvoiceJobScheduler(JobSubmissionService s) {
 *         super(s);
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

    private final JobSubmissionService submissionService;

    protected AbstractJobScheduler(JobSubmissionService submissionService) {
        this.submissionService = submissionService;
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

    /**
     * Override to schedule the job for a future time instead of immediate execution.
     * Returns {@code null} by default, meaning the job runs immediately.
     */
    protected OffsetDateTime getScheduledAt() {
        return null;
    }

    // ── Trigger ───────────────────────────────────────────────────────────────

    /**
     * The concrete subclass annotates its override with {@code @Scheduled} and
     * {@code @SchedulerLock}. ShedLock ensures exactly-once execution across nodes.
     *
     * <p>Builds a {@link JobSubmissionRequest} from the abstract methods and
     * delegates to {@link JobSubmissionService#submit(JobSubmissionRequest)}.
     */
    public void trigger() {
        UUID jobId = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now();

        log.info("Triggering job name={}", getJobName());

        JobTriggerContext ctx = new JobTriggerContext(jobId, now);
        List<String> payloads = buildTaskPayloads(ctx);

        if (payloads == null || payloads.isEmpty()) {
            log.info("Job name={} produced no tasks — skipping", getJobName());
            return;
        }

        JobSubmissionRequest request = new JobSubmissionRequest(
                getJobName(),
                getDestination(),
                getTaskType(),
                payloads,
                getDeadlineMs(),
                getTaskDeadlineMs(),
                backoffPolicy(),
                getScheduledAt(),
                null,
                null,
                false,
                null
        );

        submissionService.submit(request);
    }
}
