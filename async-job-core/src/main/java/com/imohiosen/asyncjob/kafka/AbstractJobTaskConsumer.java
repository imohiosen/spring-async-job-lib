package com.imohiosen.asyncjob.kafka;

import com.imohiosen.asyncjob.domain.BackoffPolicy;
import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskResult;
import com.imohiosen.asyncjob.domain.TaskStatus;
import com.imohiosen.asyncjob.executor.AsyncTaskExecutorBridge;
import com.imohiosen.asyncjob.lock.FencedLock;
import com.imohiosen.asyncjob.lock.TaskLockManager;
import com.imohiosen.asyncjob.repository.JobRepository;
import com.imohiosen.asyncjob.repository.TaskRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Abstract base for all Kafka task consumers in the async job library.
 *
 * <h2>How to extend</h2>
 * <pre>{@code
 * @Component
 * public class InvoiceTaskConsumer extends AbstractJobTaskConsumer {
 *
 *     @Override protected int getMaxAttempts(JobTask task) { return 5; }
 *
 *     @Override
 *     protected TaskResult processTask(JobTask task) {
 *         invoiceService.generate(task.payload());
 *         return TaskResult.success("{\"generated\":true}");
 *     }
 *
 *     @KafkaListener(topics = "${invoice.kafka.topic}", groupId = "${invoice.kafka.group-id}")
 *     @Override
 *     public void consume(ConsumerRecord<String, String> record) {
 *         super.consume(record);
 *     }
 * }
 * }</pre>
 *
 * <p><strong>Critical:</strong> The entire consumer body is wrapped in a try-catch.
 * Exceptions are logged and swallowed to prevent killing the Kafka consumer thread.
 *
 * <p><strong>No forced timeout.</strong> The consumer waits indefinitely for the
 * handler to complete. The Redisson lock is kept alive via the watchdog mechanism
 * so the task stays locked no matter how long it takes. The final status (success
 * or failure) is always persisted.
 */
public abstract class AbstractJobTaskConsumer {

    private static final Logger log = LoggerFactory.getLogger(AbstractJobTaskConsumer.class);

    private final TaskRepository         taskRepository;
    private final JobRepository          jobRepository;
    private final TaskLockManager        lockManager;
    private final AsyncTaskExecutorBridge bridge;
    private final JobKafkaProducer       kafkaProducer;

    protected AbstractJobTaskConsumer(TaskRepository taskRepository,
                                      JobRepository jobRepository,
                                      TaskLockManager lockManager,
                                      AsyncTaskExecutorBridge bridge,
                                      JobKafkaProducer kafkaProducer) {
        this.taskRepository = taskRepository;
        this.jobRepository  = jobRepository;
        this.lockManager    = lockManager;
        this.bridge         = bridge;
        this.kafkaProducer  = kafkaProducer;
    }

    // ── Contract ─────────────────────────────────────────────────────────────

    /**
     * Maximum total attempts (including first) before a task is promoted to DEAD_LETTER.
     * The task is provided so dispatching consumers can resolve per-handler limits.
     */
    protected abstract int getMaxAttempts(JobTask task);

    /**
     * Core task processing logic. Runs on the {@code @Async} thread pool, not the
     * Kafka consumer thread. Return {@link TaskResult#success(String)} or
     * {@link TaskResult#failure(Throwable)}.
     */
    protected abstract TaskResult processTask(JobTask task);

    /**
     * Submits work to an executor and returns a future. Override in subclasses
     * to use per-handler executor services.
     *
     * @param task the task being processed
     * @return a future that resolves to the task result
     */
    protected CompletableFuture<TaskResult> submitWork(JobTask task) {
        return bridge.submitAsync(() -> processTask(task));
    }

    /**
     * Resolves the backoff policy for a given task. Override in subclasses
     * to use per-handler backoff policies.
     *
     * @param task the task that failed
     * @return the backoff policy to use for retry delay calculation
     */
    protected BackoffPolicy resolveBackoffPolicy(JobTask task) {
        return task.backoffPolicy();
    }

    // ── Kafka entry point ─────────────────────────────────────────────────────

    /**
     * Fault-tolerant Kafka consumer entry point. The concrete class annotates
     * its override with {@code @KafkaListener}.
     *
     * <p>This method NEVER throws — all exceptions are caught and logged.
     */
    public void consume(ConsumerRecord<String, String> record) {
        try {
            doConsume(record);
        } catch (Exception e) {
            // !! NEVER rethrow — an uncaught exception kills the consumer thread !!
            log.error("Unrecoverable error processing Kafka record offset={} partition={}: {}",
                    record.offset(), record.partition(), e.getMessage(), e);
        }
    }

    private void doConsume(ConsumerRecord<String, String> record) throws Exception {
        JobKafkaMessage message = kafkaProducer.deserialize(record.value());
        UUID taskId = message.taskId();

        // 1. Acquire Redisson fenced lock — watchdog auto-renews while this thread is alive
        Optional<FencedLock> maybeLock = lockManager.tryLock(taskId);
        if (maybeLock.isEmpty()) {
            log.warn("Could not acquire lock for task={}, skipping (another node is processing)", taskId);
            return;
        }

        long fenceToken = maybeLock.get().token();

        try {
            // 2. Load task and verify it is still eligible (idempotency guard)
            Optional<JobTask> maybeTask = taskRepository.findEligible(taskId);
            if (maybeTask.isEmpty()) {
                log.info("Task={} is not eligible (already processed or not found), skipping", taskId);
                return;
            }

            JobTask task = maybeTask.get();

            // 3. Mark IN_PROGRESS with fence token — subsequent writes are guarded by this token
            taskRepository.markInProgress(taskId, OffsetDateTime.now(), fenceToken);

            // 4. Submit to executor (shared pool or per-handler); record asyncSubmittedAt
            OffsetDateTime submittedAt = OffsetDateTime.now();
            CompletableFuture<TaskResult> future = submitWork(task);
            taskRepository.recordAsyncSubmitted(taskId, submittedAt, fenceToken);
            log.debug("Task={} submitted to executor at {} (fence={})", taskId, submittedAt, fenceToken);

            // 5. Wait indefinitely — lock watchdog keeps us safe; no forced timeout
            TaskResult result = future.get();

            // 6. Handle result — fence token ensures stale holders cannot overwrite
            if (result.isSuccess()) {
                OffsetDateTime completedAt = OffsetDateTime.now();
                boolean written = taskRepository.markCompleted(taskId, result.payload(), completedAt, fenceToken);
                if (written) {
                    taskRepository.recordAsyncCompleted(taskId, completedAt, fenceToken);
                    log.info("Task={} completed successfully (fence={})", taskId, fenceToken);
                } else {
                    log.warn("Task={} completion rejected — fence token {} is stale (another node took over)",
                            taskId, fenceToken);
                }
            } else {
                handleFailure(task, result.error(), fenceToken);
            }

            // 7. Update parent job counters
            jobRepository.updateCounters(task.jobId());

        } finally {
            // Always release — watchdog stops on unlock; if JVM dies the watchdog
            // stops renewing and the lock expires automatically
            lockManager.unlock(taskId);
        }
    }

    private void handleFailure(JobTask task, Throwable error, long fenceToken) {
        int nextAttemptNumber = task.attemptCount() + 1;
        String errorMessage = error != null ? error.getMessage() : "Unknown error";
        String errorClass   = error != null ? error.getClass().getName() : "Unknown";
        int maxAttempts = getMaxAttempts(task);

        log.warn("Task={} failed (attempt {}/{}): {}", task.id(), nextAttemptNumber, maxAttempts, errorMessage);

        if (nextAttemptNumber >= maxAttempts) {
            boolean written = taskRepository.markDeadLetter(task.id(), nextAttemptNumber,
                    OffsetDateTime.now(), errorMessage, errorClass, fenceToken);
            if (written) {
                log.error("Task={} exhausted all {} attempts, moved to DEAD_LETTER", task.id(), maxAttempts);
            } else {
                log.warn("Task={} DEAD_LETTER update rejected — fence token {} is stale", task.id(), fenceToken);
            }
        } else {
            BackoffPolicy policy = resolveBackoffPolicy(task);
            long delayMs = policy.computeDelayMs(nextAttemptNumber);
            OffsetDateTime nextAttemptTime = OffsetDateTime.now().plusNanos(delayMs * 1_000_000L);
            boolean written = taskRepository.markFailed(task.id(), nextAttemptNumber,
                    OffsetDateTime.now(), nextAttemptTime, errorMessage, errorClass, fenceToken);
            if (written) {
                log.info("Task={} scheduled for retry at {} (delay={}ms)", task.id(), nextAttemptTime, delayMs);
            } else {
                log.warn("Task={} failure update rejected — fence token {} is stale", task.id(), fenceToken);
            }
        }
    }
}
