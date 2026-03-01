package com.imohiosen.asyncjob.application.consumer;

import com.imohiosen.asyncjob.domain.BackoffPolicy;
import com.imohiosen.asyncjob.domain.FencedLock;
import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskResult;
import com.imohiosen.asyncjob.domain.TaskStatus;
import com.imohiosen.asyncjob.application.executor.AsyncTaskExecutorBridge;
import com.imohiosen.asyncjob.port.lock.TaskLockManager;
import com.imohiosen.asyncjob.port.messaging.JobMessage;
import com.imohiosen.asyncjob.port.repository.JobRepository;
import com.imohiosen.asyncjob.port.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Abstract base for all task consumers in the async job library.
 *
 * <p>This class is <strong>transport-agnostic</strong>. It accepts a
 * {@link JobMessage} directly — the concrete subclass is responsible for
 * bridging from the underlying transport (Kafka, RabbitMQ, Redis Streams, etc.)
 * to this method.
 *
 * <h2>How to extend (Kafka example)</h2>
 * <pre>{@code
 * @Component
 * public class InvoiceTaskConsumer extends AbstractJobTaskConsumer {
 *
 *     private final KafkaJobMessageProducer deserializer;
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
 *     public void onMessage(ConsumerRecord<String, String> record) {
 *         consume(deserializer.deserialize(record.value()));
 *     }
 * }
 * }</pre>
 *
 * <p><strong>Critical:</strong> The entire consumer body is wrapped in a try-catch.
 * Exceptions are logged and swallowed to prevent killing the consumer thread.
 *
 * <p><strong>No forced timeout.</strong> The consumer waits indefinitely for the
 * handler to complete. The distributed lock is kept alive via the watchdog mechanism
 * so the task stays locked no matter how long it takes. The final status (success
 * or failure) is always persisted.
 */
public abstract class AbstractJobTaskConsumer {

    private static final Logger log = LoggerFactory.getLogger(AbstractJobTaskConsumer.class);

    private final TaskRepository         taskRepository;
    private final JobRepository          jobRepository;
    private final TaskLockManager        lockManager;
    private final AsyncTaskExecutorBridge bridge;

    protected AbstractJobTaskConsumer(TaskRepository taskRepository,
                                      JobRepository jobRepository,
                                      TaskLockManager lockManager,
                                      AsyncTaskExecutorBridge bridge) {
        this.taskRepository = taskRepository;
        this.jobRepository  = jobRepository;
        this.lockManager    = lockManager;
        this.bridge         = bridge;
    }

    // ── Contract ─────────────────────────────────────────────────────────────

    /**
     * Maximum total attempts (including first) before a task is promoted to DEAD_LETTER.
     * The task is provided so dispatching consumers can resolve per-handler limits.
     */
    protected abstract int getMaxAttempts(JobTask task);

    /**
     * Core task processing logic. Runs on the {@code @Async} thread pool, not the
     * consumer thread. Return {@link TaskResult#success(String)} or
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

    /**
     * Submits time-critical work using Resilience4j in-memory retries.
     * Override in subclasses (e.g. {@link DispatchingJobTaskConsumer}) to
     * delegate to a {@link TimeCriticalResilienceExecutor}.
     *
     * <p>Default implementation falls back to the normal {@link #submitWork(JobTask)}
     * path — subclasses must override this to enable the time-critical path.
     *
     * @param task       the time-critical task
     * @param fenceToken the fenced lock token held by this consumer
     * @return a future that resolves to the task result
     */
    protected CompletableFuture<TaskResult> submitTimeCriticalWork(JobTask task, long fenceToken) {
        return submitWork(task);
    }

    // ── Transport-agnostic entry point ────────────────────────────────────────

    /**
     * Fault-tolerant entry point. The concrete class bridges from its transport
     * (e.g. {@code @KafkaListener}, {@code @RabbitListener}) and passes the
     * deserialised {@link JobMessage} here.
     *
     * <p>This method NEVER throws — all exceptions are caught and logged.
     */
    public void consume(JobMessage message) {
        try {
            doConsume(message);
        } catch (Exception e) {
            log.error("Unrecoverable error processing message for task={}: {}",
                    message.taskId(), e.getMessage(), e);
        }
    }

    private void doConsume(JobMessage message) throws Exception {
        UUID taskId = message.taskId();

        // 1. Acquire distributed fenced lock — watchdog auto-renews while this thread is alive
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
            CompletableFuture<TaskResult> future = task.timeCritical()
                    ? submitTimeCriticalWork(task, fenceToken)
                    : submitWork(task);
            taskRepository.recordAsyncSubmitted(taskId, submittedAt, fenceToken);
            log.debug("Task={} submitted to executor at {} (fence={}, timeCritical={})",
                    taskId, submittedAt, fenceToken, task.timeCritical());

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

            // 7. Update complete

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
