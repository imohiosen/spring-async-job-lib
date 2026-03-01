package com.imohiosen.asyncjob.kafka;

import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskResult;
import com.imohiosen.asyncjob.domain.TaskStatus;
import com.imohiosen.asyncjob.executor.AsyncTaskExecutorBridge;
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
import java.util.concurrent.TimeUnit;

/**
 * Abstract base for all Kafka task consumers in the async job library.
 *
 * <h2>How to extend</h2>
 * <pre>{@code
 * @Component
 * public class InvoiceTaskConsumer extends AbstractJobTaskConsumer {
 *
 *     @Override protected int getMaxAttempts() { return 5; }
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
     * Timeout (ms) to wait for the async Future to complete.
     * Defaults to the task's remaining time until {@code deadlineAt}.
     * Override to supply a fixed timeout.
     */
    protected long asyncTimeoutMs(JobTask task) {
        if (task.deadlineAt() == null) return 300_000L; // 5 min default
        long remaining = task.deadlineAt().toInstant().toEpochMilli()
                - System.currentTimeMillis();
        return Math.max(remaining, 1_000L);
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

        // 1. Acquire Redisson distributed lock — explicit lease prevents deadlock on crash
        boolean locked = lockManager.tryLock(taskId);
        if (!locked) {
            log.warn("Could not acquire lock for task={}, skipping (another node is processing)", taskId);
            return;
        }

        try {
            // 2. Load task and verify it is still eligible (idempotency guard)
            Optional<JobTask> maybeTask = taskRepository.findEligible(taskId);
            if (maybeTask.isEmpty()) {
                log.info("Task={} is not eligible (already processed or not found), skipping", taskId);
                return;
            }

            JobTask task = maybeTask.get();

            // 3. Mark IN_PROGRESS and record startedAt
            taskRepository.markInProgress(taskId, OffsetDateTime.now());

            // 4. Step 1 — submit to @Async executor; record asyncSubmittedAt
            OffsetDateTime submittedAt = OffsetDateTime.now();
            CompletableFuture<TaskResult> future = bridge.submitAsync(() -> processTask(task));
            taskRepository.recordAsyncSubmitted(taskId, submittedAt);
            log.debug("Task={} submitted to async executor at {}", taskId, submittedAt);

            // 5. Step 2 — wait for completion with deadline-aware timeout
            TaskResult result = future.get(asyncTimeoutMs(task), TimeUnit.MILLISECONDS);

            // 6. Handle result
            if (result.isSuccess()) {
                OffsetDateTime completedAt = OffsetDateTime.now();
                taskRepository.markCompleted(taskId, result.payload(), completedAt);
                taskRepository.recordAsyncCompleted(taskId, completedAt);
                log.info("Task={} completed successfully", taskId);
            } else {
                handleFailure(task, result.error());
            }

            // 7. Update parent job counters
            jobRepository.updateCounters(task.jobId());

        } finally {
            // Always release — lease time acts as deadlock prevention if JVM dies here
            lockManager.unlock(taskId);
        }
    }

    private void handleFailure(JobTask task, Throwable error) {
        int nextAttemptNumber = task.attemptCount() + 1;
        String errorMessage = error != null ? error.getMessage() : "Unknown error";
        String errorClass   = error != null ? error.getClass().getName() : "Unknown";
        int maxAttempts = getMaxAttempts(task);

        log.warn("Task={} failed (attempt {}/{}): {}", task.id(), nextAttemptNumber, maxAttempts, errorMessage);

        if (nextAttemptNumber >= maxAttempts) {
            taskRepository.markDeadLetter(task.id(), nextAttemptNumber,
                    OffsetDateTime.now(), errorMessage, errorClass);
            log.error("Task={} exhausted all {} attempts, moved to DEAD_LETTER", task.id(), maxAttempts);
        } else {
            long delayMs = task.backoffPolicy().computeDelayMs(nextAttemptNumber);
            OffsetDateTime nextAttemptTime = OffsetDateTime.now().plusNanos(delayMs * 1_000_000L);
            taskRepository.markFailed(task.id(), nextAttemptNumber,
                    OffsetDateTime.now(), nextAttemptTime, errorMessage, errorClass);
            log.info("Task={} scheduled for retry at {} (delay={}ms)", task.id(), nextAttemptTime, delayMs);
        }
    }
}
