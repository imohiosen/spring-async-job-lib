package com.imohiosen.asyncjob.application.consumer;

import com.imohiosen.asyncjob.application.handler.JobTaskHandler;
import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskResult;
import com.imohiosen.asyncjob.domain.TimeCriticalPolicy;
import com.imohiosen.asyncjob.port.repository.TaskRepository;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Executes a time-critical task with Resilience4j in-memory retries using
 * sub-second backoff intervals. Periodically syncs retry progress to the
 * database to avoid overload during rapid retries.
 *
 * <p>This class <strong>bypasses Kafka</strong> during the retry loop —
 * all retries happen on the same node holding the fenced lock. Concurrency
 * is ensured by the fenced token passed to every DB write.
 *
 * <p>On exhaustion of all Resilience4j retries, a
 * {@link TimeCriticalRetriesExhaustedException} is thrown so the caller
 * can fall back to the normal DB-backed retry path
 * ({@code markFailed} → {@code TaskRetryScheduler} → Kafka).
 */
public class TimeCriticalResilienceExecutor {

    private static final Logger log = LoggerFactory.getLogger(TimeCriticalResilienceExecutor.class);

    private final TaskRepository taskRepository;
    private final ScheduledExecutorService syncScheduler;

    public TimeCriticalResilienceExecutor(TaskRepository taskRepository,
                                          ScheduledExecutorService syncScheduler) {
        this.taskRepository = taskRepository;
        this.syncScheduler  = syncScheduler;
    }

    /**
     * Executes the handler with Resilience4j in-memory retries.
     *
     * @param task       the time-critical task (must have {@code timeCritical() == true})
     * @param fenceToken the fenced lock token held by this consumer
     * @param handler    the handler to invoke
     * @return the successful {@link TaskResult}
     * @throws TimeCriticalRetriesExhaustedException if all retries are exhausted
     */
    public TaskResult execute(JobTask task, long fenceToken, JobTaskHandler handler) {
        TimeCriticalPolicy policy = task.timeCriticalPolicy();
        if (policy == null) {
            throw new IllegalStateException("Task " + task.id() + " is not time-critical");
        }

        // Track retry state for DB sync
        AtomicInteger totalAttempts = new AtomicInteger(0);
        AtomicReference<Throwable> lastError = new AtomicReference<>();
        AtomicInteger attemptsSinceLastSync = new AtomicInteger(0);

        // Build Resilience4j RetryConfig with exponential backoff
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(policy.maxAttempts())
                .waitDuration(Duration.ofMillis(policy.baseIntervalMs()))
                .retryOnException(e -> true)
                .retryOnResult(r -> r instanceof TaskResult tr && !tr.isSuccess())
                .build();

        Retry retry = RetryRegistry.of(config).retry("tc-" + task.id());

        // Listen for retry events to track progress
        retry.getEventPublisher().onRetry(event -> {
            totalAttempts.incrementAndGet();
            attemptsSinceLastSync.incrementAndGet();
            Throwable error = event.getLastThrowable();
            if (error != null) {
                lastError.set(error);
            }
            log.debug("Time-critical retry {}/{} for task={}: {}",
                    totalAttempts.get(), policy.maxAttempts(), task.id(),
                    error != null ? error.getMessage() : "result-based retry");
        });

        // Schedule periodic DB sync
        ScheduledFuture<?> syncFuture = syncScheduler.scheduleAtFixedRate(
                () -> flushProgress(task.id(), totalAttempts.get(), lastError.get(),
                        fenceToken, attemptsSinceLastSync),
                policy.dbSyncIntervalMs(),
                policy.dbSyncIntervalMs(),
                TimeUnit.MILLISECONDS
        );

        try {
            // Execute with Resilience4j retry wrapping
            TaskResult result = retry.executeCallable(() -> {
                TaskResult tr = invokeHandler(handler, task);
                if (!tr.isSuccess()) {
                    // Track the error from TaskResult.failure()
                    Throwable err = tr.error();
                    if (err != null) {
                        lastError.set(err);
                    }
                    totalAttempts.incrementAndGet();
                    attemptsSinceLastSync.incrementAndGet();
                }
                return tr;
            });

            if (result.isSuccess()) {
                return result;
            }

            // If we get here, maxAttempts exhausted with result-based failures
            Throwable cause = lastError.get();
            if (cause == null) {
                cause = new RuntimeException("Time-critical task failed after all retries");
            }
            flushFinalProgress(task.id(), totalAttempts.get(), cause, fenceToken);
            throw new TimeCriticalRetriesExhaustedException(totalAttempts.get(), cause);

        } catch (TimeCriticalRetriesExhaustedException e) {
            throw e; // re-throw our own exception
        } catch (Exception e) {
            // Resilience4j wraps the last exception; extract root cause
            Throwable rootCause = lastError.get() != null ? lastError.get() : e;
            int attempts = totalAttempts.get();
            flushFinalProgress(task.id(), attempts, rootCause, fenceToken);
            throw new TimeCriticalRetriesExhaustedException(attempts, rootCause);
        } finally {
            syncFuture.cancel(false);
        }
    }

    private TaskResult invokeHandler(JobTaskHandler handler, JobTask task) {
        try {
            TaskResult result = handler.handle(task);
            if (result == null) {
                throw new NullPointerException("Handler returned null TaskResult");
            }
            return result;
        } catch (Exception e) {
            throw e; // Let Resilience4j retry handle it
        }
    }

    private void flushProgress(UUID taskId, int attemptCount, Throwable lastError,
                                long fenceToken, AtomicInteger attemptsSinceLastSync) {
        if (attemptsSinceLastSync.get() == 0) {
            return; // No new retries since last sync
        }
        String errorMessage = lastError != null ? lastError.getMessage() : "Unknown error";
        String errorClass = lastError != null ? lastError.getClass().getName() : "Unknown";

        try {
            boolean written = taskRepository.persistTimeCriticalProgress(
                    taskId, attemptCount, OffsetDateTime.now(),
                    errorMessage, errorClass, fenceToken);
            attemptsSinceLastSync.set(0);
            if (!written) {
                log.warn("Time-critical sync rejected for task={} — fence token {} may be stale",
                        taskId, fenceToken);
            } else {
                log.debug("Time-critical sync for task={}: attempt={}", taskId, attemptCount);
            }
        } catch (Exception e) {
            log.warn("Failed to sync time-critical progress for task={}: {}",
                    taskId, e.getMessage(), e);
        }
    }

    private void flushFinalProgress(UUID taskId, int attemptCount, Throwable error, long fenceToken) {
        String errorMessage = error != null ? error.getMessage() : "Unknown error";
        String errorClass = error != null ? error.getClass().getName() : "Unknown";

        try {
            taskRepository.persistTimeCriticalProgress(
                    taskId, attemptCount, OffsetDateTime.now(),
                    errorMessage, errorClass, fenceToken);
        } catch (Exception e) {
            log.warn("Failed to flush final time-critical progress for task={}: {}",
                    taskId, e.getMessage(), e);
        }
    }
}
