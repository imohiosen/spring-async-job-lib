package com.imohiosen.asyncjob.port.repository;

import com.imohiosen.asyncjob.domain.JobTask;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Port for persisting and querying {@link JobTask} entities.
 *
 * <p>Implementations may target any data store: PostgreSQL (JDBC),
 * MongoDB, MySQL, etc.
 */
public interface TaskRepository {

    void insert(JobTask task);

    /**
     * Finds a task that is eligible for processing: status is PENDING or FAILED,
     * and {@code nextAttemptTime} is null or in the past.
     */
    Optional<JobTask> findEligible(UUID taskId);

    Optional<JobTask> findById(UUID taskId);

    void markInProgress(UUID taskId, OffsetDateTime startedAt, long fenceToken);

    void recordAsyncSubmitted(UUID taskId, OffsetDateTime submittedAt, long fenceToken);

    void recordAsyncCompleted(UUID taskId, OffsetDateTime completedAt, long fenceToken);

    boolean markCompleted(UUID taskId, String result, OffsetDateTime completedAt, long fenceToken);

    boolean markFailed(UUID taskId, int attemptCount, OffsetDateTime lastAttemptTime,
                       OffsetDateTime nextAttemptTime, String errorMessage, String errorClass,
                       long fenceToken);

    boolean markDeadLetter(UUID taskId, int attemptCount, OffsetDateTime lastAttemptTime,
                           String errorMessage, String errorClass, long fenceToken);

    /**
     * Persists in-flight progress for a time-critical task during its Resilience4j
     * retry loop. The task status remains {@code IN_PROGRESS} — only the attempt
     * count and error are updated. This is called periodically (every
     * {@code dbSyncIntervalMs}) to avoid DB overload during sub-second retries.
     *
     * <p>Uses the fenced-token guard: {@code WHERE id = ? AND fence_token = ?}.
     *
     * @return {@code false} if the fence token is stale (another node took over)
     */
    boolean persistTimeCriticalProgress(UUID taskId, int attemptCount,
                                        OffsetDateTime lastAttemptTime,
                                        String lastErrorMessage, String lastErrorClass,
                                        long fenceToken);

    /**
     * Flags all IN_PROGRESS tasks that have breached their deadline as stale.
     *
     * @return number of tasks flagged stale
     */
    int flagStaleTasks();

    /**
     * Finds failed tasks eligible for retry, ordered by attempt count ascending.
     *
     * @param limit maximum number of tasks to return
     * @return retryable tasks ordered by attempt_count ASC, next_attempt_time ASC
     */
    List<JobTask> findRetryableTasks(int limit);

    List<JobTask> findDeadLetterByJobId(UUID jobId);

    /**
     * Finds all tasks belonging to a job, ordered by creation time.
     *
     * @param jobId the parent job id
     * @return tasks for the given job
     */
    List<JobTask> findTasksByJobId(UUID jobId);
}
