package com.imohiosen.asyncjob.test;

import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskStatus;
import com.imohiosen.asyncjob.repository.TaskRepository;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory stub for {@link TaskRepository} for use in unit tests.
 */
public class InMemoryTaskRepository extends TaskRepository {

    private final Map<UUID, JobTask> store = new ConcurrentHashMap<>();
    private int timedOutTasksCount = 0;

    public InMemoryTaskRepository() {
        super(null);
    }

    @Override
    public void insert(JobTask task) {
        store.put(task.id(), task);
    }

    @Override
    public Optional<JobTask> findEligible(UUID taskId) {
        return Optional.ofNullable(store.get(taskId))
                .filter(JobTask::isEligible);
    }

    @Override
    public Optional<JobTask> findById(UUID taskId) {
        return Optional.ofNullable(store.get(taskId));
    }

    @Override
    public void markInProgress(UUID taskId, OffsetDateTime startedAt) {
        update(taskId, t -> new JobTask(
                t.id(), t.jobId(), t.taskType(), t.kafkaTopic(),
                t.kafkaPartition(), t.kafkaOffset(), TaskStatus.IN_PROGRESS,
                t.createdAt(), OffsetDateTime.now(), startedAt, t.completedAt(),
                t.deadlineAt(), t.timedOut(), t.attemptCount() + 1,
                OffsetDateTime.now(), t.nextAttemptTime(),
                t.baseIntervalMs(), t.multiplier(), t.maxDelayMs(),
                t.asyncSubmittedAt(), t.asyncCompletedAt(),
                t.lastErrorMessage(), t.lastErrorClass(), t.payload(), t.result()
        ));
    }

    @Override
    public void markCompleted(UUID taskId, String result, OffsetDateTime completedAt) {
        update(taskId, t -> new JobTask(
                t.id(), t.jobId(), t.taskType(), t.kafkaTopic(),
                t.kafkaPartition(), t.kafkaOffset(), TaskStatus.COMPLETED,
                t.createdAt(), OffsetDateTime.now(), t.startedAt(), completedAt,
                t.deadlineAt(), t.timedOut(), t.attemptCount(),
                t.lastAttemptTime(), null,
                t.baseIntervalMs(), t.multiplier(), t.maxDelayMs(),
                t.asyncSubmittedAt(), t.asyncCompletedAt(),
                null, null, t.payload(), result
        ));
    }

    @Override
    public void recordAsyncSubmitted(UUID taskId, OffsetDateTime submittedAt) {
        update(taskId, t -> new JobTask(
                t.id(), t.jobId(), t.taskType(), t.kafkaTopic(),
                t.kafkaPartition(), t.kafkaOffset(), t.status(),
                t.createdAt(), OffsetDateTime.now(), t.startedAt(), t.completedAt(),
                t.deadlineAt(), t.timedOut(), t.attemptCount(),
                t.lastAttemptTime(), t.nextAttemptTime(),
                t.baseIntervalMs(), t.multiplier(), t.maxDelayMs(),
                submittedAt, t.asyncCompletedAt(),
                t.lastErrorMessage(), t.lastErrorClass(), t.payload(), t.result()
        ));
    }

    @Override
    public void recordAsyncCompleted(UUID taskId, OffsetDateTime completedAt) {
        update(taskId, t -> new JobTask(
                t.id(), t.jobId(), t.taskType(), t.kafkaTopic(),
                t.kafkaPartition(), t.kafkaOffset(), t.status(),
                t.createdAt(), OffsetDateTime.now(), t.startedAt(), t.completedAt(),
                t.deadlineAt(), t.timedOut(), t.attemptCount(),
                t.lastAttemptTime(), t.nextAttemptTime(),
                t.baseIntervalMs(), t.multiplier(), t.maxDelayMs(),
                t.asyncSubmittedAt(), completedAt,
                t.lastErrorMessage(), t.lastErrorClass(), t.payload(), t.result()
        ));
    }

    @Override
    public void markFailed(UUID taskId, int attemptCount, OffsetDateTime lastAttemptTime,
                           OffsetDateTime nextAttemptTime, String errorMessage, String errorClass) {
        update(taskId, t -> new JobTask(
                t.id(), t.jobId(), t.taskType(), t.kafkaTopic(),
                t.kafkaPartition(), t.kafkaOffset(), TaskStatus.FAILED,
                t.createdAt(), OffsetDateTime.now(), t.startedAt(), t.completedAt(),
                t.deadlineAt(), t.timedOut(), attemptCount,
                lastAttemptTime, nextAttemptTime,
                t.baseIntervalMs(), t.multiplier(), t.maxDelayMs(),
                t.asyncSubmittedAt(), t.asyncCompletedAt(),
                errorMessage, errorClass, t.payload(), t.result()
        ));
    }

    @Override
    public void markDeadLetter(UUID taskId, int attemptCount, OffsetDateTime lastAttemptTime,
                               String errorMessage, String errorClass) {
        update(taskId, t -> new JobTask(
                t.id(), t.jobId(), t.taskType(), t.kafkaTopic(),
                t.kafkaPartition(), t.kafkaOffset(), TaskStatus.DEAD_LETTER,
                t.createdAt(), OffsetDateTime.now(), t.startedAt(), t.completedAt(),
                t.deadlineAt(), t.timedOut(), attemptCount,
                lastAttemptTime, null,
                t.baseIntervalMs(), t.multiplier(), t.maxDelayMs(),
                t.asyncSubmittedAt(), t.asyncCompletedAt(),
                errorMessage, errorClass, t.payload(), t.result()
        ));
    }

    @Override
    public int flagTimedOutTasks() {
        return timedOutTasksCount;
    }

    // ── Test helpers ──────────────────────────────────────────────────────────

    public void setTimedOutTasksCount(int count) { this.timedOutTasksCount = count; }
    public Collection<JobTask> all() { return Collections.unmodifiableCollection(store.values()); }
    public void clear() { store.clear(); }

    private void update(UUID taskId, java.util.function.UnaryOperator<JobTask> fn) {
        store.computeIfPresent(taskId, (k, v) -> fn.apply(v));
    }
}
