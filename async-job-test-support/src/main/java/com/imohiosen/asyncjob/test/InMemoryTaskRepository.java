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
    private int staleTasksCount = 0;

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
    public void markInProgress(UUID taskId, OffsetDateTime startedAt, long fenceToken) {
        update(taskId, t -> new JobTask(
                t.id(), t.jobId(), t.taskType(), t.kafkaTopic(),
                t.kafkaPartition(), t.kafkaOffset(), TaskStatus.IN_PROGRESS,
                t.createdAt(), OffsetDateTime.now(), startedAt, t.completedAt(),
                t.deadlineAt(), t.stale(), t.attemptCount() + 1,
                OffsetDateTime.now(), t.nextAttemptTime(),
                t.baseIntervalMs(), t.multiplier(), t.maxDelayMs(),
                t.asyncSubmittedAt(), t.asyncCompletedAt(),
                t.lastErrorMessage(), t.lastErrorClass(), fenceToken, t.payload(), t.result()
        ));
    }

    @Override
    public boolean markCompleted(UUID taskId, String result, OffsetDateTime completedAt, long fenceToken) {
        JobTask existing = store.get(taskId);
        if (existing == null || (existing.fenceToken() != null && existing.fenceToken() != fenceToken)) {
            return false;
        }
        update(taskId, t -> new JobTask(
                t.id(), t.jobId(), t.taskType(), t.kafkaTopic(),
                t.kafkaPartition(), t.kafkaOffset(), TaskStatus.COMPLETED,
                t.createdAt(), OffsetDateTime.now(), t.startedAt(), completedAt,
                t.deadlineAt(), t.stale(), t.attemptCount(),
                t.lastAttemptTime(), null,
                t.baseIntervalMs(), t.multiplier(), t.maxDelayMs(),
                t.asyncSubmittedAt(), t.asyncCompletedAt(),
                null, null, t.fenceToken(), t.payload(), result
        ));
        return true;
    }

    @Override
    public void recordAsyncSubmitted(UUID taskId, OffsetDateTime submittedAt, long fenceToken) {
        update(taskId, t -> new JobTask(
                t.id(), t.jobId(), t.taskType(), t.kafkaTopic(),
                t.kafkaPartition(), t.kafkaOffset(), t.status(),
                t.createdAt(), OffsetDateTime.now(), t.startedAt(), t.completedAt(),
                t.deadlineAt(), t.stale(), t.attemptCount(),
                t.lastAttemptTime(), t.nextAttemptTime(),
                t.baseIntervalMs(), t.multiplier(), t.maxDelayMs(),
                submittedAt, t.asyncCompletedAt(),
                t.lastErrorMessage(), t.lastErrorClass(), t.fenceToken(), t.payload(), t.result()
        ));
    }

    @Override
    public void recordAsyncCompleted(UUID taskId, OffsetDateTime completedAt, long fenceToken) {
        update(taskId, t -> new JobTask(
                t.id(), t.jobId(), t.taskType(), t.kafkaTopic(),
                t.kafkaPartition(), t.kafkaOffset(), t.status(),
                t.createdAt(), OffsetDateTime.now(), t.startedAt(), t.completedAt(),
                t.deadlineAt(), t.stale(), t.attemptCount(),
                t.lastAttemptTime(), t.nextAttemptTime(),
                t.baseIntervalMs(), t.multiplier(), t.maxDelayMs(),
                t.asyncSubmittedAt(), completedAt,
                t.lastErrorMessage(), t.lastErrorClass(), t.fenceToken(), t.payload(), t.result()
        ));
    }

    @Override
    public boolean markFailed(UUID taskId, int attemptCount, OffsetDateTime lastAttemptTime,
                           OffsetDateTime nextAttemptTime, String errorMessage, String errorClass,
                           long fenceToken) {
        JobTask existing = store.get(taskId);
        if (existing == null || (existing.fenceToken() != null && existing.fenceToken() != fenceToken)) {
            return false;
        }
        update(taskId, t -> new JobTask(
                t.id(), t.jobId(), t.taskType(), t.kafkaTopic(),
                t.kafkaPartition(), t.kafkaOffset(), TaskStatus.FAILED,
                t.createdAt(), OffsetDateTime.now(), t.startedAt(), t.completedAt(),
                t.deadlineAt(), t.stale(), attemptCount,
                lastAttemptTime, nextAttemptTime,
                t.baseIntervalMs(), t.multiplier(), t.maxDelayMs(),
                t.asyncSubmittedAt(), t.asyncCompletedAt(),
                errorMessage, errorClass, t.fenceToken(), t.payload(), t.result()
        ));
        return true;
    }

    @Override
    public boolean markDeadLetter(UUID taskId, int attemptCount, OffsetDateTime lastAttemptTime,
                               String errorMessage, String errorClass, long fenceToken) {
        JobTask existing = store.get(taskId);
        if (existing == null || (existing.fenceToken() != null && existing.fenceToken() != fenceToken)) {
            return false;
        }
        update(taskId, t -> new JobTask(
                t.id(), t.jobId(), t.taskType(), t.kafkaTopic(),
                t.kafkaPartition(), t.kafkaOffset(), TaskStatus.DEAD_LETTER,
                t.createdAt(), OffsetDateTime.now(), t.startedAt(), t.completedAt(),
                t.deadlineAt(), t.stale(), attemptCount,
                lastAttemptTime, null,
                t.baseIntervalMs(), t.multiplier(), t.maxDelayMs(),
                t.asyncSubmittedAt(), t.asyncCompletedAt(),
                errorMessage, errorClass, t.fenceToken(), t.payload(), t.result()
        ));
        return true;
    }

    @Override
    public int flagStaleTasks() {
        return staleTasksCount;
    }

    @Override
    public java.util.List<JobTask> findRetryableTasks(int limit) {
        return store.values().stream()
                .filter(t -> t.status() == TaskStatus.FAILED)
                .filter(t -> t.nextAttemptTime() != null && !t.nextAttemptTime().isAfter(OffsetDateTime.now()))
                .sorted(java.util.Comparator.comparingInt(JobTask::attemptCount)
                        .thenComparing(JobTask::nextAttemptTime))
                .limit(limit)
                .toList();
    }

    // ── Test helpers ──────────────────────────────────────────────────────────

    public void setStaleTasksCount(int count) { this.staleTasksCount = count; }
    public Collection<JobTask> all() { return Collections.unmodifiableCollection(store.values()); }
    public void clear() { store.clear(); }

    private void update(UUID taskId, java.util.function.UnaryOperator<JobTask> fn) {
        store.computeIfPresent(taskId, (k, v) -> fn.apply(v));
    }
}
