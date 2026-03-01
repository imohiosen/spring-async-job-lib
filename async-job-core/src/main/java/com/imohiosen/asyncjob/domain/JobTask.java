package com.imohiosen.asyncjob.domain;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Represents a single unit of work — one Kafka message, one DB row.
 * Maps 1:1 to the {@code job_tasks} PostgreSQL table.
 */
public record JobTask(
        UUID           id,
        UUID           jobId,
        String         taskType,
        String         kafkaTopic,
        Integer        kafkaPartition,
        Long           kafkaOffset,
        TaskStatus     status,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt,
        OffsetDateTime startedAt,
        OffsetDateTime completedAt,
        OffsetDateTime deadlineAt,
        boolean        stale,
        int            attemptCount,
        OffsetDateTime lastAttemptTime,
        OffsetDateTime nextAttemptTime,
        long           baseIntervalMs,
        double         multiplier,
        long           maxDelayMs,
        OffsetDateTime asyncSubmittedAt,
        OffsetDateTime asyncCompletedAt,
        String         lastErrorMessage,
        String         lastErrorClass,
        Long           fenceToken,
        String         payload,
        String         result
) {
    /** Returns true if this task is eligible to be processed right now. */
    public boolean isEligible() {
        return (status == TaskStatus.PENDING || status == TaskStatus.FAILED)
                && (nextAttemptTime == null || !OffsetDateTime.now().isBefore(nextAttemptTime));
    }

    /** Returns true if the task deadline has passed and the task has not been flagged stale yet. */
    public boolean isDeadlineBreached() {
        return !stale
                && deadlineAt != null
                && OffsetDateTime.now().isAfter(deadlineAt)
                && status == TaskStatus.IN_PROGRESS;
    }

    /** Derives the BackoffPolicy from the task's discrete backoff columns. */
    public BackoffPolicy backoffPolicy() {
        return new BackoffPolicy(baseIntervalMs, multiplier, maxDelayMs);
    }
}
