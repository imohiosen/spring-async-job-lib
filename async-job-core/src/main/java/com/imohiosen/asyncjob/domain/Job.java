package com.imohiosen.asyncjob.domain;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Represents a single scheduled trigger execution (parent of JobTask records).
 */
public record Job(
        UUID             id,
        String           jobName,
        String           correlationId,
        JobStatus        status,
        OffsetDateTime   createdAt,
        OffsetDateTime   updatedAt,
        OffsetDateTime   startedAt,
        OffsetDateTime   completedAt,
        OffsetDateTime   deadlineAt,
        OffsetDateTime   scheduledAt,
        boolean          stale,
        int              totalTasks,
        int              pendingTasks,
        int              inProgressTasks,
        int              completedTasks,
        int              failedTasks,
        int              deadLetterTasks,
        String           metadata
) {
    /** Returns true if all tasks have reached a terminal state. */
    public boolean isFinished() {
        return completedTasks + failedTasks + deadLetterTasks >= totalTasks && totalTasks > 0;
    }

    /** Returns true if the job deadline has passed and the job has not been flagged stale yet. */
    public boolean isDeadlineBreached() {
        return !stale
                && deadlineAt != null
                && OffsetDateTime.now().isAfter(deadlineAt)
                && status != JobStatus.SCHEDULED
                && status != JobStatus.COMPLETED
                && status != JobStatus.DEAD_LETTER;
    }

    /** Returns true if this job is scheduled for a future time and that time has arrived. */
    public boolean isScheduledAndDue() {
        return status == JobStatus.SCHEDULED
                && scheduledAt != null
                && !OffsetDateTime.now().isBefore(scheduledAt);
    }
}
