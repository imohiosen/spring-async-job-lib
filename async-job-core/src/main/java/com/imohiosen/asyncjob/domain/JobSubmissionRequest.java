package com.imohiosen.asyncjob.domain;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Immutable value object carrying all inputs needed to submit a job — either
 * for immediate execution or for future scheduling.
 *
 * <p>When {@code scheduledAt} is {@code null} or not after the current time,
 * the job is dispatched immediately. Otherwise it is persisted with
 * {@link JobStatus#SCHEDULED} and dispatched later by the
 * {@code ScheduledJobDispatcher} sweep.
 *
 * @param jobName         unique logical name for the job
 * @param destination     messaging destination (e.g. Kafka topic)
 * @param taskType        discriminator written to each task row
 * @param payloads        list of JSON payload strings — one task per entry
 * @param deadlineMs      wall-clock duration (ms) before the job is flagged stale
 * @param taskDeadlineMs  per-task deadline; defaults to {@code deadlineMs} if {@code null}
 * @param backoffPolicy   retry backoff config; defaults to {@link BackoffPolicy#DEFAULT} if {@code null}
 * @param scheduledAt     future execution time; {@code null} means run immediately
 * @param correlationId   optional external correlation id
 * @param metadata        optional JSON metadata attached to the job row
 */
public record JobSubmissionRequest(
        String          jobName,
        String          destination,
        String          taskType,
        List<String>    payloads,
        long            deadlineMs,
        Long            taskDeadlineMs,
        BackoffPolicy   backoffPolicy,
        OffsetDateTime  scheduledAt,
        String          correlationId,
        String          metadata
) {
    public JobSubmissionRequest {
        if (jobName == null || jobName.isBlank())
            throw new IllegalArgumentException("jobName must not be blank");
        if (destination == null || destination.isBlank())
            throw new IllegalArgumentException("destination must not be blank");
        if (taskType == null || taskType.isBlank())
            throw new IllegalArgumentException("taskType must not be blank");
        if (payloads == null)
            throw new IllegalArgumentException("payloads must not be null");
        if (deadlineMs <= 0)
            throw new IllegalArgumentException("deadlineMs must be > 0");

        payloads = List.copyOf(payloads);
    }

    /** Resolves the effective per-task deadline in milliseconds. */
    public long effectiveTaskDeadlineMs() {
        return taskDeadlineMs != null ? taskDeadlineMs : deadlineMs;
    }

    /** Resolves the effective backoff policy (never null). */
    public BackoffPolicy effectiveBackoffPolicy() {
        return backoffPolicy != null ? backoffPolicy : BackoffPolicy.DEFAULT;
    }

    /** Returns true if this request should be dispatched immediately. */
    public boolean isImmediate() {
        return scheduledAt == null || !scheduledAt.isAfter(OffsetDateTime.now());
    }
}
