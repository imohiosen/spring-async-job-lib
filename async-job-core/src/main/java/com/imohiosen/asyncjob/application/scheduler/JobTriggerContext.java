package com.imohiosen.asyncjob.application.scheduler;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Context object passed to {@code AbstractJobScheduler.buildTaskPayloads()}.
 * Carries the job identity and trigger timestamp for use in payload construction.
 */
public record JobTriggerContext(
        UUID           jobId,
        OffsetDateTime triggeredAt
) {}
