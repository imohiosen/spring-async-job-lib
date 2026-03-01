package com.imohiosen.asyncjob.example.api.dto;

import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskStatus;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Response DTO for job task information.
 */
public record JobTaskResponse(
        UUID id,
        UUID jobId,
        String taskType,
        String destination,
        TaskStatus status,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt,
        OffsetDateTime startedAt,
        OffsetDateTime completedAt,
        OffsetDateTime deadlineAt,
        boolean stale,
        int attemptCount,
        OffsetDateTime lastAttemptTime,
        OffsetDateTime nextAttemptTime,
        String lastErrorMessage,
        String lastErrorClass,
        String payload,
        String result,
        String metadata
) {
    public static JobTaskResponse fromTask(JobTask task) {
        return new JobTaskResponse(
                task.id(),
                task.jobId(),
                task.taskType(),
                task.destination(),
                task.status(),
                task.createdAt(),
                task.updatedAt(),
                task.startedAt(),
                task.completedAt(),
                task.deadlineAt(),
                task.stale(),
                task.attemptCount(),
                task.lastAttemptTime(),
                task.nextAttemptTime(),
                task.lastErrorMessage(),
                task.lastErrorClass(),
                task.payload(),
                task.result(),
                task.metadata()
        );
    }
}
