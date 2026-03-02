package com.imohiosen.asyncjob.example.api.dto;

import com.imohiosen.asyncjob.domain.Job;
import com.imohiosen.asyncjob.domain.JobStatus;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Response DTO for job information.
 */
public record JobResponse(
        UUID id,
        String jobName,
        String correlationId,
        JobStatus status,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt,
        OffsetDateTime startedAt,
        OffsetDateTime completedAt,
        OffsetDateTime deadlineAt,
        boolean stale,
        String metadata
) {
    public static JobResponse fromJob(Job job) {
        return new JobResponse(
                job.id(),
                job.jobName(),
                job.correlationId(),
                job.status(),
                job.createdAt(),
                job.updatedAt(),
                job.startedAt(),
                job.completedAt(),
                job.deadlineAt(),
                job.stale(),
                job.metadata()
        );
    }
}
