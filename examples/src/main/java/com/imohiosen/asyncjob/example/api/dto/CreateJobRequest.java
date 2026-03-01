package com.imohiosen.asyncjob.example.api.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

/**
 * Request DTO for creating a new job.
 */
public record CreateJobRequest(
        @NotBlank(message = "Job name is required")
        String jobName,
        
        String correlationId,
        
        @NotEmpty(message = "At least one task is required")
        @Valid
        List<TaskRequest> tasks,
        
        @Min(value = 1, message = "Deadline must be at least 1 hour")
        Integer deadlineHours,
        
        String metadata
) {
    public CreateJobRequest {
        if (deadlineHours == null) {
            deadlineHours = 24; // Default to 24 hours
        }
        if (metadata == null) {
            metadata = "{}";
        }
    }

    /**
     * Individual task in a job.
     */
    public record TaskRequest(
            @NotBlank(message = "Task type is required")
            String taskType,
            
            @NotBlank(message = "Payload is required")
            String payload,
            
            Long backoffBaseMs,
            Double backoffMultiplier,
            Long backoffMaxMs,
            String metadata
    ) {
        public TaskRequest {
            if (backoffBaseMs == null) {
                backoffBaseMs = 1000L; // 1 second
            }
            if (backoffMultiplier == null) {
                backoffMultiplier = 2.0;
            }
            if (backoffMaxMs == null) {
                backoffMaxMs = 3_600_000L; // 1 hour
            }
            if (metadata == null) {
                metadata = "{}";
            }
        }
    }
}
