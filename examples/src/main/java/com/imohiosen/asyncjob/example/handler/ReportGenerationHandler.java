package com.imohiosen.asyncjob.example.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.imohiosen.asyncjob.application.handler.JobTaskHandler;
import com.imohiosen.asyncjob.domain.BackoffPolicy;
import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Example handler for report generation tasks.
 * Demonstrates CPU-intensive work with custom backoff policy.
 */
@Component
public class ReportGenerationHandler implements JobTaskHandler {

    private static final Logger log = LoggerFactory.getLogger(ReportGenerationHandler.class);
    private final ObjectMapper objectMapper;

    public ReportGenerationHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String taskType() {
        return "REPORT_GENERATION";
    }

    @Override
    public TaskResult handle(JobTask task) {
        try {
            log.info("Processing REPORT_GENERATION task={} (attempt={})", 
                    task.id(), task.attemptCount());

            // Parse the payload
            JsonNode payload = objectMapper.readTree(task.payload());
            String reportType = payload.path("reportType").asText();
            String startDate = payload.path("startDate").asText();
            String endDate = payload.path("endDate").asText();

            // Simulate report generation (CPU-intensive work)
            log.debug("Generating {} report from {} to {}", reportType, startDate, endDate);
            Thread.sleep(2000); // Simulate processing time

            // Simulate occasional failures
            if (task.attemptCount() == 0 && Math.random() < 0.05) {
                log.warn("Simulated report generation failure for task={}", task.id());
                return TaskResult.failure(new RuntimeException("Database query timeout"));
            }

            // Generate mock report data
            String reportUrl = String.format("https://reports.example.com/%s/%s.pdf", 
                    reportType, java.util.UUID.randomUUID());

            log.info("Successfully generated {} report for task={} url={}", 
                    reportType, task.id(), reportUrl);

            // Return success with report metadata
            String result = String.format(
                "{\"generated\":true,\"reportType\":\"%s\",\"url\":\"%s\",\"pages\":42}",
                reportType, reportUrl
            );

            return TaskResult.success(result);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return TaskResult.failure(e);
        } catch (Exception e) {
            log.error("Failed to process REPORT_GENERATION task={}", task.id(), e);
            return TaskResult.failure(e);
        }
    }

    @Override
    public int maxAttempts() {
        return 5; // More attempts for important reports
    }

    @Override
    public BackoffPolicy backoffPolicy() {
        // Custom backoff: start with 5 seconds, multiply by 3, max 5 minutes
        return new BackoffPolicy(5000L, 3.0, 300_000L);
    }
}
