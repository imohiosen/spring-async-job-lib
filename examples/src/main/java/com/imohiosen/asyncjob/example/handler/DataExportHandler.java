package com.imohiosen.asyncjob.example.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.imohiosen.asyncjob.application.handler.JobTaskHandler;
import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Example handler for data export tasks.
 * Demonstrates handling large data exports with progress tracking.
 */
@Component
public class DataExportHandler implements JobTaskHandler {

    private static final Logger log = LoggerFactory.getLogger(DataExportHandler.class);
    private final ObjectMapper objectMapper;

    public DataExportHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String taskType() {
        return "DATA_EXPORT";
    }

    @Override
    public TaskResult handle(JobTask task) {
        try {
            log.info("Processing DATA_EXPORT task={} (attempt={})", 
                    task.id(), task.attemptCount());

            // Parse the payload
            JsonNode payload = objectMapper.readTree(task.payload());
            String format = payload.path("format").asText("CSV");
            int recordCount = payload.path("recordCount").asInt(1000);
            String entity = payload.path("entity").asText();

            log.debug("Exporting {} records of {} to {} format", recordCount, entity, format);

            // Simulate data export with progress
            int batchSize = 100;
            for (int i = 0; i < recordCount; i += batchSize) {
                int progress = Math.min(i + batchSize, recordCount);
                log.debug("Export progress: {}/{} records", progress, recordCount);
                Thread.sleep(100); // Simulate batch processing
            }

            String downloadUrl = String.format("https://exports.example.com/%s.%s", 
                    java.util.UUID.randomUUID(), format.toLowerCase());

            log.info("Successfully exported {} {} records to {} for task={}", 
                    recordCount, entity, format, task.id());

            // Return success with export metadata
            String result = String.format(
                "{\"exported\":true,\"recordCount\":%d,\"format\":\"%s\",\"url\":\"%s\"}",
                recordCount, format, downloadUrl
            );

            return TaskResult.success(result);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return TaskResult.failure(e);
        } catch (Exception e) {
            log.error("Failed to process DATA_EXPORT task={}", task.id(), e);
            return TaskResult.failure(e);
        }
    }

    @Override
    public int maxAttempts() {
        return 3;
    }
}
