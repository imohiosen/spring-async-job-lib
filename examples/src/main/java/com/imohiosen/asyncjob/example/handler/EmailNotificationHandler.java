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
 * Example handler for email notification tasks.
 * Simulates sending emails with configurable delays and success/failure scenarios.
 */
@Component
public class EmailNotificationHandler implements JobTaskHandler {

    private static final Logger log = LoggerFactory.getLogger(EmailNotificationHandler.class);
    private final ObjectMapper objectMapper;

    public EmailNotificationHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String taskType() {
        return "EMAIL_NOTIFICATION";
    }

    @Override
    public TaskResult handle(JobTask task) {
        try {
            log.info("Processing EMAIL_NOTIFICATION task={} (attempt={})", 
                    task.id(), task.attemptCount());

            // Parse the payload
            JsonNode payload = objectMapper.readTree(task.payload());
            String recipient = payload.path("recipient").asText();
            String subject = payload.path("subject").asText();
            String body = payload.path("body").asText();
            
            // Simulate email sending delay
            Thread.sleep(500);

            // Simulate occasional failures for retry testing (10% failure rate on first attempt)
            if (task.attemptCount() == 0 && Math.random() < 0.1) {
                log.warn("Simulated email send failure for task={}", task.id());
                return TaskResult.failure(new RuntimeException("SMTP connection timeout"));
            }

            log.info("Successfully sent email to={} subject='{}' for task={}", 
                    recipient, subject, task.id());

            // Return success with result metadata
            String result = String.format(
                "{\"sent\":true,\"recipient\":\"%s\",\"messageId\":\"%s\"}",
                recipient, java.util.UUID.randomUUID()
            );
            
            return TaskResult.success(result);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return TaskResult.failure(e);
        } catch (Exception e) {
            log.error("Failed to process EMAIL_NOTIFICATION task={}", task.id(), e);
            return TaskResult.failure(e);
        }
    }

    @Override
    public int maxAttempts() {
        return 3; // Retry up to 3 times
    }
}
