package com.imohiosen.asyncjob.example.handler;

import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskResult;
import com.imohiosen.asyncjob.domain.TaskStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.OffsetDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class EmailNotificationHandlerTest {

    @Autowired
    private EmailNotificationHandler handler;

    @Test
    void shouldReturnCorrectTaskType() {
        assertThat(handler.taskType()).isEqualTo("EMAIL_NOTIFICATION");
    }

    @Test
    void shouldReturnMaxAttemptsOfThree() {
        assertThat(handler.maxAttempts()).isEqualTo(3);
    }

    @Test
    void shouldProcessValidEmailTask() {
        // Given
        String payload = "{\"recipient\":\"test@example.com\",\"subject\":\"Test\",\"body\":\"Hello\"}";
        JobTask task = createTask("EMAIL_NOTIFICATION", payload, 0);

        // When
        TaskResult result = handler.handle(task);

        // Then
        assertThat(result.success()).isTrue();
        assertThat(result.data()).contains("sent");
        assertThat(result.data()).contains("test@example.com");
    }

    @Test
    void shouldHandleRetryAttempts() {
        // Given - second attempt (retries don't fail)
        String payload = "{\"recipient\":\"retry@example.com\",\"subject\":\"Retry\",\"body\":\"Test\"}";
        JobTask task = createTask("EMAIL_NOTIFICATION", payload, 1);

        // When
        TaskResult result = handler.handle(task);

        // Then
        assertThat(result.success()).isTrue();
    }

    @Test
    void shouldHandleInvalidPayload() {
        // Given
        String invalidPayload = "not-json";
        JobTask task = createTask("EMAIL_NOTIFICATION", invalidPayload, 0);

        // When
        TaskResult result = handler.handle(task);

        // Then
        assertThat(result.success()).isFalse();
        assertThat(result.error()).isNotNull();
    }

    private JobTask createTask(String taskType, String payload, int attemptCount) {
        OffsetDateTime now = OffsetDateTime.now();
        return new JobTask(
                UUID.randomUUID(),
                UUID.randomUUID(),
                taskType,
                "test-topic",
                null, null,
                TaskStatus.PENDING,
                now, now, null, null,
                now.plusHours(1),
                false,
                attemptCount,
                null, null,
                1000L, 2.0, 3600000L,
                null, null,
                null, null, null,
                payload,
                null,
                false, 0, 0L, 1.0, 0L, 0L
        );
    }
}
