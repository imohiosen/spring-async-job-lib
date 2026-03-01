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
class DataExportHandlerTest {

    @Autowired
    private DataExportHandler handler;

    @Test
    void shouldReturnCorrectTaskType() {
        assertThat(handler.taskType()).isEqualTo("DATA_EXPORT");
    }

    @Test
    void shouldReturnMaxAttemptsOfThree() {
        assertThat(handler.maxAttempts()).isEqualTo(3);
    }

    @Test
    void shouldProcessValidExportTask() {
        // Given
        String payload = "{\"format\":\"CSV\",\"recordCount\":1000,\"entity\":\"customers\"}";
        JobTask task = createTask("DATA_EXPORT", payload, 0);

        // When
        TaskResult result = handler.handle(task);

        // Then
        assertThat(result.success()).isTrue();
        assertThat(result.data()).contains("exported");
        assertThat(result.data()).contains("CSV");
        assertThat(result.data()).contains("1000");
    }

    @Test
    void shouldHandleDifferentFormats() {
        // Given - JSON format
        String payload = "{\"format\":\"JSON\",\"recordCount\":500,\"entity\":\"orders\"}";
        JobTask task = createTask("DATA_EXPORT", payload, 0);

        // When
        TaskResult result = handler.handle(task);

        // Then
        assertThat(result.success()).isTrue();
        assertThat(result.data()).contains("JSON");
        assertThat(result.data()).contains("url");
    }

    @Test
    void shouldHandleLargeExports() {
        // Given - large export
        String payload = "{\"format\":\"CSV\",\"recordCount\":50000,\"entity\":\"transactions\"}";
        JobTask task = createTask("DATA_EXPORT", payload, 0);

        // When
        TaskResult result = handler.handle(task);

        // Then
        assertThat(result.success()).isTrue();
        assertThat(result.data()).contains("50000");
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
                null, "{}"
        );
    }
}
