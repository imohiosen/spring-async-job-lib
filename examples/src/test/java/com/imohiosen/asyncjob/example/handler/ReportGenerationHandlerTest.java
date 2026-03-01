package com.imohiosen.asyncjob.example.handler;

import com.imohiosen.asyncjob.domain.BackoffPolicy;
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
class ReportGenerationHandlerTest {

    @Autowired
    private ReportGenerationHandler handler;

    @Test
    void shouldReturnCorrectTaskType() {
        assertThat(handler.taskType()).isEqualTo("REPORT_GENERATION");
    }

    @Test
    void shouldReturnMaxAttemptsOfFive() {
        assertThat(handler.maxAttempts()).isEqualTo(5);
    }

    @Test
    void shouldHaveCustomBackoffPolicy() {
        BackoffPolicy policy = handler.backoffPolicy();
        
        assertThat(policy.baseIntervalMs()).isEqualTo(5000L);
        assertThat(policy.multiplier()).isEqualTo(3.0);
        assertThat(policy.maxDelayMs()).isEqualTo(300_000L);
    }

    @Test
    void shouldProcessValidReportTask() {
        // Given
        String payload = "{\"reportType\":\"SALES\",\"startDate\":\"2026-01-01\",\"endDate\":\"2026-01-31\"}";
        JobTask task = createTask("REPORT_GENERATION", payload, 0);

        // When
        TaskResult result = handler.handle(task);

        // Then
        assertThat(result.success()).isTrue();
        assertThat(result.data()).contains("generated");
        assertThat(result.data()).contains("SALES");
        assertThat(result.data()).contains("url");
    }

    @Test
    void shouldIncludeReportMetadataInResult() {
        // Given
        String payload = "{\"reportType\":\"INVENTORY\",\"startDate\":\"2026-02-01\",\"endDate\":\"2026-02-28\"}";
        JobTask task = createTask("REPORT_GENERATION", payload, 0);

        // When
        TaskResult result = handler.handle(task);

        // Then
        assertThat(result.success()).isTrue();
        assertThat(result.data()).contains("reportType\":\"INVENTORY\"");
        assertThat(result.data()).contains("pages");
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
