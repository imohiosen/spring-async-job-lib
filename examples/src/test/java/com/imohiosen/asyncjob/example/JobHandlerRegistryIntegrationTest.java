package com.imohiosen.asyncjob.example;

import com.imohiosen.asyncjob.application.handler.JobTaskHandler;
import com.imohiosen.asyncjob.application.handler.JobTaskHandlerRegistry;
import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskResult;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the job handler registry configuration.
 */
class JobHandlerRegistryIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private JobTaskHandlerRegistry registry;

    @Test
    void shouldRegisterAllHandlers() {
        // Then
        assertThat(registry.registeredTypes())
                .contains("EMAIL_NOTIFICATION", "REPORT_GENERATION", "DATA_EXPORT");
    }

    @Test
    void shouldRetrieveEmailHandler() {
        // When
        Optional<JobTaskHandler> handler = registry.getHandler("EMAIL_NOTIFICATION");

        // Then
        assertThat(handler).isPresent();
        assertThat(handler.get().taskType()).isEqualTo("EMAIL_NOTIFICATION");
        assertThat(handler.get().maxAttempts()).isEqualTo(3);
    }

    @Test
    void shouldRetrieveReportHandler() {
        // When
        Optional<JobTaskHandler> handler = registry.getHandler("REPORT_GENERATION");

        // Then
        assertThat(handler).isPresent();
        assertThat(handler.get().taskType()).isEqualTo("REPORT_GENERATION");
        assertThat(handler.get().maxAttempts()).isEqualTo(5);
    }

    @Test
    void shouldRetrieveDataExportHandler() {
        // When
        Optional<JobTaskHandler> handler = registry.getHandler("DATA_EXPORT");

        // Then
        assertThat(handler).isPresent();
        assertThat(handler.get().taskType()).isEqualTo("DATA_EXPORT");
        assertThat(handler.get().maxAttempts()).isEqualTo(3);
    }

    @Test
    void shouldReturnEmptyForUnknownHandler() {
        // When
        Optional<JobTaskHandler> handler = registry.getHandler("UNKNOWN_TYPE");

        // Then
        assertThat(handler).isEmpty();
    }

    @Test
    void shouldCheckHandlerExistence() {
        // Then
        assertThat(registry.hasHandler("EMAIL_NOTIFICATION")).isTrue();
        assertThat(registry.hasHandler("REPORT_GENERATION")).isTrue();
        assertThat(registry.hasHandler("DATA_EXPORT")).isTrue();
        assertThat(registry.hasHandler("NONEXISTENT")).isFalse();
    }
}
