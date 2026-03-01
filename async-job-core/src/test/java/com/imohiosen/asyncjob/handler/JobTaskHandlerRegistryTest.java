package com.imohiosen.asyncjob.handler;

import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskResult;
import com.imohiosen.asyncjob.domain.TaskStatus;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobTaskHandlerRegistryTest {

    // ── Registration ──────────────────────────────────────────────────────────

    @Test
    void registersMultipleHandlersSuccessfully() {
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(
                List.of(handler("INVOICE"), handler("REPORT"))
        );

        assertThat(registry.registeredTypes()).containsExactlyInAnyOrder("INVOICE", "REPORT");
    }

    @Test
    void emptyHandlerList_createsEmptyRegistry() {
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of());
        assertThat(registry.registeredTypes()).isEmpty();
    }

    @Test
    void nullHandlerList_createsEmptyRegistry() {
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(null);
        assertThat(registry.registeredTypes()).isEmpty();
    }

    @Test
    void duplicateTaskType_throwsIllegalState() {
        assertThatThrownBy(() -> new JobTaskHandlerRegistry(
                List.of(handler("INVOICE"), handler("INVOICE"))
        ))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate")
                .hasMessageContaining("INVOICE");
    }

    // ── Lookup ────────────────────────────────────────────────────────────────

    @Test
    void getHandler_knownType_returnsHandler() {
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of(handler("INVOICE")));

        Optional<JobTaskHandler> result = registry.getHandler("INVOICE");

        assertThat(result).isPresent();
        assertThat(result.get().taskType()).isEqualTo("INVOICE");
    }

    @Test
    void getHandler_unknownType_returnsEmpty() {
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of(handler("INVOICE")));

        assertThat(registry.getHandler("UNKNOWN")).isEmpty();
    }

    @Test
    void hasHandler_knownType_returnsTrue() {
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of(handler("REPORT")));
        assertThat(registry.hasHandler("REPORT")).isTrue();
    }

    @Test
    void hasHandler_unknownType_returnsFalse() {
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of(handler("REPORT")));
        assertThat(registry.hasHandler("UNKNOWN")).isFalse();
    }

    // ── maxAttempts ───────────────────────────────────────────────────────────

    @Test
    void defaultMaxAttempts_isFive() {
        JobTaskHandler h = handler("TEST");
        assertThat(h.maxAttempts()).isEqualTo(5);
    }

    @Test
    void customMaxAttempts_isRespected() {
        JobTaskHandler h = handler("RETRY3", 3);
        assertThat(h.maxAttempts()).isEqualTo(3);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static JobTaskHandler handler(String taskType) {
        return handler(taskType, 5);
    }

    private static JobTaskHandler handler(String taskType, int maxAttempts) {
        return new JobTaskHandler() {
            @Override public String taskType() { return taskType; }
            @Override public TaskResult handle(JobTask task) { return TaskResult.success("{}"); }
            @Override public int maxAttempts() { return maxAttempts; }
        };
    }
}
