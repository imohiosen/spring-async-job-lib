package com.imohiosen.asyncjob.application.consumer;

import com.imohiosen.asyncjob.domain.FencedLock;
import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskResult;
import com.imohiosen.asyncjob.domain.TaskStatus;
import com.imohiosen.asyncjob.application.executor.AsyncTaskExecutorBridge;
import com.imohiosen.asyncjob.application.handler.JobTaskHandler;
import com.imohiosen.asyncjob.application.handler.JobTaskHandlerRegistry;
import com.imohiosen.asyncjob.port.lock.TaskLockManager;
import com.imohiosen.asyncjob.port.messaging.JobMessage;
import com.imohiosen.asyncjob.port.repository.JobRepository;
import com.imohiosen.asyncjob.port.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DispatchingJobTaskConsumerTest {

    @Mock TaskRepository         taskRepository;
    @Mock JobRepository          jobRepository;
    @Mock TaskLockManager        lockManager;
    @Mock AsyncTaskExecutorBridge bridge;

    DispatchingJobTaskConsumer consumer;
    UUID taskId;
    UUID jobId;

    @BeforeEach
    void setUp() {
        taskId = UUID.randomUUID();
        jobId  = UUID.randomUUID();
    }

    // ── Dispatching ───────────────────────────────────────────────────────────

    @Test
    void dispatchesToCorrectHandler_forKnownTaskType() throws Exception {
        JobTaskHandler invoiceHandler = stubHandler("INVOICE", TaskResult.success("{\"invoice\":true}"));
        JobTaskHandler reportHandler  = stubHandler("REPORT",  TaskResult.success("{\"report\":true}"));
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of(invoiceHandler, reportHandler));
        consumer = new DispatchingJobTaskConsumer(
                taskRepository, jobRepository, lockManager, bridge, registry);

        JobTask task = buildTask(taskId, jobId, "INVOICE", TaskStatus.PENDING, 0);

        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(1L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
        when(taskRepository.markCompleted(eq(taskId), any(), any(), eq(1L))).thenReturn(true);
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.success("{\"invoice\":true}")));

        consumer.consume(new JobMessage(taskId, jobId, "INVOICE", "{}"));

        verify(taskRepository).markCompleted(eq(taskId), eq("{\"invoice\":true}"), any(), eq(1L));
        verify(lockManager).unlock(taskId);
    }

    @Test
    void dispatchesToSecondHandler_whenTaskTypeMatches() throws Exception {
        JobTaskHandler invoiceHandler = stubHandler("INVOICE", TaskResult.success("{\"invoice\":true}"));
        JobTaskHandler reportHandler  = stubHandler("REPORT",  TaskResult.success("{\"report\":true}"));
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of(invoiceHandler, reportHandler));
        consumer = new DispatchingJobTaskConsumer(
                taskRepository, jobRepository, lockManager, bridge, registry);

        JobTask task = buildTask(taskId, jobId, "REPORT", TaskStatus.PENDING, 0);

        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(2L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
        when(taskRepository.markCompleted(eq(taskId), any(), any(), eq(2L))).thenReturn(true);
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.success("{\"report\":true}")));

        consumer.consume(new JobMessage(taskId, jobId, "REPORT", "{}"));

        verify(taskRepository).markCompleted(eq(taskId), eq("{\"report\":true}"), any(), eq(2L));
    }

    @Test
    void unknownTaskType_failsGracefullyWithoutKillingConsumer() {
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(
                List.of(stubHandler("INVOICE", TaskResult.success("{}"))));
        consumer = new DispatchingJobTaskConsumer(
                taskRepository, jobRepository, lockManager, bridge, registry);

        JobTask task = buildTask(taskId, jobId, "UNKNOWN", TaskStatus.PENDING, 0);

        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(3L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
        when(bridge.submitAsync(any())).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            java.util.concurrent.Callable<TaskResult> work =
                    (java.util.concurrent.Callable<TaskResult>) invocation.getArgument(0);
            try {
                return CompletableFuture.completedFuture(work.call());
            } catch (Exception e) {
                return CompletableFuture.completedFuture(TaskResult.failure(e));
            }
        });

        // Must NOT throw — consume() swallows exceptions
        consumer.consume(new JobMessage(taskId, jobId, "UNKNOWN", "{}"));
        verify(lockManager).unlock(taskId);
    }

    // ── Per-handler maxAttempts ────────────────────────────────────────────────

    @Test
    void maxAttempts_usesHandlerSpecificValue() throws Exception {
        JobTaskHandler handler = new JobTaskHandler() {
            @Override public String taskType() { return "STRICT"; }
            @Override public int maxAttempts() { return 2; }
            @Override public TaskResult handle(JobTask task) {
                return TaskResult.failure(new RuntimeException("fail"));
            }
        };
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of(handler));
        consumer = new DispatchingJobTaskConsumer(
                taskRepository, jobRepository, lockManager, bridge, registry);

        // attemptCount=1, maxAttempts=2 → should move to DEAD_LETTER
        JobTask task = buildTask(taskId, jobId, "STRICT", TaskStatus.FAILED, 1);

        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(4L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
        when(taskRepository.markDeadLetter(eq(taskId), eq(2), any(), eq("fail"),
                eq("java.lang.RuntimeException"), eq(4L))).thenReturn(true);
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.failure(new RuntimeException("fail"))));

        consumer.consume(new JobMessage(taskId, jobId, "STRICT", "{}"));

        // attempt 2 of 2 → DEAD_LETTER
        verify(taskRepository).markDeadLetter(eq(taskId), eq(2), any(), eq("fail"),
                eq("java.lang.RuntimeException"), eq(4L));
    }

    @Test
    void maxAttempts_fallsBackToDefault_forUnknownType() {
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of());
        consumer = new DispatchingJobTaskConsumer(
                taskRepository, jobRepository, lockManager, bridge, registry);

        JobTask task = buildTask(taskId, jobId, "NONEXISTENT", TaskStatus.PENDING, 0);
        assertThat(consumer.getMaxAttempts(task)).isEqualTo(5);
    }

    // ── Per-handler executor ──────────────────────────────────────────────────

    @Test
    void submitWork_handlerProvidesExecutor_usesHandlerExecutor() throws Exception {
        java.util.concurrent.ExecutorService handlerExecutor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            JobTaskHandler handler = new JobTaskHandler() {
                @Override public String taskType() { return "EXEC"; }
                @Override public TaskResult handle(JobTask task) {
                    return TaskResult.success("{\"exec\":true}");
                }
                @Override public java.util.concurrent.ExecutorService executor() {
                    return handlerExecutor;
                }
            };
            JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of(handler));
            consumer = new DispatchingJobTaskConsumer(
                    taskRepository, jobRepository, lockManager, bridge, registry);

            JobTask task = buildTask(taskId, jobId, "EXEC", TaskStatus.PENDING, 0);

            when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(5L)));
            when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
            when(taskRepository.markCompleted(eq(taskId), any(), any(), eq(5L))).thenReturn(true);

            consumer.consume(new JobMessage(taskId, jobId, "EXEC", "{}"));

            verify(taskRepository).markCompleted(eq(taskId), eq("{\"exec\":true}"), any(), eq(5L));
            // Bridge should NOT be called since handler has its own executor
            verify(bridge, never()).submitAsync(any());
        } finally {
            handlerExecutor.shutdownNow();
        }
    }

    @Test
    void submitWork_retryAttemptWithRetryExecutor_usesRetryExecutor() throws Exception {
        java.util.concurrent.ExecutorService primaryExec = java.util.concurrent.Executors.newSingleThreadExecutor();
        java.util.concurrent.ExecutorService retryExec = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            JobTaskHandler handler = new JobTaskHandler() {
                @Override public String taskType() { return "RETRY_EXEC"; }
                @Override public TaskResult handle(JobTask task) {
                    return TaskResult.success("{\"retried\":true}");
                }
                @Override public java.util.concurrent.ExecutorService executor() {
                    return primaryExec;
                }
                @Override public java.util.concurrent.ExecutorService retryExecutor() {
                    return retryExec;
                }
            };
            JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of(handler));
            consumer = new DispatchingJobTaskConsumer(
                    taskRepository, jobRepository, lockManager, bridge, registry);

            // attemptCount=1 → retry
            JobTask task = buildTask(taskId, jobId, "RETRY_EXEC", TaskStatus.FAILED, 1);

            when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(6L)));
            when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
            when(taskRepository.markCompleted(eq(taskId), any(), any(), eq(6L))).thenReturn(true);

            consumer.consume(new JobMessage(taskId, jobId, "RETRY_EXEC", "{}"));

            verify(taskRepository).markCompleted(eq(taskId), eq("{\"retried\":true}"), any(), eq(6L));
            verify(bridge, never()).submitAsync(any());
        } finally {
            primaryExec.shutdownNow();
            retryExec.shutdownNow();
        }
    }

    @Test
    void submitWork_retryAttemptNoRetryExecutor_fallsToPrimaryExecutor() throws Exception {
        java.util.concurrent.ExecutorService primaryExec = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            JobTaskHandler handler = new JobTaskHandler() {
                @Override public String taskType() { return "FALLBACK"; }
                @Override public TaskResult handle(JobTask task) {
                    return TaskResult.success("{\"fallback\":true}");
                }
                @Override public java.util.concurrent.ExecutorService executor() {
                    return primaryExec;
                }
                // retryExecutor() defaults to null
            };
            JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of(handler));
            consumer = new DispatchingJobTaskConsumer(
                    taskRepository, jobRepository, lockManager, bridge, registry);

            // attemptCount=2 → retry, no retryExecutor → falls back to primary
            JobTask task = buildTask(taskId, jobId, "FALLBACK", TaskStatus.FAILED, 2);

            when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(7L)));
            when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
            when(taskRepository.markCompleted(eq(taskId), any(), any(), eq(7L))).thenReturn(true);

            consumer.consume(new JobMessage(taskId, jobId, "FALLBACK", "{}"));

            verify(taskRepository).markCompleted(eq(taskId), eq("{\"fallback\":true}"), any(), eq(7L));
            verify(bridge, never()).submitAsync(any());
        } finally {
            primaryExec.shutdownNow();
        }
    }

    @Test
    void submitWork_noHandlerExecutor_usesSharedBridge() throws Exception {
        JobTaskHandler handler = stubHandler("SHARED", TaskResult.success("{\"shared\":true}"));
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of(handler));
        consumer = new DispatchingJobTaskConsumer(
                taskRepository, jobRepository, lockManager, bridge, registry);

        JobTask task = buildTask(taskId, jobId, "SHARED", TaskStatus.PENDING, 0);

        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(8L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
        when(taskRepository.markCompleted(eq(taskId), any(), any(), eq(8L))).thenReturn(true);
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.success("{\"shared\":true}")));

        consumer.consume(new JobMessage(taskId, jobId, "SHARED", "{}"));

        verify(bridge).submitAsync(any());
    }

    // ── Per-handler backoff policy ─────────────────────────────────────────────

    @Test
    void resolveBackoffPolicy_handlerProvidesPolicy_usesHandlerPolicy() {
        com.imohiosen.asyncjob.domain.BackoffPolicy custom =
                new com.imohiosen.asyncjob.domain.BackoffPolicy(5_000L, 3.0, 600_000L);
        JobTaskHandler handler = new JobTaskHandler() {
            @Override public String taskType() { return "CUSTOM_BACKOFF"; }
            @Override public TaskResult handle(JobTask task) { return TaskResult.success("{}"); }
            @Override public com.imohiosen.asyncjob.domain.BackoffPolicy backoffPolicy() { return custom; }
        };
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of(handler));
        consumer = new DispatchingJobTaskConsumer(
                taskRepository, jobRepository, lockManager, bridge, registry);

        JobTask task = buildTask(taskId, jobId, "CUSTOM_BACKOFF", TaskStatus.FAILED, 1);
        com.imohiosen.asyncjob.domain.BackoffPolicy resolved = consumer.resolveBackoffPolicy(task);

        assertThat(resolved).isSameAs(custom);
    }

    @Test
    void resolveBackoffPolicy_noHandler_fallsBackToTaskPolicy() {
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of());
        consumer = new DispatchingJobTaskConsumer(
                taskRepository, jobRepository, lockManager, bridge, registry);

        JobTask task = buildTask(taskId, jobId, "NONEXISTENT", TaskStatus.FAILED, 1);
        com.imohiosen.asyncjob.domain.BackoffPolicy resolved = consumer.resolveBackoffPolicy(task);

        assertThat(resolved.baseIntervalMs()).isEqualTo(1_000L);
        assertThat(resolved.multiplier()).isEqualTo(2.0);
        assertThat(resolved.maxDelayMs()).isEqualTo(3_600_000L);
    }

    // ── Time-critical dispatching ─────────────────────────────────────────────

    @Test
    void submitTimeCriticalWork_noExecutorConfigured_fallsBackToNormalSubmit() throws Exception {
        JobTaskHandler handler = stubHandler("TC_TYPE", TaskResult.success("{}"));
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of(handler));
        consumer = new DispatchingJobTaskConsumer(
                taskRepository, jobRepository, lockManager, bridge, registry);
        // No TimeCriticalResilienceExecutor passed

        JobTask task = buildTimeCriticalTask(taskId, jobId, "TC_TYPE", TaskStatus.PENDING, 0);

        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(8L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
        when(taskRepository.markCompleted(eq(taskId), any(), any(), eq(8L))).thenReturn(true);
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.success("{}")));

        consumer.consume(new JobMessage(taskId, jobId, "TC_TYPE", "{}"));

        // Falls back to normal submitWork → bridge.submitAsync
        verify(bridge).submitAsync(any());
    }

    @Test
    void submitTimeCriticalWork_withExecutor_delegatesToResilienceExecutor() throws Exception {
        TimeCriticalResilienceExecutor tcExecutor = mock(TimeCriticalResilienceExecutor.class);
        when(tcExecutor.execute(any(), anyLong(), any())).thenReturn(TaskResult.success("{}"));

        // Use a real single-thread executor so CompletableFuture.supplyAsync actually runs
        ExecutorService handlerExec = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            JobTaskHandler handler = new JobTaskHandler() {
                @Override public String taskType() { return "TC_TYPE"; }
                @Override public TaskResult handle(JobTask task) { return TaskResult.success("{}"); }
                @Override public ExecutorService executor() { return handlerExec; }
            };
            JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of(handler));
            consumer = new DispatchingJobTaskConsumer(
                    taskRepository, jobRepository, lockManager, bridge, registry, tcExecutor);

            JobTask task = buildTimeCriticalTask(taskId, jobId, "TC_TYPE", TaskStatus.PENDING, 0);

            when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(8L)));
            when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
            when(taskRepository.markCompleted(eq(taskId), any(), any(), eq(8L))).thenReturn(true);

            consumer.consume(new JobMessage(taskId, jobId, "TC_TYPE", "{}"));

            verify(tcExecutor).execute(any(), eq(8L), any());
        } finally {
            handlerExec.shutdownNow();
        }
    }

    @Test
    void submitTimeCriticalWork_withExecutor_noHandler_fallsBackToNormalSubmit() throws Exception {
        TimeCriticalResilienceExecutor tcExecutor = mock(TimeCriticalResilienceExecutor.class);
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of());
        consumer = new DispatchingJobTaskConsumer(
                taskRepository, jobRepository, lockManager, bridge, registry, tcExecutor);

        JobTask task = buildTimeCriticalTask(taskId, jobId, "UNKNOWN", TaskStatus.PENDING, 0);

        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(8L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
        when(taskRepository.markCompleted(eq(taskId), any(), any(), eq(8L))).thenReturn(true);
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.success("{}")));

        consumer.consume(new JobMessage(taskId, jobId, "UNKNOWN", "{}"));

        verify(bridge).submitAsync(any());
        verify(tcExecutor, never()).execute(any(), anyLong(), any());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private JobTask buildTask(UUID taskId, UUID jobId, String taskType,
                              TaskStatus status, int attemptCount) {
        OffsetDateTime now = OffsetDateTime.now();
        return new JobTask(
                taskId, jobId, taskType, "test-topic",
                null, null, status,
                now, now, null, null,
                now.plusHours(1), false,
                attemptCount, null, null,
                1_000L, 2.0, 3_600_000L,
                null, null, null, null, null, "{}", null,
                false, 0, 0L, 1.0, 0L, 0L
        );
    }

    private static JobTaskHandler stubHandler(String taskType, TaskResult result) {
        return new JobTaskHandler() {
            @Override public String taskType() { return taskType; }
            @Override public TaskResult handle(JobTask task) { return result; }
        };
    }

    private JobTask buildTimeCriticalTask(UUID taskId, UUID jobId, String taskType,
                              TaskStatus status, int attemptCount) {
        OffsetDateTime now = OffsetDateTime.now();
        return new JobTask(
                taskId, jobId, taskType, "test-topic",
                null, null, status,
                now, now, null, null,
                now.plusHours(1), false,
                attemptCount, null, null,
                1_000L, 2.0, 3_600_000L,
                null, null, null, null, null, "{}", null,
                true, 10, 100L, 1.5, 900L, 2000L
        );
    }
}
