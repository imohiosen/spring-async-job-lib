package com.imohiosen.asyncjob.application.consumer;

import com.imohiosen.asyncjob.application.handler.JobTaskHandler;
import com.imohiosen.asyncjob.domain.BackoffPolicy;
import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskResult;
import com.imohiosen.asyncjob.domain.TaskStatus;
import com.imohiosen.asyncjob.domain.TimeCriticalPolicy;
import com.imohiosen.asyncjob.port.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TimeCriticalResilienceExecutorTest {

    @Mock TaskRepository taskRepository;

    ScheduledExecutorService syncScheduler;
    TimeCriticalResilienceExecutor executor;

    UUID   taskId;
    UUID   jobId;
    long   fenceToken = 42L;

    @BeforeEach
    void setUp() {
        syncScheduler = Executors.newSingleThreadScheduledExecutor();
        executor = new TimeCriticalResilienceExecutor(taskRepository, syncScheduler);
        taskId = UUID.randomUUID();
        jobId  = UUID.randomUUID();
    }

    @Test
    void execute_handlerSucceedsFirstAttempt_returnsResult_noDatabaseSyncs() {
        JobTask task = buildTimeCriticalTask(taskId, jobId, 0);
        JobTaskHandler handler = stubHandler(TaskResult.success("{\"ok\":true}"));

        TaskResult result = executor.execute(task, fenceToken, handler);

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.payload()).isEqualTo("{\"ok\":true}");
        // No retries occurred → no DB sync needed
        verify(taskRepository, never()).persistTimeCriticalProgress(
                any(), anyInt(), any(), anyString(), anyString(), anyLong());
    }

    @Test
    void execute_handlerFailsThenSucceeds_retriesAndReturnsResult() {
        JobTask task = buildTimeCriticalTask(taskId, jobId, 0);
        AtomicInteger callCount = new AtomicInteger(0);
        JobTaskHandler handler = new JobTaskHandler() {
            @Override public String taskType() { return "TEST"; }
            @Override public TaskResult handle(JobTask t) {
                if (callCount.getAndIncrement() < 2) {
                    throw new RuntimeException("transient error");
                }
                return TaskResult.success("{\"retried\":true}");
            }
        };

        TaskResult result = executor.execute(task, fenceToken, handler);

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.payload()).isEqualTo("{\"retried\":true}");
        assertThat(callCount.get()).isEqualTo(3); // 2 failures + 1 success
    }

    @Test
    void execute_maxRetriesExceeded_throwsException() {
        // Policy: 3 max attempts, so after 3 failures we should get an exception
        TimeCriticalPolicy policy = new TimeCriticalPolicy(3, 10L, 1.0, 50L, 60_000L);
        JobTask task = buildTimeCriticalTask(taskId, jobId, 0, policy);
        JobTaskHandler handler = new JobTaskHandler() {
            @Override public String taskType() { return "TEST"; }
            @Override public TaskResult handle(JobTask t) {
                throw new RuntimeException("always fails");
            }
        };

        assertThatThrownBy(() -> executor.execute(task, fenceToken, handler))
                .isInstanceOf(TimeCriticalRetriesExhaustedException.class)
                .hasMessageContaining("always fails");
    }

    @Test
    void execute_periodicSyncFires_callsPersistTimeCriticalProgress() throws Exception {
        // Use a policy with very short DB sync interval to trigger sync during retries
        TimeCriticalPolicy policy = new TimeCriticalPolicy(20, 50L, 1.0, 50L, 50L);
        JobTask task = buildTimeCriticalTask(taskId, jobId, 0, policy);
        AtomicInteger callCount = new AtomicInteger(0);
        JobTaskHandler handler = new JobTaskHandler() {
            @Override public String taskType() { return "TEST"; }
            @Override public TaskResult handle(JobTask t) {
                if (callCount.getAndIncrement() < 15) {
                    throw new RuntimeException("keep failing");
                }
                return TaskResult.success("{\"ok\":true}");
            }
        };

        when(taskRepository.persistTimeCriticalProgress(
                eq(taskId), anyInt(), any(), anyString(), anyString(), eq(fenceToken)))
                .thenReturn(true);

        TaskResult result = executor.execute(task, fenceToken, handler);

        assertThat(result.isSuccess()).isTrue();
        // With 15 failures at 50ms each = 750ms, and DB sync every 50ms,
        // we should have multiple syncs
        verify(taskRepository, atLeastOnce()).persistTimeCriticalProgress(
                eq(taskId), anyInt(), any(), anyString(), anyString(), eq(fenceToken));
    }

    @Test
    void execute_syncUsesCurrentFenceToken() throws Exception {
        // Short sync interval to trigger a sync
        TimeCriticalPolicy policy = new TimeCriticalPolicy(10, 50L, 1.0, 50L, 30L);
        JobTask task = buildTimeCriticalTask(taskId, jobId, 0, policy);
        AtomicInteger callCount = new AtomicInteger(0);
        JobTaskHandler handler = new JobTaskHandler() {
            @Override public String taskType() { return "TEST"; }
            @Override public TaskResult handle(JobTask t) {
                if (callCount.getAndIncrement() < 5) {
                    throw new RuntimeException("temp error");
                }
                return TaskResult.success("{}");
            }
        };

        when(taskRepository.persistTimeCriticalProgress(
                eq(taskId), anyInt(), any(), anyString(), anyString(), eq(99L)))
                .thenReturn(true);

        executor.execute(task, 99L, handler);

        // All sync calls must use the same fence token
        verify(taskRepository, atLeastOnce()).persistTimeCriticalProgress(
                eq(taskId), anyInt(), any(), anyString(), anyString(), eq(99L));
        verify(taskRepository, never()).persistTimeCriticalProgress(
                any(), anyInt(), any(), anyString(), anyString(), not(eq(99L)));
    }

    @Test
    void execute_staleTokenOnSync_continuesRetrying() throws Exception {
        // Sync returns false (stale token) but executor should continue retrying
        TimeCriticalPolicy policy = new TimeCriticalPolicy(10, 50L, 1.0, 50L, 30L);
        JobTask task = buildTimeCriticalTask(taskId, jobId, 0, policy);
        AtomicInteger callCount = new AtomicInteger(0);
        JobTaskHandler handler = new JobTaskHandler() {
            @Override public String taskType() { return "TEST"; }
            @Override public TaskResult handle(JobTask t) {
                if (callCount.getAndIncrement() < 5) {
                    throw new RuntimeException("temp");
                }
                return TaskResult.success("{}");
            }
        };

        // Sync returns false → stale token, but execution should continue
        when(taskRepository.persistTimeCriticalProgress(
                any(), anyInt(), any(), anyString(), anyString(), anyLong()))
                .thenReturn(false);

        TaskResult result = executor.execute(task, fenceToken, handler);

        // Should still succeed despite stale sync
        assertThat(result.isSuccess()).isTrue();
    }

    @Test
    void execute_finalSyncCalledOnExhaustion_beforeRethrow() {
        TimeCriticalPolicy policy = new TimeCriticalPolicy(3, 10L, 1.0, 50L, 60_000L);
        JobTask task = buildTimeCriticalTask(taskId, jobId, 0, policy);
        JobTaskHandler handler = new JobTaskHandler() {
            @Override public String taskType() { return "TEST"; }
            @Override public TaskResult handle(JobTask t) {
                throw new RuntimeException("persistent failure");
            }
        };

        when(taskRepository.persistTimeCriticalProgress(
                eq(taskId), anyInt(), any(), anyString(), anyString(), eq(fenceToken)))
                .thenReturn(true);

        assertThatThrownBy(() -> executor.execute(task, fenceToken, handler))
                .isInstanceOf(TimeCriticalRetriesExhaustedException.class);

        // Final sync should have been called with the last attempt count
        verify(taskRepository, atLeastOnce()).persistTimeCriticalProgress(
                eq(taskId), anyInt(), any(), eq("persistent failure"),
                eq("java.lang.RuntimeException"), eq(fenceToken));
    }

    @Test
    void execute_handlerSucceeds_noFinalSyncNeeded() {
        JobTask task = buildTimeCriticalTask(taskId, jobId, 0);
        JobTaskHandler handler = stubHandler(TaskResult.success("{}"));

        executor.execute(task, fenceToken, handler);

        verify(taskRepository, never()).persistTimeCriticalProgress(
                any(), anyInt(), any(), anyString(), anyString(), anyLong());
    }

    @Test
    void execute_handlerReturnsTaskResultFailure_treatedAsException() {
        TimeCriticalPolicy policy = new TimeCriticalPolicy(2, 10L, 1.0, 50L, 60_000L);
        JobTask task = buildTimeCriticalTask(taskId, jobId, 0, policy);
        JobTaskHandler handler = new JobTaskHandler() {
            @Override public String taskType() { return "TEST"; }
            @Override public TaskResult handle(JobTask t) {
                return TaskResult.failure(new RuntimeException("soft failure"));
            }
        };

        when(taskRepository.persistTimeCriticalProgress(
                any(), anyInt(), any(), anyString(), anyString(), anyLong()))
                .thenReturn(true);

        assertThatThrownBy(() -> executor.execute(task, fenceToken, handler))
                .isInstanceOf(TimeCriticalRetriesExhaustedException.class);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private JobTask buildNonTimeCriticalTask(UUID taskId, UUID jobId) {
        OffsetDateTime now = OffsetDateTime.now();
        return new JobTask(
                taskId, jobId, "TEST", "test-topic",
                null, null, TaskStatus.PENDING,
                now, now, null, null,
                now.plusHours(1), false,
                0, null, null,
                1_000L, 2.0, 3_600_000L,
                null, null, null, null, null, "{}", null,
                false, 0, 0L, 1.0, 0L, 0L
        );
    }

    @Test
    void execute_nonTimeCriticalTask_throwsIllegalState() {
        JobTask task = buildNonTimeCriticalTask(taskId, jobId);
        JobTaskHandler handler = stubHandler(TaskResult.success("{}"));

        assertThatThrownBy(() -> executor.execute(task, fenceToken, handler))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not time-critical");
    }

    @Test
    void execute_handlerReturnsNull_treatedAsNpe() {
        TimeCriticalPolicy policy = new TimeCriticalPolicy(2, 10L, 1.0, 50L, 60_000L);
        JobTask task = buildTimeCriticalTask(taskId, jobId, 0, policy);
        JobTaskHandler handler = new JobTaskHandler() {
            @Override public String taskType() { return "TEST"; }
            @Override public TaskResult handle(JobTask t) { return null; }
        };

        when(taskRepository.persistTimeCriticalProgress(
                any(), anyInt(), any(), anyString(), anyString(), anyLong()))
                .thenReturn(true);

        assertThatThrownBy(() -> executor.execute(task, fenceToken, handler))
                .isInstanceOf(TimeCriticalRetriesExhaustedException.class);
    }

    @Test
    void execute_resultBasedExhaustion_flushesAndThrows() {
        // Handler always returns failure result (not exception) — tests the
        // "result.isSuccess() == false" branch after retry.executeCallable()
        TimeCriticalPolicy policy = new TimeCriticalPolicy(2, 10L, 1.0, 50L, 60_000L);
        JobTask task = buildTimeCriticalTask(taskId, jobId, 0, policy);
        JobTaskHandler handler = new JobTaskHandler() {
            @Override public String taskType() { return "TEST"; }
            @Override public TaskResult handle(JobTask t) {
                return TaskResult.failure(new RuntimeException("result-fail"));
            }
        };

        when(taskRepository.persistTimeCriticalProgress(
                any(), anyInt(), any(), anyString(), anyString(), anyLong()))
                .thenReturn(true);

        assertThatThrownBy(() -> executor.execute(task, fenceToken, handler))
                .isInstanceOf(TimeCriticalRetriesExhaustedException.class);

        // Final progress should have been flushed
        verify(taskRepository, atLeastOnce()).persistTimeCriticalProgress(
                eq(taskId), anyInt(), any(), anyString(), anyString(), eq(fenceToken));
    }

    @Test
    void execute_periodicSyncSkipsWhenNoNewAttempts() throws Exception {
        // Use a sync interval shorter than the handler execution time
        // so the sync fires while the handler is "working" (sleeping)
        TimeCriticalPolicy policy = new TimeCriticalPolicy(2, 200L, 1.0, 200L, 20L);
        JobTask task = buildTimeCriticalTask(taskId, jobId, 0, policy);
        // First call succeeds immediately — no retries, so attemptsSinceLastSync stays 0
        // But the periodic sync fires quickly (every 20ms) and should skip
        JobTaskHandler handler = new JobTaskHandler() {
            @Override public String taskType() { return "TEST"; }
            @Override public TaskResult handle(JobTask t) {
                try { Thread.sleep(80); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                return TaskResult.success("{}");
            }
        };

        TaskResult result = executor.execute(task, fenceToken, handler);

        assertThat(result.isSuccess()).isTrue();
        // Periodic sync should NOT have written because attemptsSinceLastSync == 0
        verify(taskRepository, never()).persistTimeCriticalProgress(
                any(), anyInt(), any(), anyString(), anyString(), anyLong());
    }

    // ── Null error branches ─────────────────────────────────────────────────

    @Test
    void execute_resultBasedExhaustionWithNullError_coversCauseNullBranch() {
        // Handler returns TaskResult.failure(null) → tr.error() returns null
        // Covers L109 (err != null → false), L124 (cause == null → true)
        TimeCriticalPolicy policy = new TimeCriticalPolicy(2, 10L, 1.0, 50L, 60_000L);
        JobTask task = buildTimeCriticalTask(taskId, jobId, 0, policy);
        JobTaskHandler handler = new JobTaskHandler() {
            @Override public String taskType() { return "TEST"; }
            @Override public TaskResult handle(JobTask t) {
                return TaskResult.failure(null);
            }
        };

        when(taskRepository.persistTimeCriticalProgress(
                any(), anyInt(), any(), anyString(), anyString(), anyLong()))
                .thenReturn(true);

        assertThatThrownBy(() -> executor.execute(task, fenceToken, handler))
                .isInstanceOf(TimeCriticalRetriesExhaustedException.class)
                .hasMessageContaining("Time-critical task failed after all retries");
    }

    @Test
    void execute_singleAttemptException_lastErrorNull_usesExceptionAsRootCause() {
        // maxAttempts=1 → handler throws on first call → onRetry never fires
        // → lastError stays null → L134 (lastError.get() != null → false)
        TimeCriticalPolicy policy = new TimeCriticalPolicy(1, 10L, 1.0, 50L, 60_000L);
        JobTask task = buildTimeCriticalTask(taskId, jobId, 0, policy);
        JobTaskHandler handler = new JobTaskHandler() {
            @Override public String taskType() { return "TEST"; }
            @Override public TaskResult handle(JobTask t) {
                throw new RuntimeException("single attempt boom");
            }
        };

        when(taskRepository.persistTimeCriticalProgress(
                any(), anyInt(), any(), anyString(), anyString(), anyLong()))
                .thenReturn(true);

        assertThatThrownBy(() -> executor.execute(task, fenceToken, handler))
                .isInstanceOf(TimeCriticalRetriesExhaustedException.class);
    }

    @Test
    void execute_flushProgressWithNullError_coversNullTernaries() {
        // Use mock scheduler to capture the periodic sync Runnable, then invoke
        // it manually after result-based exhaustion with null error.
        // Covers L160-161 (lastError == null in flushProgress ternaries).
        ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<Object> mockFuture = mock(ScheduledFuture.class);
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        doReturn(mockFuture).when(mockScheduler)
                .scheduleAtFixedRate(captor.capture(), anyLong(), anyLong(), any());

        TimeCriticalResilienceExecutor mockExec =
                new TimeCriticalResilienceExecutor(taskRepository, mockScheduler);

        TimeCriticalPolicy policy = new TimeCriticalPolicy(2, 10L, 1.0, 50L, 100L);
        JobTask task = buildTimeCriticalTask(taskId, jobId, 0, policy);
        JobTaskHandler handler = new JobTaskHandler() {
            @Override public String taskType() { return "TEST"; }
            @Override public TaskResult handle(JobTask t) {
                return TaskResult.failure(null);
            }
        };

        when(taskRepository.persistTimeCriticalProgress(
                any(), anyInt(), any(), anyString(), anyString(), anyLong()))
                .thenReturn(true);

        assertThatThrownBy(() -> mockExec.execute(task, fenceToken, handler))
                .isInstanceOf(TimeCriticalRetriesExhaustedException.class);

        // Invoke the captured periodic sync — lastError.get() is still null,
        // attemptsSinceLastSync > 0, so flushProgress runs with null Throwable
        captor.getValue().run();

        verify(taskRepository).persistTimeCriticalProgress(
                eq(taskId), anyInt(), any(), eq("Unknown error"), eq("Unknown"), eq(fenceToken));
    }

    // ── Flush exception branches (line coverage) ─────────────────────────────

    @Test
    void execute_flushFinalProgressException_doesNotPreventExceptionPropagation() {
        // persistTimeCriticalProgress throws during final flush → catch at L188-190
        TimeCriticalPolicy policy = new TimeCriticalPolicy(1, 10L, 1.0, 50L, 60_000L);
        JobTask task = buildTimeCriticalTask(taskId, jobId, 0, policy);
        JobTaskHandler handler = new JobTaskHandler() {
            @Override public String taskType() { return "TEST"; }
            @Override public TaskResult handle(JobTask t) {
                throw new RuntimeException("boom");
            }
        };

        when(taskRepository.persistTimeCriticalProgress(
                any(), anyInt(), any(), anyString(), anyString(), anyLong()))
                .thenThrow(new RuntimeException("DB down"));

        assertThatThrownBy(() -> executor.execute(task, fenceToken, handler))
                .isInstanceOf(TimeCriticalRetriesExhaustedException.class);
    }

    @Test
    void execute_flushProgressException_doesNotCrashSync() {
        // persistTimeCriticalProgress throws during periodic sync → catch at L174-176
        ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<Object> mockFuture = mock(ScheduledFuture.class);
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        doReturn(mockFuture).when(mockScheduler)
                .scheduleAtFixedRate(captor.capture(), anyLong(), anyLong(), any());

        TimeCriticalResilienceExecutor mockExec =
                new TimeCriticalResilienceExecutor(taskRepository, mockScheduler);

        TimeCriticalPolicy policy = new TimeCriticalPolicy(2, 10L, 1.0, 50L, 100L);
        JobTask task = buildTimeCriticalTask(taskId, jobId, 0, policy);
        AtomicInteger callCount = new AtomicInteger(0);
        JobTaskHandler handler = new JobTaskHandler() {
            @Override public String taskType() { return "TEST"; }
            @Override public TaskResult handle(JobTask t) {
                if (callCount.getAndIncrement() < 1) {
                    throw new RuntimeException("transient");
                }
                return TaskResult.success("{}");
            }
        };

        // Make the sync throw
        when(taskRepository.persistTimeCriticalProgress(
                any(), anyInt(), any(), anyString(), anyString(), anyLong()))
                .thenThrow(new RuntimeException("DB unavailable"));

        TaskResult result = mockExec.execute(task, fenceToken, handler);
        assertThat(result.isSuccess()).isTrue();

        // Now invoke the captured sync Runnable — it should catch the exception
        captor.getValue().run();

        // Verify it was attempted
        verify(taskRepository, atLeastOnce()).persistTimeCriticalProgress(
                eq(taskId), anyInt(), any(), anyString(), anyString(), eq(fenceToken));
    }

    // ── Original Helpers ─────────────────────────────────────────────────────

    private JobTask buildTimeCriticalTask(UUID taskId, UUID jobId, int attemptCount) {
        return buildTimeCriticalTask(taskId, jobId, attemptCount, TimeCriticalPolicy.DEFAULT);
    }

    private JobTask buildTimeCriticalTask(UUID taskId, UUID jobId, int attemptCount,
                                          TimeCriticalPolicy tcPolicy) {
        OffsetDateTime now = OffsetDateTime.now();
        return new JobTask(
                taskId, jobId, "TEST", "test-topic",
                null, null, TaskStatus.PENDING,
                now, now, null, null,
                now.plusHours(1), false,
                attemptCount, null, null,
                1_000L, 2.0, 3_600_000L,
                null, null, null, null, null, "{}", null,
                true, tcPolicy.maxAttempts(), tcPolicy.baseIntervalMs(),
                tcPolicy.multiplier(), tcPolicy.maxDelayMs(), tcPolicy.dbSyncIntervalMs()
        );
    }

    private static JobTaskHandler stubHandler(TaskResult result) {
        return new JobTaskHandler() {
            @Override public String taskType() { return "TEST"; }
            @Override public TaskResult handle(JobTask task) { return result; }
        };
    }
}
