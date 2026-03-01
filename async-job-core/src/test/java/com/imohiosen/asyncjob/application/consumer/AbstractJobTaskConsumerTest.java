package com.imohiosen.asyncjob.application.consumer;

import com.imohiosen.asyncjob.domain.BackoffPolicy;
import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskResult;
import com.imohiosen.asyncjob.domain.TaskStatus;
import com.imohiosen.asyncjob.domain.FencedLock;
import com.imohiosen.asyncjob.application.executor.AsyncTaskExecutorBridge;
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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AbstractJobTaskConsumerTest {

    @Mock TaskRepository         taskRepository;
    @Mock JobRepository          jobRepository;
    @Mock TaskLockManager        lockManager;
    @Mock AsyncTaskExecutorBridge bridge;

    TestConsumer consumer;
    UUID         taskId;
    UUID         jobId;
    JobTask      task;

    @BeforeEach
    void setUp() {
        consumer = new TestConsumer(taskRepository, jobRepository, lockManager, bridge);
        taskId   = UUID.randomUUID();
        jobId    = UUID.randomUUID();
        task     = buildTask(taskId, jobId, TaskStatus.PENDING);
    }

    @Test
    void consume_happyPath_marksTaskCompleted() throws Exception {
        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(42L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
        when(taskRepository.markCompleted(eq(taskId), any(), any(), eq(42L))).thenReturn(true);
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.success("{\"ok\":true}"))
        );

        consumer.consume(new JobMessage(taskId, jobId, "TEST", "{}"));

        verify(taskRepository).markInProgress(eq(taskId), any(), eq(42L));
        verify(taskRepository).recordAsyncSubmitted(eq(taskId), any(), eq(42L));
        verify(taskRepository).markCompleted(eq(taskId), eq("{\"ok\":true}"), any(), eq(42L));
        verify(lockManager).unlock(taskId);
    }

    @Test
    void consume_lockNotAcquired_skipsProcessing() {
        when(lockManager.tryLock(taskId)).thenReturn(Optional.empty());

        consumer.consume(new JobMessage(taskId, jobId, "TEST", "{}"));

        verify(taskRepository, never()).findEligible(any());
        verify(taskRepository, never()).markInProgress(any(), any(), anyLong());
        verify(lockManager, never()).unlock(any());
    }

    @Test
    void consume_taskNotEligible_skipsProcessingButUnlocks() {
        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(1L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.empty());

        consumer.consume(new JobMessage(taskId, jobId, "TEST", "{}"));

        verify(taskRepository, never()).markInProgress(any(), any(), anyLong());
        verify(lockManager).unlock(taskId);
    }

    @Test
    void consume_taskFails_belowMaxAttempts_marksFailedWithBackoff() throws Exception {
        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(10L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
        when(taskRepository.markFailed(eq(taskId), eq(1), any(), any(),
                eq("transient error"), eq("java.lang.RuntimeException"), eq(10L))).thenReturn(true);
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.failure(new RuntimeException("transient error")))
        );

        consumer.consume(new JobMessage(taskId, jobId, "TEST", "{}"));

        verify(taskRepository).markFailed(eq(taskId), eq(1), any(), any(),
                eq("transient error"), eq("java.lang.RuntimeException"), eq(10L));
        verify(lockManager).unlock(taskId);
    }

    @Test
    void consume_taskFails_atMaxAttempts_marksDeadLetter() throws Exception {
        JobTask exhaustedTask = buildTask(taskId, jobId, TaskStatus.FAILED, 4);
        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(20L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(exhaustedTask));
        when(taskRepository.markDeadLetter(eq(taskId), eq(5), any(), eq("fatal"),
                eq("java.lang.RuntimeException"), eq(20L))).thenReturn(true);
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.failure(new RuntimeException("fatal")))
        );

        consumer.consume(new JobMessage(taskId, jobId, "TEST", "{}"));

        verify(taskRepository).markDeadLetter(eq(taskId), eq(5), any(), eq("fatal"),
                eq("java.lang.RuntimeException"), eq(20L));
        verify(lockManager).unlock(taskId);
    }

    @Test
    void consume_exceptionMidProcessing_doesNotPropagateToKafka() {
        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(1L)));
        when(taskRepository.findEligible(taskId)).thenThrow(new RuntimeException("DB down"));

        // Must NOT throw — exception must be swallowed to protect the consumer thread
        consumer.consume(new JobMessage(taskId, jobId, "TEST", "{}"));
        verify(lockManager).unlock(taskId);
    }

    @Test
    void consume_markCompletedReturnsFalse_doesNotRecordAsyncCompleted() throws Exception {
        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(42L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
        when(taskRepository.markCompleted(eq(taskId), any(), any(), eq(42L))).thenReturn(false);
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.success("{\"ok\":true}"))
        );

        consumer.consume(new JobMessage(taskId, jobId, "TEST", "{}"));

        verify(taskRepository).markCompleted(eq(taskId), any(), any(), eq(42L));
        verify(taskRepository, never()).recordAsyncCompleted(any(), any(), anyLong());
        verify(lockManager).unlock(taskId);
    }

    @Test
    void consume_failureWithNullError_usesUnknownDefaults() throws Exception {
        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(10L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
        when(taskRepository.markFailed(eq(taskId), eq(1), any(), any(),
                eq("Unknown error"), eq("Unknown"), eq(10L))).thenReturn(true);
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.failure(null))
        );

        consumer.consume(new JobMessage(taskId, jobId, "TEST", "{}"));

        verify(taskRepository).markFailed(eq(taskId), eq(1), any(), any(),
                eq("Unknown error"), eq("Unknown"), eq(10L));
        verify(lockManager).unlock(taskId);
    }

    @Test
    void consume_markFailedReturnsFalse_staleFenceToken() throws Exception {
        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(10L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
        when(taskRepository.markFailed(eq(taskId), eq(1), any(), any(),
                eq("error"), eq("java.lang.RuntimeException"), eq(10L))).thenReturn(false);
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.failure(new RuntimeException("error")))
        );

        consumer.consume(new JobMessage(taskId, jobId, "TEST", "{}"));

        verify(taskRepository).markFailed(eq(taskId), eq(1), any(), any(),
                eq("error"), eq("java.lang.RuntimeException"), eq(10L));
        verify(lockManager).unlock(taskId);
    }

    @Test
    void consume_markDeadLetterReturnsFalse_staleFenceToken() throws Exception {
        JobTask exhaustedTask = buildTask(taskId, jobId, TaskStatus.FAILED, 4);
        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(20L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(exhaustedTask));
        when(taskRepository.markDeadLetter(eq(taskId), eq(5), any(), eq("fatal"),
                eq("java.lang.RuntimeException"), eq(20L))).thenReturn(false);
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.failure(new RuntimeException("fatal")))
        );

        consumer.consume(new JobMessage(taskId, jobId, "TEST", "{}"));

        verify(taskRepository).markDeadLetter(eq(taskId), eq(5), any(), eq("fatal"),
                eq("java.lang.RuntimeException"), eq(20L));
        verify(lockManager).unlock(taskId);
    }

    @Test
    void consume_successPath_recordsAsyncCompleted() throws Exception {
        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(42L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
        when(taskRepository.markCompleted(eq(taskId), any(), any(), eq(42L))).thenReturn(true);
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.success("{\"ok\":true}"))
        );

        consumer.consume(new JobMessage(taskId, jobId, "TEST", "{}"));

        verify(taskRepository).recordAsyncCompleted(eq(taskId), any(), eq(42L));
    }

    @Test
    void consume_timeCriticalTask_callsSubmitTimeCriticalWork() throws Exception {
        JobTask tcTask = buildTimeCriticalTask(taskId, jobId, TaskStatus.PENDING);
        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(42L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(tcTask));
        when(taskRepository.markCompleted(eq(taskId), any(), any(), eq(42L))).thenReturn(true);

        consumer.consume(new JobMessage(taskId, jobId, "TEST", "{}"));

        assertThat(consumer.timeCriticalWorkCalled).isTrue();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private JobTask buildTask(UUID taskId, UUID jobId, TaskStatus status) {
        return buildTask(taskId, jobId, status, 0);
    }

    private JobTask buildTask(UUID taskId, UUID jobId, TaskStatus status, int attemptCount) {
        OffsetDateTime now = OffsetDateTime.now();
        return new JobTask(
                taskId, jobId, "TEST", "test-topic",
                null, null, status,
                now, now, null, null,
                now.plusHours(1), false,
                attemptCount, null, null,
                1_000L, 2.0, 3_600_000L,
                null, null, null, null, null, "{}", null,
                false, 0, 0L, 1.0, 0L, 0L
        );
    }

    private JobTask buildTimeCriticalTask(UUID taskId, UUID jobId, TaskStatus status) {
        OffsetDateTime now = OffsetDateTime.now();
        return new JobTask(
                taskId, jobId, "TEST", "test-topic",
                null, null, status,
                now, now, null, null,
                now.plusHours(1), false,
                0, null, null,
                1_000L, 2.0, 3_600_000L,
                null, null, null, null, null, "{}", null,
                true, 10, 100L, 1.5, 900L, 2000L
        );
    }

    // ── Test Double ───────────────────────────────────────────────────────────

    static class TestConsumer extends AbstractJobTaskConsumer {
        boolean timeCriticalWorkCalled = false;

        TestConsumer(TaskRepository t, JobRepository j, TaskLockManager l,
                     AsyncTaskExecutorBridge b) {
            super(t, j, l, b);
        }

        @Override protected int getMaxAttempts(JobTask task) { return 5; }

        @Override
        protected TaskResult processTask(JobTask task) {
            return TaskResult.success("{}");
        }

        @Override
        protected CompletableFuture<TaskResult> submitTimeCriticalWork(JobTask task, long fenceToken) {
            timeCriticalWorkCalled = true;
            return CompletableFuture.completedFuture(TaskResult.success("{}"));
        }
    }
}
