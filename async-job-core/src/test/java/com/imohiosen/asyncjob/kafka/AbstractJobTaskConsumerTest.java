package com.imohiosen.asyncjob.kafka;

import com.imohiosen.asyncjob.domain.BackoffPolicy;
import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskResult;
import com.imohiosen.asyncjob.domain.TaskStatus;
import com.imohiosen.asyncjob.executor.AsyncTaskExecutorBridge;
import com.imohiosen.asyncjob.lock.TaskLockManager;
import com.imohiosen.asyncjob.repository.JobRepository;
import com.imohiosen.asyncjob.repository.TaskRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AbstractJobTaskConsumerTest {

    @Mock TaskRepository         taskRepository;
    @Mock JobRepository          jobRepository;
    @Mock TaskLockManager        lockManager;
    @Mock AsyncTaskExecutorBridge bridge;
    @Mock JobKafkaProducer       kafkaProducer;

    TestConsumer consumer;
    UUID         taskId;
    UUID         jobId;
    JobTask      task;

    @BeforeEach
    void setUp() {
        consumer = new TestConsumer(taskRepository, jobRepository, lockManager, bridge, kafkaProducer);
        taskId   = UUID.randomUUID();
        jobId    = UUID.randomUUID();
        task     = buildTask(taskId, jobId, TaskStatus.PENDING);

        when(kafkaProducer.deserialize(any())).thenReturn(
                new JobKafkaMessage(taskId, jobId, "TEST", "{}")
        );
    }

    @Test
    void consume_happyPath_marksTaskCompleted() throws Exception {
        when(lockManager.tryLock(taskId)).thenReturn(true);
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.success("{\"ok\":true}"))
        );

        consumer.consume(buildRecord(taskId));

        verify(taskRepository).markInProgress(eq(taskId), any());
        verify(taskRepository).recordAsyncSubmitted(eq(taskId), any());
        verify(taskRepository).markCompleted(eq(taskId), eq("{\"ok\":true}"), any());
        verify(jobRepository).updateCounters(jobId);
        verify(lockManager).unlock(taskId);
    }

    @Test
    void consume_lockNotAcquired_skipsProcessing() {
        when(lockManager.tryLock(taskId)).thenReturn(false);

        consumer.consume(buildRecord(taskId));

        verify(taskRepository, never()).findEligible(any());
        verify(taskRepository, never()).markInProgress(any(), any());
        verify(lockManager, never()).unlock(any()); // skipped before try-finally
    }

    @Test
    void consume_taskNotEligible_skipsProcessingButUnlocks() {
        when(lockManager.tryLock(taskId)).thenReturn(true);
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.empty());

        consumer.consume(buildRecord(taskId));

        verify(taskRepository, never()).markInProgress(any(), any());
        verify(lockManager).unlock(taskId); // finally block always runs
    }

    @Test
    void consume_taskFails_belowMaxAttempts_marksFailedWithBackoff() throws Exception {
        when(lockManager.tryLock(taskId)).thenReturn(true);
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.failure(new RuntimeException("transient error")))
        );

        consumer.consume(buildRecord(taskId));

        verify(taskRepository).markFailed(eq(taskId), eq(1), any(), any(),
                eq("transient error"), eq("java.lang.RuntimeException"));
        verify(lockManager).unlock(taskId);
    }

    @Test
    void consume_taskFails_atMaxAttempts_marksDeadLetter() throws Exception {
        JobTask exhaustedTask = buildTask(taskId, jobId, TaskStatus.FAILED, 4); // maxAttempts=5, next=5
        when(lockManager.tryLock(taskId)).thenReturn(true);
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(exhaustedTask));
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.failure(new RuntimeException("fatal")))
        );

        consumer.consume(buildRecord(taskId));

        verify(taskRepository).markDeadLetter(eq(taskId), eq(5), any(), eq("fatal"),
                eq("java.lang.RuntimeException"));
        verify(lockManager).unlock(taskId);
    }

    @Test
    void consume_exceptionMidProcessing_doesNotPropagateToKafka() {
        when(lockManager.tryLock(taskId)).thenReturn(true);
        when(taskRepository.findEligible(taskId)).thenThrow(new RuntimeException("DB down"));

        // Must NOT throw — exception must be swallowed to protect the consumer thread
        consumer.consume(buildRecord(taskId));
        verify(lockManager).unlock(taskId);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private ConsumerRecord<String, String> buildRecord(UUID taskId) {
        return new ConsumerRecord<>("test-topic", 0, 0L, taskId.toString(), "{}");
    }

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
                null, null, null, null, "{}", null
        );
    }

    // ── Test Double ───────────────────────────────────────────────────────────

    static class TestConsumer extends AbstractJobTaskConsumer {
        TestConsumer(TaskRepository t, JobRepository j, TaskLockManager l,
                     AsyncTaskExecutorBridge b, JobKafkaProducer p) {
            super(t, j, l, b, p);
        }

        @Override protected int getMaxAttempts(JobTask task) { return 5; }

        @Override
        protected TaskResult processTask(JobTask task) {
            return TaskResult.success("{}");
        }
    }
}
