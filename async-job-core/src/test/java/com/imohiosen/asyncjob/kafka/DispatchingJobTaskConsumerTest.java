package com.imohiosen.asyncjob.kafka;

import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskResult;
import com.imohiosen.asyncjob.domain.TaskStatus;
import com.imohiosen.asyncjob.executor.AsyncTaskExecutorBridge;
import com.imohiosen.asyncjob.handler.JobTaskHandler;
import com.imohiosen.asyncjob.handler.JobTaskHandlerRegistry;
import com.imohiosen.asyncjob.lock.FencedLock;
import com.imohiosen.asyncjob.lock.TaskLockManager;
import com.imohiosen.asyncjob.repository.JobRepository;
import com.imohiosen.asyncjob.repository.TaskRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DispatchingJobTaskConsumerTest {

    @Mock TaskRepository         taskRepository;
    @Mock JobRepository          jobRepository;
    @Mock TaskLockManager        lockManager;
    @Mock AsyncTaskExecutorBridge bridge;
    @Mock JobKafkaProducer       kafkaProducer;

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
        // Two handlers: INVOICE returns a specific payload, REPORT returns another
        JobTaskHandler invoiceHandler = stubHandler("INVOICE", TaskResult.success("{\"invoice\":true}"));
        JobTaskHandler reportHandler  = stubHandler("REPORT",  TaskResult.success("{\"report\":true}"));
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of(invoiceHandler, reportHandler));
        consumer = new DispatchingJobTaskConsumer(
                taskRepository, jobRepository, lockManager, bridge, kafkaProducer, registry);

        JobTask task = buildTask(taskId, jobId, "INVOICE", TaskStatus.PENDING, 0);

        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(1L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
        when(kafkaProducer.deserialize(any())).thenReturn(
                new JobKafkaMessage(taskId, jobId, "INVOICE", "{}"));
        when(taskRepository.markCompleted(eq(taskId), any(), any(), eq(1L))).thenReturn(true);
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.success("{\"invoice\":true}")));

        consumer.consume(buildRecord(taskId));

        verify(taskRepository).markCompleted(eq(taskId), eq("{\"invoice\":true}"), any(), eq(1L));
        verify(lockManager).unlock(taskId);
    }

    @Test
    void dispatchesToSecondHandler_whenTaskTypeMatches() throws Exception {
        JobTaskHandler invoiceHandler = stubHandler("INVOICE", TaskResult.success("{\"invoice\":true}"));
        JobTaskHandler reportHandler  = stubHandler("REPORT",  TaskResult.success("{\"report\":true}"));
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of(invoiceHandler, reportHandler));
        consumer = new DispatchingJobTaskConsumer(
                taskRepository, jobRepository, lockManager, bridge, kafkaProducer, registry);

        JobTask task = buildTask(taskId, jobId, "REPORT", TaskStatus.PENDING, 0);

        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(2L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
        when(kafkaProducer.deserialize(any())).thenReturn(
                new JobKafkaMessage(taskId, jobId, "REPORT", "{}"));
        when(taskRepository.markCompleted(eq(taskId), any(), any(), eq(2L))).thenReturn(true);
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.success("{\"report\":true}")));

        consumer.consume(buildRecord(taskId));

        verify(taskRepository).markCompleted(eq(taskId), eq("{\"report\":true}"), any(), eq(2L));
    }

    @Test
    void unknownTaskType_failsGracefullyWithoutKillingConsumer() {
        // Registry has INVOICE only — task type is UNKNOWN
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(
                List.of(stubHandler("INVOICE", TaskResult.success("{}"))));
        consumer = new DispatchingJobTaskConsumer(
                taskRepository, jobRepository, lockManager, bridge, kafkaProducer, registry);

        JobTask task = buildTask(taskId, jobId, "UNKNOWN", TaskStatus.PENDING, 0);

        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(3L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
        when(kafkaProducer.deserialize(any())).thenReturn(
                new JobKafkaMessage(taskId, jobId, "UNKNOWN", "{}"));
        // bridge.submitAsync will invoke processTask, which throws for unknown type
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
        consumer.consume(buildRecord(taskId));
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
                taskRepository, jobRepository, lockManager, bridge, kafkaProducer, registry);

        // attemptCount=1, maxAttempts=2 → should move to DEAD_LETTER
        JobTask task = buildTask(taskId, jobId, "STRICT", TaskStatus.FAILED, 1);

        when(lockManager.tryLock(taskId)).thenReturn(Optional.of(new FencedLock(4L)));
        when(taskRepository.findEligible(taskId)).thenReturn(Optional.of(task));
        when(kafkaProducer.deserialize(any())).thenReturn(
                new JobKafkaMessage(taskId, jobId, "STRICT", "{}"));
        when(taskRepository.markDeadLetter(eq(taskId), eq(2), any(), eq("fail"),
                eq("java.lang.RuntimeException"), eq(4L))).thenReturn(true);
        when(bridge.submitAsync(any())).thenReturn(
                CompletableFuture.completedFuture(TaskResult.failure(new RuntimeException("fail"))));

        consumer.consume(buildRecord(taskId));

        // attempt 2 of 2 → DEAD_LETTER
        verify(taskRepository).markDeadLetter(eq(taskId), eq(2), any(), eq("fail"),
                eq("java.lang.RuntimeException"), eq(4L));
    }

    @Test
    void maxAttempts_fallsBackToDefault_forUnknownType() {
        // empty registry → default 5
        JobTaskHandlerRegistry registry = new JobTaskHandlerRegistry(List.of());
        consumer = new DispatchingJobTaskConsumer(
                taskRepository, jobRepository, lockManager, bridge, kafkaProducer, registry);

        JobTask task = buildTask(taskId, jobId, "NONEXISTENT", TaskStatus.PENDING, 0);
        // getMaxAttempts should return default 5
        assertThat(consumer.getMaxAttempts(task)).isEqualTo(5);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private ConsumerRecord<String, String> buildRecord(UUID taskId) {
        return new ConsumerRecord<>("test-topic", 0, 0L, taskId.toString(), "{}");
    }

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
                null, null, null, null, null, "{}", null
        );
    }

    private static JobTaskHandler stubHandler(String taskType, TaskResult result) {
        return new JobTaskHandler() {
            @Override public String taskType() { return taskType; }
            @Override public TaskResult handle(JobTask task) { return result; }
        };
    }
}
