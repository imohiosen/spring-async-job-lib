package com.imohiosen.asyncjob.application.lifecycle;

import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskStatus;
import com.imohiosen.asyncjob.port.messaging.JobMessage;
import com.imohiosen.asyncjob.port.messaging.JobMessageProducer;
import com.imohiosen.asyncjob.port.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TaskRetrySchedulerTest {

    @Mock TaskRepository     taskRepository;
    @Mock JobMessageProducer messageProducer;

    TaskRetryScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new TaskRetryScheduler(taskRepository, messageProducer, 100);
    }

    @Test
    void sweep_noRetryableTasks_doesNotPublish() {
        when(taskRepository.findRetryableTasks(100)).thenReturn(List.of());

        scheduler.sweep();

        verify(messageProducer, never()).publish(any(), any());
    }

    @Test
    void sweep_multipleRetryableTasks_publishesAll() {
        UUID jobId = UUID.randomUUID();
        JobTask task1 = retryableTask(jobId, "topic-a");
        JobTask task2 = retryableTask(jobId, "topic-b");

        when(taskRepository.findRetryableTasks(100)).thenReturn(List.of(task1, task2));

        scheduler.sweep();

        verify(messageProducer).publish(eq("topic-a"), any(JobMessage.class));
        verify(messageProducer).publish(eq("topic-b"), any(JobMessage.class));
    }

    @Test
    void sweep_publishesCorrectJobMessageFields() {
        UUID jobId = UUID.randomUUID();
        JobTask task = retryableTask(jobId, "my-topic");

        when(taskRepository.findRetryableTasks(100)).thenReturn(List.of(task));

        scheduler.sweep();

        ArgumentCaptor<JobMessage> captor = ArgumentCaptor.forClass(JobMessage.class);
        verify(messageProducer).publish(eq("my-topic"), captor.capture());

        JobMessage msg = captor.getValue();
        assertThat(msg.taskId()).isEqualTo(task.id());
        assertThat(msg.jobId()).isEqualTo(jobId);
        assertThat(msg.taskType()).isEqualTo(task.taskType());
        assertThat(msg.payload()).isEqualTo(task.payload());
    }

    @Test
    void sweep_publishFailureForOneTask_continuesWithNext() {
        UUID jobId = UUID.randomUUID();
        JobTask task1 = retryableTask(jobId, "topic");
        JobTask task2 = retryableTask(jobId, "topic");

        when(taskRepository.findRetryableTasks(100)).thenReturn(List.of(task1, task2));
        doThrow(new RuntimeException("broker down"))
                .when(messageProducer).publish(eq("topic"), argThat(m -> m.taskId().equals(task1.id())));

        scheduler.sweep();

        // Both attempted — second should succeed
        verify(messageProducer, times(2)).publish(eq("topic"), any(JobMessage.class));
    }

    @Test
    void sweep_respectsBatchSize() {
        TaskRetryScheduler smallBatch = new TaskRetryScheduler(taskRepository, messageProducer, 10);
        when(taskRepository.findRetryableTasks(10)).thenReturn(List.of());

        smallBatch.sweep();

        verify(taskRepository).findRetryableTasks(10);
        verify(taskRepository, never()).findRetryableTasks(100);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static JobTask retryableTask(UUID jobId, String destination) {
        OffsetDateTime now = OffsetDateTime.now();
        return new JobTask(
                UUID.randomUUID(), jobId, "RETRY_TYPE", destination,
                null, null, TaskStatus.FAILED, now, now, now, null,
                now.plusHours(1), false, 1, now.minusMinutes(5), now.minusMinutes(1),
                1_000L, 2.0, 3_600_000L,
                null, null, "previous error", "java.lang.RuntimeException",
                null, "{\"data\":1}", null);
    }
}
