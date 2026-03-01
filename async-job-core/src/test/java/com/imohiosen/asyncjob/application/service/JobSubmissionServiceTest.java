package com.imohiosen.asyncjob.application.service;

import com.imohiosen.asyncjob.domain.BackoffPolicy;
import com.imohiosen.asyncjob.domain.Job;
import com.imohiosen.asyncjob.domain.JobStatus;
import com.imohiosen.asyncjob.domain.JobSubmissionRequest;
import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskStatus;
import com.imohiosen.asyncjob.port.messaging.JobMessage;
import com.imohiosen.asyncjob.port.messaging.JobMessageProducer;
import com.imohiosen.asyncjob.port.repository.JobRepository;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class JobSubmissionServiceTest {

    @Mock JobRepository jobRepository;
    @Mock TaskRepository taskRepository;
    @Mock JobMessageProducer messageProducer;

    JobSubmissionService service;

    @BeforeEach
    void setUp() {
        service = new JobSubmissionService(jobRepository, taskRepository, messageProducer);
    }

    // ── Immediate execution ──────────────────────────────────────────────────

    @Test
    void submit_immediate_insertsJobWithPendingStatus() {
        JobSubmissionRequest request = new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of("{\"id\":1}", "{\"id\":2}"),
                60_000L, null, null, null, null, null, false, null);

        UUID jobId = service.submit(request);

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).insert(jobCaptor.capture());
        Job inserted = jobCaptor.getValue();

        assertThat(inserted.id()).isEqualTo(jobId);
        assertThat(inserted.jobName()).isEqualTo("test-job");
        assertThat(inserted.status()).isEqualTo(JobStatus.PENDING);
        assertThat(inserted.scheduledAt()).isNull();
    }

    @Test
    void submit_immediate_insertsOneTaskPerPayload() {
        JobSubmissionRequest request = new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of("{\"a\":1}", "{\"b\":2}", "{\"c\":3}"),
                60_000L, null, null, null, null, null, false, null);

        service.submit(request);

        verify(taskRepository, times(3)).insert(any(JobTask.class));
    }

    @Test
    void submit_immediate_publishesKafkaMessagePerTask() {
        JobSubmissionRequest request = new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of("{\"a\":1}", "{\"b\":2}"),
                60_000L, null, null, null, null, null, false, null);

        UUID jobId = service.submit(request);

        ArgumentCaptor<JobMessage> msgCaptor = ArgumentCaptor.forClass(JobMessage.class);
        verify(messageProducer, times(2)).publish(eq("test-topic"), msgCaptor.capture());

        List<JobMessage> messages = msgCaptor.getAllValues();
        assertThat(messages).hasSize(2);
        assertThat(messages).allMatch(m -> m.jobId().equals(jobId));
        assertThat(messages).allMatch(m -> m.taskType().equals("TEST_TYPE"));
    }

    @Test
    void submit_immediate_updatesCountersAndMarksStarted() {
        JobSubmissionRequest request = new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of("{\"x\":1}"),
                60_000L, null, null, null, null, null, false, null);

        UUID jobId = service.submit(request);

        verify(jobRepository).updateCounters(jobId);
        verify(jobRepository).markStarted(jobId);
    }

    @Test
    void submit_immediate_withPastScheduledAt_runsImmediately() {
        OffsetDateTime past = OffsetDateTime.now().minusHours(1);
        JobSubmissionRequest request = new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of("{\"x\":1}"),
                60_000L, null, null, past, null, null, false, null);

        UUID jobId = service.submit(request);

        // Should behave as immediate — publish messages and mark started
        verify(messageProducer, times(1)).publish(eq("test-topic"), any(JobMessage.class));
        verify(jobRepository).markStarted(jobId);
    }

    @Test
    void submit_immediate_emptyPayloads_marksCompleted() {
        JobSubmissionRequest request = new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of(),
                60_000L, null, null, null, null, null, false, null);

        UUID jobId = service.submit(request);

        verify(jobRepository).updateStatus(jobId, JobStatus.COMPLETED);
        verify(taskRepository, never()).insert(any());
        verify(messageProducer, never()).publish(any(), any());
    }

    @Test
    void submit_immediate_usesCustomBackoffPolicy() {
        BackoffPolicy custom = new BackoffPolicy(5_000L, 3.0, 300_000L);
        JobSubmissionRequest request = new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of("{\"x\":1}"),
                60_000L, null, custom, null, null, null, false, null);

        service.submit(request);

        ArgumentCaptor<JobTask> taskCaptor = ArgumentCaptor.forClass(JobTask.class);
        verify(taskRepository).insert(taskCaptor.capture());
        JobTask task = taskCaptor.getValue();

        assertThat(task.baseIntervalMs()).isEqualTo(5_000L);
        assertThat(task.multiplier()).isEqualTo(3.0);
        assertThat(task.maxDelayMs()).isEqualTo(300_000L);
    }

    @Test
    void submit_immediate_setsCorrelationIdAndMetadata() {
        JobSubmissionRequest request = new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of("{\"x\":1}"),
                60_000L, null, null, null, "corr-123", "{\"source\":\"api\"}", false, null);

        service.submit(request);

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).insert(jobCaptor.capture());
        Job inserted = jobCaptor.getValue();

        assertThat(inserted.correlationId()).isEqualTo("corr-123");
        assertThat(inserted.metadata()).isEqualTo("{\"source\":\"api\"}");
    }

    @Test
    void submit_immediate_setsDeadlineFromDeadlineMs() {
        JobSubmissionRequest request = new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of("{\"x\":1}"),
                120_000L, null, null, null, null, null, false, null);

        OffsetDateTime before = OffsetDateTime.now();
        service.submit(request);
        OffsetDateTime after = OffsetDateTime.now();

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).insert(jobCaptor.capture());
        Job inserted = jobCaptor.getValue();

        assertThat(inserted.deadlineAt()).isAfter(before.plusSeconds(119));
        assertThat(inserted.deadlineAt()).isBefore(after.plusSeconds(121));
    }

    // ── Scheduled (deferred) execution ───────────────────────────────────────

    @Test
    void submit_scheduled_insertsJobWithScheduledStatus() {
        OffsetDateTime future = OffsetDateTime.now().plusHours(2);
        JobSubmissionRequest request = new JobSubmissionRequest(
                "deferred-job", "test-topic", "TEST_TYPE",
                List.of("{\"id\":1}"),
                60_000L, null, null, future, null, null, false, null);

        UUID jobId = service.submit(request);

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).insert(jobCaptor.capture());
        Job inserted = jobCaptor.getValue();

        assertThat(inserted.status()).isEqualTo(JobStatus.SCHEDULED);
        assertThat(inserted.scheduledAt()).isEqualTo(future);
    }

    @Test
    void submit_scheduled_insertsTasksButDoesNotPublish() {
        OffsetDateTime future = OffsetDateTime.now().plusHours(2);
        JobSubmissionRequest request = new JobSubmissionRequest(
                "deferred-job", "test-topic", "TEST_TYPE",
                List.of("{\"id\":1}", "{\"id\":2}"),
                60_000L, null, null, future, null, null, false, null);

        service.submit(request);

        verify(taskRepository, times(2)).insert(any(JobTask.class));
        verify(messageProducer, never()).publish(any(), any());
    }

    @Test
    void submit_scheduled_doesNotMarkStarted() {
        OffsetDateTime future = OffsetDateTime.now().plusHours(2);
        JobSubmissionRequest request = new JobSubmissionRequest(
                "deferred-job", "test-topic", "TEST_TYPE",
                List.of("{\"id\":1}"),
                60_000L, null, null, future, null, null, false, null);

        UUID jobId = service.submit(request);

        verify(jobRepository).updateCounters(jobId);
        verify(jobRepository, never()).markStarted(any());
    }

    // ── Dispatch ─────────────────────────────────────────────────────────────

    @Test
    void dispatch_publishesAllPendingTasks() {
        OffsetDateTime now = OffsetDateTime.now();
        UUID jobId = UUID.randomUUID();
        Job job = new Job(jobId, "test-job", null, JobStatus.SCHEDULED,
                now, now, null, null, now.plusHours(1), now.minusMinutes(1),
                false, 2, 2, 0, 0, 0, 0, null, false);

        JobTask task1 = new JobTask(UUID.randomUUID(), jobId, "T", "topic",
                null, null, TaskStatus.PENDING, now, now, null, null,
                now.plusHours(1), false, 0, null, null,
                1000L, 2.0, 3600000L, null, null, null, null, null, "{\"x\":1}", null, false, 0, 0L, 1.0, 0L, 0L);
        JobTask task2 = new JobTask(UUID.randomUUID(), jobId, "T", "topic",
                null, null, TaskStatus.PENDING, now, now, null, null,
                now.plusHours(1), false, 0, null, null,
                1000L, 2.0, 3600000L, null, null, null, null, null, "{\"x\":2}", null, false, 0, 0L, 1.0, 0L, 0L);

        when(taskRepository.findTasksByJobId(jobId)).thenReturn(List.of(task1, task2));

        int published = service.dispatch(job);

        assertThat(published).isEqualTo(2);
        verify(messageProducer, times(2)).publish(eq("topic"), any(JobMessage.class));
        verify(jobRepository).markStarted(jobId);
    }

    @Test
    void dispatch_skipsNonPendingTasks() {
        OffsetDateTime now = OffsetDateTime.now();
        UUID jobId = UUID.randomUUID();
        Job job = new Job(jobId, "test-job", null, JobStatus.SCHEDULED,
                now, now, null, null, now.plusHours(1), now.minusMinutes(1),
                false, 1, 0, 0, 0, 0, 0, null, false);

        // A completed task should be skipped
        JobTask completedTask = new JobTask(UUID.randomUUID(), jobId, "T", "topic",
                null, null, TaskStatus.COMPLETED, now, now, now, now,
                now.plusHours(1), false, 1, null, null,
                1000L, 2.0, 3600000L, null, null, null, null, null, "{\"x\":1}", "ok", false, 0, 0L, 1.0, 0L, 0L);

        when(taskRepository.findTasksByJobId(jobId)).thenReturn(List.of(completedTask));

        int published = service.dispatch(job);

        assertThat(published).isEqualTo(0);
        verify(messageProducer, never()).publish(any(), any());
        verify(jobRepository).markStarted(jobId);
    }

    // ── Validation ───────────────────────────────────────────────────────────

    @Test
    void request_nullPayloads_throwsException() {
        assertThatThrownBy(() -> new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                null, 60_000L, null, null, null, null, null, false, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("payloads");
    }

    @Test
    void request_blankJobName_throwsException() {
        assertThatThrownBy(() -> new JobSubmissionRequest(
                "", "test-topic", "TEST_TYPE",
                List.of(), 60_000L, null, null, null, null, null, false, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jobName");
    }

    @Test
    void request_blankDestination_throwsException() {
        assertThatThrownBy(() -> new JobSubmissionRequest(
                "test-job", "", "TEST_TYPE",
                List.of(), 60_000L, null, null, null, null, null, false, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("destination");
    }

    @Test
    void request_zeroDeadline_throwsException() {
        assertThatThrownBy(() -> new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of(), 0L, null, null, null, null, null, false, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("deadlineMs");
    }

    @Test
    void request_effectiveTaskDeadlineMs_defaultsToDeadlineMs() {
        JobSubmissionRequest request = new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of(), 60_000L, null, null, null, null, null, false, null);
        assertThat(request.effectiveTaskDeadlineMs()).isEqualTo(60_000L);
    }

    @Test
    void request_effectiveTaskDeadlineMs_usesCustomValue() {
        JobSubmissionRequest request = new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of(), 60_000L, 30_000L, null, null, null, null, false, null);
        assertThat(request.effectiveTaskDeadlineMs()).isEqualTo(30_000L);
    }

    @Test
    void request_effectiveBackoffPolicy_defaultsToDefault() {
        JobSubmissionRequest request = new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of(), 60_000L, null, null, null, null, null, false, null);
        assertThat(request.effectiveBackoffPolicy()).isEqualTo(BackoffPolicy.DEFAULT);
    }

    @Test
    void request_isImmediate_trueWhenNullScheduledAt() {
        JobSubmissionRequest request = new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of(), 60_000L, null, null, null, null, null, false, null);
        assertThat(request.isImmediate()).isTrue();
    }

    @Test
    void request_isImmediate_falseWhenFutureScheduledAt() {
        JobSubmissionRequest request = new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of(), 60_000L, null, null, OffsetDateTime.now().plusHours(1), null, null, false, null);
        assertThat(request.isImmediate()).isFalse();
    }
}
