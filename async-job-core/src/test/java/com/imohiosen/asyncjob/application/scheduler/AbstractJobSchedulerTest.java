package com.imohiosen.asyncjob.application.scheduler;

import com.imohiosen.asyncjob.domain.BackoffPolicy;
import com.imohiosen.asyncjob.domain.JobSubmissionRequest;
import com.imohiosen.asyncjob.application.service.JobSubmissionService;
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
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AbstractJobSchedulerTest {

    @Mock JobSubmissionService submissionService;

    TestJobScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new TestJobScheduler(submissionService);
    }

    @Test
    void trigger_happyPath_submitsRequestWithCorrectFields() {
        scheduler.setPayloads(List.of("{\"item\":1}", "{\"item\":2}"));
        when(submissionService.submit(any())).thenReturn(UUID.randomUUID());

        scheduler.trigger();

        ArgumentCaptor<JobSubmissionRequest> captor = ArgumentCaptor.forClass(JobSubmissionRequest.class);
        verify(submissionService).submit(captor.capture());

        JobSubmissionRequest request = captor.getValue();
        assertThat(request.jobName()).isEqualTo("test-scheduler-job");
        assertThat(request.destination()).isEqualTo("test-destination");
        assertThat(request.taskType()).isEqualTo("TEST_TASK");
        assertThat(request.payloads()).containsExactly("{\"item\":1}", "{\"item\":2}");
        assertThat(request.deadlineMs()).isEqualTo(120_000L);
        assertThat(request.backoffPolicy()).isEqualTo(BackoffPolicy.DEFAULT);
    }

    @Test
    void trigger_nullPayloads_skipsSubmission() {
        scheduler.setPayloads(null);

        scheduler.trigger();

        verify(submissionService, never()).submit(any());
    }

    @Test
    void trigger_emptyPayloads_skipsSubmission() {
        scheduler.setPayloads(List.of());

        scheduler.trigger();

        verify(submissionService, never()).submit(any());
    }

    @Test
    void backoffPolicy_defaultReturnsDefault() {
        assertThat(scheduler.backoffPolicy()).isEqualTo(BackoffPolicy.DEFAULT);
    }

    @Test
    void getTaskDeadlineMs_defaultReturnsDeadlineMs() {
        assertThat(scheduler.getTaskDeadlineMs()).isEqualTo(120_000L);
    }

    @Test
    void getScheduledAt_defaultReturnsNull() {
        assertThat(scheduler.getScheduledAt()).isNull();
    }

    @Test
    void trigger_usesCustomScheduledAt() {
        OffsetDateTime future = OffsetDateTime.now().plusHours(3);
        scheduler.setPayloads(List.of("{\"x\":1}"));
        scheduler.setCustomScheduledAt(future);
        when(submissionService.submit(any())).thenReturn(UUID.randomUUID());

        scheduler.trigger();

        ArgumentCaptor<JobSubmissionRequest> captor = ArgumentCaptor.forClass(JobSubmissionRequest.class);
        verify(submissionService).submit(captor.capture());
        assertThat(captor.getValue().scheduledAt()).isEqualTo(future);
    }

    @Test
    void trigger_usesCustomBackoffPolicy() {
        BackoffPolicy custom = new BackoffPolicy(5_000L, 3.0, 600_000L);
        scheduler.setPayloads(List.of("{\"x\":1}"));
        scheduler.setCustomBackoffPolicy(custom);
        when(submissionService.submit(any())).thenReturn(UUID.randomUUID());

        scheduler.trigger();

        ArgumentCaptor<JobSubmissionRequest> captor = ArgumentCaptor.forClass(JobSubmissionRequest.class);
        verify(submissionService).submit(captor.capture());
        assertThat(captor.getValue().backoffPolicy()).isEqualTo(custom);
    }

    @Test
    void trigger_usesCustomTaskDeadlineMs() {
        scheduler.setPayloads(List.of("{\"x\":1}"));
        scheduler.setCustomTaskDeadlineMs(30_000L);
        when(submissionService.submit(any())).thenReturn(UUID.randomUUID());

        scheduler.trigger();

        ArgumentCaptor<JobSubmissionRequest> captor = ArgumentCaptor.forClass(JobSubmissionRequest.class);
        verify(submissionService).submit(captor.capture());
        assertThat(captor.getValue().taskDeadlineMs()).isEqualTo(30_000L);
    }

    // ── Test Double ───────────────────────────────────────────────────────────

    static class TestJobScheduler extends AbstractJobScheduler {

        private List<String> payloads = List.of();
        private OffsetDateTime customScheduledAt = null;
        private BackoffPolicy customBackoffPolicy = null;
        private Long customTaskDeadlineMs = null;

        TestJobScheduler(JobSubmissionService submissionService) {
            super(submissionService);
        }

        void setPayloads(List<String> payloads) { this.payloads = payloads; }
        void setCustomScheduledAt(OffsetDateTime t) { this.customScheduledAt = t; }
        void setCustomBackoffPolicy(BackoffPolicy p) { this.customBackoffPolicy = p; }
        void setCustomTaskDeadlineMs(Long ms) { this.customTaskDeadlineMs = ms; }

        @Override protected String getJobName()     { return "test-scheduler-job"; }
        @Override protected long getDeadlineMs()     { return 120_000L; }
        @Override protected String getDestination()  { return "test-destination"; }
        @Override protected String getTaskType()     { return "TEST_TASK"; }

        @Override
        protected List<String> buildTaskPayloads(JobTriggerContext ctx) {
            return payloads;
        }

        @Override
        protected OffsetDateTime getScheduledAt() {
            return customScheduledAt != null ? customScheduledAt : super.getScheduledAt();
        }

        @Override
        protected BackoffPolicy backoffPolicy() {
            return customBackoffPolicy != null ? customBackoffPolicy : super.backoffPolicy();
        }

        @Override
        protected long getTaskDeadlineMs() {
            return customTaskDeadlineMs != null ? customTaskDeadlineMs : super.getTaskDeadlineMs();
        }
    }
}
