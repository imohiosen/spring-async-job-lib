package com.imohiosen.asyncjob.application.lifecycle;

import com.imohiosen.asyncjob.application.service.JobSubmissionService;
import com.imohiosen.asyncjob.domain.Job;
import com.imohiosen.asyncjob.domain.JobStatus;
import com.imohiosen.asyncjob.port.repository.JobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ScheduledJobDispatcherTest {

    @Mock JobRepository jobRepository;
    @Mock JobSubmissionService submissionService;

    ScheduledJobDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcher = new ScheduledJobDispatcher(jobRepository, submissionService, 50);
    }

    @Test
    void sweep_dispatchesDueScheduledJob() {
        OffsetDateTime now = OffsetDateTime.now();
        UUID jobId = UUID.randomUUID();
        Job job = dueJob(jobId, now);

        when(jobRepository.findScheduledJobsDue(50)).thenReturn(List.of(job));
        when(submissionService.dispatch(job)).thenReturn(2);

        dispatcher.sweep();

        verify(submissionService).dispatch(job);
    }

    @Test
    void sweep_dispatchesMultipleDueJobs() {
        OffsetDateTime now = OffsetDateTime.now();
        Job job1 = dueJob(UUID.randomUUID(), now);
        Job job2 = dueJob(UUID.randomUUID(), now);

        when(jobRepository.findScheduledJobsDue(50)).thenReturn(List.of(job1, job2));
        when(submissionService.dispatch(any(Job.class))).thenReturn(1);

        dispatcher.sweep();

        verify(submissionService).dispatch(job1);
        verify(submissionService).dispatch(job2);
    }

    @Test
    void sweep_doesNothingWhenNoScheduledJobs() {
        when(jobRepository.findScheduledJobsDue(50)).thenReturn(List.of());

        dispatcher.sweep();

        verify(submissionService, never()).dispatch(any());
    }

    @Test
    void sweep_respectsBatchSize() {
        ScheduledJobDispatcher smallBatch = new ScheduledJobDispatcher(jobRepository, submissionService, 5);

        when(jobRepository.findScheduledJobsDue(5)).thenReturn(List.of());

        smallBatch.sweep();

        // Verify the batch size was passed through
        verify(jobRepository).findScheduledJobsDue(5);
        verify(jobRepository, never()).findScheduledJobsDue(50);
    }

    @Test
    void sweep_continuesOnDispatchFailure() {
        OffsetDateTime now = OffsetDateTime.now();
        Job job1 = dueJob(UUID.randomUUID(), now);
        Job job2 = dueJob(UUID.randomUUID(), now);

        when(jobRepository.findScheduledJobsDue(50)).thenReturn(List.of(job1, job2));
        when(submissionService.dispatch(job1)).thenThrow(new RuntimeException("boom"));
        when(submissionService.dispatch(job2)).thenReturn(1);

        dispatcher.sweep();

        // Both dispatched — first fails, second succeeds
        verify(submissionService).dispatch(job1);
        verify(submissionService).dispatch(job2);
    }

    // ── Helper ───────────────────────────────────────────────────────────────

    private static Job dueJob(UUID jobId, OffsetDateTime now) {
        return new Job(jobId, "scheduled-job", null, JobStatus.SCHEDULED,
                now, now, null, null, now.plusHours(1), now.minusMinutes(5),
                false, 2, 2, 0, 0, 0, 0, null, false);
    }
}
