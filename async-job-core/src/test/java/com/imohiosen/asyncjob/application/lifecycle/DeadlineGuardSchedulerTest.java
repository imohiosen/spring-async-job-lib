package com.imohiosen.asyncjob.application.lifecycle;

import com.imohiosen.asyncjob.port.repository.JobRepository;
import com.imohiosen.asyncjob.port.repository.TaskRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DeadlineGuardSchedulerTest {

    @Mock JobRepository  jobRepository;
    @Mock TaskRepository taskRepository;

    @InjectMocks DeadlineGuardScheduler scheduler;

    @Test
    void sweep_noViolations_completesSilently() {
        when(jobRepository.flagStaleJobs()).thenReturn(0);
        when(taskRepository.flagStaleTasks()).thenReturn(0);

        scheduler.sweep();

        verify(jobRepository).flagStaleJobs();
        verify(taskRepository).flagStaleTasks();
    }

    @Test
    void sweep_withStaleJobsAndTasks_flagsBoth() {
        when(jobRepository.flagStaleJobs()).thenReturn(2);
        when(taskRepository.flagStaleTasks()).thenReturn(7);

        scheduler.sweep();

        verify(jobRepository).flagStaleJobs();
        verify(taskRepository).flagStaleTasks();
    }

    @Test
    void sweep_alwaysQueriesBothRepositories() {
        when(jobRepository.flagStaleJobs()).thenReturn(1);
        when(taskRepository.flagStaleTasks()).thenReturn(0);

        scheduler.sweep();

        verify(jobRepository, times(1)).flagStaleJobs();
        verify(taskRepository, times(1)).flagStaleTasks();
    }

    @Test
    void sweep_onlyStaleTasks_noStaleJobs() {
        when(jobRepository.flagStaleJobs()).thenReturn(0);
        when(taskRepository.flagStaleTasks()).thenReturn(3);

        scheduler.sweep();

        verify(jobRepository).flagStaleJobs();
        verify(taskRepository).flagStaleTasks();
    }
}
