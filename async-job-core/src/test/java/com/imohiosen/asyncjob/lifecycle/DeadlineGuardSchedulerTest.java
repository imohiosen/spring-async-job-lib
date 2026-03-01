package com.imohiosen.asyncjob.lifecycle;

import com.imohiosen.asyncjob.repository.JobRepository;
import com.imohiosen.asyncjob.repository.TaskRepository;
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
        when(jobRepository.flagTimedOutJobs()).thenReturn(0);
        when(taskRepository.flagTimedOutTasks()).thenReturn(0);

        scheduler.sweep();

        verify(jobRepository).flagTimedOutJobs();
        verify(taskRepository).flagTimedOutTasks();
    }

    @Test
    void sweep_withTimedOutJobsAndTasks_flagsBoth() {
        when(jobRepository.flagTimedOutJobs()).thenReturn(2);
        when(taskRepository.flagTimedOutTasks()).thenReturn(7);

        scheduler.sweep();

        verify(jobRepository).flagTimedOutJobs();
        verify(taskRepository).flagTimedOutTasks();
    }

    @Test
    void sweep_alwaysQueriesBothRepositories() {
        when(jobRepository.flagTimedOutJobs()).thenReturn(1);
        when(taskRepository.flagTimedOutTasks()).thenReturn(0);

        scheduler.sweep();

        verify(jobRepository, times(1)).flagTimedOutJobs();
        verify(taskRepository, times(1)).flagTimedOutTasks();
    }
}
