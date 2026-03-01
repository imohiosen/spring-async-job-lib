package com.imohiosen.asyncjob.domain.exception;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ExceptionTest {

    @Test
    void taskDeadlineExceededException_messageContainsTaskId() {
        TaskDeadlineExceededException ex = new TaskDeadlineExceededException("task-123");
        assertThat(ex).isInstanceOf(RuntimeException.class);
        assertThat(ex.getMessage()).isEqualTo("Task deadline exceeded: task-123");
    }

    @Test
    void asyncHandoffFailedException_messageAndCauseCorrect() {
        Throwable cause = new RuntimeException("connection refused");
        AsyncHandoffFailedException ex = new AsyncHandoffFailedException("task-456", cause);
        assertThat(ex).isInstanceOf(RuntimeException.class);
        assertThat(ex.getMessage()).isEqualTo("Async hand-off failed for task: task-456");
        assertThat(ex.getCause()).isSameAs(cause);
    }

    @Test
    void jobNotFoundException_messageContainsJobId() {
        JobNotFoundException ex = new JobNotFoundException("job-789");
        assertThat(ex).isInstanceOf(RuntimeException.class);
        assertThat(ex.getMessage()).isEqualTo("Job not found: job-789");
    }

    @Test
    void taskLockAcquisitionException_messageContainsTaskId() {
        TaskLockAcquisitionException ex = new TaskLockAcquisitionException("task-abc");
        assertThat(ex).isInstanceOf(RuntimeException.class);
        assertThat(ex.getMessage()).isEqualTo("Failed to acquire lock for task: task-abc");
    }
}
