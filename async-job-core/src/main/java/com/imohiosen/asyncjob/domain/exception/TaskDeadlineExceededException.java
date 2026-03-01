package com.imohiosen.asyncjob.domain.exception;

public class TaskDeadlineExceededException extends RuntimeException {
    public TaskDeadlineExceededException(String taskId) {
        super("Task deadline exceeded: " + taskId);
    }
}
