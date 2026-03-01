package com.imohiosen.asyncjob.exception;

public class TaskDeadlineExceededException extends RuntimeException {
    public TaskDeadlineExceededException(String taskId) {
        super("Task deadline exceeded: " + taskId);
    }
}
