package com.imohiosen.asyncjob.domain.exception;

public class TaskLockAcquisitionException extends RuntimeException {
    public TaskLockAcquisitionException(String taskId) {
        super("Failed to acquire lock for task: " + taskId);
    }
}
