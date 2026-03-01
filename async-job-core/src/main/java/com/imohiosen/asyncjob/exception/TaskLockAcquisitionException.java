package com.imohiosen.asyncjob.exception;

public class TaskLockAcquisitionException extends RuntimeException {
    public TaskLockAcquisitionException(String taskId) {
        super("Failed to acquire lock for task: " + taskId);
    }
}
