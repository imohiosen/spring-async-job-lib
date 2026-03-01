package com.imohiosen.asyncjob.domain.exception;

public class AsyncHandoffFailedException extends RuntimeException {
    public AsyncHandoffFailedException(String taskId, Throwable cause) {
        super("Async hand-off failed for task: " + taskId, cause);
    }
}
