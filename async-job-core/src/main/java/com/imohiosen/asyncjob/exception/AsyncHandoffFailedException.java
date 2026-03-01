package com.imohiosen.asyncjob.exception;

public class AsyncHandoffFailedException extends RuntimeException {
    public AsyncHandoffFailedException(String taskId, Throwable cause) {
        super("Async hand-off failed for task: " + taskId, cause);
    }
}
