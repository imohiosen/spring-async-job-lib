package com.imohiosen.asyncjob.application.executor;

import com.imohiosen.asyncjob.domain.TaskResult;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class AsyncTaskExecutorBridgeTest {

    private final AsyncTaskExecutorBridge bridge = new AsyncTaskExecutorBridge();

    @Test
    void submitAsync_callableSucceeds_returnsSuccessResult() throws Exception {
        CompletableFuture<TaskResult> future = bridge.submitAsync(
                () -> TaskResult.success("{\"done\":true}"));

        TaskResult result = future.get();
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.payload()).isEqualTo("{\"done\":true}");
    }

    @Test
    void submitAsync_callableThrows_returnsFailureResult() throws Exception {
        RuntimeException ex = new RuntimeException("boom");
        CompletableFuture<TaskResult> future = bridge.submitAsync(() -> { throw ex; });

        TaskResult result = future.get();
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.error()).isSameAs(ex);
    }

    @Test
    void submitAsync_executesOnCallingThread() throws Exception {
        Thread callerThread = Thread.currentThread();
        Thread[] executionThread = new Thread[1];

        CompletableFuture<TaskResult> future = bridge.submitAsync(() -> {
            executionThread[0] = Thread.currentThread();
            return TaskResult.success("{}");
        });

        future.get();
        assertThat(executionThread[0]).isSameAs(callerThread);
    }

    @Test
    void submitAsync_callableReturnsFailureResult_propagatesDirectly() throws Exception {
        Throwable error = new IllegalStateException("bad state");
        CompletableFuture<TaskResult> future = bridge.submitAsync(
                () -> TaskResult.failure(error));

        TaskResult result = future.get();
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.error()).isSameAs(error);
    }
}
