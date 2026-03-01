package com.imohiosen.asyncjob.spring.executor;

import com.imohiosen.asyncjob.domain.TaskResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class SpringAsyncTaskExecutorBridgeTest {

    SpringAsyncTaskExecutorBridge bridge;

    @BeforeEach
    void setUp() {
        bridge = new SpringAsyncTaskExecutorBridge();
    }

    @Test
    void submitAsync_successfulCallable_returnsSuccessResult() throws Exception {
        TaskResult expected = TaskResult.success("done");

        CompletableFuture<TaskResult> future = bridge.submitAsync(() -> expected);

        assertThat(future).isCompleted();
        TaskResult result = future.get();
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.payload()).isEqualTo("done");
    }

    @Test
    void submitAsync_failingCallable_returnsFailureResult() throws Exception {
        RuntimeException ex = new RuntimeException("task failed");

        CompletableFuture<TaskResult> future = bridge.submitAsync(() -> { throw ex; });

        assertThat(future).isCompleted();
        TaskResult result = future.get();
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.error()).isEqualTo(ex);
    }

    @Test
    void submitAsync_returnsCompletedFuture_notPending() {
        CompletableFuture<TaskResult> future = bridge.submitAsync(() -> TaskResult.success("ok"));

        // Without a Spring @Async proxy, the default implementation runs inline
        assertThat(future.isDone()).isTrue();
    }

    @Test
    void submitAsync_nullPayload_successResult() throws Exception {
        CompletableFuture<TaskResult> future = bridge.submitAsync(() -> TaskResult.success(null));

        TaskResult result = future.get();
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.payload()).isNull();
    }

    @Test
    void submitAsync_checkedExceptionFromCallable_returnsFailure() throws Exception {
        Exception checked = new Exception("checked error");

        CompletableFuture<TaskResult> future = bridge.submitAsync(() -> { throw checked; });

        TaskResult result = future.get();
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.error()).isEqualTo(checked);
    }
}
