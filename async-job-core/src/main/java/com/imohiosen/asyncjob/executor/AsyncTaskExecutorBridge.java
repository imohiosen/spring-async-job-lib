package com.imohiosen.asyncjob.executor;

import com.imohiosen.asyncjob.domain.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * Bridges the Kafka consumer thread to the {@code @Async} thread pool.
 *
 * <p>This bean <strong>must</strong> be Spring-managed for {@code @Async} proxying to apply.
 * Import it via {@code AsyncJobLibraryConfig}.
 *
 * <h2>Two-step async hand-off</h2>
 * <ol>
 *   <li><strong>Step 1:</strong> {@link #submitAsync(Callable)} — submits work to the
 *       {@code asyncJobTaskExecutor} pool and returns a {@link CompletableFuture}.</li>
 *   <li><strong>Step 2:</strong> The caller calls {@code future.get(timeout, unit)} to
 *       confirm completion and obtain the {@link TaskResult}.</li>
 * </ol>
 */
public class AsyncTaskExecutorBridge {

    private static final Logger log = LoggerFactory.getLogger(AsyncTaskExecutorBridge.class);

    /**
     * Step 1 — submits the given callable to the {@code asyncJobTaskExecutor} thread pool.
     *
     * <p>{@code @Async} causes this method to run on the configured executor.
     * The returned {@link CompletableFuture} resolves when the callable completes
     * (success or failure). The future itself never fails — exceptions from the
     * callable are wrapped in {@link TaskResult#failure(Throwable)}.
     *
     * @param work the task processing logic to execute asynchronously
     * @return a future that resolves to the task result
     */
    @Async("asyncJobTaskExecutor")
    public CompletableFuture<TaskResult> submitAsync(Callable<TaskResult> work) {
        try {
            TaskResult result = work.call();
            log.debug("Async task completed on thread={}", Thread.currentThread().getName());
            return CompletableFuture.completedFuture(result);
        } catch (Exception e) {
            log.error("Async task threw an exception: {}", e.getMessage(), e);
            return CompletableFuture.completedFuture(TaskResult.failure(e));
        }
    }
}
