package com.imohiosen.asyncjob.application.executor;

import com.imohiosen.asyncjob.domain.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * Bridges the consumer thread to an async thread pool.
 *
 * <p>The default implementation runs the callable on the calling thread's
 * executor. A framework-specific subclass (e.g. Spring {@code @Async}) may
 * override {@link #submitAsync(Callable)} to dispatch to a managed thread pool.
 *
 * <h2>Two-step async hand-off</h2>
 * <ol>
 *   <li><strong>Step 1:</strong> {@link #submitAsync(Callable)} — submits work to the
 *       thread pool and returns a {@link CompletableFuture}.</li>
 *   <li><strong>Step 2:</strong> The caller calls {@code future.get()} to
 *       confirm completion and obtain the {@link TaskResult}.</li>
 * </ol>
 */
public class AsyncTaskExecutorBridge {

    private static final Logger log = LoggerFactory.getLogger(AsyncTaskExecutorBridge.class);

    /**
     * Submits the given callable for asynchronous execution.
     *
     * <p>The default implementation runs inline. Framework-specific subclasses
     * (e.g. Spring {@code @Async}) override this method to dispatch to a
     * managed thread pool.
     *
     * @param work the task processing logic to execute asynchronously
     * @return a future that resolves to the task result
     */
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
