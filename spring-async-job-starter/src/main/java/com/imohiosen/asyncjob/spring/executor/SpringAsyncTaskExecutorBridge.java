package com.imohiosen.asyncjob.spring.executor;

import com.imohiosen.asyncjob.application.executor.AsyncTaskExecutorBridge;
import com.imohiosen.asyncjob.domain.TaskResult;
import org.springframework.scheduling.annotation.Async;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * Spring-managed {@link AsyncTaskExecutorBridge} that uses {@code @Async} to
 * dispatch work to the {@code asyncJobTaskExecutor} thread pool.
 *
 * <p>This class <strong>must</strong> be a Spring bean for the {@code @Async}
 * proxy to be activated.
 */
public class SpringAsyncTaskExecutorBridge extends AsyncTaskExecutorBridge {

    @Async("asyncJobTaskExecutor")
    @Override
    public CompletableFuture<TaskResult> submitAsync(Callable<TaskResult> work) {
        return super.submitAsync(work);
    }
}
