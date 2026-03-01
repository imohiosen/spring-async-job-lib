package com.imohiosen.asyncjob.application.handler;

import com.imohiosen.asyncjob.domain.BackoffPolicy;
import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskResult;

import java.util.concurrent.ExecutorService;

/**
 * Strategy interface for processing a specific type of job task.
 *
 * <p>Implement one handler per task type and register it as a Spring bean.
 * The {@link JobTaskHandlerRegistry} collects all handlers, and the
 * {@link com.imohiosen.asyncjob.application.consumer.DispatchingJobTaskConsumer DispatchingJobTaskConsumer}
 * routes each incoming message to the matching handler based on
 * {@link JobTask#taskType()}.
 *
 * <h2>Example</h2>
 * <pre>{@code
 * @Component
 * public class InvoiceTaskHandler implements JobTaskHandler {
 *
 *     @Override public String taskType() { return "INVOICE_GENERATION"; }
 *
 *     @Override public int maxAttempts() { return 3; }
 *
 *     @Override
 *     public BackoffPolicy backoffPolicy() {
 *         return new BackoffPolicy(2_000L, 3.0, 300_000L);
 *     }
 *
 *     @Override
 *     public TaskResult handle(JobTask task) {
 *         invoiceService.generate(task.payload());
 *         return TaskResult.success("{\"generated\":true}");
 *     }
 * }
 * }</pre>
 */
public interface JobTaskHandler {

    /**
     * The task type this handler is responsible for.
     * Must match the value stored in {@link JobTask#taskType()}.
     */
    String taskType();

    /**
     * Process the given task and return a result.
     *
     * @param task the task to process
     * @return {@link TaskResult#success(String)} or {@link TaskResult#failure(Throwable)}
     */
    TaskResult handle(JobTask task);

    /**
     * Maximum total attempts (including the first) before a task is promoted to DEAD_LETTER.
     * Defaults to {@code 5}.
     */
    default int maxAttempts() {
        return 5;
    }

    /**
     * Backoff policy used when retrying failed tasks of this type.
     * Overrides the task's stored backoff columns at retry time.
     * Defaults to {@link BackoffPolicy#DEFAULT}.
     */
    default BackoffPolicy backoffPolicy() {
        return BackoffPolicy.DEFAULT;
    }

    /**
     * Optional per-handler executor service. When non-null, the
     * {@link com.imohiosen.asyncjob.application.consumer.DispatchingJobTaskConsumer DispatchingJobTaskConsumer}
     * submits work to this executor instead of the shared {@code asyncJobTaskExecutor} pool.
     *
     * <p>Return a Spring-managed {@link ExecutorService} (e.g. from a {@code @Bean} method)
     * to ensure proper lifecycle management (graceful shutdown).
     *
     * <p>Defaults to {@code null} — uses the shared pool.
     */
    default ExecutorService executor() {
        return null;
    }
}
