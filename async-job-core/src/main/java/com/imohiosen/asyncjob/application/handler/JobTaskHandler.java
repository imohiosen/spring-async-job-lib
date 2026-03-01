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
 *     private final ExecutorService primaryExecutor;
 *     private final ExecutorService retryExecutor;
 *
 *     public InvoiceTaskHandler(
 *         @Qualifier("invoicePrimaryExecutor") ExecutorService primaryExecutor,
 *         @Qualifier("invoiceRetryExecutor") ExecutorService retryExecutor) {
 *         this.primaryExecutor = primaryExecutor;
 *         this.retryExecutor = retryExecutor;
 *     }
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
 *     public ExecutorService executor() {
 *         return primaryExecutor;  // For initial attempts
 *     }
 *
 *     @Override
 *     public ExecutorService retryExecutor() {
 *         return retryExecutor;  // For retry attempts
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
     * Optional per-handler executor service for primary task execution (attemptCount == 0).
     * When non-null, the {@link com.imohiosen.asyncjob.application.consumer.DispatchingJobTaskConsumer DispatchingJobTaskConsumer}
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

    /**
     * Optional per-handler retry executor service. When non-null, the
     * {@link com.imohiosen.asyncjob.application.consumer.DispatchingJobTaskConsumer DispatchingJobTaskConsumer}
     * submits retry work (attemptCount > 0) to this executor instead of the primary executor.
     *
     * <p>This allows handlers to isolate retry processing from primary processing,
     * for example using a smaller thread pool for retries to prevent retry storms
     * from starving primary task processing.
     *
     * <p><strong>Fallback behavior:</strong>
     * <ul>
     *   <li>If both {@code retryExecutor()} and {@code executor()} return null → shared pool</li>
     *   <li>If {@code retryExecutor()} returns null but {@code executor()} returns non-null → use primary executor for retries</li>
     *   <li>If {@code retryExecutor()} returns non-null → use it for all retry attempts</li>
     * </ul>
     *
     * <p>Return a Spring-managed {@link ExecutorService} (e.g. from a {@code @Bean} method)
     * to ensure proper lifecycle management (graceful shutdown).
     *
     * <p>Defaults to {@code null} — retries use the same executor as primary tasks.
     */
    default ExecutorService retryExecutor() {
        return null;
    }
}
