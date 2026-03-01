package com.imohiosen.asyncjob.handler;

import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskResult;

/**
 * Strategy interface for processing a specific type of job task.
 *
 * <p>Implement one handler per task type and register it as a Spring bean.
 * The {@link JobTaskHandlerRegistry} collects all handlers, and the
 * {@link com.imohiosen.asyncjob.kafka.DispatchingJobTaskConsumer DispatchingJobTaskConsumer}
 * routes each incoming Kafka message to the matching handler based on
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
}
