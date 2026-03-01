package com.imohiosen.asyncjob.application.consumer;

import com.imohiosen.asyncjob.domain.BackoffPolicy;
import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskResult;
import com.imohiosen.asyncjob.application.executor.AsyncTaskExecutorBridge;
import com.imohiosen.asyncjob.application.handler.JobTaskHandler;
import com.imohiosen.asyncjob.application.handler.JobTaskHandlerRegistry;
import com.imohiosen.asyncjob.port.lock.TaskLockManager;
import com.imohiosen.asyncjob.port.repository.JobRepository;
import com.imohiosen.asyncjob.port.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * A concrete {@link AbstractJobTaskConsumer} that dispatches each incoming task
 * to the {@link JobTaskHandler} registered for its {@link JobTask#taskType()}.
 *
 * <h2>Per-handler isolation</h2>
 * <ul>
 *   <li><strong>Executor:</strong> If a handler provides a non-null
 *       {@link JobTaskHandler#executor()}, work is submitted there for primary
 *       attempts. If {@link JobTaskHandler#retryExecutor()} is non-null, retry
 *       attempts use that executor instead. This allows isolation of retry
 *       processing from primary processing.</li>
 *   <li><strong>Backoff:</strong> Each handler may define its own
 *       {@link JobTaskHandler#backoffPolicy()} which is used at retry time
 *       instead of the task's stored backoff columns.</li>
 *   <li><strong>Max attempts:</strong> Resolved per handler via
 *       {@link JobTaskHandler#maxAttempts()}.</li>
 * </ul>
 *
 * <h2>How to use</h2>
 * <ol>
 *   <li>Implement one {@link JobTaskHandler} per task type and register each as
 *       a Spring bean.</li>
 *   <li>Wire a {@link JobTaskHandlerRegistry} bean with all handlers.</li>
 *   <li>Create a thin subclass that bridges from your transport:
 * <pre>{@code
 * @Component
 * public class MyJobTaskConsumer extends DispatchingJobTaskConsumer {
 *
 *     private final KafkaJobMessageProducer deserializer;
 *
 *     public MyJobTaskConsumer(TaskRepository t, JobRepository j,
 *                              TaskLockManager l, AsyncTaskExecutorBridge b,
 *                              JobTaskHandlerRegistry r,
 *                              KafkaJobMessageProducer deserializer) {
 *         super(t, j, l, b, r);
 *         this.deserializer = deserializer;
 *     }
 *
 *     @KafkaListener(topics = "${job.kafka.topic}", groupId = "${job.kafka.group-id}")
 *     public void onMessage(ConsumerRecord<String, String> record) {
 *         consume(deserializer.deserialize(record.value()));
 *     }
 * }
 * }</pre>
 *   </li>
 * </ol>
 */
public class DispatchingJobTaskConsumer extends AbstractJobTaskConsumer {

    private static final Logger log = LoggerFactory.getLogger(DispatchingJobTaskConsumer.class);
    private static final int DEFAULT_MAX_ATTEMPTS = 5;

    private final JobTaskHandlerRegistry registry;
    private final TimeCriticalResilienceExecutor timeCriticalExecutor;

    public DispatchingJobTaskConsumer(TaskRepository taskRepository,
                                     JobRepository jobRepository,
                                     TaskLockManager lockManager,
                                     AsyncTaskExecutorBridge bridge,
                                     JobTaskHandlerRegistry registry) {
        this(taskRepository, jobRepository, lockManager, bridge, registry, null);
    }

    public DispatchingJobTaskConsumer(TaskRepository taskRepository,
                                     JobRepository jobRepository,
                                     TaskLockManager lockManager,
                                     AsyncTaskExecutorBridge bridge,
                                     JobTaskHandlerRegistry registry,
                                     TimeCriticalResilienceExecutor timeCriticalExecutor) {
        super(taskRepository, jobRepository, lockManager, bridge);
        this.registry = registry;
        this.timeCriticalExecutor = timeCriticalExecutor;
    }

    @Override
    protected int getMaxAttempts(JobTask task) {
        return registry.getHandler(task.taskType())
                .map(JobTaskHandler::maxAttempts)
                .orElse(DEFAULT_MAX_ATTEMPTS);
    }

    @Override
    protected TaskResult processTask(JobTask task) {
        String taskType = task.taskType();

        JobTaskHandler handler = registry.getHandler(taskType)
                .orElseThrow(() -> new IllegalStateException(
                        "No JobTaskHandler registered for task type: " + taskType));

        log.debug("Dispatching task={} to handler for type={}", task.id(), taskType);
        return handler.handle(task);
    }

    /**
     * If the handler provides a dedicated executor, submit directly to it.
     * For retry attempts (attemptCount > 0), use retryExecutor() if available,
     * otherwise fall back to executor(). If both are null, use the shared @Async bridge.
     */
    @Override
    protected CompletableFuture<TaskResult> submitWork(JobTask task) {
        return registry.getHandler(task.taskType())
                .map(handler -> {
                    // Determine which executor to use based on attempt count
                    ExecutorService exec = selectExecutor(task, handler);
                    
                    if (exec != null) {
                        boolean isRetry = task.attemptCount() > 0;
                        log.debug("Using per-handler {} executor for type={}, attemptCount={}",
                                isRetry ? "retry" : "primary", task.taskType(), task.attemptCount());
                        return CompletableFuture.supplyAsync(() -> {
                            try {
                                return handler.handle(task);
                            } catch (Exception e) {
                                return TaskResult.failure(e);
                            }
                        }, exec);
                    }
                    return super.submitWork(task);
                })
                .orElseGet(() -> super.submitWork(task));
    }

    /**
     * Selects the appropriate executor based on whether this is a retry attempt.
     *
     * @param task the task being processed
     * @param handler the handler for this task type
     * @return the executor to use, or null to use the shared pool
     */
    private ExecutorService selectExecutor(JobTask task, JobTaskHandler handler) {
        if (task.attemptCount() > 0) {
            // This is a retry — prefer retryExecutor, fall back to primary executor
            ExecutorService retryExec = handler.retryExecutor();
            return retryExec != null ? retryExec : handler.executor();
        } else {
            // This is the primary (first) attempt
            return handler.executor();
        }
    }

    /**
     * Uses the handler's backoff policy if available, otherwise falls back
     * to the task's stored backoff columns.
     */
    @Override
    protected BackoffPolicy resolveBackoffPolicy(JobTask task) {
        return registry.getHandler(task.taskType())
                .map(JobTaskHandler::backoffPolicy)
                .orElse(task.backoffPolicy());
    }

    /**
     * Submits time-critical work via Resilience4j in-memory retries.
     * If no {@link TimeCriticalResilienceExecutor} is configured, falls back
     * to the normal {@link #submitWork(JobTask)} path.
     *
     * <p>The handler is resolved by task type, and the appropriate executor
     * (primary or retry) is selected based on the attempt count.
     */
    @Override
    protected CompletableFuture<TaskResult> submitTimeCriticalWork(JobTask task, long fenceToken) {
        if (timeCriticalExecutor == null) {
            log.warn("Task={} is time-critical but no TimeCriticalResilienceExecutor configured, " +
                    "falling back to normal path", task.id());
            return submitWork(task);
        }

        return registry.getHandler(task.taskType())
                .map(handler -> {
                    ExecutorService exec = selectExecutor(task, handler);
                    if (exec != null) {
                        return CompletableFuture.supplyAsync(() -> {
                            try {
                                return timeCriticalExecutor.execute(task, fenceToken, handler);
                            } catch (Exception e) {
                                return TaskResult.failure(e);
                            }
                        }, exec);
                    }
                    // Use the shared bridge executor
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            return timeCriticalExecutor.execute(task, fenceToken, handler);
                        } catch (Exception e) {
                            return TaskResult.failure(e);
                        }
                    });
                })
                .orElseGet(() -> {
                    log.warn("No handler for task type={} — falling back to normal submit",
                            task.taskType());
                    return submitWork(task);
                });
    }
}
