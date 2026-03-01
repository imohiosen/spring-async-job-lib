package com.imohiosen.asyncjob.kafka;

import com.imohiosen.asyncjob.domain.BackoffPolicy;
import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskResult;
import com.imohiosen.asyncjob.executor.AsyncTaskExecutorBridge;
import com.imohiosen.asyncjob.handler.JobTaskHandler;
import com.imohiosen.asyncjob.handler.JobTaskHandlerRegistry;
import com.imohiosen.asyncjob.lock.TaskLockManager;
import com.imohiosen.asyncjob.repository.JobRepository;
import com.imohiosen.asyncjob.repository.TaskRepository;
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
 *       {@link JobTaskHandler#executor()}, work is submitted there instead of
 *       the shared {@code asyncJobTaskExecutor} pool.</li>
 *   <li><strong>Backoff:</strong> Each handler may define its own
 *       {@link JobTaskHandler#backoffPolicy()} which is used at retry time
 *       instead of the task’s stored backoff columns.</li>
 *   <li><strong>Max attempts:</strong> Resolved per handler via
 *       {@link JobTaskHandler#maxAttempts()}.</li>
 * </ul>
 *
 * <h2>How to use</h2>
 * <ol>
 *   <li>Implement one {@link JobTaskHandler} per task type and register each as
 *       a Spring bean.</li>
 *   <li>Wire a {@link JobTaskHandlerRegistry} bean with all handlers.</li>
 *   <li>Create a thin subclass that adds the {@code @KafkaListener} annotation:   
 * <pre>{@code
 * @Component
 * public class MyJobTaskConsumer extends DispatchingJobTaskConsumer {
 *
 *     public MyJobTaskConsumer(TaskRepository t, JobRepository j,
 *                              TaskLockManager l, AsyncTaskExecutorBridge b,
 *                              JobKafkaProducer p, JobTaskHandlerRegistry r) {
 *         super(t, j, l, b, p, r);
 *     }
 *
 *     @KafkaListener(topics = "${job.kafka.topic}", groupId = "${job.kafka.group-id}")
 *     @Override
 *     public void consume(ConsumerRecord<String, String> record) {
 *         super.consume(record);
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

    public DispatchingJobTaskConsumer(TaskRepository taskRepository,
                                     JobRepository jobRepository,
                                     TaskLockManager lockManager,
                                     AsyncTaskExecutorBridge bridge,
                                     JobKafkaProducer kafkaProducer,
                                     JobTaskHandlerRegistry registry) {
        super(taskRepository, jobRepository, lockManager, bridge, kafkaProducer);
        this.registry = registry;
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
     * Otherwise, fall back to the shared {@code @Async} bridge.
     */
    @Override
    protected CompletableFuture<TaskResult> submitWork(JobTask task) {
        return registry.getHandler(task.taskType())
                .map(handler -> {
                    ExecutorService exec = handler.executor();
                    if (exec != null) {
                        log.debug("Using per-handler executor for type={}", task.taskType());
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
     * Uses the handler's backoff policy if available, otherwise falls back
     * to the task's stored backoff columns.
     */
    @Override
    protected BackoffPolicy resolveBackoffPolicy(JobTask task) {
        return registry.getHandler(task.taskType())
                .map(JobTaskHandler::backoffPolicy)
                .orElse(task.backoffPolicy());
    }
}
