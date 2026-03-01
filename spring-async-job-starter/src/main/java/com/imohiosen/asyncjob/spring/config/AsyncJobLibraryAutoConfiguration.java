package com.imohiosen.asyncjob.spring.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.imohiosen.asyncjob.application.executor.AsyncExecutorProperties;
import com.imohiosen.asyncjob.application.executor.AsyncTaskExecutorBridge;
import com.imohiosen.asyncjob.application.lifecycle.DeadlineGuardScheduler;
import com.imohiosen.asyncjob.application.lifecycle.ScheduledJobDispatcher;
import com.imohiosen.asyncjob.application.lifecycle.TaskRetryScheduler;
import com.imohiosen.asyncjob.application.service.JobSubmissionService;
import com.imohiosen.asyncjob.infrastructure.lock.redisson.LockProperties;
import com.imohiosen.asyncjob.infrastructure.lock.redisson.RedissonTaskLockManager;
import com.imohiosen.asyncjob.infrastructure.persistence.jdbc.JdbcJobRepository;
import com.imohiosen.asyncjob.infrastructure.persistence.jdbc.JdbcTaskRepository;
import com.imohiosen.asyncjob.port.lock.TaskLockManager;
import com.imohiosen.asyncjob.port.messaging.JobMessageProducer;
import com.imohiosen.asyncjob.port.repository.JobRepository;
import com.imohiosen.asyncjob.port.repository.TaskRepository;
import com.imohiosen.asyncjob.spring.executor.SpringAsyncTaskExecutorBridge;
import com.imohiosen.asyncjob.spring.messaging.KafkaJobMessageProducer;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;

/**
 * Spring auto-configuration for the async job library.
 *
 * <p>Wires all core beans, infrastructure adapters (JDBC, Kafka, Redis),
 * and Spring-managed scheduling / async execution.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * @Configuration
 * @Import(AsyncJobLibraryAutoConfiguration.class)
 * public class MyAppConfig { }
 * }</pre>
 *
 * <p>The consuming application must provide:
 * <ul>
 *   <li>{@link DataSource} — backed by PostgreSQL</li>
 *   <li>{@link RedissonClient} — connected to Redis</li>
 *   <li>{@link KafkaTemplate}{@code <String, String>} — configured with bootstrap servers</li>
 * </ul>
 *
 * <p>To swap infrastructure (e.g. Mongo instead of JDBC, RabbitMQ instead of Kafka),
 * exclude this configuration and provide your own beans implementing
 * {@link JobRepository}, {@link TaskRepository}, {@link JobMessageProducer}, and
 * {@link TaskLockManager}.
 */
@Configuration
@EnableAsync
@EnableScheduling
public class AsyncJobLibraryAutoConfiguration {

    // ── Repositories ─────────────────────────────────────────────────────────

    @Bean
    public JobRepository jobRepository(DataSource dataSource) {
        return new JdbcJobRepository(dataSource);
    }

    @Bean
    public TaskRepository taskRepository(DataSource dataSource) {
        return new JdbcTaskRepository(dataSource);
    }

    // ── Lock ─────────────────────────────────────────────────────────────────

    @Bean
    public LockProperties lockProperties(
            @Value("${asyncjob.lock.lease-time-ms:30000}") long leaseTimeMs,
            @Value("${asyncjob.lock.wait-time-ms:0}")   long waitTimeMs) {
        return new LockProperties(leaseTimeMs, waitTimeMs);
    }

    @Bean
    public TaskLockManager taskLockManager(RedissonClient redissonClient,
                                           LockProperties lockProperties) {
        return new RedissonTaskLockManager(redissonClient, lockProperties);
    }

    // ── Messaging ────────────────────────────────────────────────────────────

    @Bean
    public ObjectMapper asyncJobObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Bean
    public JobMessageProducer jobMessageProducer(KafkaTemplate<String, String> kafkaTemplate,
                                                  ObjectMapper asyncJobObjectMapper) {
        return new KafkaJobMessageProducer(kafkaTemplate, asyncJobObjectMapper);
    }

    // ── Async Executor ───────────────────────────────────────────────────────

    @Bean
    public AsyncExecutorProperties asyncExecutorProperties(
            @Value("${asyncjob.executor.core-pool-size:4}")       int corePoolSize,
            @Value("${asyncjob.executor.max-pool-size:16}")        int maxPoolSize,
            @Value("${asyncjob.executor.queue-capacity:100}")      int queueCapacity,
            @Value("${asyncjob.executor.thread-name-prefix:async-job-}") String threadNamePrefix) {
        return new AsyncExecutorProperties(corePoolSize, maxPoolSize, queueCapacity, threadNamePrefix);
    }

    @Bean(name = "asyncJobTaskExecutor")
    public ThreadPoolTaskExecutor asyncJobTaskExecutor(AsyncExecutorProperties props) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(props.corePoolSize());
        executor.setMaxPoolSize(props.maxPoolSize());
        executor.setQueueCapacity(props.queueCapacity());
        executor.setThreadNamePrefix(props.threadNamePrefix());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();
        return executor;
    }

    @Bean
    public AsyncTaskExecutorBridge asyncTaskExecutorBridge() {
        return new SpringAsyncTaskExecutorBridge();
    }

    // ── Submission Service ────────────────────────────────────────────────────

    @Bean
    public JobSubmissionService jobSubmissionService(JobRepository jobRepository,
                                                     TaskRepository taskRepository,
                                                     JobMessageProducer jobMessageProducer) {
        return new JobSubmissionService(jobRepository, taskRepository, jobMessageProducer);
    }

    // ── Lifecycle — Spring @Scheduled wrappers ────────────────────────────────

    @Bean
    public DeadlineGuardScheduler deadlineGuardScheduler(JobRepository jobRepository,
                                                          TaskRepository taskRepository) {
        return new DeadlineGuardScheduler(jobRepository, taskRepository);
    }

    @Bean
    public TaskRetryScheduler taskRetryScheduler(
            TaskRepository taskRepository,
            JobMessageProducer jobMessageProducer,
            @Value("${asyncjob.retry.batch-size:100}") int batchSize) {
        return new TaskRetryScheduler(taskRepository, jobMessageProducer, batchSize);
    }

    @Bean
    public ScheduledJobDispatcher scheduledJobDispatcher(
            JobRepository jobRepository,
            JobSubmissionService jobSubmissionService,
            @Value("${asyncjob.schedule.batch-size:50}") int batchSize) {
        return new ScheduledJobDispatcher(jobRepository, jobSubmissionService, batchSize);
    }

    /**
     * Spring-managed scheduling bridge that invokes the framework-agnostic
     * lifecycle sweep methods via {@code @Scheduled}.
     */
    @Bean
    public SchedulingBridge schedulingBridge(DeadlineGuardScheduler deadlineGuard,
                                             TaskRetryScheduler taskRetry,
                                             ScheduledJobDispatcher scheduledJobDispatcher) {
        return new SchedulingBridge(deadlineGuard, taskRetry, scheduledJobDispatcher);
    }

    /**
     * Inner bean that applies Spring {@code @Scheduled} to the core lifecycle sweeps.
     * This keeps scheduling concerns in the starter — the core classes remain POJOs.
     */
    public static class SchedulingBridge {

        private final DeadlineGuardScheduler deadlineGuard;
        private final TaskRetryScheduler     taskRetry;
        private final ScheduledJobDispatcher scheduledJobDispatcher;

        public SchedulingBridge(DeadlineGuardScheduler deadlineGuard,
                                TaskRetryScheduler taskRetry,
                                ScheduledJobDispatcher scheduledJobDispatcher) {
            this.deadlineGuard          = deadlineGuard;
            this.taskRetry              = taskRetry;
            this.scheduledJobDispatcher = scheduledJobDispatcher;
        }

        @Scheduled(fixedDelayString = "${asyncjob.deadline.sweep-interval-ms:30000}")
        public void sweepDeadlines() {
            deadlineGuard.sweep();
        }

        @Scheduled(fixedDelayString = "${asyncjob.retry.sweep-interval-ms:15000}")
        public void sweepRetries() {
            taskRetry.sweep();
        }

        @Scheduled(fixedDelayString = "${asyncjob.schedule.sweep-interval-ms:10000}")
        public void sweepScheduledJobs() {
            scheduledJobDispatcher.sweep();
        }
    }
}
