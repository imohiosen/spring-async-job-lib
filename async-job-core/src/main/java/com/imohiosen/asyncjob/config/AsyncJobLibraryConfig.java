package com.imohiosen.asyncjob.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.imohiosen.asyncjob.executor.AsyncExecutorProperties;
import com.imohiosen.asyncjob.executor.AsyncTaskExecutorBridge;
import com.imohiosen.asyncjob.kafka.JobKafkaProducer;
import com.imohiosen.asyncjob.lifecycle.DeadlineGuardScheduler;
import com.imohiosen.asyncjob.lifecycle.TaskRetryScheduler;
import com.imohiosen.asyncjob.lock.LockProperties;
import com.imohiosen.asyncjob.lock.TaskLockManager;
import com.imohiosen.asyncjob.repository.JobRepository;
import com.imohiosen.asyncjob.repository.TaskRepository;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Core Spring configuration for the async job library.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * @Configuration
 * @Import(AsyncJobLibraryConfig.class)
 * @EnableScheduling
 * @EnableSchedulerLock(defaultLockAtMostFor = "PT10M")
 * public class MyAppConfig { }
 * }</pre>
 *
 * <p>The consuming application must provide the following beans in its own context:
 * <ul>
 *   <li>{@link JdbcTemplate} — backed by a DataSource connected to PostgreSQL</li>
 *   <li>{@link RedissonClient} — connected to Redis</li>
 *   <li>{@link KafkaTemplate}{@code <String, String>} — configured with bootstrap servers</li>
 * </ul>
 */
@Configuration
@EnableAsync
public class AsyncJobLibraryConfig {

    // ── Repositories ─────────────────────────────────────────────────────────

    @Bean
    public JobRepository jobRepository(JdbcTemplate jdbcTemplate) {
        return new JobRepository(jdbcTemplate);
    }

    @Bean
    public TaskRepository taskRepository(JdbcTemplate jdbcTemplate) {
        return new TaskRepository(jdbcTemplate);
    }

    // ── Lock ─────────────────────────────────────────────────────────────────

    @Bean
    public LockProperties lockProperties(
            @Value("${asyncjob.lock.lease-time-ms:-1}") long leaseTimeMs,
            @Value("${asyncjob.lock.wait-time-ms:0}")   long waitTimeMs) {
        return new LockProperties(leaseTimeMs, waitTimeMs);
    }

    @Bean
    public TaskLockManager taskLockManager(RedissonClient redissonClient,
                                           LockProperties lockProperties) {
        return new TaskLockManager(redissonClient, lockProperties);
    }

    // ── Kafka Producer ───────────────────────────────────────────────────────

    @Bean
    public ObjectMapper asyncJobObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Bean
    public JobKafkaProducer jobKafkaProducer(KafkaTemplate<String, String> kafkaTemplate,
                                             ObjectMapper asyncJobObjectMapper) {
        return new JobKafkaProducer(kafkaTemplate, asyncJobObjectMapper);
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
        return new AsyncTaskExecutorBridge();
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Bean
    public DeadlineGuardScheduler deadlineGuardScheduler(JobRepository jobRepository,
                                                          TaskRepository taskRepository) {
        return new DeadlineGuardScheduler(jobRepository, taskRepository);
    }

    @Bean
    public TaskRetryScheduler taskRetryScheduler(
            TaskRepository taskRepository,
            JobKafkaProducer kafkaProducer,
            @Value("${asyncjob.retry.batch-size:100}") int batchSize) {
        return new TaskRetryScheduler(taskRepository, kafkaProducer, batchSize);
    }
}
