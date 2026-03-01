package com.imohiosen.asyncjob.lifecycle;

import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.kafka.JobKafkaProducer;
import com.imohiosen.asyncjob.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;

/**
 * Periodically scans for failed tasks whose {@code next_attempt_time} has
 * passed and re-publishes them to Kafka for retry.
 *
 * <p>Tasks with <strong>fewer attempts are prioritised</strong> so that fresh
 * failures are retried before long-running retry chains, reducing head-of-line
 * blocking in the consumer.
 *
 * <p>Configured via {@code asyncjob.retry.sweep-interval-ms} (default: 15000ms)
 * and {@code asyncjob.retry.batch-size} (default: 100).
 */
public class TaskRetryScheduler {

    private static final Logger log = LoggerFactory.getLogger(TaskRetryScheduler.class);

    private final TaskRepository  taskRepository;
    private final JobKafkaProducer kafkaProducer;
    private final int batchSize;

    public TaskRetryScheduler(TaskRepository taskRepository,
                              JobKafkaProducer kafkaProducer,
                              int batchSize) {
        this.taskRepository = taskRepository;
        this.kafkaProducer  = kafkaProducer;
        this.batchSize      = batchSize;
    }

    /**
     * Sweeps for retryable tasks and re-publishes them to Kafka.
     * Tasks are ordered by {@code attempt_count ASC, next_attempt_time ASC}
     * so lower-attempt tasks are prioritised.
     */
    @Scheduled(fixedDelayString = "${asyncjob.retry.sweep-interval-ms:15000}")
    public void sweep() {
        List<JobTask> retryable = taskRepository.findRetryableTasks(batchSize);

        if (retryable.isEmpty()) {
            log.debug("Retry sweep — no retryable tasks found");
            return;
        }

        int published = 0;
        for (JobTask task : retryable) {
            try {
                kafkaProducer.produce(
                        task.kafkaTopic(),
                        task.id(),
                        task.jobId(),
                        task.taskType(),
                        task.payload()
                );
                published++;
            } catch (Exception e) {
                log.error("Failed to re-publish task={} for retry: {}", task.id(), e.getMessage(), e);
            }
        }

        log.info("Retry sweep re-published {}/{} tasks (batch limit {})",
                published, retryable.size(), batchSize);
    }
}
