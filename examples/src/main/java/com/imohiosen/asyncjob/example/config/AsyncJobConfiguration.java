package com.imohiosen.asyncjob.example.config;

import com.imohiosen.asyncjob.application.consumer.DispatchingJobTaskConsumer;
import com.imohiosen.asyncjob.application.executor.AsyncTaskExecutorBridge;
import com.imohiosen.asyncjob.application.handler.JobTaskHandler;
import com.imohiosen.asyncjob.application.handler.JobTaskHandlerRegistry;
import com.imohiosen.asyncjob.port.lock.TaskLockManager;
import com.imohiosen.asyncjob.port.messaging.JobMessage;
import com.imohiosen.asyncjob.port.repository.JobRepository;
import com.imohiosen.asyncjob.port.repository.TaskRepository;
import com.imohiosen.asyncjob.spring.config.AsyncJobLibraryAutoConfiguration;
import com.imohiosen.asyncjob.spring.messaging.KafkaJobMessageProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Configuration for the async job library integration.
 */
@Configuration
@Import(AsyncJobLibraryAutoConfiguration.class)
public class AsyncJobConfiguration {

    /**
     * Registry that collects all JobTaskHandler beans.
     */
    @Bean
    public JobTaskHandlerRegistry jobTaskHandlerRegistry(List<JobTaskHandler> handlers) {
        return new JobTaskHandlerRegistry(handlers);
    }

    /**
     * Kafka consumer that bridges from Kafka records to the async job processing pipeline.
     */
    @Component
    public static class AsyncJobKafkaConsumer {

        private static final Logger log = LoggerFactory.getLogger(AsyncJobKafkaConsumer.class);

        private final DispatchingJobTaskConsumer consumer;
        private final KafkaJobMessageProducer messageProducer;

        public AsyncJobKafkaConsumer(
                TaskRepository taskRepository,
                JobRepository jobRepository,
                TaskLockManager lockManager,
                AsyncTaskExecutorBridge bridge,
                JobTaskHandlerRegistry registry,
                KafkaJobMessageProducer messageProducer) {
            
            this.consumer = new DispatchingJobTaskConsumer(
                    taskRepository, jobRepository, lockManager, bridge, registry);
            this.messageProducer = messageProducer;
        }

        /**
         * Kafka listener that consumes job task messages and passes them to the consumer.
         */
        @KafkaListener(
                topics = "${asyncjob.kafka.topic}",
                groupId = "${spring.kafka.consumer.group-id}",
                concurrency = "3"  // Run 3 concurrent consumers
        )
        public void onMessage(ConsumerRecord<String, String> record) {
            try {
                log.debug("Received Kafka message: key={} partition={} offset={}", 
                        record.key(), record.partition(), record.offset());

                // Deserialize the message
                JobMessage message = messageProducer.deserialize(record.value());

                // Pass to the consumer for processing
                consumer.consume(message);

            } catch (Exception e) {
                log.error("Failed to process Kafka message: partition={} offset={}", 
                        record.partition(), record.offset(), e);
                // In production, you might want to send this to a dead-letter topic
            }
        }
    }
}
