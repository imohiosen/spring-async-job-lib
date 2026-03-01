package com.imohiosen.asyncjob.infrastructure.messaging.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.imohiosen.asyncjob.port.messaging.JobMessage;
import com.imohiosen.asyncjob.port.messaging.JobMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

/**
 * Kafka-based implementation of {@link JobMessageProducer}.
 *
 * <p>Serialises {@link JobMessage} to JSON and produces it as a Kafka record
 * with the taskId as the message key (ensuring per-task ordering).
 */
public class KafkaJobMessageProducer implements JobMessageProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaJobMessageProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaJobMessageProducer(KafkaTemplate<String, String> kafkaTemplate,
                                   ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper  = objectMapper;
    }

    @Override
    public void publish(String destination, JobMessage message) {
        String json;
        try {
            json = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    "Failed to serialize JobMessage for task " + message.taskId(), e);
        }

        CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(destination, message.taskId().toString(), json);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to produce message for task={} job={}: {}",
                        message.taskId(), message.jobId(), ex.getMessage(), ex);
            } else {
                log.debug("Produced task={} to topic={} partition={} offset={}",
                        message.taskId(), destination,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }

    /**
     * Deserialises a raw JSON string into a {@link JobMessage}.
     *
     * <p>This is a Kafka-specific convenience — the consuming application's
     * {@code @KafkaListener} can use this to bridge from {@code ConsumerRecord}
     * to the transport-agnostic {@link JobMessage}.
     */
    public JobMessage deserialize(String json) {
        try {
            return objectMapper.readValue(json, JobMessage.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to deserialize JobMessage: " + json, e);
        }
    }
}
