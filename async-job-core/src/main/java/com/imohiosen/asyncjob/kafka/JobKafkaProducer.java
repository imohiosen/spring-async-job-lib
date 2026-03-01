package com.imohiosen.asyncjob.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Wraps {@link KafkaTemplate} to produce {@link JobKafkaMessage} instances to a topic.
 * The taskId is used as the Kafka message key to ensure ordering per task.
 */
public class JobKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(JobKafkaProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public JobKafkaProducer(KafkaTemplate<String, String> kafkaTemplate,
                            ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper  = objectMapper;
    }

    /**
     * Produces a task message to the specified Kafka topic.
     *
     * @param topic    destination Kafka topic
     * @param taskId   task UUID — used as message key
     * @param jobId    parent job UUID
     * @param taskType logical task type discriminator
     * @param payload  JSON string payload
     * @return CompletableFuture that resolves to the SendResult on broker acknowledgement
     */
    public CompletableFuture<SendResult<String, String>> produce(
            String topic, UUID taskId, UUID jobId, String taskType, String payload) {

        JobKafkaMessage message = new JobKafkaMessage(taskId, jobId, taskType, payload);
        String json;
        try {
            json = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize JobKafkaMessage for task " + taskId, e);
        }

        CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(topic, taskId.toString(), json);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to produce Kafka message for task={} job={}: {}",
                        taskId, jobId, ex.getMessage(), ex);
            } else {
                log.debug("Produced task={} to topic={} partition={} offset={}",
                        taskId, topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * Deserializes a raw JSON string into a {@link JobKafkaMessage}.
     * Exposed as a utility so consumers can use the same ObjectMapper instance.
     */
    public JobKafkaMessage deserialize(String json) {
        try {
            return objectMapper.readValue(json, JobKafkaMessage.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to deserialize JobKafkaMessage: " + json, e);
        }
    }
}
