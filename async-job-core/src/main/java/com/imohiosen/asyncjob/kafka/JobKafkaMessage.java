package com.imohiosen.asyncjob.kafka;

import java.util.UUID;

/**
 * Envelope produced to Kafka for every task. Consumers deserialize this
 * to obtain the taskId and route to the correct processing logic.
 */
public record JobKafkaMessage(
        UUID   taskId,
        UUID   jobId,
        String taskType,
        String payload
) {}
