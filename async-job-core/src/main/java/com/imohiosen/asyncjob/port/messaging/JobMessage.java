package com.imohiosen.asyncjob.port.messaging;

import java.util.UUID;

/**
 * Transport-agnostic envelope for a task message.
 *
 * <p>Produced by {@link JobMessageProducer} and consumed by the application
 * layer. The underlying transport (Kafka, RabbitMQ, Redis Streams, etc.)
 * serialises and deserialises this record as needed.
 */
public record JobMessage(
        UUID   taskId,
        UUID   jobId,
        String taskType,
        String payload
) {}
