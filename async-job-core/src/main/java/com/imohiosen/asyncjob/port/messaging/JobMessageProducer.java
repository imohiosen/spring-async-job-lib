package com.imohiosen.asyncjob.port.messaging;

/**
 * Port for publishing task messages to a messaging system.
 *
 * <p>Implementations may target Kafka, RabbitMQ, Redis Streams, or any
 * other message broker.
 *
 * @see JobMessage
 */
public interface JobMessageProducer {

    /**
     * Publishes a task message to the specified destination.
     *
     * @param destination transport-specific target (e.g. Kafka topic, RabbitMQ queue, Redis stream key)
     * @param message     the task message to publish
     */
    void publish(String destination, JobMessage message);
}
