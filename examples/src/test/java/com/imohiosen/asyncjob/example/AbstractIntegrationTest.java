package com.imohiosen.asyncjob.example;

import com.redis.testcontainers.RedisContainer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base test class that provides Testcontainers infrastructure for integration tests.
 * <p>
 * This sets up:
 * - PostgreSQL for job/task persistence
 * - Redis for distributed locking
 * - Kafka for message distribution
 * <p>
 * Uses the singleton container pattern so that containers are shared across
 * all test classes and not stopped/restarted between classes.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class AbstractIntegrationTest {

    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("asyncjob_test")
            .withUsername("test")
            .withPassword("test")
            .withInitScript("schema.sql");

    static RedisContainer redis = new RedisContainer(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    static {
        postgres.start();
        redis.start();
        kafka.start();
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        // Kafka configuration
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("asyncjob.kafka.topic", () -> "async-job-tasks-test");

        // Redis configuration
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", redis::getFirstMappedPort);

        // PostgreSQL is auto-configured via @ServiceConnection
        
        // Reduce sweep intervals for faster testing
        registry.add("asyncjob.deadline.sweep-interval-ms", () -> "5000");
        registry.add("asyncjob.retry.sweep-interval-ms", () -> "3000");
        registry.add("asyncjob.schedule.sweep-interval-ms", () -> "3000");
    }
}
