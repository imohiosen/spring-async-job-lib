package com.imohiosen.asyncjob.example;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests to verify Testcontainers infrastructure is properly configured.
 */
class TestcontainersConfigurationTest extends AbstractIntegrationTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void shouldConnectToPostgres() {
        // When
        Integer result = jdbcTemplate.queryForObject("SELECT 1", Integer.class);

        // Then
        assertThat(result).isEqualTo(1);
    }

    @Test
    void shouldHaveJobsTable() {
        // When
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'jobs'",
                Integer.class
        );

        // Then
        assertThat(count).isEqualTo(1);
    }

    @Test
    void shouldHaveJobTasksTable() {
        // When
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'job_tasks'",
                Integer.class
        );

        // Then
        assertThat(count).isEqualTo(1);
    }

    @Test
    void shouldHaveShedlockTable() {
        // When
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'shedlock'",
                Integer.class
        );

        // Then
        assertThat(count).isEqualTo(1);
    }

    @Test
    void shouldHaveJobStatusEnum() {
        // When
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pg_type WHERE typname = 'job_status'",
                Integer.class
        );

        // Then
        assertThat(count).isEqualTo(1);
    }

    @Test
    void shouldHaveTaskStatusEnum() {
        // When
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pg_type WHERE typname = 'task_status'",
                Integer.class
        );

        // Then
        assertThat(count).isEqualTo(1);
    }

    @Test
    void shouldHaveRequiredIndexes() {
        // When
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pg_indexes WHERE tablename IN ('jobs', 'job_tasks')",
                Integer.class
        );

        // Then - Should have multiple indexes
        assertThat(count).isGreaterThan(5);
    }

    @Test
    void postgresContainerShouldBeRunning() {
        assertThat(postgres.isRunning()).isTrue();
    }

    @Test
    void redisContainerShouldBeRunning() {
        assertThat(redis.isRunning()).isTrue();
    }

    @Test
    void kafkaContainerShouldBeRunning() {
        assertThat(kafka.isRunning()).isTrue();
    }
}
