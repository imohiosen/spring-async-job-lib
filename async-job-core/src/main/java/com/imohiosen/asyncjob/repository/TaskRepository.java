package com.imohiosen.asyncjob.repository;

import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * JDBC-based repository for the {@code job_tasks} table.
 */
public class TaskRepository {

    private final JdbcTemplate jdbc;

    public TaskRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(JobTask task) {
        jdbc.update("""
                INSERT INTO job_tasks (id, job_id, task_type, kafka_topic, status,
                    created_at, updated_at, deadline_at, timed_out,
                    attempt_count, base_interval_ms, multiplier, max_delay_ms, payload)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
                """,
                task.id(), task.jobId(), task.taskType(), task.kafkaTopic(),
                task.status().name(),
                toTimestamp(task.createdAt()), toTimestamp(task.updatedAt()),
                toTimestamp(task.deadlineAt()), task.timedOut(),
                task.attemptCount(),
                task.baseIntervalMs(), task.multiplier(), task.maxDelayMs(),
                task.payload());
    }

    /**
     * Finds a task that is eligible for processing: status is PENDING or FAILED,
     * and {@code next_attempt_time} is null or in the past.
     * This is the idempotency guard — prevents double-processing by concurrent consumers.
     */
    public Optional<JobTask> findEligible(UUID taskId) {
        List<JobTask> results = jdbc.query("""
                SELECT * FROM job_tasks
                WHERE id = ?
                  AND status IN ('PENDING', 'FAILED')
                  AND (next_attempt_time IS NULL OR next_attempt_time <= NOW())
                """, new TaskRowMapper(), taskId);
        return results.stream().findFirst();
    }

    public Optional<JobTask> findById(UUID taskId) {
        List<JobTask> results = jdbc.query(
                "SELECT * FROM job_tasks WHERE id = ?", new TaskRowMapper(), taskId);
        return results.stream().findFirst();
    }

    public void markInProgress(UUID taskId, OffsetDateTime startedAt, long fenceToken) {
        jdbc.update("""
                UPDATE job_tasks
                SET status = 'IN_PROGRESS', started_at = ?, attempt_count = attempt_count + 1,
                    last_attempt_time = NOW(), fence_token = ?, updated_at = NOW()
                WHERE id = ?
                """, toTimestamp(startedAt), fenceToken, taskId);
    }

    public void recordAsyncSubmitted(UUID taskId, OffsetDateTime submittedAt, long fenceToken) {
        jdbc.update("""
                UPDATE job_tasks SET async_submitted_at = ?, updated_at = NOW()
                WHERE id = ? AND fence_token = ?
                """, toTimestamp(submittedAt), taskId, fenceToken);
    }

    public void recordAsyncCompleted(UUID taskId, OffsetDateTime completedAt, long fenceToken) {
        jdbc.update("""
                UPDATE job_tasks SET async_completed_at = ?, updated_at = NOW()
                WHERE id = ? AND fence_token = ?
                """, toTimestamp(completedAt), taskId, fenceToken);
    }

    public boolean markCompleted(UUID taskId, String result, OffsetDateTime completedAt, long fenceToken) {
        int rows = jdbc.update("""
                UPDATE job_tasks
                SET status = 'COMPLETED', completed_at = ?, result = ?::jsonb,
                    last_error_message = NULL, last_error_class = NULL, updated_at = NOW()
                WHERE id = ? AND fence_token = ?
                """, toTimestamp(completedAt), result, taskId, fenceToken);
        return rows > 0;
    }

    public boolean markFailed(UUID taskId, int attemptCount, OffsetDateTime lastAttemptTime,
                           OffsetDateTime nextAttemptTime, String errorMessage, String errorClass,
                           long fenceToken) {
        int rows = jdbc.update("""
                UPDATE job_tasks
                SET status = 'FAILED', attempt_count = ?, last_attempt_time = ?,
                    next_attempt_time = ?, last_error_message = ?, last_error_class = ?,
                    updated_at = NOW()
                WHERE id = ? AND fence_token = ?
                """,
                attemptCount, toTimestamp(lastAttemptTime),
                toTimestamp(nextAttemptTime), errorMessage, errorClass, taskId, fenceToken);
        return rows > 0;
    }

    public boolean markDeadLetter(UUID taskId, int attemptCount, OffsetDateTime lastAttemptTime,
                               String errorMessage, String errorClass, long fenceToken) {
        int rows = jdbc.update("""
                UPDATE job_tasks
                SET status = 'DEAD_LETTER', attempt_count = ?, last_attempt_time = ?,
                    next_attempt_time = NULL, last_error_message = ?, last_error_class = ?,
                    updated_at = NOW()
                WHERE id = ? AND fence_token = ?
                """,
                attemptCount, toTimestamp(lastAttemptTime), errorMessage, errorClass, taskId, fenceToken);
        return rows > 0;
    }

    /**
     * Flags all IN_PROGRESS tasks that have breached their deadline.
     *
     * @return number of tasks flagged
     */
    public int flagTimedOutTasks() {
        return jdbc.update("""
                UPDATE job_tasks
                SET timed_out = TRUE, status = 'FAILED',
                    last_error_message = 'Task deadline exceeded',
                    last_error_class = 'com.imohiosen.asyncjob.exception.TaskDeadlineExceededException',
                    updated_at = NOW()
                WHERE deadline_at < NOW()
                  AND timed_out = FALSE
                  AND status = 'IN_PROGRESS'
                """);
    }

    public List<JobTask> findDeadLetterByJobId(UUID jobId) {
        return jdbc.query("""
                SELECT * FROM job_tasks WHERE job_id = ? AND status = 'DEAD_LETTER'
                ORDER BY updated_at DESC
                """, new TaskRowMapper(), jobId);
    }

    private static Timestamp toTimestamp(OffsetDateTime odt) {
        return odt == null ? null : Timestamp.from(odt.toInstant());
    }

    // ── Row Mapper ─────────────────────────────────────────────────────────────

    public static class TaskRowMapper implements RowMapper<JobTask> {
        @Override
        public JobTask mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new JobTask(
                    UUID.fromString(rs.getString("id")),
                    UUID.fromString(rs.getString("job_id")),
                    rs.getString("task_type"),
                    rs.getString("kafka_topic"),
                    (Integer) rs.getObject("kafka_partition"),
                    (Long)    rs.getObject("kafka_offset"),
                    TaskStatus.valueOf(rs.getString("status")),
                    toOdt(rs.getTimestamp("created_at")),
                    toOdt(rs.getTimestamp("updated_at")),
                    toOdt(rs.getTimestamp("started_at")),
                    toOdt(rs.getTimestamp("completed_at")),
                    toOdt(rs.getTimestamp("deadline_at")),
                    rs.getBoolean("timed_out"),
                    rs.getInt("attempt_count"),
                    toOdt(rs.getTimestamp("last_attempt_time")),
                    toOdt(rs.getTimestamp("next_attempt_time")),
                    rs.getLong("base_interval_ms"),
                    rs.getDouble("multiplier"),
                    rs.getLong("max_delay_ms"),
                    toOdt(rs.getTimestamp("async_submitted_at")),
                    toOdt(rs.getTimestamp("async_completed_at")),
                    rs.getString("last_error_message"),
                    rs.getString("last_error_class"),
                    (Long) rs.getObject("fence_token"),
                    rs.getString("payload"),
                    rs.getString("result")
            );
        }

        private static OffsetDateTime toOdt(Timestamp ts) {
            return ts == null ? null : ts.toInstant().atOffset(ZoneOffset.UTC);
        }
    }
}
