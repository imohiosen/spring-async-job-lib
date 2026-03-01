package com.imohiosen.asyncjob.infrastructure.persistence.jdbc;

import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskStatus;
import com.imohiosen.asyncjob.port.repository.TaskRepository;

import javax.sql.DataSource;
import java.sql.*;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * JDBC-based implementation of {@link TaskRepository} for the {@code job_tasks} table.
 * Uses plain {@link DataSource} — no Spring, no JPA, no Hibernate.
 */
public class JdbcTaskRepository implements TaskRepository {

    private final DataSource dataSource;

    public JdbcTaskRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void insert(JobTask task) {
        String sql = """
                INSERT INTO job_tasks (id, job_id, task_type, destination, status,
                    created_at, updated_at, deadline_at, stale,
                    attempt_count, base_interval_ms, multiplier, max_delay_ms, payload,
                    time_critical, tc_max_attempts, tc_base_interval_ms, tc_multiplier,
                    tc_max_delay_ms, tc_db_sync_interval_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?)
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, task.id().toString());
            ps.setString(2, task.jobId().toString());
            ps.setString(3, task.taskType());
            ps.setString(4, task.destination());
            ps.setString(5, task.status().name());
            ps.setTimestamp(6, toTimestamp(task.createdAt()));
            ps.setTimestamp(7, toTimestamp(task.updatedAt()));
            ps.setTimestamp(8, toTimestamp(task.deadlineAt()));
            ps.setBoolean(9, task.stale());
            ps.setInt(10, task.attemptCount());
            ps.setLong(11, task.baseIntervalMs());
            ps.setDouble(12, task.multiplier());
            ps.setLong(13, task.maxDelayMs());
            ps.setString(14, task.payload());
            ps.setBoolean(15, task.timeCritical());
            if (task.timeCritical()) {
                ps.setInt(16, task.tcMaxAttempts());
                ps.setLong(17, task.tcBaseIntervalMs());
                ps.setDouble(18, task.tcMultiplier());
                ps.setLong(19, task.tcMaxDelayMs());
                ps.setLong(20, task.tcDbSyncIntervalMs());
            } else {
                ps.setNull(16, Types.INTEGER);
                ps.setNull(17, Types.BIGINT);
                ps.setNull(18, Types.NUMERIC);
                ps.setNull(19, Types.BIGINT);
                ps.setNull(20, Types.BIGINT);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to insert task " + task.id(), e);
        }
    }

    @Override
    public Optional<JobTask> findEligible(UUID taskId) {
        String sql = """
                SELECT * FROM job_tasks
                WHERE id = ?
                  AND status IN ('PENDING', 'FAILED')
                  AND (next_attempt_time IS NULL OR next_attempt_time <= NOW())
                """;
        return querySingle(sql, taskId);
    }

    @Override
    public Optional<JobTask> findById(UUID taskId) {
        return querySingle("SELECT * FROM job_tasks WHERE id = ?", taskId);
    }

    @Override
    public void markInProgress(UUID taskId, OffsetDateTime startedAt, long fenceToken) {
        String sql = """
                UPDATE job_tasks
                SET status = 'IN_PROGRESS', started_at = ?, attempt_count = attempt_count + 1,
                    last_attempt_time = NOW(), fence_token = ?, updated_at = NOW()
                WHERE id = ?
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setTimestamp(1, toTimestamp(startedAt));
            ps.setLong(2, fenceToken);
            ps.setString(3, taskId.toString());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark task in-progress " + taskId, e);
        }
    }

    @Override
    public void recordAsyncSubmitted(UUID taskId, OffsetDateTime submittedAt, long fenceToken) {
        String sql = """
                UPDATE job_tasks SET async_submitted_at = ?, updated_at = NOW()
                WHERE id = ? AND fence_token = ?
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setTimestamp(1, toTimestamp(submittedAt));
            ps.setString(2, taskId.toString());
            ps.setLong(3, fenceToken);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to record async submitted " + taskId, e);
        }
    }

    @Override
    public void recordAsyncCompleted(UUID taskId, OffsetDateTime completedAt, long fenceToken) {
        String sql = """
                UPDATE job_tasks SET async_completed_at = ?, updated_at = NOW()
                WHERE id = ? AND fence_token = ?
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setTimestamp(1, toTimestamp(completedAt));
            ps.setString(2, taskId.toString());
            ps.setLong(3, fenceToken);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to record async completed " + taskId, e);
        }
    }

    @Override
    public boolean markCompleted(UUID taskId, String result, OffsetDateTime completedAt, long fenceToken) {
        String sql = """
                UPDATE job_tasks
                SET status = 'COMPLETED', completed_at = ?, result = ?::jsonb,
                    last_error_message = NULL, last_error_class = NULL, updated_at = NOW()
                WHERE id = ? AND fence_token = ?
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setTimestamp(1, toTimestamp(completedAt));
            ps.setString(2, result);
            ps.setString(3, taskId.toString());
            ps.setLong(4, fenceToken);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark task completed " + taskId, e);
        }
    }

    @Override
    public boolean markFailed(UUID taskId, int attemptCount, OffsetDateTime lastAttemptTime,
                           OffsetDateTime nextAttemptTime, String errorMessage, String errorClass,
                           long fenceToken) {
        String sql = """
                UPDATE job_tasks
                SET status = 'FAILED', attempt_count = ?, last_attempt_time = ?,
                    next_attempt_time = ?, last_error_message = ?, last_error_class = ?,
                    updated_at = NOW()
                WHERE id = ? AND fence_token = ?
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, attemptCount);
            ps.setTimestamp(2, toTimestamp(lastAttemptTime));
            ps.setTimestamp(3, toTimestamp(nextAttemptTime));
            ps.setString(4, errorMessage);
            ps.setString(5, errorClass);
            ps.setString(6, taskId.toString());
            ps.setLong(7, fenceToken);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark task failed " + taskId, e);
        }
    }

    @Override
    public boolean markDeadLetter(UUID taskId, int attemptCount, OffsetDateTime lastAttemptTime,
                               String errorMessage, String errorClass, long fenceToken) {
        String sql = """
                UPDATE job_tasks
                SET status = 'DEAD_LETTER', attempt_count = ?, last_attempt_time = ?,
                    next_attempt_time = NULL, last_error_message = ?, last_error_class = ?,
                    updated_at = NOW()
                WHERE id = ? AND fence_token = ?
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, attemptCount);
            ps.setTimestamp(2, toTimestamp(lastAttemptTime));
            ps.setString(3, errorMessage);
            ps.setString(4, errorClass);
            ps.setString(5, taskId.toString());
            ps.setLong(6, fenceToken);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark task dead-lettered " + taskId, e);
        }
    }

    @Override
    public int flagStaleTasks() {
        String sql = """
                UPDATE job_tasks
                SET stale = TRUE, updated_at = NOW()
                WHERE deadline_at < NOW()
                  AND stale = FALSE
                  AND status = 'IN_PROGRESS'
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to flag stale tasks", e);
        }
    }

    @Override
    public List<JobTask> findRetryableTasks(int limit) {
        String sql = """
                SELECT * FROM job_tasks
                WHERE status = 'FAILED'
                  AND next_attempt_time IS NOT NULL
                  AND next_attempt_time <= NOW()
                ORDER BY attempt_count ASC, next_attempt_time ASC
                LIMIT ?
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, limit);
            return queryList(ps);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find retryable tasks", e);
        }
    }

    @Override
    public List<JobTask> findDeadLetterByJobId(UUID jobId) {
        String sql = """
                SELECT * FROM job_tasks WHERE job_id = ? AND status = 'DEAD_LETTER'
                ORDER BY updated_at DESC
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, jobId.toString());
            return queryList(ps);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find dead letter tasks for job " + jobId, e);
        }
    }

    @Override
    public List<JobTask> findTasksByJobId(UUID jobId) {
        String sql = """
                SELECT * FROM job_tasks WHERE job_id = ?
                ORDER BY created_at ASC
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, jobId.toString());
            return queryList(ps);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find tasks for job " + jobId, e);
        }
    }

    @Override
    public boolean persistTimeCriticalProgress(UUID taskId, int attemptCount,
                                                OffsetDateTime lastAttemptTime,
                                                String lastErrorMessage, String lastErrorClass,
                                                long fenceToken) {
        String sql = """
                UPDATE job_tasks
                SET attempt_count = ?, last_attempt_time = ?,
                    last_error_message = ?, last_error_class = ?,
                    updated_at = NOW()
                WHERE id = ? AND fence_token = ?
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, attemptCount);
            ps.setTimestamp(2, toTimestamp(lastAttemptTime));
            ps.setString(3, lastErrorMessage);
            ps.setString(4, lastErrorClass);
            ps.setString(5, taskId.toString());
            ps.setLong(6, fenceToken);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to persist time-critical progress for task " + taskId, e);
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private Optional<JobTask> querySingle(String sql, UUID taskId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskId.toString());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRow(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query task " + taskId, e);
        }
    }

    private static List<JobTask> queryList(PreparedStatement ps) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            List<JobTask> results = new ArrayList<>();
            while (rs.next()) {
                results.add(mapRow(rs));
            }
            return results;
        }
    }

    private static Timestamp toTimestamp(OffsetDateTime odt) {
        return odt == null ? null : Timestamp.from(odt.toInstant());
    }

    // ── Row Mapper ─────────────────────────────────────────────────────────────

    static JobTask mapRow(ResultSet rs) throws SQLException {
        return new JobTask(
                UUID.fromString(rs.getString("id")),
                UUID.fromString(rs.getString("job_id")),
                rs.getString("task_type"),
                rs.getString("destination"),
                (Integer) rs.getObject("partition"),
                (Long)    rs.getObject("offset"),
                TaskStatus.valueOf(rs.getString("status")),
                toOdt(rs.getTimestamp("created_at")),
                toOdt(rs.getTimestamp("updated_at")),
                toOdt(rs.getTimestamp("started_at")),
                toOdt(rs.getTimestamp("completed_at")),
                toOdt(rs.getTimestamp("deadline_at")),
                rs.getBoolean("stale"),
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
                rs.getString("result"),
                rs.getBoolean("time_critical"),
                rs.getObject("tc_max_attempts") != null ? rs.getInt("tc_max_attempts") : 0,
                rs.getObject("tc_base_interval_ms") != null ? rs.getLong("tc_base_interval_ms") : 0L,
                rs.getObject("tc_multiplier") != null ? rs.getDouble("tc_multiplier") : 1.0,
                rs.getObject("tc_max_delay_ms") != null ? rs.getLong("tc_max_delay_ms") : 0L,
                rs.getObject("tc_db_sync_interval_ms") != null ? rs.getLong("tc_db_sync_interval_ms") : 0L
        );
    }

    private static OffsetDateTime toOdt(Timestamp ts) {
        return ts == null ? null : ts.toInstant().atOffset(ZoneOffset.UTC);
    }
}
