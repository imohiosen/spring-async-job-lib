package com.imohiosen.asyncjob.infrastructure.persistence.jdbc;

import com.imohiosen.asyncjob.domain.Job;
import com.imohiosen.asyncjob.domain.JobStatus;
import com.imohiosen.asyncjob.port.repository.JobRepository;
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
 * JDBC-based implementation of {@link JobRepository} for the {@code jobs} table.
 * Uses {@link JdbcTemplate} — no JPA, no Hibernate.
 */
public class JdbcJobRepository implements JobRepository {

    private final JdbcTemplate jdbc;

    public JdbcJobRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void insert(Job job) {
        jdbc.update("""
                INSERT INTO jobs (id, job_name, correlation_id, status, created_at, updated_at,
                                  deadline_at, stale, total_tasks, pending_tasks,
                                  in_progress_tasks, completed_tasks, failed_tasks, dead_letter_tasks, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
                """,
                job.id().toString(), job.jobName(), job.correlationId(), job.status().name(),
                toTimestamp(job.createdAt()), toTimestamp(job.updatedAt()),
                toTimestamp(job.deadlineAt()), job.stale(),
                job.totalTasks(), job.pendingTasks(), job.inProgressTasks(),
                job.completedTasks(), job.failedTasks(), job.deadLetterTasks(),
                job.metadata());
    }

    @Override
    public Optional<Job> findById(UUID id) {
        List<Job> results = jdbc.query(
                "SELECT * FROM jobs WHERE id = ?", new JobRowMapper(), id.toString());
        return results.stream().findFirst();
    }

    @Override
    public void updateStatus(UUID id, JobStatus status) {
        jdbc.update("UPDATE jobs SET status = ?, updated_at = NOW() WHERE id = ?",
                status.name(), id.toString());
    }

    @Override
    public void markStarted(UUID id) {
        jdbc.update("""
                UPDATE jobs SET status = 'IN_PROGRESS', started_at = NOW(), updated_at = NOW()
                WHERE id = ?
                """, id.toString());
    }

    @Override
    public void markCompleted(UUID id) {
        jdbc.update("""
                UPDATE jobs SET status = 'COMPLETED', completed_at = NOW(), updated_at = NOW()
                WHERE id = ?
                """, id.toString());
    }

    @Override
    public void updateCounters(UUID jobId) {
        jdbc.update("""
                UPDATE jobs j SET
                    pending_tasks     = (SELECT COUNT(*) FROM job_tasks WHERE job_id = j.id AND status = 'PENDING'),
                    in_progress_tasks = (SELECT COUNT(*) FROM job_tasks WHERE job_id = j.id AND status = 'IN_PROGRESS'),
                    completed_tasks   = (SELECT COUNT(*) FROM job_tasks WHERE job_id = j.id AND status = 'COMPLETED'),
                    failed_tasks      = (SELECT COUNT(*) FROM job_tasks WHERE job_id = j.id AND status = 'FAILED'),
                    dead_letter_tasks = (SELECT COUNT(*) FROM job_tasks WHERE job_id = j.id AND status = 'DEAD_LETTER'),
                    updated_at        = NOW()
                WHERE id = ?
                """, jobId.toString());
    }

    @Override
    public int flagStaleJobs() {
        return jdbc.update("""
                UPDATE jobs SET stale = TRUE, updated_at = NOW()
                WHERE deadline_at < NOW()
                  AND stale = FALSE
                  AND status NOT IN ('COMPLETED', 'DEAD_LETTER')
                """);
    }

    private static Timestamp toTimestamp(OffsetDateTime odt) {
        return odt == null ? null : Timestamp.from(odt.toInstant());
    }

    // ── Row Mapper ────────────────────────────────────────────────────────────

    public static class JobRowMapper implements RowMapper<Job> {
        @Override
        public Job mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new Job(
                    UUID.fromString(rs.getString("id")),
                    rs.getString("job_name"),
                    rs.getString("correlation_id"),
                    JobStatus.valueOf(rs.getString("status")),
                    toOdt(rs.getTimestamp("created_at")),
                    toOdt(rs.getTimestamp("updated_at")),
                    toOdt(rs.getTimestamp("started_at")),
                    toOdt(rs.getTimestamp("completed_at")),
                    toOdt(rs.getTimestamp("deadline_at")),
                    rs.getBoolean("stale"),
                    rs.getInt("total_tasks"),
                    rs.getInt("pending_tasks"),
                    rs.getInt("in_progress_tasks"),
                    rs.getInt("completed_tasks"),
                    rs.getInt("failed_tasks"),
                    rs.getInt("dead_letter_tasks"),
                    rs.getString("metadata")
            );
        }

        private static OffsetDateTime toOdt(Timestamp ts) {
            return ts == null ? null : ts.toInstant().atOffset(ZoneOffset.UTC);
        }
    }
}
