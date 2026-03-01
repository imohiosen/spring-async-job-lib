package com.imohiosen.asyncjob.infrastructure.persistence.jdbc;

import com.imohiosen.asyncjob.domain.Job;
import com.imohiosen.asyncjob.domain.JobStatus;
import com.imohiosen.asyncjob.port.repository.JobRepository;

import javax.sql.DataSource;
import java.sql.*;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * JDBC-based implementation of {@link JobRepository} for the {@code jobs} table.
 * Uses plain {@link DataSource} — no Spring, no JPA, no Hibernate.
 */
public class JdbcJobRepository implements JobRepository {

    private final DataSource dataSource;

    public JdbcJobRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void insert(Job job) {
        String sql = """
                INSERT INTO jobs (id, job_name, correlation_id, status, created_at, updated_at,
                                  deadline_at, scheduled_at, stale, metadata,
                                  time_critical)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, job.id().toString());
            ps.setString(2, job.jobName());
            ps.setString(3, job.correlationId());
            ps.setString(4, job.status().name());
            ps.setTimestamp(5, toTimestamp(job.createdAt()));
            ps.setTimestamp(6, toTimestamp(job.updatedAt()));
            ps.setTimestamp(7, toTimestamp(job.deadlineAt()));
            ps.setTimestamp(8, toTimestamp(job.scheduledAt()));
            ps.setBoolean(9, job.stale());
            ps.setString(10, job.metadata());
            ps.setBoolean(11, job.timeCritical());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to insert job " + job.id(), e);
        }
    }

    @Override
    public Optional<Job> findById(UUID id) {
        String sql = "SELECT * FROM jobs WHERE id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id.toString());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRow(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find job " + id, e);
        }
    }

    @Override
    public void updateStatus(UUID id, JobStatus status) {
        String sql = "UPDATE jobs SET status = ?, updated_at = NOW() WHERE id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, status.name());
            ps.setString(2, id.toString());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update status for job " + id, e);
        }
    }

    @Override
    public boolean tryMarkStarted(UUID id) {
        String sql = """
                UPDATE jobs SET status = 'IN_PROGRESS', started_at = NOW(), updated_at = NOW()
                WHERE id = ?
                  AND status IN ('PENDING', 'SCHEDULED')
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id.toString());
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark job started " + id, e);
        }
    }

    @Override
    public void markCompleted(UUID id) {
        String sql = """
                UPDATE jobs SET status = 'COMPLETED', completed_at = NOW(), updated_at = NOW()
                WHERE id = ?
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id.toString());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark job completed " + id, e);
        }
    }

    @Override
    public int flagStaleJobs() {
        String sql = """
                UPDATE jobs SET stale = TRUE, updated_at = NOW()
                WHERE deadline_at < NOW()
                  AND stale = FALSE
                  AND status NOT IN ('SCHEDULED', 'COMPLETED', 'DEAD_LETTER')
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to flag stale jobs", e);
        }
    }

    @Override
    public List<Job> claimScheduledJobsDue(int limit) {
        String sql = """
                SELECT * FROM jobs
                WHERE status = 'SCHEDULED'
                  AND scheduled_at IS NOT NULL
                  AND scheduled_at <= NOW()
                ORDER BY scheduled_at ASC
                LIMIT ?
                FOR UPDATE SKIP LOCKED
                """;
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, limit);
                try (ResultSet rs = ps.executeQuery()) {
                    List<Job> results = new ArrayList<>();
                    while (rs.next()) {
                        results.add(mapRow(rs));
                    }
                    return results;
                }
            } finally {
                conn.commit();
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to claim scheduled jobs due", e);
        }
    }

    private static Timestamp toTimestamp(OffsetDateTime odt) {
        return odt == null ? null : Timestamp.from(odt.toInstant());
    }

    // ── Row Mapper ────────────────────────────────────────────────────────────

    static Job mapRow(ResultSet rs) throws SQLException {
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
                toOdt(rs.getTimestamp("scheduled_at")),
                rs.getBoolean("stale"),
                rs.getString("metadata"),
                rs.getBoolean("time_critical")
        );
    }

    private static OffsetDateTime toOdt(Timestamp ts) {
        return ts == null ? null : ts.toInstant().atOffset(ZoneOffset.UTC);
    }
}
