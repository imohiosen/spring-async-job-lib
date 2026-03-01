package com.imohiosen.asyncjob.test;

import com.imohiosen.asyncjob.domain.Job;
import com.imohiosen.asyncjob.domain.JobStatus;
import com.imohiosen.asyncjob.repository.JobRepository;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory stub for {@link JobRepository} for use in unit tests.
 * No database required.
 */
public class InMemoryJobRepository extends JobRepository {

    private final Map<UUID, Job> store = new ConcurrentHashMap<>();
    private int timedOutJobsCount = 0;

    public InMemoryJobRepository() {
        super(null);
    }

    @Override
    public void insert(Job job) {
        store.put(job.id(), job);
    }

    @Override
    public Optional<Job> findById(UUID id) {
        return Optional.ofNullable(store.get(id));
    }

    @Override
    public void updateStatus(UUID id, JobStatus status) {
        store.computeIfPresent(id, (k, j) -> new Job(
                j.id(), j.jobName(), j.correlationId(), status,
                j.createdAt(), j.updatedAt(), j.startedAt(), j.completedAt(),
                j.deadlineAt(), j.timedOut(), j.totalTasks(), j.pendingTasks(),
                j.inProgressTasks(), j.completedTasks(), j.failedTasks(),
                j.deadLetterTasks(), j.metadata()
        ));
    }

    @Override
    public void updateCounters(UUID jobId) {
        // no-op in test stub
    }

    @Override
    public void markStarted(UUID id) {
        updateStatus(id, JobStatus.IN_PROGRESS);
    }

    @Override
    public int flagTimedOutJobs() {
        return timedOutJobsCount;
    }

    // ── Test helpers ─────────────────────────────────────────────────────────

    public void setTimedOutJobsCount(int count) {
        this.timedOutJobsCount = count;
    }

    public Collection<Job> all() {
        return Collections.unmodifiableCollection(store.values());
    }

    public void clear() {
        store.clear();
    }
}
