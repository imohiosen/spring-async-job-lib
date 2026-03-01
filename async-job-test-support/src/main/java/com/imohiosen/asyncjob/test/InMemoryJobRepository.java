package com.imohiosen.asyncjob.test;

import com.imohiosen.asyncjob.domain.Job;
import com.imohiosen.asyncjob.domain.JobStatus;
import com.imohiosen.asyncjob.port.repository.JobRepository;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * In-memory implementation of {@link JobRepository} for unit tests.
 * No database required.
 */
public class InMemoryJobRepository implements JobRepository {

    private final Map<UUID, Job> store = new ConcurrentHashMap<>();
    private int staleJobsCount = 0;

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
                j.deadlineAt(), j.scheduledAt(), j.stale(), j.totalTasks(), j.pendingTasks(),
                j.inProgressTasks(), j.completedTasks(), j.failedTasks(),
                j.deadLetterTasks(), j.metadata()
        ));
    }

    @Override
    public void markStarted(UUID id) {
        updateStatus(id, JobStatus.IN_PROGRESS);
    }

    @Override
    public void markCompleted(UUID id) {
        updateStatus(id, JobStatus.COMPLETED);
    }

    @Override
    public void updateCounters(UUID jobId) {
        // no-op in test stub
    }

    @Override
    public int flagStaleJobs() {
        return staleJobsCount;
    }

    @Override
    public List<Job> findScheduledJobsDue(int limit) {
        OffsetDateTime now = OffsetDateTime.now();
        return store.values().stream()
                .filter(j -> j.status() == JobStatus.SCHEDULED)
                .filter(j -> j.scheduledAt() != null && !j.scheduledAt().isAfter(now))
                .sorted(Comparator.comparing(Job::scheduledAt))
                .limit(limit)
                .collect(Collectors.toList());
    }

    // ── Test helpers ─────────────────────────────────────────────────────────

    public void setStaleJobsCount(int count) {
        this.staleJobsCount = count;
    }

    public Collection<Job> all() {
        return Collections.unmodifiableCollection(store.values());
    }

    public void clear() {
        store.clear();
    }
}
