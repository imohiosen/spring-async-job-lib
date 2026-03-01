package com.imohiosen.asyncjob.test;

import com.imohiosen.asyncjob.domain.Job;
import com.imohiosen.asyncjob.domain.JobStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryJobRepositoryTest {

    InMemoryJobRepository repository;

    @BeforeEach
    void setUp() {
        repository = new InMemoryJobRepository();
    }

    // ── insert / findById ────────────────────────────────────────────────────

    @Test
    void insert_and_findById_returnsInsertedJob() {
        Job job = job(UUID.randomUUID(), JobStatus.PENDING);

        repository.insert(job);

        Optional<Job> found = repository.findById(job.id());
        assertThat(found).isPresent();
        assertThat(found.get()).isEqualTo(job);
    }

    @Test
    void findById_unknownId_returnsEmpty() {
        assertThat(repository.findById(UUID.randomUUID())).isEmpty();
    }

    // ── updateStatus ────────────────────────────────────────────────────────

    @Test
    void updateStatus_changesJobStatus() {
        Job job = job(UUID.randomUUID(), JobStatus.PENDING);
        repository.insert(job);

        repository.updateStatus(job.id(), JobStatus.IN_PROGRESS);

        Job updated = repository.findById(job.id()).orElseThrow();
        assertThat(updated.status()).isEqualTo(JobStatus.IN_PROGRESS);
    }

    @Test
    void updateStatus_preservesOtherFields() {
        Job job = job(UUID.randomUUID(), JobStatus.PENDING);
        repository.insert(job);

        repository.updateStatus(job.id(), JobStatus.COMPLETED);

        Job updated = repository.findById(job.id()).orElseThrow();
        assertThat(updated.jobName()).isEqualTo(job.jobName());
        assertThat(updated.totalTasks()).isEqualTo(job.totalTasks());
        assertThat(updated.correlationId()).isEqualTo(job.correlationId());
    }

    @Test
    void updateStatus_unknownId_noOp() {
        // Should not throw
        repository.updateStatus(UUID.randomUUID(), JobStatus.COMPLETED);
    }

    // ── tryMarkStarted / markCompleted ────────────────────────────────────────

    @Test
    void tryMarkStarted_pendingJob_returnsTrue_setsStatusToInProgress() {
        Job job = job(UUID.randomUUID(), JobStatus.PENDING);
        repository.insert(job);

        boolean result = repository.tryMarkStarted(job.id());

        assertThat(result).isTrue();
        assertThat(repository.findById(job.id()).orElseThrow().status())
                .isEqualTo(JobStatus.IN_PROGRESS);
    }

    @Test
    void tryMarkStarted_scheduledJob_returnsTrue_setsStatusToInProgress() {
        Job job = job(UUID.randomUUID(), JobStatus.SCHEDULED);
        repository.insert(job);

        boolean result = repository.tryMarkStarted(job.id());

        assertThat(result).isTrue();
        assertThat(repository.findById(job.id()).orElseThrow().status())
                .isEqualTo(JobStatus.IN_PROGRESS);
    }

    @Test
    void tryMarkStarted_alreadyInProgress_returnsFalse() {
        Job job = job(UUID.randomUUID(), JobStatus.IN_PROGRESS);
        repository.insert(job);

        boolean result = repository.tryMarkStarted(job.id());

        assertThat(result).isFalse();
        assertThat(repository.findById(job.id()).orElseThrow().status())
                .isEqualTo(JobStatus.IN_PROGRESS);
    }

    @Test
    void markCompleted_setsStatusToCompleted() {
        Job job = job(UUID.randomUUID(), JobStatus.IN_PROGRESS);
        repository.insert(job);

        repository.markCompleted(job.id());

        assertThat(repository.findById(job.id()).orElseThrow().status())
                .isEqualTo(JobStatus.COMPLETED);
    }

    // ── flagStaleJobs ────────────────────────────────────────────────────────

    @Test
    void flagStaleJobs_returnsConfiguredCount() {
        repository.setStaleJobsCount(5);
        assertThat(repository.flagStaleJobs()).isEqualTo(5);
    }

    @Test
    void flagStaleJobs_defaultsToZero() {
        assertThat(repository.flagStaleJobs()).isEqualTo(0);
    }

    // ── claimScheduledJobsDue ────────────────────────────────────────────────

    @Test
    void claimScheduledJobsDue_returnsDueScheduledJobs() {
        OffsetDateTime now = OffsetDateTime.now();
        Job due = scheduledJob(UUID.randomUUID(), now.minusMinutes(5));
        repository.insert(due);

        List<Job> result = repository.claimScheduledJobsDue(10);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).id()).isEqualTo(due.id());
    }

    @Test
    void claimScheduledJobsDue_excludesFutureScheduledJobs() {
        OffsetDateTime now = OffsetDateTime.now();
        Job future = scheduledJob(UUID.randomUUID(), now.plusHours(1));
        repository.insert(future);

        List<Job> result = repository.claimScheduledJobsDue(10);

        assertThat(result).isEmpty();
    }

    @Test
    void claimScheduledJobsDue_excludesNonScheduledStatus() {
        OffsetDateTime now = OffsetDateTime.now();
        Job pending = job(UUID.randomUUID(), JobStatus.PENDING);
        repository.insert(pending);

        List<Job> result = repository.claimScheduledJobsDue(10);

        assertThat(result).isEmpty();
    }

    @Test
    void claimScheduledJobsDue_respectsLimit() {
        OffsetDateTime now = OffsetDateTime.now();
        for (int i = 0; i < 5; i++) {
            repository.insert(scheduledJob(UUID.randomUUID(), now.minusMinutes(i + 1)));
        }

        List<Job> result = repository.claimScheduledJobsDue(3);

        assertThat(result).hasSize(3);
    }

    @Test
    void claimScheduledJobsDue_orderedByScheduledAt() {
        OffsetDateTime now = OffsetDateTime.now();
        Job earlier = scheduledJob(UUID.randomUUID(), now.minusMinutes(10));
        Job later = scheduledJob(UUID.randomUUID(), now.minusMinutes(1));
        repository.insert(later);
        repository.insert(earlier);

        List<Job> result = repository.claimScheduledJobsDue(10);

        assertThat(result).hasSize(2);
        assertThat(result.get(0).id()).isEqualTo(earlier.id());
        assertThat(result.get(1).id()).isEqualTo(later.id());
    }

    // ── all / clear ──────────────────────────────────────────────────────────

    @Test
    void all_returnsAllInsertedJobs() {
        Job j1 = job(UUID.randomUUID(), JobStatus.PENDING);
        Job j2 = job(UUID.randomUUID(), JobStatus.COMPLETED);
        repository.insert(j1);
        repository.insert(j2);

        assertThat(repository.all()).hasSize(2);
    }

    @Test
    void clear_removesAllJobs() {
        repository.insert(job(UUID.randomUUID(), JobStatus.PENDING));
        repository.insert(job(UUID.randomUUID(), JobStatus.PENDING));

        repository.clear();

        assertThat(repository.all()).isEmpty();
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static Job job(UUID id, JobStatus status) {
        OffsetDateTime now = OffsetDateTime.now();
        return new Job(id, "test-job", null, status,
                now, now, null, null, now.plusHours(1), null,
                false, 2, 2, 0, 0, 0, 0, null, false);
    }

    private static Job scheduledJob(UUID id, OffsetDateTime scheduledAt) {
        OffsetDateTime now = OffsetDateTime.now();
        return new Job(id, "scheduled-job", null, JobStatus.SCHEDULED,
                now, now, null, null, now.plusHours(1), scheduledAt,
                false, 2, 2, 0, 0, 0, 0, null, false);
    }
}
