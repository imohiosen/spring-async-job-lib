package com.imohiosen.asyncjob.test;

import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryTaskRepositoryTest {

    InMemoryTaskRepository repository;

    @BeforeEach
    void setUp() {
        repository = new InMemoryTaskRepository();
    }

    // ── insert / findById ────────────────────────────────────────────────────

    @Test
    void insert_and_findById_returnsInsertedTask() {
        JobTask task = pendingTask(UUID.randomUUID(), UUID.randomUUID());

        repository.insert(task);

        Optional<JobTask> found = repository.findById(task.id());
        assertThat(found).isPresent();
        assertThat(found.get()).isEqualTo(task);
    }

    @Test
    void findById_unknownId_returnsEmpty() {
        assertThat(repository.findById(UUID.randomUUID())).isEmpty();
    }

    // ── findEligible ────────────────────────────────────────────────────────

    @Test
    void findEligible_pendingTask_returnsTask() {
        JobTask task = pendingTask(UUID.randomUUID(), UUID.randomUUID());
        repository.insert(task);

        Optional<JobTask> found = repository.findEligible(task.id());
        assertThat(found).isPresent();
    }

    @Test
    void findEligible_inProgressTask_returnsEmpty() {
        JobTask task = taskWithStatus(UUID.randomUUID(), UUID.randomUUID(), TaskStatus.IN_PROGRESS);
        repository.insert(task);

        assertThat(repository.findEligible(task.id())).isEmpty();
    }

    @Test
    void findEligible_completedTask_returnsEmpty() {
        JobTask task = taskWithStatus(UUID.randomUUID(), UUID.randomUUID(), TaskStatus.COMPLETED);
        repository.insert(task);

        assertThat(repository.findEligible(task.id())).isEmpty();
    }

    @Test
    void findEligible_failedTaskWithPastNextAttempt_returnsTask() {
        OffsetDateTime now = OffsetDateTime.now();
        JobTask task = failedTaskWithNextAttempt(UUID.randomUUID(), UUID.randomUUID(), now.minusMinutes(1));
        repository.insert(task);

        assertThat(repository.findEligible(task.id())).isPresent();
    }

    @Test
    void findEligible_failedTaskWithFutureNextAttempt_returnsEmpty() {
        OffsetDateTime now = OffsetDateTime.now();
        JobTask task = failedTaskWithNextAttempt(UUID.randomUUID(), UUID.randomUUID(), now.plusMinutes(10));
        repository.insert(task);

        assertThat(repository.findEligible(task.id())).isEmpty();
    }

    // ── markInProgress ──────────────────────────────────────────────────────

    @Test
    void markInProgress_updatesStatusAndFenceToken() {
        UUID taskId = UUID.randomUUID();
        JobTask task = pendingTask(taskId, UUID.randomUUID());
        repository.insert(task);

        OffsetDateTime startedAt = OffsetDateTime.now();
        repository.markInProgress(taskId, startedAt, 42L);

        JobTask updated = repository.findById(taskId).orElseThrow();
        assertThat(updated.status()).isEqualTo(TaskStatus.IN_PROGRESS);
        assertThat(updated.fenceToken()).isEqualTo(42L);
        assertThat(updated.startedAt()).isEqualTo(startedAt);
        assertThat(updated.attemptCount()).isEqualTo(task.attemptCount() + 1);
    }

    // ── markCompleted ───────────────────────────────────────────────────────

    @Test
    void markCompleted_withMatchingFenceToken_succeeds() {
        UUID taskId = UUID.randomUUID();
        JobTask task = pendingTask(taskId, UUID.randomUUID());
        repository.insert(task);
        repository.markInProgress(taskId, OffsetDateTime.now(), 1L);

        boolean result = repository.markCompleted(taskId, "ok", OffsetDateTime.now(), 1L);

        assertThat(result).isTrue();
        JobTask updated = repository.findById(taskId).orElseThrow();
        assertThat(updated.status()).isEqualTo(TaskStatus.COMPLETED);
        assertThat(updated.result()).isEqualTo("ok");
    }

    @Test
    void markCompleted_withMismatchedFenceToken_returnsFalse() {
        UUID taskId = UUID.randomUUID();
        JobTask task = pendingTask(taskId, UUID.randomUUID());
        repository.insert(task);
        repository.markInProgress(taskId, OffsetDateTime.now(), 1L);

        boolean result = repository.markCompleted(taskId, "ok", OffsetDateTime.now(), 999L);

        assertThat(result).isFalse();
        // Status should not have changed
        assertThat(repository.findById(taskId).orElseThrow().status()).isEqualTo(TaskStatus.IN_PROGRESS);
    }

    @Test
    void markCompleted_unknownTaskId_returnsFalse() {
        boolean result = repository.markCompleted(UUID.randomUUID(), "ok", OffsetDateTime.now(), 1L);
        assertThat(result).isFalse();
    }

    // ── markFailed ──────────────────────────────────────────────────────────

    @Test
    void markFailed_withMatchingFenceToken_succeeds() {
        UUID taskId = UUID.randomUUID();
        repository.insert(pendingTask(taskId, UUID.randomUUID()));
        repository.markInProgress(taskId, OffsetDateTime.now(), 1L);

        OffsetDateTime now = OffsetDateTime.now();
        boolean result = repository.markFailed(taskId, 2, now, now.plusMinutes(5),
                "error msg", "RuntimeException", 1L);

        assertThat(result).isTrue();
        JobTask updated = repository.findById(taskId).orElseThrow();
        assertThat(updated.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(updated.attemptCount()).isEqualTo(2);
        assertThat(updated.lastErrorMessage()).isEqualTo("error msg");
        assertThat(updated.lastErrorClass()).isEqualTo("RuntimeException");
        assertThat(updated.nextAttemptTime()).isEqualTo(now.plusMinutes(5));
    }

    @Test
    void markFailed_withMismatchedFenceToken_returnsFalse() {
        UUID taskId = UUID.randomUUID();
        repository.insert(pendingTask(taskId, UUID.randomUUID()));
        repository.markInProgress(taskId, OffsetDateTime.now(), 1L);

        boolean result = repository.markFailed(taskId, 2, OffsetDateTime.now(),
                OffsetDateTime.now().plusMinutes(5), "err", "Ex", 999L);

        assertThat(result).isFalse();
    }

    // ── markDeadLetter ──────────────────────────────────────────────────────

    @Test
    void markDeadLetter_withMatchingFenceToken_succeeds() {
        UUID taskId = UUID.randomUUID();
        repository.insert(pendingTask(taskId, UUID.randomUUID()));
        repository.markInProgress(taskId, OffsetDateTime.now(), 1L);

        boolean result = repository.markDeadLetter(taskId, 5, OffsetDateTime.now(),
                "final error", "FatalException", 1L);

        assertThat(result).isTrue();
        JobTask updated = repository.findById(taskId).orElseThrow();
        assertThat(updated.status()).isEqualTo(TaskStatus.DEAD_LETTER);
        assertThat(updated.attemptCount()).isEqualTo(5);
        assertThat(updated.lastErrorMessage()).isEqualTo("final error");
        assertThat(updated.nextAttemptTime()).isNull();
    }

    @Test
    void markDeadLetter_withMismatchedFenceToken_returnsFalse() {
        UUID taskId = UUID.randomUUID();
        repository.insert(pendingTask(taskId, UUID.randomUUID()));
        repository.markInProgress(taskId, OffsetDateTime.now(), 1L);

        boolean result = repository.markDeadLetter(taskId, 5, OffsetDateTime.now(),
                "err", "Ex", 999L);

        assertThat(result).isFalse();
    }

    // ── recordAsyncSubmitted / recordAsyncCompleted ──────────────────────────

    @Test
    void recordAsyncSubmitted_setsTimestamp() {
        UUID taskId = UUID.randomUUID();
        repository.insert(pendingTask(taskId, UUID.randomUUID()));

        OffsetDateTime submittedAt = OffsetDateTime.now();
        repository.recordAsyncSubmitted(taskId, submittedAt, 1L);

        JobTask updated = repository.findById(taskId).orElseThrow();
        assertThat(updated.asyncSubmittedAt()).isEqualTo(submittedAt);
    }

    @Test
    void recordAsyncCompleted_setsTimestamp() {
        UUID taskId = UUID.randomUUID();
        repository.insert(pendingTask(taskId, UUID.randomUUID()));

        OffsetDateTime completedAt = OffsetDateTime.now();
        repository.recordAsyncCompleted(taskId, completedAt, 1L);

        JobTask updated = repository.findById(taskId).orElseThrow();
        assertThat(updated.asyncCompletedAt()).isEqualTo(completedAt);
    }

    // ── flagStaleTasks ──────────────────────────────────────────────────────

    @Test
    void flagStaleTasks_returnsConfiguredCount() {
        repository.setStaleTasksCount(3);
        assertThat(repository.flagStaleTasks()).isEqualTo(3);
    }

    @Test
    void flagStaleTasks_defaultsToZero() {
        assertThat(repository.flagStaleTasks()).isEqualTo(0);
    }

    // ── claimRetryableTasks ─────────────────────────────────────────────────

    @Test
    void claimRetryableTasks_returnsFailedTasksWithPastNextAttemptTime() {
        UUID jobId = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now();

        JobTask retryable = failedTaskWithNextAttempt(UUID.randomUUID(), jobId, now.minusMinutes(1));
        JobTask notYet = failedTaskWithNextAttempt(UUID.randomUUID(), jobId, now.plusMinutes(10));
        JobTask completed = taskWithStatus(UUID.randomUUID(), jobId, TaskStatus.COMPLETED);

        repository.insert(retryable);
        repository.insert(notYet);
        repository.insert(completed);

        List<JobTask> result = repository.claimRetryableTasks(10);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).id()).isEqualTo(retryable.id());
    }

    @Test
    void claimRetryableTasks_orderedByAttemptCountAsc() {
        UUID jobId = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now();

        JobTask highAttempts = failedTaskWithAttempts(UUID.randomUUID(), jobId, now.minusMinutes(1), 5);
        JobTask lowAttempts = failedTaskWithAttempts(UUID.randomUUID(), jobId, now.minusMinutes(1), 1);

        repository.insert(highAttempts);
        repository.insert(lowAttempts);

        List<JobTask> result = repository.claimRetryableTasks(10);

        assertThat(result).hasSize(2);
        assertThat(result.get(0).attemptCount()).isLessThanOrEqualTo(result.get(1).attemptCount());
    }

    @Test
    void claimRetryableTasks_respectsLimit() {
        UUID jobId = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now();

        for (int i = 0; i < 5; i++) {
            repository.insert(failedTaskWithNextAttempt(UUID.randomUUID(), jobId, now.minusMinutes(i + 1)));
        }

        List<JobTask> result = repository.claimRetryableTasks(3);

        assertThat(result).hasSize(3);
    }

    @Test
    void claimRetryableTasks_emptyWhenNoRetryable() {
        assertThat(repository.claimRetryableTasks(10)).isEmpty();
    }

    // ── findDeadLetterByJobId ───────────────────────────────────────────────

    @Test
    void findDeadLetterByJobId_returnsOnlyDeadLetterTasksForJob() {
        UUID jobId = UUID.randomUUID();
        UUID otherJobId = UUID.randomUUID();

        JobTask dl = taskWithStatus(UUID.randomUUID(), jobId, TaskStatus.DEAD_LETTER);
        JobTask completed = taskWithStatus(UUID.randomUUID(), jobId, TaskStatus.COMPLETED);
        JobTask otherDl = taskWithStatus(UUID.randomUUID(), otherJobId, TaskStatus.DEAD_LETTER);

        repository.insert(dl);
        repository.insert(completed);
        repository.insert(otherDl);

        List<JobTask> result = repository.findDeadLetterByJobId(jobId);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).id()).isEqualTo(dl.id());
    }

    @Test
    void findDeadLetterByJobId_emptyWhenNone() {
        assertThat(repository.findDeadLetterByJobId(UUID.randomUUID())).isEmpty();
    }

    // ── findTasksByJobId ────────────────────────────────────────────────────

    @Test
    void findTasksByJobId_returnsAllTasksForJob() {
        UUID jobId = UUID.randomUUID();
        UUID otherId = UUID.randomUUID();

        JobTask t1 = pendingTask(UUID.randomUUID(), jobId);
        JobTask t2 = taskWithStatus(UUID.randomUUID(), jobId, TaskStatus.COMPLETED);
        JobTask other = pendingTask(UUID.randomUUID(), otherId);

        repository.insert(t1);
        repository.insert(t2);
        repository.insert(other);

        List<JobTask> result = repository.findTasksByJobId(jobId);

        assertThat(result).hasSize(2);
        assertThat(result).extracting(JobTask::jobId).containsOnly(jobId);
    }

    @Test
    void findTasksByJobId_emptyWhenNone() {
        assertThat(repository.findTasksByJobId(UUID.randomUUID())).isEmpty();
    }

    // ── all / clear ─────────────────────────────────────────────────────────

    @Test
    void all_returnsAllInserted() {
        repository.insert(pendingTask(UUID.randomUUID(), UUID.randomUUID()));
        repository.insert(pendingTask(UUID.randomUUID(), UUID.randomUUID()));

        assertThat(repository.all()).hasSize(2);
    }

    @Test
    void clear_removesAll() {
        repository.insert(pendingTask(UUID.randomUUID(), UUID.randomUUID()));
        repository.clear();

        assertThat(repository.all()).isEmpty();
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static JobTask pendingTask(UUID taskId, UUID jobId) {
        OffsetDateTime now = OffsetDateTime.now();
        return new JobTask(
                taskId, jobId, "TEST_TYPE", "test-topic",
                null, null, TaskStatus.PENDING,
                now, now, null, null,
                now.plusHours(1), false,
                0, null, null,
                1000L, 2.0, 3_600_000L,
                null, null,
                null, null,
                null, "{\"data\":1}", null,
                false, 0, 0L, 1.0, 0L, 0L
        );
    }

    private static JobTask taskWithStatus(UUID taskId, UUID jobId, TaskStatus status) {
        OffsetDateTime now = OffsetDateTime.now();
        return new JobTask(
                taskId, jobId, "TEST_TYPE", "test-topic",
                null, null, status,
                now, now, null, null,
                now.plusHours(1), false,
                0, null, null,
                1000L, 2.0, 3_600_000L,
                null, null,
                null, null,
                null, "{\"data\":1}", null,
                false, 0, 0L, 1.0, 0L, 0L
        );
    }

    private static JobTask failedTaskWithNextAttempt(UUID taskId, UUID jobId, OffsetDateTime nextAttemptTime) {
        OffsetDateTime now = OffsetDateTime.now();
        return new JobTask(
                taskId, jobId, "TEST_TYPE", "test-topic",
                null, null, TaskStatus.FAILED,
                now, now, now, null,
                now.plusHours(1), false,
                1, now.minusMinutes(5), nextAttemptTime,
                1000L, 2.0, 3_600_000L,
                null, null,
                "error", "RuntimeException",
                null, "{\"data\":1}", null,
                false, 0, 0L, 1.0, 0L, 0L
        );
    }

    private static JobTask failedTaskWithAttempts(UUID taskId, UUID jobId,
                                                   OffsetDateTime nextAttemptTime, int attemptCount) {
        OffsetDateTime now = OffsetDateTime.now();
        return new JobTask(
                taskId, jobId, "TEST_TYPE", "test-topic",
                null, null, TaskStatus.FAILED,
                now, now, now, null,
                now.plusHours(1), false,
                attemptCount, now.minusMinutes(5), nextAttemptTime,
                1000L, 2.0, 3_600_000L,
                null, null,
                "error", "RuntimeException",
                null, "{\"data\":1}", null,
                false, 0, 0L, 1.0, 0L, 0L
        );
    }
}
