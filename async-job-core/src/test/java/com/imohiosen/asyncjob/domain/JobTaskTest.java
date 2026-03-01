package com.imohiosen.asyncjob.domain;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class JobTaskTest {

    // ── isEligible ────────────────────────────────────────────────────────────

    @Test
    void isEligible_pendingWithNullNextAttempt_returnsTrue() {
        JobTask task = task(TaskStatus.PENDING, null);
        assertThat(task.isEligible()).isTrue();
    }

    @Test
    void isEligible_failedWithPastNextAttempt_returnsTrue() {
        JobTask task = task(TaskStatus.FAILED, OffsetDateTime.now().minusMinutes(5));
        assertThat(task.isEligible()).isTrue();
    }

    @Test
    void isEligible_failedWithNullNextAttempt_returnsTrue() {
        JobTask task = task(TaskStatus.FAILED, null);
        assertThat(task.isEligible()).isTrue();
    }

    @Test
    void isEligible_failedWithFutureNextAttempt_returnsFalse() {
        JobTask task = task(TaskStatus.FAILED, OffsetDateTime.now().plusHours(1));
        assertThat(task.isEligible()).isFalse();
    }

    @Test
    void isEligible_pendingWithFutureNextAttempt_returnsFalse() {
        JobTask task = task(TaskStatus.PENDING, OffsetDateTime.now().plusHours(1));
        assertThat(task.isEligible()).isFalse();
    }

    @Test
    void isEligible_inProgressStatus_returnsFalse() {
        JobTask task = task(TaskStatus.IN_PROGRESS, null);
        assertThat(task.isEligible()).isFalse();
    }

    @Test
    void isEligible_completedStatus_returnsFalse() {
        JobTask task = task(TaskStatus.COMPLETED, null);
        assertThat(task.isEligible()).isFalse();
    }

    @Test
    void isEligible_deadLetterStatus_returnsFalse() {
        JobTask task = task(TaskStatus.DEAD_LETTER, null);
        assertThat(task.isEligible()).isFalse();
    }

    // ── isDeadlineBreached ────────────────────────────────────────────────────

    @Test
    void isDeadlineBreached_inProgressPastDeadlineNotStale_returnsTrue() {
        JobTask task = taskWithDeadline(TaskStatus.IN_PROGRESS, OffsetDateTime.now().minusMinutes(5), false);
        assertThat(task.isDeadlineBreached()).isTrue();
    }

    @Test
    void isDeadlineBreached_stale_returnsFalse() {
        JobTask task = taskWithDeadline(TaskStatus.IN_PROGRESS, OffsetDateTime.now().minusMinutes(5), true);
        assertThat(task.isDeadlineBreached()).isFalse();
    }

    @Test
    void isDeadlineBreached_nullDeadline_returnsFalse() {
        JobTask task = taskWithDeadline(TaskStatus.IN_PROGRESS, null, false);
        assertThat(task.isDeadlineBreached()).isFalse();
    }

    @Test
    void isDeadlineBreached_futureDeadline_returnsFalse() {
        JobTask task = taskWithDeadline(TaskStatus.IN_PROGRESS, OffsetDateTime.now().plusHours(1), false);
        assertThat(task.isDeadlineBreached()).isFalse();
    }

    @Test
    void isDeadlineBreached_pendingStatus_returnsFalse() {
        JobTask task = taskWithDeadline(TaskStatus.PENDING, OffsetDateTime.now().minusMinutes(5), false);
        assertThat(task.isDeadlineBreached()).isFalse();
    }

    @Test
    void isDeadlineBreached_completedStatus_returnsFalse() {
        JobTask task = taskWithDeadline(TaskStatus.COMPLETED, OffsetDateTime.now().minusMinutes(5), false);
        assertThat(task.isDeadlineBreached()).isFalse();
    }

    @Test
    void isDeadlineBreached_failedStatus_returnsFalse() {
        JobTask task = taskWithDeadline(TaskStatus.FAILED, OffsetDateTime.now().minusMinutes(5), false);
        assertThat(task.isDeadlineBreached()).isFalse();
    }

    // ── backoffPolicy ─────────────────────────────────────────────────────────

    @Test
    void backoffPolicy_wrapsColumnsCorrectly() {
        OffsetDateTime now = OffsetDateTime.now();
        JobTask task = new JobTask(
                UUID.randomUUID(), UUID.randomUUID(), "TEST", "topic",
                null, null, TaskStatus.PENDING, now, now, null, null,
                now.plusHours(1), false, 0, null, null,
                5_000L, 3.0, 300_000L,
                null, null, null, null, null, "{}", null,
                false, 0, 0L, 1.0, 0L, 0L);

        BackoffPolicy policy = task.backoffPolicy();

        assertThat(policy.baseIntervalMs()).isEqualTo(5_000L);
        assertThat(policy.multiplier()).isEqualTo(3.0);
        assertThat(policy.maxDelayMs()).isEqualTo(300_000L);
    }

    @Test
    void timeCriticalPolicy_whenTimeCritical_returnsPolicy() {
        OffsetDateTime now = OffsetDateTime.now();
        JobTask task = new JobTask(
                UUID.randomUUID(), UUID.randomUUID(), "TEST", "test-topic",
                null, null, TaskStatus.PENDING, now, now, null, null,
                now.plusHours(1), false, 0, null, null,
                1_000L, 2.0, 3_600_000L,
                null, null, null, null, null, "{}", null,
                true, 10, 100L, 1.5, 900L, 2000L);

        TimeCriticalPolicy policy = task.timeCriticalPolicy();
        assertThat(policy).isNotNull();
        assertThat(policy.maxAttempts()).isEqualTo(10);
        assertThat(policy.baseIntervalMs()).isEqualTo(100L);
    }

    @Test
    void timeCriticalPolicy_whenNotTimeCritical_returnsNull() {
        OffsetDateTime now = OffsetDateTime.now();
        JobTask task = new JobTask(
                UUID.randomUUID(), UUID.randomUUID(), "TEST", "test-topic",
                null, null, TaskStatus.PENDING, now, now, null, null,
                now.plusHours(1), false, 0, null, null,
                1_000L, 2.0, 3_600_000L,
                null, null, null, null, null, "{}", null,
                false, 0, 0L, 1.0, 0L, 0L);

        assertThat(task.timeCriticalPolicy()).isNull();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static JobTask task(TaskStatus status, OffsetDateTime nextAttemptTime) {
        OffsetDateTime now = OffsetDateTime.now();
        return new JobTask(
                UUID.randomUUID(), UUID.randomUUID(), "TEST", "test-topic",
                null, null, status, now, now, null, null,
                now.plusHours(1), false, 0, null, nextAttemptTime,
                1_000L, 2.0, 3_600_000L,
                null, null, null, null, null, "{}", null,
                false, 0, 0L, 1.0, 0L, 0L);
    }

    private static JobTask taskWithDeadline(TaskStatus status, OffsetDateTime deadlineAt, boolean stale) {
        OffsetDateTime now = OffsetDateTime.now();
        return new JobTask(
                UUID.randomUUID(), UUID.randomUUID(), "TEST", "test-topic",
                null, null, status, now, now, null, null,
                deadlineAt, stale, 0, null, null,
                1_000L, 2.0, 3_600_000L,
                null, null, null, null, null, "{}", null,
                false, 0, 0L, 1.0, 0L, 0L);
    }
}
