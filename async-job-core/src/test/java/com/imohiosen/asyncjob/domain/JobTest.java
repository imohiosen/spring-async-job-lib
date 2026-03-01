package com.imohiosen.asyncjob.domain;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class JobTest {

    // ── isDeadlineBreached ────────────────────────────────────────────────────

    @Test
    void isDeadlineBreached_pastDeadlineActiveStatus_returnsTrue() {
        Job job = jobWithDeadline(OffsetDateTime.now().minusMinutes(5), false, JobStatus.IN_PROGRESS);
        assertThat(job.isDeadlineBreached()).isTrue();
    }

    @Test
    void isDeadlineBreached_pastDeadlinePendingStatus_returnsTrue() {
        Job job = jobWithDeadline(OffsetDateTime.now().minusMinutes(5), false, JobStatus.PENDING);
        assertThat(job.isDeadlineBreached()).isTrue();
    }

    @Test
    void isDeadlineBreached_pastDeadlineFailedStatus_returnsTrue() {
        Job job = jobWithDeadline(OffsetDateTime.now().minusMinutes(5), false, JobStatus.FAILED);
        assertThat(job.isDeadlineBreached()).isTrue();
    }

    @Test
    void isDeadlineBreached_alreadyStale_returnsFalse() {
        Job job = jobWithDeadline(OffsetDateTime.now().minusMinutes(5), true, JobStatus.IN_PROGRESS);
        assertThat(job.isDeadlineBreached()).isFalse();
    }

    @Test
    void isDeadlineBreached_nullDeadline_returnsFalse() {
        Job job = jobWithDeadline(null, false, JobStatus.IN_PROGRESS);
        assertThat(job.isDeadlineBreached()).isFalse();
    }

    @Test
    void isDeadlineBreached_futureDeadline_returnsFalse() {
        Job job = jobWithDeadline(OffsetDateTime.now().plusHours(1), false, JobStatus.IN_PROGRESS);
        assertThat(job.isDeadlineBreached()).isFalse();
    }

    @Test
    void isDeadlineBreached_scheduledStatus_returnsFalse() {
        Job job = jobWithDeadline(OffsetDateTime.now().minusMinutes(5), false, JobStatus.SCHEDULED);
        assertThat(job.isDeadlineBreached()).isFalse();
    }

    @Test
    void isDeadlineBreached_completedStatus_returnsFalse() {
        Job job = jobWithDeadline(OffsetDateTime.now().minusMinutes(5), false, JobStatus.COMPLETED);
        assertThat(job.isDeadlineBreached()).isFalse();
    }

    @Test
    void isDeadlineBreached_deadLetterStatus_returnsFalse() {
        Job job = jobWithDeadline(OffsetDateTime.now().minusMinutes(5), false, JobStatus.DEAD_LETTER);
        assertThat(job.isDeadlineBreached()).isFalse();
    }

    // ── isScheduledAndDue ─────────────────────────────────────────────────────

    @Test
    void isScheduledAndDue_scheduledWithPastTime_returnsTrue() {
        Job job = jobWithSchedule(JobStatus.SCHEDULED, OffsetDateTime.now().minusMinutes(1));
        assertThat(job.isScheduledAndDue()).isTrue();
    }

    @Test
    void isScheduledAndDue_scheduledWithFutureTime_returnsFalse() {
        Job job = jobWithSchedule(JobStatus.SCHEDULED, OffsetDateTime.now().plusHours(1));
        assertThat(job.isScheduledAndDue()).isFalse();
    }

    @Test
    void isScheduledAndDue_nonScheduledStatus_returnsFalse() {
        Job job = jobWithSchedule(JobStatus.PENDING, OffsetDateTime.now().minusMinutes(1));
        assertThat(job.isScheduledAndDue()).isFalse();
    }

    @Test
    void isScheduledAndDue_nullScheduledAt_returnsFalse() {
        Job job = jobWithSchedule(JobStatus.SCHEDULED, null);
        assertThat(job.isScheduledAndDue()).isFalse();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static Job jobWithDeadline(OffsetDateTime deadlineAt, boolean stale, JobStatus status) {
        OffsetDateTime now = OffsetDateTime.now();
        return new Job(UUID.randomUUID(), "test-job", null, status,
                now, now, now, null, deadlineAt, null, stale,
                null, false);
    }

    private static Job jobWithSchedule(JobStatus status, OffsetDateTime scheduledAt) {
        OffsetDateTime now = OffsetDateTime.now();
        return new Job(UUID.randomUUID(), "test-job", null, status,
                now, now, null, null, now.plusHours(1), scheduledAt, false,
                null, false);
    }
}
