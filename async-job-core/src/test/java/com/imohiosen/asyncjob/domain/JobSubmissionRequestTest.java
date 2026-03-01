package com.imohiosen.asyncjob.domain;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobSubmissionRequestTest {

    @Test
    void blankTaskType_throwsException() {
        assertThatThrownBy(() -> new JobSubmissionRequest(
                "test-job", "test-topic", " ",
                List.of(), 60_000L, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("taskType");
    }

    @Test
    void nullTaskType_throwsException() {
        assertThatThrownBy(() -> new JobSubmissionRequest(
                "test-job", "test-topic", null,
                List.of(), 60_000L, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("taskType");
    }

    @Test
    void negativeDeadlineMs_throwsException() {
        assertThatThrownBy(() -> new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of(), -1L, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("deadlineMs");
    }

    @Test
    void nullJobName_throwsException() {
        assertThatThrownBy(() -> new JobSubmissionRequest(
                null, "test-topic", "TEST_TYPE",
                List.of(), 60_000L, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jobName");
    }

    @Test
    void nullDestination_throwsException() {
        assertThatThrownBy(() -> new JobSubmissionRequest(
                "test-job", null, "TEST_TYPE",
                List.of(), 60_000L, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("destination");
    }

    @Test
    void isImmediate_withPastScheduledAt_returnsTrue() {
        JobSubmissionRequest request = new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of(), 60_000L, null, null,
                OffsetDateTime.now().minusHours(2), null, null);
        assertThat(request.isImmediate()).isTrue();
    }

    @Test
    void payloads_defensiveCopy_mutationDoesNotAffectRecord() {
        List<String> mutable = new ArrayList<>(List.of("a", "b", "c"));
        JobSubmissionRequest request = new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                mutable, 60_000L, null, null, null, null, null);

        mutable.add("d");

        assertThat(request.payloads()).hasSize(3);
        assertThat(request.payloads()).containsExactly("a", "b", "c");
    }

    @Test
    void payloads_returnedListIsUnmodifiable() {
        JobSubmissionRequest request = new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of("a"), 60_000L, null, null, null, null, null);

        assertThatThrownBy(() -> request.payloads().add("b"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void effectiveBackoffPolicy_withCustomPolicy_returnsCustom() {
        BackoffPolicy custom = new BackoffPolicy(5_000L, 3.0, 300_000L);
        JobSubmissionRequest request = new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of(), 60_000L, null, custom, null, null, null);
        assertThat(request.effectiveBackoffPolicy()).isSameAs(custom);
    }

    @Test
    void effectiveTaskDeadlineMs_withCustomValue_returnsCustom() {
        JobSubmissionRequest request = new JobSubmissionRequest(
                "test-job", "test-topic", "TEST_TYPE",
                List.of(), 60_000L, 15_000L, null, null, null, null);
        assertThat(request.effectiveTaskDeadlineMs()).isEqualTo(15_000L);
    }
}
