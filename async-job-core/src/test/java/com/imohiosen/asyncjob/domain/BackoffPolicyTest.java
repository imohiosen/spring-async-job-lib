package com.imohiosen.asyncjob.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BackoffPolicyTest {

    @Test
    void computeDelayMs_firstAttempt_returnsBaseInterval() {
        BackoffPolicy policy = new BackoffPolicy(1_000L, 2.0, 3_600_000L);
        assertThat(policy.computeDelayMs(0)).isEqualTo(1_000L);
    }

    @Test
    void computeDelayMs_secondAttempt_doublesDelay() {
        BackoffPolicy policy = new BackoffPolicy(1_000L, 2.0, 3_600_000L);
        assertThat(policy.computeDelayMs(1)).isEqualTo(2_000L);
    }

    @Test
    void computeDelayMs_thirdAttempt_quadruplesDelay() {
        BackoffPolicy policy = new BackoffPolicy(1_000L, 2.0, 3_600_000L);
        assertThat(policy.computeDelayMs(2)).isEqualTo(4_000L);
    }

    @Test
    void computeDelayMs_highAttemptCount_capsAtMaxDelay() {
        BackoffPolicy policy = new BackoffPolicy(1_000L, 2.0, 10_000L);
        // 1000 * 2^20 = 1,048,576,000 — well above cap of 10,000
        assertThat(policy.computeDelayMs(20)).isEqualTo(10_000L);
    }

    @Test
    void computeDelayMs_customMultiplier_appliesCorrectly() {
        BackoffPolicy policy = new BackoffPolicy(500L, 3.0, 3_600_000L);
        // 500 * 3^2 = 4500
        assertThat(policy.computeDelayMs(2)).isEqualTo(4_500L);
    }

    @Test
    void constructor_invalidBaseInterval_throwsException() {
        assertThatThrownBy(() -> new BackoffPolicy(0, 2.0, 3_600_000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("baseIntervalMs");
    }

    @Test
    void constructor_multiplierBelowOne_throwsException() {
        assertThatThrownBy(() -> new BackoffPolicy(1_000L, 0.5, 3_600_000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multiplier");
    }

    @Test
    void defaultPolicy_hasExpectedValues() {
        assertThat(BackoffPolicy.DEFAULT.baseIntervalMs()).isEqualTo(1_000L);
        assertThat(BackoffPolicy.DEFAULT.multiplier()).isEqualTo(2.0);
        assertThat(BackoffPolicy.DEFAULT.maxDelayMs()).isEqualTo(3_600_000L);
    }
}
