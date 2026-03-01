package com.imohiosen.asyncjob.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TimeCriticalPolicyTest {

    @Test
    void DEFAULT_hasExpectedValues() {
        TimeCriticalPolicy policy = TimeCriticalPolicy.DEFAULT;
        assertThat(policy.maxAttempts()).isEqualTo(10);
        assertThat(policy.baseIntervalMs()).isEqualTo(100L);
        assertThat(policy.multiplier()).isEqualTo(1.5);
        assertThat(policy.maxDelayMs()).isEqualTo(900L);
        assertThat(policy.dbSyncIntervalMs()).isEqualTo(2_000L);
    }

    @Test
    void computeDelayMs_firstAttempt_returnsBaseInterval() {
        TimeCriticalPolicy policy = new TimeCriticalPolicy(5, 100L, 2.0, 1_000L, 2_000L);
        assertThat(policy.computeDelayMs(0)).isEqualTo(100L);
    }

    @Test
    void computeDelayMs_secondAttempt_appliesMultiplier() {
        TimeCriticalPolicy policy = new TimeCriticalPolicy(5, 100L, 2.0, 1_000L, 2_000L);
        // 100 * 2^1 = 200
        assertThat(policy.computeDelayMs(1)).isEqualTo(200L);
    }

    @Test
    void computeDelayMs_thirdAttempt_appliesMultiplierSquared() {
        TimeCriticalPolicy policy = new TimeCriticalPolicy(5, 100L, 2.0, 1_000L, 2_000L);
        // 100 * 2^2 = 400
        assertThat(policy.computeDelayMs(2)).isEqualTo(400L);
    }

    @Test
    void computeDelayMs_highAttemptCount_capsAtMaxDelay() {
        TimeCriticalPolicy policy = new TimeCriticalPolicy(20, 100L, 2.0, 500L, 2_000L);
        // 100 * 2^10 = 102400, capped at 500
        assertThat(policy.computeDelayMs(10)).isEqualTo(500L);
    }

    @Test
    void computeDelayMs_customMultiplier_appliesCorrectly() {
        TimeCriticalPolicy policy = new TimeCriticalPolicy(5, 50L, 3.0, 5_000L, 2_000L);
        // 50 * 3^2 = 450
        assertThat(policy.computeDelayMs(2)).isEqualTo(450L);
    }

    @Test
    void constructor_negativeBaseInterval_throwsIllegalArgument() {
        assertThatThrownBy(() -> new TimeCriticalPolicy(5, -1L, 2.0, 1_000L, 2_000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("baseIntervalMs");
    }

    @Test
    void constructor_zeroBaseInterval_throwsIllegalArgument() {
        assertThatThrownBy(() -> new TimeCriticalPolicy(5, 0L, 2.0, 1_000L, 2_000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("baseIntervalMs");
    }

    @Test
    void constructor_zeroMaxAttempts_throwsIllegalArgument() {
        assertThatThrownBy(() -> new TimeCriticalPolicy(0, 100L, 2.0, 1_000L, 2_000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAttempts");
    }

    @Test
    void constructor_negativeMaxDelayMs_throwsIllegalArgument() {
        assertThatThrownBy(() -> new TimeCriticalPolicy(5, 100L, 2.0, -1L, 2_000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxDelayMs");
    }

    @Test
    void constructor_zeroDbSyncIntervalMs_throwsIllegalArgument() {
        assertThatThrownBy(() -> new TimeCriticalPolicy(5, 100L, 2.0, 1_000L, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dbSyncIntervalMs");
    }

    @Test
    void constructor_multiplierBelowOne_throwsIllegalArgument() {
        assertThatThrownBy(() -> new TimeCriticalPolicy(5, 100L, 0.5, 1_000L, 2_000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multiplier");
    }

    @Test
    void computeDelayMs_multiplierExactlyOne_noGrowth() {
        TimeCriticalPolicy policy = new TimeCriticalPolicy(5, 100L, 1.0, 1_000L, 2_000L);
        assertThat(policy.computeDelayMs(0)).isEqualTo(100L);
        assertThat(policy.computeDelayMs(5)).isEqualTo(100L);
    }
}
