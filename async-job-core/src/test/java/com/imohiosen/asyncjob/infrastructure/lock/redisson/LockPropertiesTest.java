package com.imohiosen.asyncjob.infrastructure.lock.redisson;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LockPropertiesTest {

    @Test
    void validLeaseTime_createsProperties() {
        LockProperties props = new LockProperties(30_000L, 0L);
        assertThat(props.leaseTimeMs()).isEqualTo(30_000L);
        assertThat(props.waitTimeMs()).isEqualTo(0L);
    }

    @Test
    void zeroLeaseTime_throwsException() {
        assertThatThrownBy(() -> new LockProperties(0L, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leaseTimeMs must be > 0");
    }

    @Test
    void negativeLeaseTime_throwsException() {
        assertThatThrownBy(() -> new LockProperties(-1L, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leaseTimeMs must be > 0");
    }

    @Test
    void negativeLeaseTime_differentValue_throwsException() {
        assertThatThrownBy(() -> new LockProperties(-100L, 5_000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leaseTimeMs must be > 0");
    }

    @Test
    void defaultConstant_hasExpectedValues() {
        assertThat(LockProperties.DEFAULT.leaseTimeMs()).isEqualTo(30_000L);
        assertThat(LockProperties.DEFAULT.waitTimeMs()).isEqualTo(0L);
    }
}
