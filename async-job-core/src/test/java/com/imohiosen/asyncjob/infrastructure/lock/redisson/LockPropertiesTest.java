package com.imohiosen.asyncjob.infrastructure.lock.redisson;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LockPropertiesTest {

    @Test
    void useWatchdog_negativeLeaseTime_returnsTrue() {
        LockProperties props = new LockProperties(-1L, 0L);
        assertThat(props.useWatchdog()).isTrue();
    }

    @Test
    void useWatchdog_negativeLeaseTime_differentValue_returnsTrue() {
        LockProperties props = new LockProperties(-100L, 5_000L);
        assertThat(props.useWatchdog()).isTrue();
    }

    @Test
    void useWatchdog_zeroLeaseTime_returnsFalse() {
        LockProperties props = new LockProperties(0L, 0L);
        assertThat(props.useWatchdog()).isFalse();
    }

    @Test
    void useWatchdog_positiveLeaseTime_returnsFalse() {
        LockProperties props = new LockProperties(30_000L, 0L);
        assertThat(props.useWatchdog()).isFalse();
    }

    @Test
    void defaultConstant_hasExpectedValues() {
        assertThat(LockProperties.DEFAULT.leaseTimeMs()).isEqualTo(-1L);
        assertThat(LockProperties.DEFAULT.waitTimeMs()).isEqualTo(0L);
        assertThat(LockProperties.DEFAULT.useWatchdog()).isTrue();
    }
}
