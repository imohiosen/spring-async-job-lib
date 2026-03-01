package com.imohiosen.asyncjob.infrastructure.lock.noop;

import com.imohiosen.asyncjob.domain.FencedLock;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class NoOpTaskLockManagerTest {

    private final NoOpTaskLockManager lockManager = new NoOpTaskLockManager();

    @Test
    void tryLock_alwaysAcquires() {
        UUID taskId = UUID.randomUUID();
        Optional<FencedLock> result = lockManager.tryLock(taskId);
        assertThat(result).isPresent();
    }

    @Test
    void tryLock_tokensIncrementMonotonically() {
        UUID taskId = UUID.randomUUID();
        long token1 = lockManager.tryLock(taskId).orElseThrow().token();
        long token2 = lockManager.tryLock(taskId).orElseThrow().token();
        long token3 = lockManager.tryLock(UUID.randomUUID()).orElseThrow().token();

        assertThat(token2).isGreaterThan(token1);
        assertThat(token3).isGreaterThan(token2);
    }

    @Test
    void tryLock_firstTokenIsOne() {
        NoOpTaskLockManager fresh = new NoOpTaskLockManager();
        long token = fresh.tryLock(UUID.randomUUID()).orElseThrow().token();
        assertThat(token).isEqualTo(1L);
    }

    @Test
    void unlock_doesNotThrow() {
        UUID taskId = UUID.randomUUID();
        assertThatCode(() -> lockManager.unlock(taskId)).doesNotThrowAnyException();
    }

    @Test
    void unlock_afterLock_doesNotThrow() {
        UUID taskId = UUID.randomUUID();
        lockManager.tryLock(taskId);
        assertThatCode(() -> lockManager.unlock(taskId)).doesNotThrowAnyException();
    }
}
