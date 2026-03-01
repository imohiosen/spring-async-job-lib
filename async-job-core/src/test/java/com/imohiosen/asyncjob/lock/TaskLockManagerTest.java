package com.imohiosen.asyncjob.lock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RFencedLock;
import org.redisson.api.RedissonClient;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TaskLockManagerTest {

    @Mock RedissonClient redissonClient;
    @Mock RFencedLock    fencedLock;

    TaskLockManager lockManager;
    UUID            taskId;

    @BeforeEach
    void setUp() {
        lockManager = new TaskLockManager(redissonClient, LockProperties.DEFAULT);
        taskId = UUID.randomUUID();
        lenient().when(redissonClient.getFencedLock(anyString())).thenReturn(fencedLock);
    }

    @Test
    void tryLock_whenLockAcquired_returnsFencedLockWithToken() throws InterruptedException {
        // DEFAULT uses watchdog mode (leaseTimeMs = -1), so 2-param overload is used
        when(fencedLock.tryLockAndGetToken(
                eq(LockProperties.DEFAULT.waitTimeMs()),
                eq(TimeUnit.MILLISECONDS)
        )).thenReturn(42L);

        Optional<FencedLock> result = lockManager.tryLock(taskId);

        assertThat(result).isPresent();
        assertThat(result.get().token()).isEqualTo(42L);
        verify(redissonClient).getFencedLock("async-job-task-lock:" + taskId);
    }

    @Test
    void tryLock_whenLockNotAcquired_returnsEmpty() throws InterruptedException {
        when(fencedLock.tryLockAndGetToken(anyLong(), any(TimeUnit.class))).thenReturn(null);

        Optional<FencedLock> result = lockManager.tryLock(taskId);

        assertThat(result).isEmpty();
    }

    @Test
    void tryLock_onException_returnsEmpty() {
        when(fencedLock.tryLockAndGetToken(anyLong(), any(TimeUnit.class)))
                .thenThrow(new RuntimeException("Redis connection lost"));

        Optional<FencedLock> result = lockManager.tryLock(taskId);

        assertThat(result).isEmpty();
    }

    @Test
    void tryLock_successiveAcquisitions_returnIncreasingTokens() throws InterruptedException {
        when(fencedLock.tryLockAndGetToken(anyLong(), any(TimeUnit.class)))
                .thenReturn(7L)
                .thenReturn(8L);

        Optional<FencedLock> first  = lockManager.tryLock(taskId);
        Optional<FencedLock> second = lockManager.tryLock(taskId);

        assertThat(first).isPresent();
        assertThat(second).isPresent();
        assertThat(second.get().token()).isGreaterThan(first.get().token());
    }

    @Test
    void unlock_whenHeldByCurrentThread_releasesLock() {
        when(fencedLock.isHeldByCurrentThread()).thenReturn(true);
        lockManager.unlock(taskId);
        verify(fencedLock).unlock();
    }

    @Test
    void unlock_whenNotHeldByCurrentThread_doesNotUnlock() {
        when(fencedLock.isHeldByCurrentThread()).thenReturn(false);
        lockManager.unlock(taskId);
        verify(fencedLock, never()).unlock();
    }

    @Test
    void noOpLockManager_alwaysAcquiresWithIncreasingTokens() {
        NoOpTaskLockManager noOp = new NoOpTaskLockManager();

        Optional<FencedLock> first  = noOp.tryLock(taskId);
        Optional<FencedLock> second = noOp.tryLock(taskId);

        assertThat(first).isPresent();
        assertThat(second).isPresent();
        assertThat(second.get().token()).isGreaterThan(first.get().token());
        noOp.unlock(taskId); // should not throw
    }
}
