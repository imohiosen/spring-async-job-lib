package com.imohiosen.asyncjob.lock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TaskLockManagerTest {

    @Mock RedissonClient redissonClient;
    @Mock RLock          rLock;

    TaskLockManager lockManager;
    UUID            taskId;

    @BeforeEach
    void setUp() {
        lockManager = new TaskLockManager(redissonClient, LockProperties.DEFAULT);
        taskId = UUID.randomUUID();
        when(redissonClient.getLock(anyString())).thenReturn(rLock);
    }

    @Test
    void tryLock_whenLockAcquired_returnsTrue() throws InterruptedException {
        when(rLock.tryLock(
                eq(LockProperties.DEFAULT.waitTimeMs()),
                eq(LockProperties.DEFAULT.leaseTimeMs()),
                eq(TimeUnit.MILLISECONDS)
        )).thenReturn(true);

        assertThat(lockManager.tryLock(taskId)).isTrue();
        verify(redissonClient).getLock("async-job-task-lock:" + taskId);
    }

    @Test
    void tryLock_whenLockNotAcquired_returnsFalse() throws InterruptedException {
        when(rLock.tryLock(anyLong(), anyLong(), any())).thenReturn(false);
        assertThat(lockManager.tryLock(taskId)).isFalse();
    }

    @Test
    void tryLock_onInterruption_returnsFalseAndRestoresInterruptFlag() throws InterruptedException {
        when(rLock.tryLock(anyLong(), anyLong(), any())).thenThrow(new InterruptedException());
        boolean result = lockManager.tryLock(taskId);
        assertThat(result).isFalse();
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        Thread.interrupted(); // clean up
    }

    @Test
    void unlock_whenHeldByCurrentThread_releasesLock() {
        when(rLock.isHeldByCurrentThread()).thenReturn(true);
        lockManager.unlock(taskId);
        verify(rLock).unlock();
    }

    @Test
    void unlock_whenNotHeldByCurrentThread_doesNotUnlock() {
        when(rLock.isHeldByCurrentThread()).thenReturn(false);
        lockManager.unlock(taskId);
        verify(rLock, never()).unlock();
    }

    @Test
    void noOpLockManager_alwaysAcquires() {
        NoOpTaskLockManager noOp = new NoOpTaskLockManager();
        assertThat(noOp.tryLock(taskId)).isTrue();
        noOp.unlock(taskId); // should not throw
    }
}
