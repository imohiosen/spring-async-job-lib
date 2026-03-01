package com.imohiosen.asyncjob.infrastructure.lock.redisson;

import com.imohiosen.asyncjob.domain.FencedLock;
import com.imohiosen.asyncjob.infrastructure.lock.noop.NoOpTaskLockManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RFencedLock;
import org.redisson.api.RKeys;
import org.redisson.api.RedissonClient;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RedissonTaskLockManagerTest {

    @Mock RedissonClient redissonClient;
    @Mock RFencedLock    fencedLock;
    @Mock RKeys          keys;
    @Mock ScheduledExecutorService renewalScheduler;
    @Mock ScheduledFuture<?> scheduledFuture;

    RedissonTaskLockManager lockManager;
    UUID                    taskId;

    @BeforeEach
    void setUp() {
        // Use package-private constructor so renewal goes through the mock scheduler
        lockManager = new RedissonTaskLockManager(redissonClient, LockProperties.DEFAULT, renewalScheduler);
        taskId = UUID.randomUUID();
        lenient().when(redissonClient.getFencedLock(anyString())).thenReturn(fencedLock);
        lenient().when(redissonClient.getKeys()).thenReturn(keys);
        lenient().doReturn(scheduledFuture).when(renewalScheduler)
                .scheduleAtFixedRate(any(), anyLong(), anyLong(), any());
    }

    // ── tryLock ──────────────────────────────────────────────────────────────

    @Test
    void tryLock_whenLockAcquired_returnsFencedLockWithToken() throws InterruptedException {
        when(fencedLock.tryLockAndGetToken(
                eq(0L), eq(30_000L), eq(TimeUnit.MILLISECONDS)
        )).thenReturn(42L);

        Optional<FencedLock> result = lockManager.tryLock(taskId);

        assertThat(result).isPresent();
        assertThat(result.get().token()).isEqualTo(42L);
        verify(redissonClient).getFencedLock("async-job-task-lock:" + taskId);
    }

    @Test
    void tryLock_whenLockNotAcquired_returnsEmpty() throws InterruptedException {
        when(fencedLock.tryLockAndGetToken(anyLong(), anyLong(), any(TimeUnit.class)))
                .thenReturn(null);

        Optional<FencedLock> result = lockManager.tryLock(taskId);

        assertThat(result).isEmpty();
        verifyNoInteractions(renewalScheduler);
    }

    @Test
    void tryLock_onRuntimeException_returnsEmpty() {
        when(fencedLock.tryLockAndGetToken(anyLong(), anyLong(), any(TimeUnit.class)))
                .thenThrow(new RuntimeException("Redis connection lost"));

        Optional<FencedLock> result = lockManager.tryLock(taskId);

        assertThat(result).isEmpty();
    }

    @Test
    void tryLock_onInterruptedException_returnsEmptyAndRestoresInterrupt() {
        // tryLockAndGetToken doesn't declare InterruptedException, but the catch
        // block handles it defensively.  Use doAnswer to bypass Mockito's check.
        doAnswer(invocation -> { throw new InterruptedException("interrupted"); })
                .when(fencedLock).tryLockAndGetToken(anyLong(), anyLong(), any(TimeUnit.class));

        Optional<FencedLock> result = lockManager.tryLock(taskId);

        assertThat(result).isEmpty();
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        // Clear the interrupt flag so it doesn't affect other tests
        Thread.interrupted();
    }

    @Test
    void tryLock_successiveAcquisitions_returnIncreasingTokens() throws InterruptedException {
        when(fencedLock.tryLockAndGetToken(anyLong(), anyLong(), any(TimeUnit.class)))
                .thenReturn(7L)
                .thenReturn(8L);

        Optional<FencedLock> first  = lockManager.tryLock(taskId);
        Optional<FencedLock> second = lockManager.tryLock(taskId);

        assertThat(first).isPresent();
        assertThat(second).isPresent();
        assertThat(second.get().token()).isGreaterThan(first.get().token());
    }

    @Test
    void tryLock_customLeaseAndWait_usesConfiguredValues() throws InterruptedException {
        LockProperties custom = new LockProperties(60_000L, 500L);
        RedissonTaskLockManager mgr = new RedissonTaskLockManager(redissonClient, custom, renewalScheduler);

        when(fencedLock.tryLockAndGetToken(eq(500L), eq(60_000L), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(99L);

        Optional<FencedLock> result = mgr.tryLock(taskId);

        assertThat(result).isPresent();
        assertThat(result.get().token()).isEqualTo(99L);
        verify(fencedLock).tryLockAndGetToken(500L, 60_000L, TimeUnit.MILLISECONDS);
    }

    // ── unlock ───────────────────────────────────────────────────────────────

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
    void unlock_withoutPriorLock_doesNotThrow() {
        // cancelRenewal with no active renewal — exercises the null branch
        when(fencedLock.isHeldByCurrentThread()).thenReturn(false);
        lockManager.unlock(UUID.randomUUID());
        verify(fencedLock, never()).unlock();
    }

    // ── NoOpTaskLockManager ──────────────────────────────────────────────────

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

    // ── Lease renewal ────────────────────────────────────────────────────────

    @Test
    void tryLock_schedulesRenewalAtOneThirdOfLease() throws InterruptedException {
        when(fencedLock.tryLockAndGetToken(anyLong(), anyLong(), any(TimeUnit.class)))
                .thenReturn(50L);

        lockManager.tryLock(taskId);

        verify(renewalScheduler).scheduleAtFixedRate(
                any(Runnable.class), eq(10_000L), eq(10_000L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void tryLock_tokenNull_doesNotScheduleRenewal() throws InterruptedException {
        when(fencedLock.tryLockAndGetToken(anyLong(), anyLong(), any(TimeUnit.class)))
                .thenReturn(null);

        lockManager.tryLock(taskId);

        verifyNoInteractions(renewalScheduler);
    }

    @Test
    void unlock_cancelsActiveRenewal() throws InterruptedException {
        when(fencedLock.tryLockAndGetToken(anyLong(), anyLong(), any(TimeUnit.class)))
                .thenReturn(50L);
        when(fencedLock.isHeldByCurrentThread()).thenReturn(true);

        lockManager.tryLock(taskId);
        lockManager.unlock(taskId);

        verify(scheduledFuture).cancel(false);
        verify(fencedLock).unlock();
    }

    @Test
    void renewal_extendsLease_whenTtlPositive() throws InterruptedException {
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        doReturn(scheduledFuture).when(renewalScheduler)
                .scheduleAtFixedRate(captor.capture(), anyLong(), anyLong(), any());

        when(fencedLock.tryLockAndGetToken(anyLong(), anyLong(), any(TimeUnit.class)))
                .thenReturn(50L);
        when(fencedLock.remainTimeToLive()).thenReturn(20_000L);
        when(keys.expire(anyString(), anyLong(), any(TimeUnit.class))).thenReturn(true);

        lockManager.tryLock(taskId);
        captor.getValue().run();

        verify(keys).expire("async-job-task-lock:" + taskId, 30_000L, TimeUnit.MILLISECONDS);
    }

    @Test
    void renewal_stopsWhenLockExpired() throws InterruptedException {
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        doReturn(scheduledFuture).when(renewalScheduler)
                .scheduleAtFixedRate(captor.capture(), anyLong(), anyLong(), any());

        when(fencedLock.tryLockAndGetToken(anyLong(), anyLong(), any(TimeUnit.class)))
                .thenReturn(50L);
        when(fencedLock.remainTimeToLive()).thenReturn(-2L);

        lockManager.tryLock(taskId);
        captor.getValue().run();

        verify(keys, never()).expire(anyString(), anyLong(), any(TimeUnit.class));
        verify(scheduledFuture).cancel(false);
    }

    @Test
    void renewal_stopsOnException() throws InterruptedException {
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        doReturn(scheduledFuture).when(renewalScheduler)
                .scheduleAtFixedRate(captor.capture(), anyLong(), anyLong(), any());

        when(fencedLock.tryLockAndGetToken(anyLong(), anyLong(), any(TimeUnit.class)))
                .thenReturn(50L);
        when(fencedLock.remainTimeToLive()).thenThrow(new RuntimeException("Redis down"));

        lockManager.tryLock(taskId);
        captor.getValue().run();

        verify(keys, never()).expire(anyString(), anyLong(), any(TimeUnit.class));
        verify(scheduledFuture).cancel(false);
    }

    @Test
    void renewal_ttlZero_treatedAsExpired() throws InterruptedException {
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        doReturn(scheduledFuture).when(renewalScheduler)
                .scheduleAtFixedRate(captor.capture(), anyLong(), anyLong(), any());

        when(fencedLock.tryLockAndGetToken(anyLong(), anyLong(), any(TimeUnit.class)))
                .thenReturn(50L);
        when(fencedLock.remainTimeToLive()).thenReturn(0L);

        lockManager.tryLock(taskId);
        captor.getValue().run();

        verify(keys, never()).expire(anyString(), anyLong(), any(TimeUnit.class));
        verify(scheduledFuture).cancel(false);
    }
}
