package com.imohiosen.asyncjob.infrastructure.lock.redisson;

import com.imohiosen.asyncjob.domain.FencedLock;
import com.imohiosen.asyncjob.port.lock.TaskLockManager;
import org.redisson.api.RFencedLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Redisson-based implementation of {@link TaskLockManager} using fenced locks.
 *
 * <p>Lock key pattern: {@code async-job-task-lock:{taskId}}
 *
 * <h2>Fenced-token guarantee</h2>
 * <p>Each successful acquisition returns a {@link FencedLock} whose token is a
 * monotonically increasing {@code long}. The consumer stores this token in the
 * database when marking a task {@code IN_PROGRESS}. All subsequent writes
 * include {@code WHERE fence_token = ?}, so a stale holder whose lease expired
 * cannot overwrite state written by a newer holder with a higher token.
 *
 * <h2>Lease renewal</h2>
 * <p>After acquiring a lock with an explicit lease time, this manager schedules
 * periodic renewal at {@code leaseTimeMs / 3} intervals. If the lock's remaining
 * TTL is still positive, the TTL is reset to the full lease duration, preventing
 * premature expiry while the task is still running. Renewal is cancelled when
 * {@link #unlock(UUID)} is called or when the lock expires before a renewal fires.
 */
public class RedissonTaskLockManager implements TaskLockManager {

    private static final Logger log = LoggerFactory.getLogger(RedissonTaskLockManager.class);
    private static final String LOCK_PREFIX = "async-job-task-lock:";

    private final RedissonClient redissonClient;
    private final LockProperties props;
    private final ScheduledExecutorService renewalScheduler;
    private final ConcurrentHashMap<UUID, ScheduledFuture<?>> activeRenewals = new ConcurrentHashMap<>();

    public RedissonTaskLockManager(RedissonClient redissonClient, LockProperties props) {
        this(redissonClient, props, createRenewalScheduler());
    }

    /** Package-private constructor for testing with a controlled scheduler. */
    RedissonTaskLockManager(RedissonClient redissonClient, LockProperties props,
                            ScheduledExecutorService renewalScheduler) {
        this.redissonClient   = redissonClient;
        this.props            = props;
        this.renewalScheduler = renewalScheduler;
    }

    private static ScheduledExecutorService createRenewalScheduler() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "lock-lease-renewal");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public Optional<FencedLock> tryLock(UUID taskId) {
        RFencedLock lock = redissonClient.getFencedLock(LOCK_PREFIX + taskId);
        try {
            Long token = lock.tryLockAndGetToken(
                    props.waitTimeMs(),
                    props.leaseTimeMs(),
                    TimeUnit.MILLISECONDS
            );
            if (token != null) {
                scheduleRenewal(taskId, lock);
                log.debug("Fenced lock acquired for task={} token={} lease={}ms",
                        taskId, token, props.leaseTimeMs());
                return Optional.of(new FencedLock(token));
            } else {
                log.debug("Lock NOT acquired for task={} (held by another node)", taskId);
                return Optional.empty();
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.warn("Lock acquisition failed for task={}: {}", taskId, e.getMessage());
            return Optional.empty();
        }
    }

    private void scheduleRenewal(UUID taskId, RFencedLock lock) {
        long renewalPeriod = props.leaseTimeMs() / 3;
        String lockKey = LOCK_PREFIX + taskId;
        ScheduledFuture<?> future = renewalScheduler.scheduleAtFixedRate(() -> {
            try {
                long ttl = lock.remainTimeToLive();
                if (ttl > 0) {
                    redissonClient.getKeys().expire(lockKey, props.leaseTimeMs(), TimeUnit.MILLISECONDS);
                    log.debug("Renewed lease for task={} (ttl was {}ms)", taskId, ttl);
                } else {
                    log.warn("Lock for task={} expired before renewal, stopping", taskId);
                    cancelRenewal(taskId);
                }
            } catch (Exception e) {
                log.warn("Lease renewal failed for task={}: {}", taskId, e.getMessage());
                cancelRenewal(taskId);
            }
        }, renewalPeriod, renewalPeriod, TimeUnit.MILLISECONDS);
        activeRenewals.put(taskId, future);
    }

    private void cancelRenewal(UUID taskId) {
        ScheduledFuture<?> future = activeRenewals.remove(taskId);
        if (future != null) {
            future.cancel(false);
        }
    }

    @Override
    public void unlock(UUID taskId) {
        cancelRenewal(taskId);
        RFencedLock lock = redissonClient.getFencedLock(LOCK_PREFIX + taskId);
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
            log.debug("Fenced lock released for task={}", taskId);
        }
    }
}
