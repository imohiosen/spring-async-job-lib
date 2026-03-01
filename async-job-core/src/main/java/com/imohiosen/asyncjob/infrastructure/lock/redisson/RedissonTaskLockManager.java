package com.imohiosen.asyncjob.infrastructure.lock.redisson;

import com.imohiosen.asyncjob.domain.FencedLock;
import com.imohiosen.asyncjob.port.lock.TaskLockManager;
import org.redisson.api.RFencedLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;
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
 * <h2>Watchdog mode</h2>
 * <p>When {@link LockProperties#useWatchdog()} is {@code true} (default), the
 * lock is acquired without an explicit lease time. Redisson's internal watchdog
 * automatically renews the lock while the owning thread is alive, making it
 * safe for tasks of unpredictable duration.
 */
public class RedissonTaskLockManager implements TaskLockManager {

    private static final Logger log = LoggerFactory.getLogger(RedissonTaskLockManager.class);
    private static final String LOCK_PREFIX = "async-job-task-lock:";

    private final RedissonClient redissonClient;
    private final LockProperties props;

    public RedissonTaskLockManager(RedissonClient redissonClient, LockProperties props) {
        this.redissonClient = redissonClient;
        this.props          = props;
    }

    @Override
    public Optional<FencedLock> tryLock(UUID taskId) {
        RFencedLock lock = redissonClient.getFencedLock(LOCK_PREFIX + taskId);
        try {
            Long token;
            if (props.useWatchdog()) {
                token = lock.tryLockAndGetToken(
                        props.waitTimeMs(),
                        TimeUnit.MILLISECONDS
                );
            } else {
                token = lock.tryLockAndGetToken(
                        props.waitTimeMs(),
                        props.leaseTimeMs(),
                        TimeUnit.MILLISECONDS
                );
            }
            if (token != null) {
                log.debug("Fenced lock acquired for task={} token={} watchdog={}",
                        taskId, token, props.useWatchdog());
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

    @Override
    public void unlock(UUID taskId) {
        RFencedLock lock = redissonClient.getFencedLock(LOCK_PREFIX + taskId);
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
            log.debug("Fenced lock released for task={}", taskId);
        }
    }
}
