package com.imohiosen.asyncjob.lock;

import org.redisson.api.RFencedLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Manages per-task distributed locks using Redisson's <strong>fenced lock</strong>.
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
public class TaskLockManager {

    private static final Logger log = LoggerFactory.getLogger(TaskLockManager.class);
    private static final String LOCK_PREFIX = "async-job-task-lock:";

    private final RedissonClient redissonClient;
    private final LockProperties props;

    public TaskLockManager(RedissonClient redissonClient, LockProperties props) {
        this.redissonClient = redissonClient;
        this.props          = props;
    }

    /**
     * Attempts to acquire a fenced distributed lock for the given task.
     *
     * <p>In watchdog mode (default), the lock auto-renews while the owning
     * thread is alive. In explicit-lease mode, the lock expires after
     * {@link LockProperties#leaseTimeMs()} milliseconds.
     *
     * @param taskId task UUID to lock
     * @return a {@link FencedLock} with a monotonically increasing token if the lock was
     *         acquired; {@link Optional#empty()} if another node holds it
     */
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

    /**
     * Releases the fenced lock for the given task.
     * Safe to call even if the lock was not acquired by the current thread —
     * the {@code isHeldByCurrentThread()} guard prevents {@link IllegalMonitorStateException}.
     */
    public void unlock(UUID taskId) {
        RFencedLock lock = redissonClient.getFencedLock(LOCK_PREFIX + taskId);
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
            log.debug("Fenced lock released for task={}", taskId);
        }
    }
}
