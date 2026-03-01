package com.imohiosen.asyncjob.lock;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Manages per-task distributed locks using Redisson.
 *
 * <p>Lock key pattern: {@code async-job-task-lock:{taskId}}
 *
 * <p><strong>Critical:</strong> All locks use an explicit lease time ({@link LockProperties#leaseTimeMs()})
 * to ensure the lock is automatically released if the JVM dies before the
 * {@code finally} block can execute. This prevents deadlocks across cluster nodes.
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
     * Attempts to acquire a distributed lock for the given task.
     *
     * @param taskId task UUID to lock
     * @return {@code true} if the lock was acquired; {@code false} if another node holds it
     */
    public boolean tryLock(UUID taskId) {
        RLock lock = redissonClient.getLock(LOCK_PREFIX + taskId);
        try {
            boolean acquired = lock.tryLock(
                    props.waitTimeMs(),
                    props.leaseTimeMs(),
                    TimeUnit.MILLISECONDS
            );
            if (acquired) {
                log.debug("Lock acquired for task={}", taskId);
            } else {
                log.debug("Lock NOT acquired for task={} (held by another node)", taskId);
            }
            return acquired;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Lock acquisition interrupted for task={}", taskId);
            return false;
        }
    }

    /**
     * Releases the lock for the given task.
     * Safe to call even if the lock was not acquired by the current thread —
     * the {@code isHeldByCurrentThread()} guard prevents {@link IllegalMonitorStateException}.
     */
    public void unlock(UUID taskId) {
        RLock lock = redissonClient.getLock(LOCK_PREFIX + taskId);
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
            log.debug("Lock released for task={}", taskId);
        }
    }
}
