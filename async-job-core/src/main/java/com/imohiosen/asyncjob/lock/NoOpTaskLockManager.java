package com.imohiosen.asyncjob.lock;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * No-op implementation of lock management for unit tests.
 * Always acquires the lock immediately with a monotonically increasing
 * fence token. Unlock is a no-op.
 */
public class NoOpTaskLockManager extends TaskLockManager {

    private final AtomicLong tokenSequence = new AtomicLong(0);

    public NoOpTaskLockManager() {
        super(null, LockProperties.DEFAULT);
    }

    @Override
    public Optional<FencedLock> tryLock(UUID taskId) {
        return Optional.of(new FencedLock(tokenSequence.incrementAndGet()));
    }

    @Override
    public void unlock(UUID taskId) {
        // no-op
    }
}
