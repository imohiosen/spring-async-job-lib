package com.imohiosen.asyncjob.infrastructure.lock.noop;

import com.imohiosen.asyncjob.domain.FencedLock;
import com.imohiosen.asyncjob.port.lock.TaskLockManager;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * No-op implementation of {@link TaskLockManager} for unit tests.
 *
 * <p>Always acquires the lock immediately with a monotonically increasing
 * fence token. Unlock is a no-op.
 */
public class NoOpTaskLockManager implements TaskLockManager {

    private final AtomicLong tokenSequence = new AtomicLong(0);

    @Override
    public Optional<FencedLock> tryLock(UUID taskId) {
        return Optional.of(new FencedLock(tokenSequence.incrementAndGet()));
    }

    @Override
    public void unlock(UUID taskId) {
        // no-op
    }
}
