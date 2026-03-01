package com.imohiosen.asyncjob.lock;

import java.util.UUID;

/**
 * No-op implementation of lock management for unit tests.
 * Always acquires the lock immediately and unlock is a no-op.
 */
public class NoOpTaskLockManager extends TaskLockManager {

    public NoOpTaskLockManager() {
        super(null, LockProperties.DEFAULT);
    }

    @Override
    public boolean tryLock(UUID taskId) {
        return true;
    }

    @Override
    public void unlock(UUID taskId) {
        // no-op
    }
}
