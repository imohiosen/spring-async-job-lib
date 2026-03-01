package com.imohiosen.asyncjob.port.lock;

import com.imohiosen.asyncjob.domain.FencedLock;

import java.util.Optional;
import java.util.UUID;

/**
 * Port for distributed task locking with fenced tokens.
 *
 * <p>Implementations may use Redisson (Redis), ZooKeeper, database-based
 * locks, or any other distributed locking mechanism.
 *
 * <p>The fenced token guarantees linearizability: every database write includes
 * the token in its {@code WHERE} clause, preventing stale lock holders from
 * overwriting newer state.
 */
public interface TaskLockManager {

    /**
     * Attempts to acquire a fenced distributed lock for the given task.
     *
     * @param taskId task UUID to lock
     * @return a {@link FencedLock} with a monotonically increasing token if acquired;
     *         {@link Optional#empty()} if another node holds it
     */
    Optional<FencedLock> tryLock(UUID taskId);

    /**
     * Releases the lock for the given task. Safe to call even if the lock
     * was not acquired by the current thread.
     */
    void unlock(UUID taskId);
}
