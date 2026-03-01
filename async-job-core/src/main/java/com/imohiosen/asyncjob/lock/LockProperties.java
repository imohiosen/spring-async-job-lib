package com.imohiosen.asyncjob.lock;

/**
 * Configuration properties for Redisson distributed task locking.
 *
 * @param leaseTimeMs Time (ms) the lock is held before automatic release.
 *                    Prevents deadlocks if the JVM dies. Must be > expected task duration.
 * @param waitTimeMs  Max time (ms) to wait for lock acquisition. 0 = try-once, no waiting.
 */
public record LockProperties(
        long leaseTimeMs,
        long waitTimeMs
) {
    public static final LockProperties DEFAULT = new LockProperties(30_000L, 0L);
}
