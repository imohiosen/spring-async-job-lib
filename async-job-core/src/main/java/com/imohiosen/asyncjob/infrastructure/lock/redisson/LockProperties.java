package com.imohiosen.asyncjob.infrastructure.lock.redisson;

/**
 * Configuration properties for Redisson distributed task locking.
 *
 * <p>A valid lease time must always be provided. The lock manager
 * automatically renews the lease at {@code leaseTimeMs / 3} intervals
 * while the task is still running, so tasks of unpredictable duration
 * are safe.
 *
 * @param leaseTimeMs Time (ms) the lock is held before automatic release.
 *                    Must be &gt; 0. The lock manager renews the lease
 *                    periodically so it will not expire while processing.
 * @param waitTimeMs  Max time (ms) to wait for lock acquisition. 0 = try-once, no waiting.
 */
public record LockProperties(
        long leaseTimeMs,
        long waitTimeMs
) {
    public LockProperties {
        if (leaseTimeMs <= 0) {
            throw new IllegalArgumentException(
                    "leaseTimeMs must be > 0, got " + leaseTimeMs);
        }
    }

    /** Default: 30-second lease with auto-renewal, try-once acquisition. */
    public static final LockProperties DEFAULT = new LockProperties(30_000L, 0L);
}
