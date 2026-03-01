package com.imohiosen.asyncjob.lock;

/**
 * Configuration properties for Redisson distributed task locking.
 *
 * @param leaseTimeMs Time (ms) the lock is held before automatic release.
 *                    Set to {@code -1} to enable Redisson's watchdog which
 *                    automatically renews the lock while the owning thread
 *                    is alive — suitable for tasks with unpredictable duration.
 * @param waitTimeMs  Max time (ms) to wait for lock acquisition. 0 = try-once, no waiting.
 */
public record LockProperties(
        long leaseTimeMs,
        long waitTimeMs
) {
    /** Default: watchdog mode (auto-renew), try-once acquisition. */
    public static final LockProperties DEFAULT = new LockProperties(-1L, 0L);

    /** Returns true when the lock should use Redisson's watchdog auto-renewal. */
    public boolean useWatchdog() {
        return leaseTimeMs < 0;
    }
}
