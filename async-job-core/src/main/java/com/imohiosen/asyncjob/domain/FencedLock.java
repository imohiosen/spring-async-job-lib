package com.imohiosen.asyncjob.domain;

/**
 * Represents a successfully acquired fenced lock.
 *
 * <p>The fence token is a monotonically increasing {@code long} that acts as a
 * <strong>linearizability guard</strong>: every write to the database includes
 * the token in its {@code WHERE} clause, so a stale lock holder whose lease
 * expired cannot overwrite state set by a newer holder that obtained a
 * higher token.
 *
 * <h2>Why fenced tokens matter</h2>
 * <pre>
 *   Node-A acquires lock  → token = 7 → markInProgress(fence=7) → GC pause…
 *   Lock lease expires
 *   Node-B acquires lock  → token = 8 → markInProgress(fence=8) → completes task
 *   Node-A resumes        → markCompleted(fence=7)  → WHERE fence_token=7 → 0 rows updated
 * </pre>
 *
 * @param token the monotonically increasing fence value
 */
public record FencedLock(long token) {

    /** Sentinel used by no-op lock managers in tests. */
    public static final FencedLock NOOP = new FencedLock(0L);
}
