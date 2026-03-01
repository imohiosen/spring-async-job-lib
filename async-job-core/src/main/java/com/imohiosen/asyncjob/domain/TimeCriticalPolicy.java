package com.imohiosen.asyncjob.domain;

/**
 * Encapsulates the sub-second retry configuration for time-critical tasks.
 *
 * <p>Time-critical tasks are retried in-memory using Resilience4j with sub-second
 * backoff intervals, bypassing the normal Kafka round-trip retry path. Progress
 * is periodically flushed to the database every {@code dbSyncIntervalMs} to avoid
 * DB overload during rapid retries.
 *
 * <p>Delay formula: {@code LEAST(baseIntervalMs * multiplier^attemptCount, maxDelayMs)}
 *
 * @param maxAttempts       maximum in-memory retry attempts before falling back to normal retry
 * @param baseIntervalMs    base delay between retries (sub-second, e.g. 100 ms)
 * @param multiplier        exponential growth factor (must be >= 1.0)
 * @param maxDelayMs        maximum delay cap (sub-second, e.g. 900 ms)
 * @param dbSyncIntervalMs  how often to flush retry progress to the database
 */
public record TimeCriticalPolicy(
        int maxAttempts,
        long baseIntervalMs,
        double multiplier,
        long maxDelayMs,
        long dbSyncIntervalMs
) {
    /** Default: 10 attempts, 100 ms base, 1.5× multiplier, 900 ms max delay, 2 s DB sync. */
    public static final TimeCriticalPolicy DEFAULT = new TimeCriticalPolicy(10, 100L, 1.5, 900L, 2_000L);

    public TimeCriticalPolicy {
        if (maxAttempts < 1)        throw new IllegalArgumentException("maxAttempts must be >= 1");
        if (baseIntervalMs <= 0)    throw new IllegalArgumentException("baseIntervalMs must be > 0");
        if (multiplier < 1.0)       throw new IllegalArgumentException("multiplier must be >= 1.0");
        if (maxDelayMs <= 0)        throw new IllegalArgumentException("maxDelayMs must be > 0");
        if (dbSyncIntervalMs <= 0)  throw new IllegalArgumentException("dbSyncIntervalMs must be > 0");
    }

    /**
     * Computes the delay in milliseconds for a given attempt number.
     *
     * @param attemptCount zero-based attempt index
     * @return delay in milliseconds, capped at {@code maxDelayMs}
     */
    public long computeDelayMs(int attemptCount) {
        double raw = baseIntervalMs * Math.pow(multiplier, attemptCount);
        return Math.min((long) raw, maxDelayMs);
    }
}
