package com.imohiosen.asyncjob.domain;

/**
 * Encapsulates the exponential backoff configuration for a task.
 * <p>
 * Delay formula: {@code LEAST(baseIntervalMs * multiplier^attemptCount, maxDelayMs)}
 */
public record BackoffPolicy(
        long baseIntervalMs,
        double multiplier,
        long maxDelayMs
) {
    public static final BackoffPolicy DEFAULT = new BackoffPolicy(1_000L, 2.0, 3_600_000L);

    public BackoffPolicy {
        if (baseIntervalMs <= 0) throw new IllegalArgumentException("baseIntervalMs must be > 0");
        if (multiplier < 1.0)   throw new IllegalArgumentException("multiplier must be >= 1.0");
        if (maxDelayMs <= 0)    throw new IllegalArgumentException("maxDelayMs must be > 0");
    }

    /**
     * Computes the delay in milliseconds for a given attempt number.
     *
     * @param attemptCount zero-based attempt index
     * @return delay in milliseconds, capped at maxDelayMs
     */
    public long computeDelayMs(int attemptCount) {
        double raw = baseIntervalMs * Math.pow(multiplier, attemptCount);
        return Math.min((long) raw, maxDelayMs);
    }
}
