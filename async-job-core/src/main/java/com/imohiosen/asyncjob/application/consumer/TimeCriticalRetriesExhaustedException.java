package com.imohiosen.asyncjob.application.consumer;

/**
 * Thrown when all in-memory Resilience4j retries for a time-critical task
 * have been exhausted. The caller should fall back to the normal DB-backed
 * retry path ({@code markFailed} → {@code TaskRetryScheduler} → Kafka).
 */
public class TimeCriticalRetriesExhaustedException extends RuntimeException {

    private final int attemptCount;

    public TimeCriticalRetriesExhaustedException(int attemptCount, Throwable cause) {
        super("Time-critical retries exhausted after " + attemptCount + " attempts: " + cause.getMessage(), cause);
        this.attemptCount = attemptCount;
    }

    /** Total number of attempts that were made (including the initial attempt). */
    public int getAttemptCount() {
        return attemptCount;
    }
}
