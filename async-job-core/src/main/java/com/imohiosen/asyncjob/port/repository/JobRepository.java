package com.imohiosen.asyncjob.port.repository;

import com.imohiosen.asyncjob.domain.Job;
import com.imohiosen.asyncjob.domain.JobStatus;

import java.util.Optional;
import java.util.UUID;

/**
 * Port for persisting and querying {@link Job} aggregates.
 *
 * <p>Implementations may target any data store: PostgreSQL (JDBC),
 * MongoDB, MySQL, etc.
 */
public interface JobRepository {

    void insert(Job job);

    Optional<Job> findById(UUID id);

    void updateStatus(UUID id, JobStatus status);

    void markStarted(UUID id);

    void markCompleted(UUID id);

    /**
     * Atomically recalculates all task counters for a job by querying child task statuses.
     * Called after every task state transition.
     */
    void updateCounters(UUID jobId);

    /**
     * Flags all jobs that have breached their deadline as stale.
     * Does NOT change the job's status — child tasks continue running.
     *
     * @return number of jobs flagged stale
     */
    int flagStaleJobs();
}
