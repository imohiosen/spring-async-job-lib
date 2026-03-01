package com.imohiosen.asyncjob.port.repository;

import com.imohiosen.asyncjob.domain.Job;
import com.imohiosen.asyncjob.domain.JobStatus;

import java.util.List;
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

    /**
     * Atomically transitions a job from {@code SCHEDULED} to {@code IN_PROGRESS}.
     *
     * <p>This is a compare-and-swap (CAS) operation. It must only succeed when
     * the job's current status is {@code SCHEDULED}. If another node has already
     * transitioned the job, this call returns {@code false}.
     *
     * <h3>Implementation guidance</h3>
     * <ul>
     *   <li><strong>JDBC/SQL:</strong> {@code UPDATE … SET status='IN_PROGRESS' WHERE id=? AND status='SCHEDULED'}, return {@code affected > 0}</li>
     *   <li><strong>MongoDB:</strong> {@code findOneAndUpdate({_id:id, status:'SCHEDULED'}, {$set:{status:'IN_PROGRESS'}})}, return {@code result != null}</li>
     *   <li><strong>DynamoDB:</strong> {@code UpdateItem} with {@code ConditionExpression: #s = :scheduled}</li>
     * </ul>
     *
     * @param id the job id
     * @return {@code true} if the transition succeeded; {@code false} if the job was already started
     */
    boolean tryMarkStarted(UUID id);

    void markCompleted(UUID id);

    /**
     * Flags all jobs that have breached their deadline as stale.
     * Does NOT change the job's status — child tasks continue running.
     *
     * @return number of jobs flagged stale
     */
    int flagStaleJobs();

    /**
     * Atomically claims and returns scheduled jobs whose {@code scheduledAt}
     * time has arrived.
     *
     * <p><strong>Exclusive-claim contract:</strong> rows returned by this method
     * must <em>not</em> be returned by any concurrent call from another node or
     * thread. This is what prevents duplicate dispatching in multi-instance
     * deployments.
     *
     * <h3>Implementation guidance</h3>
     * <ul>
     *   <li><strong>JDBC/SQL (PostgreSQL):</strong>
     *       {@code SELECT … WHERE status='SCHEDULED' AND scheduled_at <= NOW()
     *       ORDER BY scheduled_at LIMIT ? FOR UPDATE SKIP LOCKED}</li>
     *   <li><strong>MongoDB:</strong> loop calling
     *       {@code findOneAndUpdate({status:'SCHEDULED', scheduledAt:{$lte:now}},
     *       {$set:{status:'DISPATCHING'}})} up to {@code limit} times</li>
     *   <li><strong>DynamoDB:</strong> query a GSI on status + scheduledAt, then
     *       conditionally update each item's status to {@code DISPATCHING}</li>
     * </ul>
     *
     * @param limit maximum number of jobs to claim
     * @return exclusively claimed due scheduled jobs (disjoint across callers)
     */
    List<Job> claimScheduledJobsDue(int limit);
}
