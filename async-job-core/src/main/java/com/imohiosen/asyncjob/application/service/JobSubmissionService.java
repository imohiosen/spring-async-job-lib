package com.imohiosen.asyncjob.application.service;

import com.imohiosen.asyncjob.domain.BackoffPolicy;
import com.imohiosen.asyncjob.domain.Job;
import com.imohiosen.asyncjob.domain.JobStatus;
import com.imohiosen.asyncjob.domain.JobSubmissionRequest;
import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskStatus;
import com.imohiosen.asyncjob.domain.TimeCriticalPolicy;
import com.imohiosen.asyncjob.port.messaging.JobMessage;
import com.imohiosen.asyncjob.port.messaging.JobMessageProducer;
import com.imohiosen.asyncjob.port.repository.JobRepository;
import com.imohiosen.asyncjob.port.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Application service that accepts a {@link JobSubmissionRequest} and orchestrates
 * either immediate dispatch or deferred scheduling.
 *
 * <h2>Immediate path</h2>
 * When {@link JobSubmissionRequest#isImmediate()} returns {@code true}:
 * <ol>
 *   <li>Inserts a {@link Job} row (status {@code PENDING})</li>
 *   <li>Inserts {@link JobTask} rows for each payload</li>
 *   <li>Publishes a Kafka message per task</li>
 *   <li>Updates job counters and marks the job {@code IN_PROGRESS}</li>
 * </ol>
 *
 * <h2>Deferred (scheduled) path</h2>
 * When {@link JobSubmissionRequest#scheduledAt()} is in the future:
 * <ol>
 *   <li>Inserts a {@link Job} row (status {@code SCHEDULED}, {@code scheduledAt} set)</li>
 *   <li>Inserts {@link JobTask} rows for each payload (no Kafka publish)</li>
 *   <li>Updates job counters</li>
 * </ol>
 * The {@link com.imohiosen.asyncjob.application.lifecycle.ScheduledJobDispatcher}
 * sweep picks up the job once its scheduled time arrives.
 */
public class JobSubmissionService {

    private static final Logger log = LoggerFactory.getLogger(JobSubmissionService.class);

    private final JobRepository      jobRepository;
    private final TaskRepository     taskRepository;
    private final JobMessageProducer messageProducer;

    public JobSubmissionService(JobRepository jobRepository,
                                TaskRepository taskRepository,
                                JobMessageProducer messageProducer) {
        this.jobRepository  = jobRepository;
        this.taskRepository = taskRepository;
        this.messageProducer = messageProducer;
    }

    /**
     * Submits a job for execution.
     *
     * @param request the submission request
     * @return the generated job id
     */
    public UUID submit(JobSubmissionRequest request) {
        UUID jobId       = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now();
        boolean immediate = request.isImmediate();

        log.info("Submitting job={} name={} immediate={} scheduledAt={}",
                jobId, request.jobName(), immediate, request.scheduledAt());

        OffsetDateTime deadline = now.plusNanos(request.deadlineMs() * 1_000_000L);
        JobStatus initialStatus = immediate ? JobStatus.PENDING : JobStatus.SCHEDULED;

        // 1. Insert the parent job row
        Job job = new Job(
                jobId, request.jobName(), request.correlationId(), initialStatus,
                now, now, null, null, deadline,
                immediate ? null : request.scheduledAt(),
                false, 0, 0, 0, 0, 0, 0, request.metadata(),
                request.timeCritical());
        jobRepository.insert(job);

        List<String> payloads = request.payloads();
        if (payloads.isEmpty()) {
            log.info("Job={} has no tasks — marking COMPLETED immediately", jobId);
            jobRepository.updateStatus(jobId, JobStatus.COMPLETED);
            return jobId;
        }

        // 2. Insert task rows
        BackoffPolicy policy = request.effectiveBackoffPolicy();
        TimeCriticalPolicy tcPolicy = request.effectiveTimeCriticalPolicy();
        OffsetDateTime taskDeadline = now.plusNanos(request.effectiveTaskDeadlineMs() * 1_000_000L);
        String destination = request.destination();
        String taskType = request.taskType();

        int produced = 0;
        for (String payload : payloads) {
            UUID taskId = UUID.randomUUID();

            JobTask task = new JobTask(
                    taskId, jobId, taskType, destination,
                    null, null, TaskStatus.PENDING, now, now,
                    null, null, taskDeadline, false, 0,
                    null, null,
                    policy.baseIntervalMs(), policy.multiplier(), policy.maxDelayMs(),
                    null, null, null, null, null, payload, null,
                    request.timeCritical(),
                    tcPolicy != null ? tcPolicy.maxAttempts() : 0,
                    tcPolicy != null ? tcPolicy.baseIntervalMs() : 0L,
                    tcPolicy != null ? tcPolicy.multiplier() : 1.0,
                    tcPolicy != null ? tcPolicy.maxDelayMs() : 0L,
                    tcPolicy != null ? tcPolicy.dbSyncIntervalMs() : 0L);
            taskRepository.insert(task);

            // 3. Publish to messaging system (only for immediate jobs)
            if (immediate) {
                messageProducer.publish(destination,
                        new JobMessage(taskId, jobId, taskType, payload));
            }
            produced++;
        }

        // 4. Update counters and set status
        jobRepository.updateCounters(jobId);
        if (immediate) {
            jobRepository.tryMarkStarted(jobId);
            log.info("Job={} started immediately with {} tasks", jobId, produced);
        } else {
            log.info("Job={} scheduled for {} with {} tasks", jobId, request.scheduledAt(), produced);
        }

        return jobId;
    }

    /**
     * Dispatches all tasks for a previously-scheduled job by publishing them
     * to the messaging system and transitioning the job to {@code IN_PROGRESS}.
     *
     * <p>Called by {@link com.imohiosen.asyncjob.application.lifecycle.ScheduledJobDispatcher}
     * when the job's scheduled time arrives.
     *
     * @param job the scheduled job to dispatch
     * @return number of tasks published
     */
    public int dispatch(Job job) {
        List<JobTask> tasks = taskRepository.findTasksByJobId(job.id());
        String destination = null;
        int published = 0;

        for (JobTask task : tasks) {
            if (task.status() != TaskStatus.PENDING) {
                continue;
            }
            destination = task.destination();
            messageProducer.publish(destination,
                    new JobMessage(task.id(), task.jobId(), task.taskType(), task.payload()));
            published++;
        }

        jobRepository.tryMarkStarted(job.id());
        log.info("Dispatched {} tasks for scheduled job={}", published, job.id());
        return published;
    }
}
