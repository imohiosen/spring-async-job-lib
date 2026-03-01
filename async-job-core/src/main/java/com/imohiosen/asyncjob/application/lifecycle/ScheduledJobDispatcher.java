package com.imohiosen.asyncjob.application.lifecycle;

import com.imohiosen.asyncjob.application.service.JobSubmissionService;
import com.imohiosen.asyncjob.domain.Job;
import com.imohiosen.asyncjob.port.repository.JobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Periodically scans for {@code SCHEDULED} jobs whose {@code scheduledAt} time
 * has arrived and dispatches their tasks to the messaging system.
 *
 * <p>Configured via {@code asyncjob.schedule.sweep-interval-ms} (default: 10 000 ms)
 * and {@code asyncjob.schedule.batch-size} (default: 50).
 */
public class ScheduledJobDispatcher {

    private static final Logger log = LoggerFactory.getLogger(ScheduledJobDispatcher.class);

    private final JobRepository       jobRepository;
    private final JobSubmissionService submissionService;
    private final int                 batchSize;

    public ScheduledJobDispatcher(JobRepository jobRepository,
                                  JobSubmissionService submissionService,
                                  int batchSize) {
        this.jobRepository    = jobRepository;
        this.submissionService = submissionService;
        this.batchSize        = batchSize;
    }

    /**
     * Sweeps for scheduled jobs that are due and dispatches them.
     */
    public void sweep() {
        List<Job> dueJobs = jobRepository.findScheduledJobsDue(batchSize);

        if (dueJobs.isEmpty()) {
            log.debug("Schedule sweep — no due scheduled jobs found");
            return;
        }

        int dispatched = 0;
        for (Job job : dueJobs) {
            try {
                submissionService.dispatch(job);
                dispatched++;
            } catch (Exception e) {
                log.error("Failed to dispatch scheduled job={}: {}", job.id(), e.getMessage(), e);
            }
        }

        log.info("Schedule sweep dispatched {}/{} jobs (batch limit {})",
                dispatched, dueJobs.size(), batchSize);
    }
}
