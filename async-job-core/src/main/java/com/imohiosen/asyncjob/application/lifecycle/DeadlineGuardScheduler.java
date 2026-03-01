package com.imohiosen.asyncjob.application.lifecycle;

import com.imohiosen.asyncjob.port.repository.JobRepository;
import com.imohiosen.asyncjob.port.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically scans for jobs and tasks that have breached their deadline
 * and flags them as <strong>stale</strong>.
 *
 * <p>The guard <strong>does NOT change task or job status</strong>. The handler
 * continues running to completion. The stale flag is informational — it lets
 * operators and dashboards surface jobs that are taking longer than expected.
 *
 * <p>Because the guard only sets a boolean flag (no status mutation), there is
 * no race with the fence-token-protected writes executed by the consumer.
 */
public class DeadlineGuardScheduler {

    private static final Logger log = LoggerFactory.getLogger(DeadlineGuardScheduler.class);

    private final JobRepository  jobRepository;
    private final TaskRepository taskRepository;

    public DeadlineGuardScheduler(JobRepository jobRepository, TaskRepository taskRepository) {
        this.jobRepository  = jobRepository;
        this.taskRepository = taskRepository;
    }

    /**
     * Sweeps for stale jobs and tasks. Run on a fixed interval configured via
     * {@code asyncjob.deadline.sweep-interval-ms} (default: 30 000ms).
     */
    public void sweep() {
        int staleJobs  = jobRepository.flagStaleJobs();
        int staleTasks = taskRepository.flagStaleTasks();

        if (staleJobs > 0 || staleTasks > 0) {
            log.warn("Stale guard flagged {} job(s) and {} task(s) as stale", staleJobs, staleTasks);
        } else {
            log.debug("Stale guard sweep — nothing to flag");
        }
    }
}
