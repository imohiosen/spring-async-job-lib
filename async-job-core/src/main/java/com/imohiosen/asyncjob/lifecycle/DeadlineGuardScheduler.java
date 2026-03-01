package com.imohiosen.asyncjob.lifecycle;

import com.imohiosen.asyncjob.repository.JobRepository;
import com.imohiosen.asyncjob.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Periodically sweeps the database for jobs and tasks that have breached
 * their {@code deadline_at} and flags them as <strong>stale</strong>.
 *
 * <p>Stale-flagging is <strong>informational only</strong> — it does not
 * change the task's status or interrupt processing. The consumer continues
 * to run the task to completion, and the final result is always persisted.
 * Downstream systems can query the {@code stale} column for SLA reporting.
 *
 * <p>Registered as a Spring bean via {@code AsyncJobLibraryConfig}.
 * The sweep interval is configurable via {@code asyncjob.deadline.sweep-interval-ms}.
 */
public class DeadlineGuardScheduler {

    private static final Logger log = LoggerFactory.getLogger(DeadlineGuardScheduler.class);

    private final JobRepository  jobRepository;
    private final TaskRepository taskRepository;

    public DeadlineGuardScheduler(JobRepository jobRepository,
                                  TaskRepository taskRepository) {
        this.jobRepository  = jobRepository;
        this.taskRepository = taskRepository;
    }

    /**
     * Runs on the configured interval. Flags stale jobs and tasks without
     * changing their status — the handlers are left to run to completion.
     *
     * <p>Configured via: {@code asyncjob.deadline.sweep-interval-ms} (default: 30000ms)
     */
    @Scheduled(fixedDelayString = "${asyncjob.deadline.sweep-interval-ms:30000}")
    public void sweep() {
        log.debug("Stale guard sweep starting");

        int staleJobs  = jobRepository.flagStaleJobs();
        int staleTasks = taskRepository.flagStaleTasks();

        if (staleJobs > 0 || staleTasks > 0) {
            log.warn("Stale guard flagged {} job(s) and {} task(s) as stale (deadline breached)",
                    staleJobs, staleTasks);
        } else {
            log.debug("Stale guard sweep complete — no breaches found");
        }
    }
}
