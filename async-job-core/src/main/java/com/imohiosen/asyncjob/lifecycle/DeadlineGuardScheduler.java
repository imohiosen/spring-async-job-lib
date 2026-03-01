package com.imohiosen.asyncjob.lifecycle;

import com.imohiosen.asyncjob.repository.JobRepository;
import com.imohiosen.asyncjob.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Periodically sweeps the database for jobs and tasks that have breached
 * their {@code deadline_at} without reaching a terminal state, and flags
 * them as timed out.
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
     * Runs on the configured interval. Both UPDATE statements execute in a single
     * transaction to avoid partial flagging.
     *
     * <p>Configured via: {@code asyncjob.deadline.sweep-interval-ms} (default: 30000ms)
     */
    @Scheduled(fixedDelayString = "${asyncjob.deadline.sweep-interval-ms:30000}")
    public void sweep() {
        log.debug("Deadline guard sweep starting");

        int timedOutJobs  = jobRepository.flagTimedOutJobs();
        int timedOutTasks = taskRepository.flagTimedOutTasks();

        if (timedOutJobs > 0 || timedOutTasks > 0) {
            log.warn("Deadline guard flagged {} job(s) and {} task(s) as timed out",
                    timedOutJobs, timedOutTasks);
        } else {
            log.debug("Deadline guard sweep complete — no violations found");
        }
    }
}
