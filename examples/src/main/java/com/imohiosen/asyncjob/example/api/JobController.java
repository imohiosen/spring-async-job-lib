package com.imohiosen.asyncjob.example.api;

import com.imohiosen.asyncjob.application.service.JobSubmissionService;
import com.imohiosen.asyncjob.domain.Job;
import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.example.api.dto.CreateJobRequest;
import com.imohiosen.asyncjob.example.api.dto.JobResponse;
import com.imohiosen.asyncjob.example.api.dto.JobTaskResponse;
import com.imohiosen.asyncjob.port.repository.JobRepository;
import com.imohiosen.asyncjob.port.repository.TaskRepository;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * REST API controller for managing async jobs.
 */
@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private static final Logger log = LoggerFactory.getLogger(JobController.class);

    private final JobSubmissionService submissionService;
    private final JobRepository jobRepository;
    private final TaskRepository taskRepository;
    private final String kafkaTopic;

    public JobController(
            JobSubmissionService submissionService,
            JobRepository jobRepository,
            TaskRepository taskRepository,
            @Value("${asyncjob.kafka.topic}") String kafkaTopic) {
        this.submissionService = submissionService;
        this.jobRepository = jobRepository;
        this.taskRepository = taskRepository;
        this.kafkaTopic = kafkaTopic;
    }

    /**
     * Submit a new job with tasks.
     */
    @PostMapping
    public ResponseEntity<JobResponse> createJob(@Valid @RequestBody CreateJobRequest request) {
        log.info("Creating job: name={} taskCount={}", request.jobName(), request.tasks().size());

        try {
            // Create Job
            Job job = new Job(
                    UUID.randomUUID(),
                    request.jobName(),
                    request.correlationId(),
                    com.imohiosen.asyncjob.domain.JobStatus.PENDING,
                    OffsetDateTime.now(),
                    OffsetDateTime.now(),
                    null,
                    null,
                    OffsetDateTime.now().plusHours(request.deadlineHours()),
                    null,
                    false,
                    request.tasks().size(),
                    request.tasks().size(),
                    0, 0, 0, 0,
                    request.metadata(),
                    false
            );

            // Create tasks
            List<JobTask> tasks = request.tasks().stream()
                    .map(taskReq -> new JobTask(
                            UUID.randomUUID(),
                            job.id(),
                            taskReq.taskType(),
                            kafkaTopic,
                            null, null,
                            com.imohiosen.asyncjob.domain.TaskStatus.PENDING,
                            OffsetDateTime.now(),
                            OffsetDateTime.now(),
                            null, null,
                            OffsetDateTime.now().plusHours(request.deadlineHours()),
                            false,
                            0, null, null,
                            taskReq.backoffBaseMs(),
                            taskReq.backoffMultiplier(),
                            taskReq.backoffMaxMs(),
                            null, null,
                            null, null, null,
                            taskReq.payload(),
                            null,
                            false, 0, 0L, 1.0, 0L, 0L
                    ))
                    .collect(Collectors.toList());

            // Submit job and tasks
            submissionService.submit(job, tasks);

            log.info("Successfully created job: id={} taskCount={}", job.id(), tasks.size());

            return ResponseEntity.status(HttpStatus.CREATED)
                    .body(JobResponse.fromJob(job));

        } catch (Exception e) {
            log.error("Failed to create job: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Get job by ID.
     */
    @GetMapping("/{jobId}")
    public ResponseEntity<JobResponse> getJob(@PathVariable UUID jobId) {
        return jobRepository.findById(jobId)
                .map(JobResponse::fromJob)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Get all tasks for a job.
     */
    @GetMapping("/{jobId}/tasks")
    public ResponseEntity<List<JobTaskResponse>> getJobTasks(@PathVariable UUID jobId) {
        List<JobTask> tasks = taskRepository.findByJobId(jobId);
        List<JobTaskResponse> response = tasks.stream()
                .map(JobTaskResponse::fromTask)
                .collect(Collectors.toList());
        return ResponseEntity.ok(response);
    }

    /**
     * Get task by ID.
     */
    @GetMapping("/tasks/{taskId}")
    public ResponseEntity<JobTaskResponse> getTask(@PathVariable UUID taskId) {
        return taskRepository.findById(taskId)
                .map(JobTaskResponse::fromTask)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Health check endpoint.
     */
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("OK");
    }
}
