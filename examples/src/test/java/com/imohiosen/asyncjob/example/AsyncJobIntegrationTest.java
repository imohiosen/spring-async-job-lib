package com.imohiosen.asyncjob.example;

import com.imohiosen.asyncjob.application.service.JobSubmissionService;
import com.imohiosen.asyncjob.domain.Job;
import com.imohiosen.asyncjob.domain.JobStatus;
import com.imohiosen.asyncjob.domain.JobTask;
import com.imohiosen.asyncjob.domain.TaskStatus;
import com.imohiosen.asyncjob.port.repository.JobRepository;
import com.imohiosen.asyncjob.port.repository.TaskRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test that verifies the complete async job processing flow
 * using Testcontainers for PostgreSQL, Redis, and Kafka.
 */
class AsyncJobIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private JobSubmissionService submissionService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private TaskRepository taskRepository;

    @Value("${asyncjob.kafka.topic}")
    private String kafkaTopic;

    @Test
    void shouldSubmitAndProcessEmailNotificationJob() {
        // Given - Create a job with email tasks
        UUID jobId = UUID.randomUUID();
        Job job = createJob(jobId, "test-email-job");

        String emailPayload = "{\"recipient\":\"test@example.com\",\"subject\":\"Test\",\"body\":\"Hello\"}";
        JobTask task = createTask(UUID.randomUUID(), jobId, "EMAIL_NOTIFICATION", emailPayload);

        // When - Submit the job
        submissionService.submit(job, List.of(task));

        // Then - Verify job is persisted
        Job persistedJob = jobRepository.findById(jobId).orElseThrow();
        assertThat(persistedJob.status()).isEqualTo(JobStatus.PENDING);
        assertThat(persistedJob.totalTasks()).isEqualTo(1);

        // And - Wait for task to be processed
        await()
                .atMost(15, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    JobTask processedTask = taskRepository.findById(task.id()).orElseThrow();
                    assertThat(processedTask.status()).isEqualTo(TaskStatus.COMPLETED);
                    assertThat(processedTask.result()).contains("sent");
                });

        // And - Verify job status is updated
        await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    Job completedJob = jobRepository.findById(jobId).orElseThrow();
                    assertThat(completedJob.completedTasks()).isEqualTo(1);
                });
    }

    @Test
    void shouldProcessMultipleTasksInParallel() {
        // Given - Create a job with multiple tasks
        UUID jobId = UUID.randomUUID();
        Job job = createJob(jobId, "multi-task-job");

        String emailPayload1 = "{\"recipient\":\"user1@example.com\",\"subject\":\"Test\",\"body\":\"Hello\"}";
        String emailPayload2 = "{\"recipient\":\"user2@example.com\",\"subject\":\"Test\",\"body\":\"Hello\"}";
        String reportPayload = "{\"reportType\":\"SALES\",\"startDate\":\"2026-01-01\",\"endDate\":\"2026-01-31\"}";

        JobTask task1 = createTask(UUID.randomUUID(), jobId, "EMAIL_NOTIFICATION", emailPayload1);
        JobTask task2 = createTask(UUID.randomUUID(), jobId, "EMAIL_NOTIFICATION", emailPayload2);
        JobTask task3 = createTask(UUID.randomUUID(), jobId, "REPORT_GENERATION", reportPayload);

        // When - Submit the job
        submissionService.submit(job, List.of(task1, task2, task3));

        // Then - Wait for all tasks to complete
        await()
                .atMost(20, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    List<JobTask> tasks = taskRepository.findByJobId(jobId);
                    long completedCount = tasks.stream()
                            .filter(t -> t.status() == TaskStatus.COMPLETED)
                            .count();
                    assertThat(completedCount).isEqualTo(3);
                });

        // And - Verify job is completed
        await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    Job completedJob = jobRepository.findById(jobId).orElseThrow();
                    assertThat(completedJob.completedTasks()).isEqualTo(3);
                });
    }

    @Test
    void shouldHandleDataExportTasks() {
        // Given
        UUID jobId = UUID.randomUUID();
        Job job = createJob(jobId, "export-job");

        String exportPayload = "{\"entity\":\"customers\",\"format\":\"CSV\",\"recordCount\":1000}";
        JobTask task = createTask(UUID.randomUUID(), jobId, "DATA_EXPORT", exportPayload);

        // When
        submissionService.submit(job, List.of(task));

        // Then
        await()
                .atMost(15, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    JobTask processedTask = taskRepository.findById(task.id()).orElseThrow();
                    assertThat(processedTask.status()).isEqualTo(TaskStatus.COMPLETED);
                    assertThat(processedTask.result()).contains("exported");
                    assertThat(processedTask.result()).contains("CSV");
                });
    }

    @Test
    void shouldPersistTaskResultsCorrectly() {
        // Given
        UUID jobId = UUID.randomUUID();
        Job job = createJob(jobId, "result-test-job");

        String reportPayload = "{\"reportType\":\"INVENTORY\",\"startDate\":\"2026-02-01\",\"endDate\":\"2026-02-28\"}";
        JobTask task = createTask(UUID.randomUUID(), jobId, "REPORT_GENERATION", reportPayload);

        // When
        submissionService.submit(job, List.of(task));

        // Then - Wait and verify result contains expected data
        await()
                .atMost(15, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    JobTask processedTask = taskRepository.findById(task.id()).orElseThrow();
                    assertThat(processedTask.status()).isEqualTo(TaskStatus.COMPLETED);
                    assertThat(processedTask.result()).isNotNull();
                    assertThat(processedTask.result()).contains("reportType");
                    assertThat(processedTask.result()).contains("INVENTORY");
                    assertThat(processedTask.result()).contains("url");
                    assertThat(processedTask.completedAt()).isNotNull();
                });
    }

    @Test
    void shouldUpdateJobCountersAsTasksComplete() {
        // Given
        UUID jobId = UUID.randomUUID();
        Job job = createJob(jobId, "counter-test-job");

        JobTask task1 = createTask(UUID.randomUUID(), jobId, "EMAIL_NOTIFICATION",
                "{\"recipient\":\"a@example.com\",\"subject\":\"Test\",\"body\":\"Hi\"}");
        JobTask task2 = createTask(UUID.randomUUID(), jobId, "EMAIL_NOTIFICATION",
                "{\"recipient\":\"b@example.com\",\"subject\":\"Test\",\"body\":\"Hi\"}");

        // When
        submissionService.submit(job, List.of(task1, task2));

        // Then - Initially all pending
        Job initialJob = jobRepository.findById(jobId).orElseThrow();
        assertThat(initialJob.pendingTasks()).isEqualTo(2);
        assertThat(initialJob.completedTasks()).isEqualTo(0);

        // Wait for completion
        await()
                .atMost(20, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    Job updatedJob = jobRepository.findById(jobId).orElseThrow();
                    assertThat(updatedJob.completedTasks()).isEqualTo(2);
                    assertThat(updatedJob.pendingTasks()).isEqualTo(0);
                });
    }

    // ── Helper Methods ────────────────────────────────────────────────────────

    private Job createJob(UUID jobId, String jobName) {
        OffsetDateTime now = OffsetDateTime.now();
        return new Job(
                jobId,
                jobName,
                null, // correlationId
                JobStatus.PENDING,
                now,
                now,
                null, // startedAt
                null, // completedAt
                now.plusHours(2), // deadlineAt
                null, // scheduledAt
                false,
                0, 0, 0, 0, 0, 0, // task counters (will be set by submission)
                "{}",
                false
        );
    }

    private JobTask createTask(UUID taskId, UUID jobId, String taskType, String payload) {
        OffsetDateTime now = OffsetDateTime.now();
        return new JobTask(
                taskId,
                jobId,
                taskType,
                kafkaTopic,
                null, null,
                TaskStatus.PENDING,
                now,
                now,
                null, null,
                now.plusHours(2),
                false,
                0, null, null,
                1000L, 2.0, 3_600_000L,
                null, null,
                null, null, null,
                payload,
                null,
                false, 0, 0L, 1.0, 0L, 0L
        );
    }
}
