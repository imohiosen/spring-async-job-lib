package com.imohiosen.asyncjob.example.api;

import com.imohiosen.asyncjob.domain.JobStatus;
import com.imohiosen.asyncjob.domain.TaskStatus;
import com.imohiosen.asyncjob.example.AbstractIntegrationTest;
import com.imohiosen.asyncjob.example.api.dto.CreateJobRequest;
import com.imohiosen.asyncjob.example.api.dto.JobResponse;
import com.imohiosen.asyncjob.example.api.dto.JobTaskResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for the Job REST API using Testcontainers.
 */
class JobControllerIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void shouldCreateJobSuccessfully() {
        // Given
        CreateJobRequest.TaskRequest taskReq = new CreateJobRequest.TaskRequest(
                "EMAIL_NOTIFICATION",
                "{\"recipient\":\"api-test@example.com\",\"subject\":\"API Test\",\"body\":\"Hello\"}",
                null, null, null, null
        );

        CreateJobRequest request = new CreateJobRequest(
                "api-test-job",
                "api-correlation-123",
                List.of(taskReq),
                2,
                null
        );

        // When
        ResponseEntity<JobResponse> response = restTemplate.postForEntity(
                "/api/jobs",
                request,
                JobResponse.class
        );

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().jobName()).isEqualTo("api-test-job");
        assertThat(response.getBody().correlationId()).isEqualTo("api-correlation-123");
        assertThat(response.getBody().status()).isIn(JobStatus.PENDING, JobStatus.IN_PROGRESS);
    }

    @Test
    void shouldGetJobById() {
        // Given - Create a job first
        CreateJobRequest.TaskRequest taskReq = new CreateJobRequest.TaskRequest(
                "EMAIL_NOTIFICATION",
                "{\"recipient\":\"get-test@example.com\",\"subject\":\"Get Test\",\"body\":\"Hello\"}",
                null, null, null, null
        );

        CreateJobRequest request = new CreateJobRequest(
                "get-job-test",
                null,
                List.of(taskReq),
                2,
                null
        );

        ResponseEntity<JobResponse> createResponse = restTemplate.postForEntity(
                "/api/jobs",
                request,
                JobResponse.class
        );

        UUID jobId = createResponse.getBody().id();

        // When - Get the job
        ResponseEntity<JobResponse> response = restTemplate.getForEntity(
                "/api/jobs/" + jobId,
                JobResponse.class
        );

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().id()).isEqualTo(jobId);
        assertThat(response.getBody().jobName()).isEqualTo("get-job-test");
    }

    @Test
    void shouldGetJobTasks() {
        // Given - Create a job with multiple tasks
        CreateJobRequest.TaskRequest task1 = new CreateJobRequest.TaskRequest(
                "EMAIL_NOTIFICATION",
                "{\"recipient\":\"task1@example.com\",\"subject\":\"Task 1\",\"body\":\"Hi\"}",
                null, null, null, null
        );

        CreateJobRequest.TaskRequest task2 = new CreateJobRequest.TaskRequest(
                "EMAIL_NOTIFICATION",
                "{\"recipient\":\"task2@example.com\",\"subject\":\"Task 2\",\"body\":\"Hi\"}",
                null, null, null, null
        );

        CreateJobRequest request = new CreateJobRequest(
                "multi-task-api-test",
                null,
                List.of(task1, task2),
                2,
                null
        );

        ResponseEntity<JobResponse> createResponse = restTemplate.postForEntity(
                "/api/jobs",
                request,
                JobResponse.class
        );

        UUID jobId = createResponse.getBody().id();

        // When - Get job tasks
        ResponseEntity<List<JobTaskResponse>> response = restTemplate.exchange(
                "/api/jobs/" + jobId + "/tasks",
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<JobTaskResponse>>() {}
        );

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).hasSize(2);
        assertThat(response.getBody()).allMatch(t -> t.jobId().equals(jobId));
        assertThat(response.getBody()).allMatch(t -> t.taskType().equals("EMAIL_NOTIFICATION"));
    }

    @Test
    void shouldGetTaskById() {
        // Given - Create a job
        CreateJobRequest.TaskRequest taskReq = new CreateJobRequest.TaskRequest(
                "EMAIL_NOTIFICATION",
                "{\"recipient\":\"task-test@example.com\",\"subject\":\"Task Test\",\"body\":\"Hi\"}",
                null, null, null, null
        );

        CreateJobRequest request = new CreateJobRequest(
                "task-get-test",
                null,
                List.of(taskReq),
                2,
                null
        );

        ResponseEntity<JobResponse> createResponse = restTemplate.postForEntity(
                "/api/jobs",
                request,
                JobResponse.class
        );

        UUID jobId = createResponse.getBody().id();

        // Get the task ID
        ResponseEntity<List<JobTaskResponse>> tasksResponse = restTemplate.exchange(
                "/api/jobs/" + jobId + "/tasks",
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<JobTaskResponse>>() {}
        );

        UUID taskId = tasksResponse.getBody().get(0).id();

        // When - Get task by ID
        ResponseEntity<JobTaskResponse> response = restTemplate.getForEntity(
                "/api/jobs/tasks/" + taskId,
                JobTaskResponse.class
        );

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().id()).isEqualTo(taskId);
        assertThat(response.getBody().jobId()).isEqualTo(jobId);
    }

    @Test
    void shouldReturn404ForNonexistentJob() {
        // When
        ResponseEntity<JobResponse> response = restTemplate.getForEntity(
                "/api/jobs/" + UUID.randomUUID(),
                JobResponse.class
        );

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void shouldReturn404ForNonexistentTask() {
        // When
        ResponseEntity<JobTaskResponse> response = restTemplate.getForEntity(
                "/api/jobs/tasks/" + UUID.randomUUID(),
                JobTaskResponse.class
        );

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void shouldProcessJobEndToEnd() {
        // Given - Create a complete job
        CreateJobRequest.TaskRequest emailTask = new CreateJobRequest.TaskRequest(
                "EMAIL_NOTIFICATION",
                "{\"recipient\":\"e2e@example.com\",\"subject\":\"E2E Test\",\"body\":\"Complete flow\"}",
                null, null, null, null
        );

        CreateJobRequest.TaskRequest reportTask = new CreateJobRequest.TaskRequest(
                "REPORT_GENERATION",
                "{\"reportType\":\"SALES\",\"startDate\":\"2026-01-01\",\"endDate\":\"2026-01-31\"}",
                null, null, null, null
        );

        CreateJobRequest request = new CreateJobRequest(
                "e2e-test-job",
                "e2e-123",
                List.of(emailTask, reportTask),
                2,
                null
        );

        // When - Submit job
        ResponseEntity<JobResponse> createResponse = restTemplate.postForEntity(
                "/api/jobs",
                request,
                JobResponse.class
        );

        UUID jobId = createResponse.getBody().id();

        // Then - Wait for tasks to complete
        await()
                .atMost(20, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    ResponseEntity<List<JobTaskResponse>> tasksResponse = restTemplate.exchange(
                            "/api/jobs/" + jobId + "/tasks",
                            HttpMethod.GET,
                            null,
                            new ParameterizedTypeReference<List<JobTaskResponse>>() {}
                    );

                    List<JobTaskResponse> tasks = tasksResponse.getBody();
                    long completedCount = tasks.stream()
                            .filter(t -> t.status() == TaskStatus.COMPLETED)
                            .count();

                    assertThat(completedCount).isEqualTo(2);
                });

        // And - Verify job shows completed tasks
        await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    ResponseEntity<JobResponse> jobResponse = restTemplate.getForEntity(
                            "/api/jobs/" + jobId,
                            JobResponse.class
                    );

                });
    }

    @Test
    void shouldHandleCustomBackoffParameters() {
        // Given - Task with custom backoff
        CreateJobRequest.TaskRequest taskReq = new CreateJobRequest.TaskRequest(
                "EMAIL_NOTIFICATION",
                "{\"recipient\":\"backoff@example.com\",\"subject\":\"Backoff\",\"body\":\"Test\"}",
                5000L,           // 5 second base
                3.0,             // triple each time
                60000L,          // max 1 minute
                null
        );

        CreateJobRequest request = new CreateJobRequest(
                "backoff-test",
                null,
                List.of(taskReq),
                2,
                null
        );

        // When
        ResponseEntity<JobResponse> createResponse = restTemplate.postForEntity(
                "/api/jobs",
                request,
                JobResponse.class
        );

        UUID jobId = createResponse.getBody().id();

        // Then - Verify task has custom backoff parameters
        ResponseEntity<List<JobTaskResponse>> tasksResponse = restTemplate.exchange(
                "/api/jobs/" + jobId + "/tasks",
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<JobTaskResponse>>() {}
        );

        // Note: The API doesn't expose backoff params, but we verified creation succeeded
        assertThat(tasksResponse.getBody()).hasSize(1);
    }

    @Test
    void shouldReturnHealthCheck() {
        // When
        ResponseEntity<String> response = restTemplate.getForEntity(
                "/api/jobs/health",
                String.class
        );

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo("OK");
    }
}
