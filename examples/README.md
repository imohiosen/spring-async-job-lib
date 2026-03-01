# Async Job Example Application

This is a complete example Spring Boot application that demonstrates the `spring-async-job-starter` library with PostgreSQL, Redis (Redisson), and Kafka.

## Architecture

The example implements a production-ready distributed async job processing system with:

- **PostgreSQL**: Stores job and task state
- **Redis**: Provides distributed locking via Redisson
- **Kafka**: Message queue for task distribution
- **Spring Boot**: Application framework

## Features Demonstrated

1. **Three Task Types**:
   - `EMAIL_NOTIFICATION`: Simulated email sending with retry logic
   - `REPORT_GENERATION`: CPU-intensive work with custom backoff policy
   - `DATA_EXPORT`: Large data exports with progress tracking

2. **Fault Tolerance**:
   - Automatic retries with exponential backoff
   - Per-handler retry policies
   - Distributed locking to prevent duplicate processing
   - Deadline monitoring for stuck tasks

3. **REST API**:
   - Submit jobs with multiple tasks
   - Query job and task status
   - Track progress and results

## Quick Start

### Prerequisites

- **Java 21** or higher
- **Docker** and **Docker Compose**
- **Maven 3.6+**

### Running Tests

The project includes comprehensive integration tests using Testcontainers:

```bash
# Run all tests (starts containers automatically)
mvn test

# Run specific test
mvn test -Dtest=AsyncJobIntegrationTest

# Run with coverage
mvn verify
```

See [src/test/README.md](src/test/README.md) for detailed testing documentation.

### 1. Start Infrastructure Services

```bash
# From the examples directory
docker-compose up -d

# Wait for services to be healthy (approximately 30 seconds)
docker-compose ps
```

This starts:
- PostgreSQL on `localhost:5432`
- Redis on `localhost:6379`
- Kafka on `localhost:9092`
- Zookeeper on `localhost:2181`

### 2. Build the Parent Library

```bash
# From the project root
cd /Users/jaidussel/Downloads/spring-async-job-lib
mvn clean install -DskipTests
```

### 3. Run the Application

```bash
# From the examples directory
cd examples
mvn spring-boot:run
```

The application will start on `http://localhost:8080`

## Usage Examples

### Submit an Email Notification Job

```bash
curl -X POST http://localhost:8080/api/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "jobName": "send-welcome-emails",
    "correlationId": "campaign-123",
    "deadlineHours": 2,
    "tasks": [
      {
        "taskType": "EMAIL_NOTIFICATION",
        "payload": "{\"recipient\":\"user@example.com\",\"subject\":\"Welcome!\",\"body\":\"Thanks for signing up.\"}"
      },
      {
        "taskType": "EMAIL_NOTIFICATION",
        "payload": "{\"recipient\":\"admin@example.com\",\"subject\":\"New User\",\"body\":\"A new user signed up.\"}"
      }
    ]
  }'
```

### Submit a Report Generation Job

```bash
curl -X POST http://localhost:8080/api/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "jobName": "monthly-reports",
    "correlationId": "report-2026-03",
    "deadlineHours": 4,
    "tasks": [
      {
        "taskType": "REPORT_GENERATION",
        "payload": "{\"reportType\":\"SALES\",\"startDate\":\"2026-03-01\",\"endDate\":\"2026-03-31\"}"
      }
    ]
  }'
```

### Submit a Data Export Job

```bash
curl -X POST http://localhost:8080/api/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "jobName": "customer-export",
    "correlationId": "export-001",
    "deadlineHours": 6,
    "tasks": [
      {
        "taskType": "DATA_EXPORT",
        "payload": "{\"entity\":\"customers\",\"format\":\"CSV\",\"recordCount\":5000}"
      }
    ]
  }'
```

### Query Job Status

```bash
# Get job by ID (use the ID returned from the create response)
curl http://localhost:8080/api/jobs/{jobId}

# Get all tasks for a job
curl http://localhost:8080/api/jobs/{jobId}/tasks

# Get a specific task
curl http://localhost:8080/api/jobs/tasks/{taskId}
```

### Example Response

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "jobName": "send-welcome-emails",
  "correlationId": "campaign-123",
  "status": "IN_PROGRESS",
  "createdAt": "2026-03-01T10:00:00Z",
  "updatedAt": "2026-03-01T10:00:05Z",
  "startedAt": "2026-03-01T10:00:01Z",
  "completedAt": null,
  "deadlineAt": "2026-03-01T12:00:00Z",
  "stale": false,
  "totalTasks": 2,
  "pendingTasks": 0,
  "inProgressTasks": 1,
  "completedTasks": 1,
  "failedTasks": 0,
  "deadLetterTasks": 0,
  "metadata": "{}"
}
```

## Configuration

Key configuration properties in `application.yml`:

```yaml
# Database
spring.datasource.url: jdbc:postgresql://localhost:5432/asyncjob
spring.datasource.username: asyncjob
spring.datasource.password: asyncjob123

# Kafka
spring.kafka.bootstrap-servers: localhost:9092
asyncjob.kafka.topic: async-job-tasks

# Redis
spring.data.redis.host: localhost
spring.data.redis.port: 6379

# Async Job Library
asyncjob:
  executor:
    core-pool-size: 8
    max-pool-size: 32
  deadline.sweep-interval-ms: 30000
  retry.sweep-interval-ms: 15000
```

## Task Handlers

Each task handler implements `JobTaskHandler`:

```java
@Component
public class EmailNotificationHandler implements JobTaskHandler {
    
    @Override
    public String taskType() {
        return "EMAIL_NOTIFICATION";
    }
    
    @Override
    public TaskResult handle(JobTask task) {
        // Process the task
        return TaskResult.success("{\"sent\":true}");
    }
    
    @Override
    public int maxAttempts() {
        return 3;  // Retry up to 3 times
    }
}
```

## Monitoring

### Check Infrastructure Health

```bash
# PostgreSQL
docker exec -it async-job-postgres pg_isready -U asyncjob

# Redis
docker exec -it async-job-redis redis-cli ping

# Kafka
docker exec -it async-job-kafka kafka-topics --bootstrap-server localhost:29092 --list
```

### Application Health

```bash
curl http://localhost:8080/actuator/health
```

### View Logs

```bash
# Application logs
mvn spring-boot:run

# Docker logs
docker-compose logs -f postgres
docker-compose logs -f kafka
docker-compose logs -f redis
```

### Database Queries

```bash
# Connect to PostgreSQL
docker exec -it async-job-postgres psql -U asyncjob -d asyncjob

# View jobs
SELECT id, job_name, status, total_tasks, completed_tasks, failed_tasks FROM jobs;

# View tasks
SELECT id, task_type, status, attempt_count, last_error_message FROM job_tasks;

# View failed tasks
SELECT * FROM job_tasks WHERE status = 'FAILED';
```

## Simulated Failures

The example handlers include random failures for testing:

- **EmailNotificationHandler**: 10% failure rate on first attempt
- **ReportGenerationHandler**: 5% failure rate on first attempt

Watch the logs to see automatic retry behavior:

```
DEBUG - Task task=xxx failed, scheduling retry (attempt 1/3)
DEBUG - Using exponential backoff: delay=2000ms
```

## Cleanup

```bash
# Stop the application (Ctrl+C)

# Stop and remove containers
docker-compose down

# Remove volumes (deletes all data)
docker-compose down -v
```

## Project Structure

```
examples/
├── docker-compose.yml              # Infrastructure services
├── init-db.sql                     # Database schema
├── pom.xml                         # Maven dependencies
└── src/main/
    ├── java/com/imohiosen/asyncjob/example/
    │   ├── AsyncJobExampleApplication.java    # Main application
    │   ├── api/
    │   │   ├── JobController.java             # REST endpoints
    │   │   └── dto/                           # Request/Response DTOs
    │   ├── config/
    │   │   └── AsyncJobConfiguration.java     # Library configuration
    │   └── handler/
    │       ├── EmailNotificationHandler.java  # Email task handler
    │       ├── ReportGenerationHandler.java   # Report task handler
    │       └── DataExportHandler.java         # Export task handler
    └── resources/
        └── application.yml                    # Application config
```

## Scaling

The application supports horizontal scaling:

1. **Multiple Application Instances**: Each instance will consume from Kafka
2. **Kafka Consumer Groups**: Tasks are distributed across consumers
3. **Distributed Locking**: Redis prevents duplicate task processing
4. **Database Connection Pooling**: HikariCP for efficient connections

## Troubleshooting

### Kafka Connection Issues

```bash
# Verify Kafka is accessible
kafka-console-consumer --bootstrap-server localhost:9092 --topic async-job-tasks --from-beginning
```

### Database Connection Issues

```bash
# Check PostgreSQL is running
docker-compose ps postgres

# Test connection
docker exec -it async-job-postgres psql -U asyncjob -d asyncjob -c "SELECT 1"
```

### Redis Connection Issues

```bash
# Check Redis is running
docker-compose ps redis

# Test connection
docker exec -it async-job-redis redis-cli ping
```

## Next Steps

- Add custom task handlers for your business logic
- Configure production-grade infrastructure (managed services)
- Add monitoring with Prometheus/Grafana
- Implement custom error handling and alerting
- Add integration tests with Testcontainers

## License

This example is part of the spring-async-job-lib project.
