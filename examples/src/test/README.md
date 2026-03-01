# Async Job Example - Testcontainers Integration Tests

This directory contains comprehensive integration tests using [Testcontainers](https://testcontainers.com/) to verify the complete async job processing flow with real PostgreSQL, Redis, and Kafka instances.

## Test Coverage

### Infrastructure Tests
- **TestcontainersConfigurationTest**: Verifies containers are running and database schema is correct
- **JobHandlerRegistryIntegrationTest**: Tests Spring bean wiring and handler registration

### Handler Unit Tests  
- **EmailNotificationHandlerTest**: Tests email task processing
- **ReportGenerationHandlerTest**: Tests report generation with custom backoff
- **DataExportHandlerTest**: Tests data export tasks

### Integration Tests
- **AsyncJobIntegrationTest**: End-to-end job submission and processing through Kafka
- **JobControllerIntegrationTest**: REST API endpoints with full infrastructure

## Running Tests

### Run All Tests
```bash
mvn test
```

### Run Specific Test Class
```bash
mvn test -Dtest=AsyncJobIntegrationTest
```

### Run with Docker Already Running
The tests will automatically start and stop containers. If you have Docker Compose running, the tests use separate container instances.

## Test Features

✅ **Real Infrastructure**: PostgreSQL, Redis, Kafka via Testcontainers  
✅ **Automatic Cleanup**: Containers stopped after tests  
✅ **Parallel Execution**: Tests can run concurrently  
✅ **Isolated State**: Each test uses clean database state  
✅ **Full Coverage**: Handler logic, API endpoints, job processing flow  

## Test Configuration

Tests use:
- Faster sweep intervals (3-5 seconds vs 10-30 seconds in production)
- Smaller thread pools
- Test-specific Kafka topic
- Ephemeral containers (auto-removed)

## Dependencies

```xml
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>postgresql</artifactId>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>kafka</artifactId>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>com.redis.testcontainers</groupId>
    <artifactId>testcontainers-redis</artifactId>
    <scope>test</scope>
</dependency>
```

## Debugging Tests

### View Test Logs
```bash
mvn test -X
```

### Keep Containers Running
Add to test class:
```java
static {
    postgres.withReuse(true);
    redis.withReuse(true);
    kafka.withReuse(true);
}
```

### Connect to Test Database
```bash
# Get container details from logs, then:
docker exec -it <container-id> psql -U test -d asyncjob_test
```

## CI/CD Integration

Tests work in CI environments with Docker:
```yaml
- name: Run Integration Tests
  run: mvn verify
```

Requires:
- Docker daemon running
- ~2GB RAM for containers
- Network access for pulling images
