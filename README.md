# spring-async-job-lib

A production-grade distributed asynchronous job library for Spring applications.

## Features

| Concern | Technology |
|---|---|
| Distributed scheduling (exactly-once) | Spring `@Scheduled` + ShedLock |
| Message backpressure | Apache Kafka |
| Record-level concurrency control | Redisson (explicit lease times) |
| Relational state persistence | PostgreSQL via `JdbcTemplate` |
| Two-step async hand-off | Spring `@Async` + `CompletableFuture` |
| Exponential backoff retry | Per-task configurable policy |
| Deadline guarding | Scheduled sweep with `timed_out` flag |

## Requirements

- Java 21
- Spring Framework 6.x
- PostgreSQL 14+
- Apache Kafka 3.x
- Redis (for Redisson)

## Modules

- **`async-job-core`** — the library itself
- **`async-job-test-support`** — in-memory repository stubs for unit tests

## Quick Start

### 1. Add to your POM

```xml
<dependency>
    <groupId>com.imohiosen</groupId>
    <artifactId>async-job-core</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>

<!-- For tests -->
<dependency>
    <groupId>com.imohiosen</groupId>
    <artifactId>async-job-test-support</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <scope>test</scope>
</dependency>
```

### 2. Apply the database schema

```bash
psql -d yourdb -f schema.sql
```

### 3. Import the library config

```java
@Configuration
@Import(AsyncJobLibraryConfig.class)
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "PT10M")
public class MyAppConfig { }
```

Your application context must provide these beans:
- `JdbcTemplate` — connected to PostgreSQL
- `RedissonClient` — connected to Redis
- `KafkaTemplate<String, String>` — configured with Kafka bootstrap servers

### 4. Implement your scheduler

```java
@Component
public class InvoiceJobScheduler extends AbstractJobScheduler {

    public InvoiceJobScheduler(JobRepository r, TaskRepository t, JobKafkaProducer p) {
        super(r, t, p);
    }

    @Override protected String getJobName()    { return "invoice-job"; }
    @Override protected long   getDeadlineMs() { return 3_600_000L; }   // 1 hour
    @Override protected String getKafkaTopic() { return "invoice-tasks"; }
    @Override protected String getTaskType()   { return "INVOICE_GENERATION"; }

    @Override
    protected List<String> buildTaskPayloads(JobTriggerContext ctx) {
        return invoiceService.getPendingIds().stream()
            .map(id -> "{\"invoiceId\":\"" + id + "\"}")
            .toList();
    }

    @Scheduled(cron = "${invoice.job.cron:0 0 2 * * *}")
    @SchedulerLock(name = "invoice-job", lockAtMostFor = "PT1H", lockAtLeastFor = "PT30S")
    @Override
    public void trigger() { super.trigger(); }
}
```

### 5. Implement your consumer

```java
@Component
public class InvoiceTaskConsumer extends AbstractJobTaskConsumer {

    @Value("${invoice.job.max-attempts:5}")
    private int maxAttempts;

    public InvoiceTaskConsumer(TaskRepository t, JobRepository j,
                               TaskLockManager l, AsyncTaskExecutorBridge b,
                               JobKafkaProducer p) {
        super(t, j, l, b, p);
    }

    @Override protected int getMaxAttempts() { return maxAttempts; }

    @Override
    protected TaskResult processTask(JobTask task) {
        invoiceService.generate(task.payload());
        return TaskResult.success("{\"generated\":true}");
    }

    @KafkaListener(
        topics      = "${invoice.job.kafka.topic:invoice-tasks}",
        groupId     = "${invoice.job.kafka.group-id:invoice-consumers}",
        concurrency = "${invoice.job.kafka.concurrency:4}"
    )
    @Override
    public void consume(ConsumerRecord<String, String> record) {
        super.consume(record);
    }
}
```

## Configuration Reference

```yaml
asyncjob:
  lock:
    lease-time-ms: 30000        # Redisson lock lease — prevents deadlock on JVM crash
    wait-time-ms:  0            # 0 = try-once; no waiting for lock

  retry:
    max-attempts:      5
    base-interval-ms:  1000     # delay = LEAST(base * multiplier^n, max)
    multiplier:        2.0
    max-delay-ms:      3600000  # cap at 1 hour

  deadline:
    sweep-interval-ms:         30000    # how often the deadline guard runs
    default-job-deadline-ms:   3600000  # 1 hour

  executor:
    core-pool-size:      4
    max-pool-size:       16
    queue-capacity:      100
    thread-name-prefix:  async-job-
```

## What the library handles automatically

- Job and task row creation
- Kafka message production with producer callback logging
- Redisson lock acquire and release (with deadlock-safe lease times)
- `IN_PROGRESS` state transitions and `started_at` timestamps
- `attempt_count` incrementation
- Exponential backoff `next_attempt_time` calculation
- `DEAD_LETTER` promotion after max attempts
- `async_submitted_at` and `async_completed_at` two-step tracking
- Deadline guard sweeping (`timed_out` flag)
- Fault-tolerant consumer (exceptions never kill the Kafka consumer thread)

## What you implement

- `getJobName()`, `getDeadlineMs()`, `getKafkaTopic()`, `getTaskType()`
- `buildTaskPayloads(ctx)` — your business query to produce task payloads
- `processTask(task)` — your core business logic
- `getMaxAttempts()` — your retry budget
- `@Scheduled` + `@SchedulerLock` — your scheduling cadence
- `@KafkaListener` — your topic, group-id, concurrency

## License

Internal — All rights reserved.
