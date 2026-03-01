# RFC-0001: Job Key Taxonomy — Lock, Idempotency, and Group Keys

| Field       | Value                                          |
|-------------|------------------------------------------------|
| **Status**  | Draft                                          |
| **Authors** | —                                              |
| **Created** | 2026-03-01                                     |
| **Updated** | 2026-03-01                                     |
| **Modules** | `async-job-core`, `spring-async-job-starter`   |

---

## 1  Problem Statement

The library currently uses a single `correlationId` field on the `Job` record as a
caller-supplied opaque tag.  In practice, production systems need **three
distinct key concepts** that serve fundamentally different purposes:

| Concern              | Question it answers                                    | Today        |
|----------------------|--------------------------------------------------------|--------------|
| **Lock key**         | "Who is allowed to mutate this task right now?"        | Implemented  |
| **Idempotency key**  | "Has this exact job already been submitted?"           | Missing      |
| **Group key**        | "Which jobs belong together for query/cancel/await?"   | Missing      |

Overloading `correlationId` for any of these is fragile: it has no unique
constraint, no index optimised for lookup, and no lifecycle semantics.

---

## 2  Goals

1. Formalise a key taxonomy with clear ownership per infrastructure layer.
2. Add **idempotency key** support with at-most-once submission guarantees.
3. Add **group key** support for cross-job correlation and bulk operations.
4. Preserve backward compatibility — both new fields are nullable and optional.
5. Maintain the existing lock key design unchanged.

## 3  Non-Goals

- Group-level semaphores ("only N jobs from this group may run concurrently").
  This is a future extension that would layer on top of the group key.
- Cross-service idempotency (e.g. via an API gateway idempotency header).
  That is outside the library boundary.
- Changing the Kafka message key (remains `taskId`).

---

## 4  Key Taxonomy

### 4.1  Lock Key (existing — no changes)

**Purpose:** Mutual exclusion during task processing.  Prevents two consumer
instances from executing the same task concurrently, even across Kafka consumer
group rebalances.

**Lifecycle:**

```
Producer
  │
  ▼
Kafka(key = taskId)          ← partition affinity (soft exclusion)
  │
  ▼
Consumer thread
  │
  ├─► Redis: LOCK async-job-task-lock:{taskId}   ← hard exclusion
  │         fenced token = N (monotonically increasing)
  │
  └─► PostgreSQL: UPDATE job_tasks ... WHERE fence_token = N
                                             ← durable linearisation
```

**Layer responsibilities:**

| Layer      | Artefact                             | Guarantee                                   |
|------------|--------------------------------------|---------------------------------------------|
| Kafka      | Message key = `taskId`               | All messages for a task land on the same partition → same consumer within a group |
| Redis      | `async-job-task-lock:{taskId}`       | At most one thread holds the lock at any instant (distributed mutex) |
| PostgreSQL | `fence_token` column + `WHERE` guard | Stale lock holders cannot overwrite newer state, even after lease expiry |

**Lease model:**

| Mode      | Config                       | Behaviour                                              |
|-----------|------------------------------|--------------------------------------------------------|
| Watchdog  | `leaseTimeMs = -1` (default) | Redisson auto-renews while the owning thread is alive  |
| Explicit  | `leaseTimeMs > 0`            | Lock released automatically after the specified TTL    |

For **time-critical tasks**, the Resilience4j retry loop keeps the thread alive,
so watchdog mode continues to auto-renew — no special handling required.

**Fenced token flow:**

```
Consumer C1 acquires lock → token=7 → UPDATE ... SET fence_token=7 WHERE id=?
      │
      │  rebalance → C1 loses lock
      ▼
Consumer C2 acquires lock → token=8 → UPDATE ... SET fence_token=8 WHERE fence_token=7
      │
      │  C1 wakes up (stale) → UPDATE ... WHERE fence_token=7 → 0 rows affected → no-op
```

---

### 4.2  Idempotency Key (new)

**Purpose:** At-most-once job submission.  Calling `submit()` twice with the same
idempotency key returns the existing `jobId` rather than creating a duplicate.

#### 4.2.1  Design Decisions

| Decision                          | Choice                        | Rationale                                                     |
|-----------------------------------|-------------------------------|---------------------------------------------------------------|
| Source of truth                   | PostgreSQL                    | Must survive Redis restarts; durability is non-negotiable      |
| Kafka involvement                | None                          | Idempotency is a submission-level concept; doesn't affect message routing |
| Redis involvement                | Optional read-through cache   | Fast-path dedup avoids PG round-trip for hot keys              |
| Scope                            | Per `(idempotency_key)` value | Library-wide uniqueness; callers can namespace with prefixes   |
| Window                           | Active job lifetime           | Key becomes reusable after job reaches a terminal state        |

#### 4.2.2  Schema

```sql
ALTER TABLE jobs ADD COLUMN idempotency_key VARCHAR(255);

-- Partial unique index: only enforced while the job is active.
-- Once COMPLETED / DEAD_LETTER, the key is released for reuse.
CREATE UNIQUE INDEX idx_jobs_idempotency_key
    ON jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL
      AND status NOT IN ('COMPLETED', 'DEAD_LETTER');
```

#### 4.2.3  Domain Changes

```java
// JobSubmissionRequest — add nullable field
public record JobSubmissionRequest(
        // ... existing fields ...
        String idempotencyKey,      // NEW — nullable
        // ...
) { }

// Job — add nullable field
public record Job(
        // ... existing fields ...
        String idempotencyKey,      // NEW — nullable
        // ...
) { }
```

#### 4.2.4  Submission Flow

```
Client → submit(idempotencyKey = "order-12345")
  │
  ├─► [Optional] Redis GET idempotency:{key}
  │     ├─ HIT  → return cached jobId (skip PG)
  │     └─ MISS → continue
  │
  ├─► PG: INSERT INTO jobs (..., idempotency_key)
  │       VALUES (..., 'order-12345')
  │       ON CONFLICT (idempotency_key)
  │         WHERE status NOT IN ('COMPLETED', 'DEAD_LETTER')
  │       DO NOTHING
  │       RETURNING id
  │
  ├─► If INSERT returned a row → new job
  │     ├─ Insert tasks, publish Kafka messages
  │     └─ [Optional] Redis SET idempotency:{key} → jobId, TTL=deadlineMs
  │
  └─► If INSERT returned nothing → duplicate
        ├─ PG: SELECT id FROM jobs WHERE idempotency_key = ? AND status NOT IN (...)
        └─ Return existing jobId
```

#### 4.2.5  Redis Cache (Optional)

```
Key:    idempotency:{idempotencyKey}
Value:  jobId (UUID string)
TTL:    deadlineMs of the original job
```

The cache is a **performance optimisation only**.  Correctness is guaranteed by
the PG unique index.  If Redis is unavailable, the system degrades to a PG
round-trip on every submit — no data loss, no duplicates.

#### 4.2.6  Interaction with Other Keys

- **Lock key:** No interaction.  Idempotency is checked at submission time,
  before any tasks or locks exist.
- **Group key:** Independent.  A job can have both an idempotency key and a
  group key.  The idempotency key prevents duplicates; the group key organises
  non-duplicate jobs into logical batches.
- **Kafka key:** No change.  Task messages still use `taskId` as the Kafka key.
  The idempotency key never appears in Kafka records.

---

### 4.3  Group Key (new)

**Purpose:** Correlate multiple jobs into a logical group for querying,
monitoring, and bulk lifecycle operations (cancel, await).

#### 4.3.1  Design Decisions

| Decision             | Choice              | Rationale                                                    |
|----------------------|----------------------|--------------------------------------------------------------|
| Source of truth      | PostgreSQL           | Groups are a query/lifecycle concept — no need for Redis or Kafka |
| Kafka involvement    | None                 | Group key doesn't affect message routing or partitioning      |
| Redis involvement    | None (by default)    | See §4.3.5 for optional group-level semaphore extension       |
| Cardinality          | Many jobs per group  | A group key is a label, not a parent entity                   |

#### 4.3.2  Schema

```sql
ALTER TABLE jobs ADD COLUMN group_key VARCHAR(255);

CREATE INDEX idx_jobs_group_key
    ON jobs (group_key)
    WHERE group_key IS NOT NULL;
```

#### 4.3.3  Domain Changes

```java
// JobSubmissionRequest — add nullable field
public record JobSubmissionRequest(
        // ... existing fields ...
        String groupKey,            // NEW — nullable
        // ...
) { }

// Job — add nullable field
public record Job(
        // ... existing fields ...
        String groupKey,            // NEW — nullable
        // ...
) { }
```

#### 4.3.4  Repository Port Extensions

```java
public interface JobRepository {
    // ... existing methods ...

    /** Returns all jobs belonging to the given group. */
    List<Job> findByGroupKey(String groupKey);

    /** Cancels all active jobs in the group. Returns the number of jobs cancelled. */
    int cancelByGroupKey(String groupKey);

    /** Returns a summary of job statuses within the group. */
    Map<JobStatus, Integer> groupStatusSummary(String groupKey);
}
```

#### 4.3.5  Future Extension — Group-Level Semaphore

If a future use case requires "only N jobs from group X may be IN_PROGRESS at a
time", this would introduce a Redis key:

```
Key:    async-job-group-semaphore:{groupKey}
Type:   Redisson RSemaphore (or counter)
```

This is explicitly **out of scope** for this RFC.  The group key column is
designed to support it later without schema changes.

#### 4.3.6  Interaction with Other Keys

- **Lock key:** No interaction.  Lock granularity remains per-task.
- **Idempotency key:** Independent.  A group can contain jobs with different
  idempotency keys.  The group key is for organisation; the idempotency key is
  for dedup.
- **Kafka key:** No change.  The group key is purely a PG-side concept.

---

## 5  Cross-Cutting: Key-to-Layer Mapping

Each key type should appear **only in the layer where its guarantee is
enforced**.  This table summarises the full taxonomy:

| Key type        | PostgreSQL                            | Kafka                         | Redis                                    | Lease / TTL                          |
|-----------------|---------------------------------------|-------------------------------|------------------------------------------|--------------------------------------|
| **Lock**        | `fence_token` (write guard)           | `taskId` (partition affinity) | `async-job-task-lock:{taskId}` (mutex)   | Watchdog or explicit `leaseTimeMs`   |
| **Idempotency** | `idempotency_key` UNIQUE partial idx  | None                          | Optional: `idempotency:{key} → jobId`    | TTL = `deadlineMs`                   |
| **Group**       | `group_key` + B-tree index            | None                          | None (unless semaphore extension)        | N/A                                  |

Design principle: **don't let a key leak into a layer that doesn't enforce its
guarantee.**  The lock key is the one exception where all three layers
collaborate — Kafka for routing, Redis for exclusion, PG for durability — but
they all protect the same invariant (single-writer per task) at different levels.

---

## 6  Migration

Both columns are nullable with no default, so the migration is
backward-compatible:

```sql
-- Migration V2: Add idempotency and group keys
ALTER TABLE jobs ADD COLUMN idempotency_key VARCHAR(255);
ALTER TABLE jobs ADD COLUMN group_key       VARCHAR(255);

CREATE UNIQUE INDEX idx_jobs_idempotency_key
    ON jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL
      AND status NOT IN ('COMPLETED', 'DEAD_LETTER');

CREATE INDEX idx_jobs_group_key
    ON jobs (group_key)
    WHERE group_key IS NOT NULL;
```

Existing jobs continue to work with both fields as `NULL`.  No data backfill
required.

---

## 7  Implementation Plan

### Phase 1 — Idempotency Key (high value, moderate effort)

| Step | Module               | Change                                                           |
|------|----------------------|------------------------------------------------------------------|
| 1    | `domain`             | Add `idempotencyKey` to `JobSubmissionRequest`, `Job`            |
| 2    | `schema.sql`         | Add column + partial unique index                                |
| 3    | `JobRepository`      | Update `insert()` to use `ON CONFLICT DO NOTHING RETURNING id`   |
| 4    | `JdbcJobRepository`  | Implement upsert SQL                                             |
| 5    | `JobSubmissionService` | Handle duplicate detection: return existing `jobId`            |
| 6    | `InMemoryJobRepository` | Update test double                                            |
| 7    | Tests                | Duplicate submission, null key passthrough, reuse after terminal  |

### Phase 2 — Group Key (moderate value, low effort)

| Step | Module               | Change                                                           |
|------|----------------------|------------------------------------------------------------------|
| 1    | `domain`             | Add `groupKey` to `JobSubmissionRequest`, `Job`                  |
| 2    | `schema.sql`         | Add column + index                                               |
| 3    | `JobRepository`      | Add `findByGroupKey`, `cancelByGroupKey`, `groupStatusSummary`   |
| 4    | `JdbcJobRepository`  | Implement queries                                                |
| 5    | `InMemoryJobRepository` | Update test double                                            |
| 6    | Tests                | Group queries, cancel, status summary                            |

### Phase 3 — Optional Redis Idempotency Cache

| Step | Module                     | Change                                                    |
|------|----------------------------|-----------------------------------------------------------|
| 1    | `port`                     | New `IdempotencyCache` port interface                     |
| 2    | `infrastructure`           | `RedisIdempotencyCache` implementation                    |
| 3    | `JobSubmissionService`     | Inject optional cache, check before PG                    |
| 4    | `spring-async-job-starter` | Auto-configure when Redisson is present                   |

---

## 8  Rejected Alternatives

### 8.1  Use `correlationId` for idempotency

Rejected because `correlationId` is caller-supplied metadata with no uniqueness
constraint.  Adding a unique index to it would break existing users who reuse
the same correlation ID across jobs.

### 8.2  Store idempotency state in Redis only

Rejected because Redis is ephemeral.  A Redis restart would lose the
idempotency mapping, allowing duplicate job creation.  PostgreSQL is the only
layer that provides the durability guarantee.

### 8.3  Use Kafka key for group routing

Rejected because the Kafka message key must remain `taskId` to ensure per-task
partition affinity and ordering.  Changing it to `groupKey` would break the
lock key's soft-exclusion guarantee (all retries for a task landing on the same
partition).

### 8.4  Single composite key instead of three separate keys

Rejected because the three keys serve different purposes at different times in
the lifecycle:
- **Idempotency key** is checked at submission time (before tasks exist).
- **Lock key** is used at execution time (per-task, per-attempt).
- **Group key** is used at query/management time (across jobs).

Combining them into a single concept would force awkward compromises in each
layer.

---

## 9  Open Questions

1. **Idempotency key TTL after terminal state:** Should we automatically purge
   completed jobs' idempotency keys after a configurable retention period, or
   is the partial index (`WHERE status NOT IN ('COMPLETED', 'DEAD_LETTER')`)
   sufficient to release them for reuse?

2. **Group key on tasks:** Should `group_key` also be denormalised onto the
   `job_tasks` table for direct task-level group queries, or is joining through
   `jobs` acceptable?

3. **Idempotency response enrichment:** When a duplicate submission is
   detected, should `submit()` return just the `jobId`, or a richer response
   indicating `{jobId, duplicate: true, status: IN_PROGRESS}`?

---

## 10  References

- Martin Kleppmann, *Designing Data-Intensive Applications*, §8.4 — Fencing tokens
- Stripe API — [Idempotent Requests](https://stripe.com/docs/api/idempotent_requests)
- Kafka documentation — [Message Keys and Partitioning](https://kafka.apache.org/documentation/#producerconfigs_partitioner.class)
- Redisson — [RFencedLock](https://github.com/redisson/redisson/wiki/8.-distributed-locks-and-synchronizers#84-fenced-lock)
