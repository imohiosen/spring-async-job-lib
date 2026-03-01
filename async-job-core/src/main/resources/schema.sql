-- =============================================================================
-- Async Job Library — PostgreSQL Schema
-- com.imohiosen / spring-async-job-lib
-- =============================================================================

CREATE TYPE job_status  AS ENUM ('PENDING','IN_PROGRESS','COMPLETED','FAILED','DEAD_LETTER');
CREATE TYPE task_status AS ENUM ('PENDING','IN_PROGRESS','COMPLETED','FAILED','DEAD_LETTER');

-- ---------------------------------------------------------------------------
-- JOBS — one row per scheduled trigger execution
-- ---------------------------------------------------------------------------
CREATE TABLE jobs (
    id                  VARCHAR(36)     PRIMARY KEY,
    job_name            VARCHAR(255)    NOT NULL,
    correlation_id      VARCHAR(255),
    status              job_status      NOT NULL DEFAULT 'PENDING',
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    deadline_at         TIMESTAMPTZ     NOT NULL,
    stale               BOOLEAN         NOT NULL DEFAULT FALSE,
    total_tasks         INT             NOT NULL DEFAULT 0,
    pending_tasks       INT             NOT NULL DEFAULT 0,
    in_progress_tasks   INT             NOT NULL DEFAULT 0,
    completed_tasks     INT             NOT NULL DEFAULT 0,
    failed_tasks        INT             NOT NULL DEFAULT 0,
    dead_letter_tasks   INT             NOT NULL DEFAULT 0,
    metadata            JSONB
);

-- ---------------------------------------------------------------------------
-- JOB_TASKS — one row per message / unit of work
-- ---------------------------------------------------------------------------
CREATE TABLE job_tasks (
    id                  VARCHAR(36)     PRIMARY KEY,
    job_id              VARCHAR(36)     NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    task_type           VARCHAR(255)    NOT NULL,
    destination         VARCHAR(500)    NOT NULL,
    partition           INT,
    "offset"            BIGINT,
    status              task_status     NOT NULL DEFAULT 'PENDING',
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    deadline_at         TIMESTAMPTZ     NOT NULL,
    stale               BOOLEAN         NOT NULL DEFAULT FALSE,
    attempt_count       INT             NOT NULL DEFAULT 0,
    last_attempt_time   TIMESTAMPTZ,
    next_attempt_time   TIMESTAMPTZ,
    base_interval_ms    BIGINT          NOT NULL DEFAULT 1000,
    multiplier          NUMERIC(5, 2)   NOT NULL DEFAULT 2.0,
    max_delay_ms        BIGINT          NOT NULL DEFAULT 3600000,
    async_submitted_at  TIMESTAMPTZ,
    async_completed_at  TIMESTAMPTZ,
    last_error_message  TEXT,
    last_error_class    VARCHAR(500),
    fence_token         BIGINT,
    payload             JSONB           NOT NULL,
    result              JSONB,
    metadata            JSONB
);

-- ---------------------------------------------------------------------------
-- SHEDLOCK — required by ShedLock for distributed scheduling
-- ---------------------------------------------------------------------------
CREATE TABLE shedlock (
    name       VARCHAR(64)  NOT NULL,
    lock_until TIMESTAMPTZ  NOT NULL,
    locked_at  TIMESTAMPTZ  NOT NULL,
    locked_by  VARCHAR(255) NOT NULL,
    PRIMARY KEY (name)
);

-- ---------------------------------------------------------------------------
-- INDEXES
-- ---------------------------------------------------------------------------
CREATE INDEX idx_jobs_stale_deadline
    ON jobs (deadline_at)
    WHERE stale = FALSE AND status NOT IN ('COMPLETED', 'DEAD_LETTER');

CREATE INDEX idx_job_tasks_consumer_eligibility
    ON job_tasks (job_id, status, next_attempt_time)
    WHERE status IN ('PENDING', 'FAILED');

CREATE INDEX idx_job_tasks_stale_guard
    ON job_tasks (deadline_at)
    WHERE stale = FALSE AND status = 'IN_PROGRESS';

CREATE INDEX idx_job_tasks_retry
    ON job_tasks (attempt_count ASC, next_attempt_time)
    WHERE status = 'FAILED' AND next_attempt_time IS NOT NULL;

CREATE INDEX idx_job_tasks_job_id
    ON job_tasks (job_id);

CREATE INDEX idx_job_tasks_dead_letter
    ON job_tasks (job_id, updated_at DESC)
    WHERE status = 'DEAD_LETTER';

-- ---------------------------------------------------------------------------
-- TRIGGERS — auto-maintain updated_at
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_jobs_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_job_tasks_updated_at
    BEFORE UPDATE ON job_tasks
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ---------------------------------------------------------------------------
-- BACKOFF HELPER FUNCTION
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION compute_next_attempt_time(
    p_base_interval_ms  BIGINT,
    p_multiplier        NUMERIC,
    p_max_delay_ms      BIGINT,
    p_attempt_count     INT
)
RETURNS TIMESTAMPTZ AS $$
DECLARE
    v_delay_ms BIGINT;
BEGIN
    v_delay_ms := LEAST(
        (p_base_interval_ms * POWER(p_multiplier, p_attempt_count))::BIGINT,
        p_max_delay_ms
    );
    RETURN NOW() + (v_delay_ms || ' milliseconds')::INTERVAL;
END;
$$ LANGUAGE plpgsql;
