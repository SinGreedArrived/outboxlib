-- +goose Up
-- migrations/001_init.sql
CREATE TABLE IF NOT EXISTS pipelines (
    id              UUID        PRIMARY KEY,
    state           TEXT        NOT NULL,
    stages          JSONB       NOT NULL,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error      TEXT        NOT NULL DEFAULT '',
    locked_by       TEXT,
    locked_until    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Частичный индекс под pending: ClaimDue по нему идёт.
CREATE INDEX IF NOT EXISTS idx_pipelines_pending_due
    ON pipelines (next_attempt_at)
    WHERE state = 'pending';

-- Частичный индекс под stale running: перехват упавших подов.
CREATE INDEX IF NOT EXISTS idx_pipelines_running_stale
    ON pipelines (locked_until)
    WHERE state = 'running';

-- Опционально: если часто ищешь по locked_by (например, "что держит под X").
CREATE INDEX IF NOT EXISTS idx_pipelines_locked_by
    ON pipelines (locked_by)
    WHERE locked_by IS NOT NULL;

CREATE TABLE IF NOT EXISTS task_logs (
    id          UUID        PRIMARY KEY,
    task_id     UUID        NOT NULL,
    pipeline_id UUID        NOT NULL,
    attempt     INT         NOT NULL,
    response    JSONB,
    error       TEXT        NOT NULL DEFAULT '',
    start_time  TIMESTAMPTZ NOT NULL,
    end_time    TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_task_logs_pipeline ON task_logs (pipeline_id, start_time);
CREATE INDEX IF NOT EXISTS idx_task_logs_task     ON task_logs (task_id, start_time);

-- +goose Down
DROP IF EXISTS pipelines;
DROP IF EXISTS task_logs;
