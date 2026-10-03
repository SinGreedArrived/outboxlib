-- +goose Up
-- Меняем старый
DROP INDEX IF EXISTS idx_pipelines_due;

-- Новый покрывает обе ветки OR
CREATE INDEX IF NOT EXISTS idx_pipelines_claim
    ON pipelines (next_attempt_at)
    WHERE state = 'pending';

CREATE INDEX IF NOT EXISTS idx_pipelines_stale_running
    ON pipelines (locked_until)
    WHERE state = 'running' AND locked_until IS NOT NULL;

-- +goose Down
