-- +goose Up
-- migrations/003_add_filter_for_pipeline.sql

ALTER TABLE pipelines 
  ADD COLUMN IF NOT EXISTS filter JSONB;

-- +goose Down

ALTER TABLE pipelines
  DROP COLUMN IF EXISTS filter;
