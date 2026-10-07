# CHANGELOG

## [Unreleased]

## [0.0.0] - 2026-10-07

### Added
- New method `WithFilters[T any](v T)` to `PipelineBuilder` - allows setting filter criteria for pipelines
- `Filters` field (JSONB) to `Pipeline` struct

### Changed
- Updated SQL migration to add `filter` column to `pipelines` table
- Updated `postgres.go` to persist filter data when saving pipelines

### Examples
- Updated example code to demonstrate pipeline builder with filters
