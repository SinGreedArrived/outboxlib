package outboxlib

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"uuid"
)

// PostgresStore — реализация PipelineStore поверх database/sql.
// Работает с любой SQL-совместимой обёрткой (pgx/v5/stdlib, lib/pq).
type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

// ---------------------------------------------------------------------
// Save — создаёт новый pipeline. Используется только в AddPipeline.
// ---------------------------------------------------------------------
func (s *PostgresStore) Save(ctx context.Context, p Pipeline) error {
	stages, err := json.Marshal(p.Stages)
	if err != nil {
		return fmt.Errorf("marshal stages: %w", err)
	}
	if p.NextAttemptAt.IsZero() {
		p.NextAttemptAt = time.Now()
	}
	if p.CreatedAt.IsZero() {
		p.CreatedAt = time.Now()
	}
	if p.UpdatedAt.IsZero() {
		p.UpdatedAt = p.CreatedAt
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO pipelines (
			id, state, stages, next_attempt_at, last_error,
			locked_by, locked_until, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, NULL, NULL, $6, $7)
	`,
		p.ID,
		string(p.State),
		stages,
		p.NextAttemptAt,
		p.LastError,
		p.CreatedAt,
		p.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert pipeline: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------
// Get — читает pipeline целиком.
// ---------------------------------------------------------------------
func (s *PostgresStore) Get(ctx context.Context, id uuid.UUID) (Pipeline, error) {
	var (
		p        Pipeline
		state    string
		stages   []byte
		lockedBy sql.NullString
		lockedUn sql.NullTime
	)

	err := s.db.QueryRowContext(ctx, `
		SELECT
			id, state, stages, next_attempt_at, last_error,
			locked_by, locked_until, created_at, updated_at
		FROM pipelines
		WHERE id = $1
	`, id).Scan(
		&p.ID,
		&state,
		&stages,
		&p.NextAttemptAt,
		&p.LastError,
		&lockedBy,
		&lockedUn,
		&p.CreatedAt,
		&p.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return Pipeline{}, ErrNotFound
		}
		return Pipeline{}, fmt.Errorf("select pipeline: %w", err)
	}

	p.State = State(state)
	if lockedBy.Valid {
		p.LockedBy = lockedBy.String
	}
	if lockedUn.Valid {
		p.LockedUntil = lockedUn.Time
	}
	if err := json.Unmarshal(stages, &p.Stages); err != nil {
		return Pipeline{}, fmt.Errorf("unmarshal stages: %w", err)
	}
	return p, nil
}

// ---------------------------------------------------------------------
// ClaimDue — атомарный захват пачки pipeline'ов.
//
// Условия попадания:
//  1. pending и next_attempt_at <= now() и lease не держится/истёк;
//  2. running, но lease истёк (под упал).
//
// FOR UPDATE SKIP LOCKED даёт параллельным подам забрать разные строки
// без блокировок друг друга.
// ---------------------------------------------------------------------
func (s *PostgresStore) ClaimDue(
	ctx context.Context,
	instanceID string,
	lease time.Duration,
	limit int,
) ([]Pipeline, error) {
	if limit <= 0 {
		return nil, nil
	}

	rows, err := s.db.QueryContext(ctx, `
		WITH due AS (
			SELECT id
			FROM pipelines
			WHERE
				(
					state = 'pending'
					AND next_attempt_at <= now()
					AND (locked_until IS NULL OR locked_until < now())
				)
				OR
				(
					state = 'running'
					AND locked_until IS NOT NULL
					AND locked_until < now()
				)
			ORDER BY next_attempt_at
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)
		UPDATE pipelines p
		SET
			locked_by    = $1,
			locked_until = now() + ($2::bigint * interval '1 millisecond'),
			state        = 'running',
			updated_at   = now()
		FROM due
		WHERE p.id = due.id
		RETURNING
			p.id, p.state, p.stages, p.next_attempt_at, p.last_error,
			p.locked_by, p.locked_until, p.created_at, p.updated_at
	`, instanceID, lease.Milliseconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("claim due: %w", err)
	}
	defer rows.Close()

	out := make([]Pipeline, 0, limit)
	for rows.Next() {
		var (
			p        Pipeline
			state    string
			stages   []byte
			lockedBy sql.NullString
			lockedUn sql.NullTime
		)
		if err := rows.Scan(
			&p.ID,
			&state,
			&stages,
			&p.NextAttemptAt,
			&p.LastError,
			&lockedBy,
			&lockedUn,
			&p.CreatedAt,
			&p.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan claimed: %w", err)
		}
		p.State = State(state)
		if lockedBy.Valid {
			p.LockedBy = lockedBy.String
		}
		if lockedUn.Valid {
			p.LockedUntil = lockedUn.Time
		}
		if err := json.Unmarshal(stages, &p.Stages); err != nil {
			return nil, fmt.Errorf("unmarshal stages: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return out, nil
}

// ---------------------------------------------------------------------
// Heartbeat — продлевает lease. Возвращает ID, которые всё ещё наши.
// Если ID не в ответе — lease потерян, работу надо прекратить.
// ---------------------------------------------------------------------
func (s *PostgresStore) Heartbeat(
	ctx context.Context,
	instanceID string,
	ids []uuid.UUID,
	lease time.Duration,
) ([]uuid.UUID, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := s.db.QueryContext(ctx, `
		UPDATE pipelines
		SET
			locked_until = now() + ($3::bigint * interval '1 millisecond'),
			updated_at   = now()
		WHERE locked_by = $1
		  AND id = ANY($2)
		RETURNING id
	`, instanceID, ids, lease.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("heartbeat: %w", err)
	}
	defer rows.Close()

	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan heartbeat: %w", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return out, nil
}

// ---------------------------------------------------------------------
// UpdateProgress — сохраняет stages и агрегаты (next_attempt_at, last_error).
// Не трогает lease. Если lease потерян (n=0) — возвращает ошибку,
// чтобы воркер знал, что запись уже не его.
// ---------------------------------------------------------------------
func (s *PostgresStore) UpdateProgress(
	ctx context.Context,
	instanceID string,
	p Pipeline,
) error {
	stages, err := json.Marshal(p.Stages)
	if err != nil {
		return fmt.Errorf("marshal stages: %w", err)
	}

	res, err := s.db.ExecContext(ctx, `
		UPDATE pipelines
		SET
			stages          = $3,
			next_attempt_at = $4,
			last_error      = $5,
			updated_at      = now()
		WHERE id = $1
		  AND locked_by = $2
	`, p.ID, instanceID, stages, p.NextAttemptAt, p.LastError)
	if err != nil {
		return fmt.Errorf("update progress: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("progress update: lease lost for %s", p.ID)
	}
	return nil
}

// ---------------------------------------------------------------------
// Complete — терминальный успех.
// ---------------------------------------------------------------------
func (s *PostgresStore) Complete(
	ctx context.Context,
	instanceID string,
	id uuid.UUID,
) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE pipelines
		SET
			state        = 'complete',
			last_error   = '',
			locked_by    = NULL,
			locked_until = NULL,
			updated_at   = now()
		WHERE id = $1
		  AND locked_by = $2
	`, id, instanceID)
	if err != nil {
		return fmt.Errorf("complete: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------
// ScheduleRetry — возвращает pipeline в pending с новым next_attempt_at.
// last_error игнорируется (он уже сохранён через UpdateProgress),
// но сигнатура оставлена для совместимости с интерфейсом.
// ---------------------------------------------------------------------
func (s *PostgresStore) ScheduleRetry(
	ctx context.Context,
	instanceID string,
	id uuid.UUID,
	nextAt time.Time,
	_ string,
) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE pipelines
		SET
			state           = 'pending',
			next_attempt_at = $3,
			locked_by       = NULL,
			locked_until    = NULL,
			updated_at      = now()
		WHERE id = $1
		  AND locked_by = $2
	`, id, instanceID, nextAt)
	if err != nil {
		return fmt.Errorf("schedule retry: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------
// Fail — терминальный провал. lastErr пишется как есть,
// подробности лежат в task_logs.
// ---------------------------------------------------------------------
func (s *PostgresStore) Fail(
	ctx context.Context,
	instanceID string,
	id uuid.UUID,
	lastErr string,
) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE pipelines
		SET
			state        = 'failed',
			last_error   = $3,
			locked_by    = NULL,
			locked_until = NULL,
			updated_at   = now()
		WHERE id = $1
		  AND locked_by = $2
	`, id, instanceID, lastErr)
	if err != nil {
		return fmt.Errorf("fail: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------
// ReleaseLease — graceful shutdown: снимаем lease, состояние не меняем
// (чтобы другой под подхватил и разобрался, что там с задачами).
// ---------------------------------------------------------------------
func (s *PostgresStore) ReleaseLease(
	ctx context.Context,
	instanceID string,
	id uuid.UUID,
) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE pipelines
		SET
			locked_by    = NULL,
			locked_until = NULL,
			state        = 'pending',
			updated_at   = now()
		WHERE id = $1
		  AND locked_by = $2
	`, id, instanceID)
	if err != nil {
		return fmt.Errorf("release lease: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------
// ListPending — используется при старте для восстановления очереди.
// Отдаёт только реально готовые к выполнению pipeline'ы.
// ---------------------------------------------------------------------
func (s *PostgresStore) ListPending(ctx context.Context) ([]Pipeline, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			id, state, stages, next_attempt_at, last_error,
			locked_by, locked_until, created_at, updated_at
		FROM pipelines
		WHERE state = 'pending'
		  AND next_attempt_at <= now()
		  AND (locked_until IS NULL OR locked_until < now())
		ORDER BY next_attempt_at
	`)
	if err != nil {
		return nil, fmt.Errorf("list pending: %w", err)
	}
	defer rows.Close()

	var out []Pipeline
	for rows.Next() {
		var (
			p        Pipeline
			state    string
			stages   []byte
			lockedBy sql.NullString
			lockedUn sql.NullTime
		)
		if err := rows.Scan(
			&p.ID,
			&state,
			&stages,
			&p.NextAttemptAt,
			&p.LastError,
			&lockedBy,
			&lockedUn,
			&p.CreatedAt,
			&p.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan pending: %w", err)
		}
		p.State = State(state)
		if lockedBy.Valid {
			p.LockedBy = lockedBy.String
		}
		if lockedUn.Valid {
			p.LockedUntil = lockedUn.Time
		}
		if err := json.Unmarshal(stages, &p.Stages); err != nil {
			return nil, fmt.Errorf("unmarshal stages: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return out, nil
}

// =====================================================================
// TaskLogStore
// =====================================================================

type PostgresTaskLogStore struct {
	db *sql.DB
}

func NewPostgresTaskLogStore(db *sql.DB) *PostgresTaskLogStore {
	return &PostgresTaskLogStore{db: db}
}

func (s *PostgresTaskLogStore) Save(ctx context.Context, l TaskLog) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO task_logs (
			id, task_id, pipeline_id, attempt,
			response, error, start_time, end_time
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`,
		l.ID,
		l.TaskID,
		l.PipelineID,
		l.Attempt,
		l.Response,
		l.Error,
		l.StartTime,
		l.EndTime,
	)
	if err != nil {
		return fmt.Errorf("insert task log: %w", err)
	}
	return nil
}

func (s *PostgresTaskLogStore) ListByPipeline(
	ctx context.Context,
	pipelineID uuid.UUID,
) ([]TaskLog, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			id, task_id, pipeline_id, attempt,
			response, error, start_time, end_time
		FROM task_logs
		WHERE pipeline_id = $1
		ORDER BY start_time
	`, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("list task logs: %w", err)
	}
	defer rows.Close()

	var out []TaskLog
	for rows.Next() {
		var l TaskLog
		if err := rows.Scan(
			&l.ID,
			&l.TaskID,
			&l.PipelineID,
			&l.Attempt,
			&l.Response,
			&l.Error,
			&l.StartTime,
			&l.EndTime,
		); err != nil {
			return nil, fmt.Errorf("scan task log: %w", err)
		}
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return out, nil
}

func (s *PostgresTaskLogStore) ListByTask(
	ctx context.Context,
	taskID uuid.UUID,
) ([]TaskLog, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			id, task_id, pipeline_id, attempt,
			response, error, start_time, end_time
		FROM task_logs
		WHERE task_id = $1
		ORDER BY start_time
	`, taskID)
	if err != nil {
		return nil, fmt.Errorf("list task logs by task: %w", err)
	}
	defer rows.Close()

	var out []TaskLog
	for rows.Next() {
		var l TaskLog
		if err := rows.Scan(
			&l.ID,
			&l.TaskID,
			&l.PipelineID,
			&l.Attempt,
			&l.Response,
			&l.Error,
			&l.StartTime,
			&l.EndTime,
		); err != nil {
			return nil, fmt.Errorf("scan task log: %w", err)
		}
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return out, nil
}
