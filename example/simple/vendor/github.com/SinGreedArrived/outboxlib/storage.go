package outboxlib

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
	"uuid"
)

var ErrNotFound = errors.New("outbox: not found")

type PipelineStore interface {
	// Save создаёт новый pipeline с next_attempt_at = now().
	Save(ctx context.Context, p Pipeline) error

	Get(ctx context.Context, id uuid.UUID) (Pipeline, error)

	// ClaimDue атомарно захватывает до limit pipeline'ов,
	// у которых next_attempt_at <= now() и lease истёк/отсутствует.
	// Проставляет locked_by = instanceID и locked_until = now()+lease.
	ClaimDue(
		ctx context.Context,
		instanceID string,
		lease time.Duration,
		limit int,
	) ([]Pipeline, error)

	// Heartbeat продлевает lease и возвращает те ID, которые ещё принадлежат нам.
	// Если ID нет в ответе — lease потерян, надо остановить выполнение.
	Heartbeat(
		ctx context.Context,
		instanceID string,
		ids []uuid.UUID,
		lease time.Duration,
	) ([]uuid.UUID, error)

	// UpdateProgress сохраняет Stages (Done флаги), не трогая lease.
	UpdateProgress(ctx context.Context, instanceID string, p Pipeline) error

	Complete(ctx context.Context, instanceID string, id uuid.UUID) error
	ScheduleRetry(
		ctx context.Context,
		instanceID string,
		id uuid.UUID,
		nextAt time.Time,
		lastErr string,
	) error
	Fail(ctx context.Context, instanceID string, id uuid.UUID, lastErr string) error

	// ReleaseLease — снять lease без изменения состояния (graceful shutdown).
	ReleaseLease(ctx context.Context, instanceID string, id uuid.UUID) error
}

type TaskLogStore interface {
	Save(ctx context.Context, log TaskLog) error
	ListByPipeline(ctx context.Context, pipelineID uuid.UUID) ([]TaskLog, error)
}

// ---- in-memory реализации ----

type MemoryPipelineStore struct {
	mu sync.RWMutex
	m  map[uuid.UUID]Pipeline
}

func NewMemoryPipelineStore() *MemoryPipelineStore {
	return &MemoryPipelineStore{m: make(map[uuid.UUID]Pipeline)}
}

func (s *MemoryPipelineStore) Save(_ context.Context, p Pipeline) error {
	s.mu.Lock()
	s.m[p.ID] = p
	s.mu.Unlock()
	return nil
}

func (s *MemoryPipelineStore) Update(ctx context.Context, p Pipeline) error {
	return s.Save(ctx, p)
}

func (s *MemoryPipelineStore) Get(_ context.Context, id uuid.UUID) (Pipeline, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.m[id]
	if !ok {
		return Pipeline{}, ErrNotFound
	}
	return p, nil
}

func (s *MemoryPipelineStore) ListPending(_ context.Context) ([]Pipeline, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Pipeline, 0)
	for _, p := range s.m {
		if p.State == Pending {
			out = append(out, p)
		}
	}
	return out, nil
}

type MemoryTaskLogStore struct {
	mu   sync.RWMutex
	logs []TaskLog
}

func NewMemoryTaskLogStore() *MemoryTaskLogStore {
	return &MemoryTaskLogStore{}
}

func (s *MemoryTaskLogStore) Save(ctx context.Context, log TaskLog) error {
	slog.With("log", log).InfoContext(ctx, "New LOG")
	s.mu.Lock()
	s.logs = append(s.logs, log)
	s.mu.Unlock()
	return nil
}

func (s *MemoryTaskLogStore) ListByPipeline(_ context.Context, pid uuid.UUID) ([]TaskLog, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]TaskLog, 0)
	for _, l := range s.logs {
		if l.TaskID == pid { // при желании связывайте через Task→Pipeline
			out = append(out, l)
		}
	}
	return out, nil
}

func (s *MemoryTaskLogStore) ListByTask(_ context.Context, taskID uuid.UUID) ([]TaskLog, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]TaskLog, 0)
	for _, l := range s.logs {
		if l.TaskID == taskID {
			out = append(out, l)
		}
	}
	return out, nil
}
