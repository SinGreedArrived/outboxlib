// worker.go
package outboxlib

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"
	"uuid"
)

type HandlerStore interface {
	Call(ctx context.Context, handlerName HandlerName, req json.RawMessage) (json.RawMessage, error)
}

type runOutcome int

const (
	outcomeComplete runOutcome = iota
	outcomeRetry
	outcomeFailed
)

type WorkerPool struct {
	cfg      Config
	store    PipelineStore
	logs     TaskLogStore
	handlers HandlerStore
	logger   *slog.Logger

	sem chan struct{}
	wg  sync.WaitGroup
}

func NewWorkerPool(
	cfg Config,
	store PipelineStore,
	logs TaskLogStore,
	handlers HandlerStore,
	logger *slog.Logger,
) *WorkerPool {
	if logger == nil {
		logger = slog.Default()
	}
	return &WorkerPool{
		cfg:      cfg,
		store:    store,
		logs:     logs,
		handlers: handlers,
		logger:   logger,
		sem:      make(chan struct{}, cfg.Concurrency),
	}
}

// // Run — основной цикл инстанса. Блокирующий, завершается по ctx.
func (w *WorkerPool) Run(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.drain()
			return
		case <-ticker.C:
			w.claimOnce(ctx)
		}
	}
}

func (w *WorkerPool) claimOnce(ctx context.Context) {
	free := cap(w.sem) - len(w.sem)
	if free <= 0 {
		return
	}
	if free > w.cfg.BatchSize {
		free = w.cfg.BatchSize
	}

	pipelines, err := w.store.ClaimDue(ctx, w.cfg.InstanceID, w.cfg.LeaseDuration, free)
	if err != nil {
		w.logger.Error("claim due", "err", err)
		return
	}
	for _, p := range pipelines {
		w.sem <- struct{}{}
		w.wg.Add(1)
		go func(p Pipeline) {
			defer func() { <-w.sem; w.wg.Done() }()
			w.runOne(ctx, p)
		}(p)
	}
}

func (w *WorkerPool) heartbeat(ctx context.Context, id uuid.UUID, lost chan<- struct{}) {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()

	ids := []uuid.UUID{id}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := w.store.Heartbeat(ctx, w.cfg.InstanceID, ids, w.cfg.LeaseDuration)
			if err != nil {
				w.logger.Error("heartbeat", "pipeline", id, "err", err)
				continue
			}
			if len(ok) == 0 {
				select {
				case lost <- struct{}{}:
				default:
				}
				return
			}
		}
	}
}

// drain ждёт завершения запущенных pipeline'ов (ограниченное время).
func (w *WorkerPool) drain() {
	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
	}
}

func (w *WorkerPool) runOne(ctx context.Context, p Pipeline) {
	// heartbeat (не меняем — как было)
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	lostLease := make(chan struct{}, 1)
	go w.heartbeat(hbCtx, p.ID, lostLease)

	runCtx, runCancel := context.WithCancel(hbCtx)
	go func() {
		select {
		case <-lostLease:
			runCancel()
		case <-runCtx.Done():
		}
	}()

	outcome := w.executePipeline(runCtx, &p)
	runCancel()
	hbCancel()

	select {
	case <-lostLease:
		w.logger.Warn("lease lost", "pipeline", p.ID)
		return
	default:
	}

	// Всегда сохраняем прогресс и next_attempt_at
	if err := w.store.UpdateProgress(ctx, w.cfg.InstanceID, p); err != nil {
		w.logger.Error("update progress", "pipeline", p.ID, "err", err)
		return
	}

	switch outcome {
	case outcomeComplete:
		if err := w.store.Complete(ctx, w.cfg.InstanceID, p.ID); err != nil {
			w.logger.Error("complete", "pipeline", p.ID, "err", err)
		}
	case outcomeFailed:
		if err := w.store.Fail(ctx, w.cfg.InstanceID, p.ID, p.LastError); err != nil {
			w.logger.Error("fail", "pipeline", p.ID, "err", err)
		}
	case outcomeRetry:
		if err := w.store.ScheduleRetry(
			ctx,
			w.cfg.InstanceID,
			p.ID,
			p.NextAttemptAt,
			p.LastError,
		); err != nil {
			w.logger.Error("schedule retry", "pipeline", p.ID, "err", err)
		}
	}
}

// executePipeline идёт по стейджам. Стейдж завершён → следующий.
// Любая незавершённая задача с исчерпанными попытками → outcomeFailed.
// Любая незавершённая retryable задача → outcomeRetry и стоп.
func (w *WorkerPool) executePipeline(ctx context.Context, p *Pipeline) runOutcome {
	for si := range p.Stages {
		stage := &p.Stages[si]

		if isStageComplete(stage) {
			continue
		}

		w.executeStage(ctx, p, stage)

		if hasFailedTask(stage) {
			p.NextAttemptAt = computeNextAttempt(p)
			p.LastError = composeLastError(p)
			return outcomeFailed
		}
		if !isStageComplete(stage) {
			p.NextAttemptAt = computeNextAttempt(p)
			p.LastError = composeLastError(p)
			return outcomeRetry
		}
	}

	p.NextAttemptAt = time.Now()
	p.LastError = ""
	return outcomeComplete
}

// executeStage запускает все готовые к запуску задачи параллельно.
// Задачи, чей NextAttemptAt ещё в будущем, пропускаются.
func (w *WorkerPool) executeStage(ctx context.Context, p *Pipeline, stage *Stage) {
	var wg sync.WaitGroup
	now := time.Now()

	for i := range stage.Tasks {
		t := &stage.Tasks[i]
		if t.Done || t.Failed {
			continue
		}
		// не пришло время (может быть, если в стейдже разные backoff'ы)
		if t.Attempt > 0 && now.Before(t.NextAttemptAt) {
			continue
		}

		wg.Add(1)
		go func(task *Task) {
			defer wg.Done()
			task.Attempt++
			err := w.runTask(ctx, p, task)
			if err == nil {
				return
			}
			task.LastError = err.Error()

			if task.MaxAttempts != nil && *task.MaxAttempts > 0 &&
				task.Attempt >= *task.MaxAttempts {
				task.Failed = true
				return
			}

			task.NextAttemptAt = time.Now().Add(task.Backoff.Delay(task.Attempt))
		}(t)
	}
	wg.Wait()
}

func (w *WorkerPool) runTask(ctx context.Context, p *Pipeline, task *Task) (err error) {
	log := TaskLog{
		ID:         uuid.New(),
		TaskID:     task.ID,
		PipelineID: p.ID,
		Attempt:    task.Attempt,
		StartTime:  time.Now(),
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in handler: %v", r)
			log.Error = err.Error()
		}
		log.EndTime = time.Now()
		_ = w.logs.Save(context.WithoutCancel(ctx), log)
	}()

	resp, err := w.handlers.Call(ctx, task.HandlerName, task.Payload)
	if err != nil {
		log.Error = err.Error()
		return err
	}
	log.Response = resp
	task.Done = true
	task.Failed = false
	task.LastError = ""
	return nil
}

// ---- helpers ----

func isStageComplete(s *Stage) bool {
	for _, t := range s.Tasks {
		if !t.Done && !t.Failed {
			return false
		}
	}
	return true
}

func hasFailedTask(s *Stage) bool {
	for _, t := range s.Tasks {
		if t.Failed {
			return true
		}
	}
	return false
}

func computeNextAttempt(p *Pipeline) time.Time {
	var min time.Time
	for _, s := range p.Stages {
		for _, t := range s.Tasks {
			if t.Done || t.Failed {
				continue
			}
			if min.IsZero() || t.NextAttemptAt.Before(min) {
				min = t.NextAttemptAt
			}
		}
	}
	if min.IsZero() {
		min = time.Now()
	}
	return min
}

func composeLastError(p *Pipeline) string {
	for _, s := range p.Stages {
		for _, t := range s.Tasks {
			if t.Failed {
				return fmt.Sprintf("task %s (%s) failed after %d attempts: %s",
					t.ID, t.HandlerName, t.Attempt, t.LastError)
			}
		}
	}
	// нет failed — вернём первую retryable
	for _, s := range p.Stages {
		for _, t := range s.Tasks {
			if !t.Done && t.LastError != "" {
				return fmt.Sprintf("task %s (%s) attempt %d: %s",
					t.ID, t.HandlerName, t.Attempt, t.LastError)
			}
		}
	}
	return ""
}
