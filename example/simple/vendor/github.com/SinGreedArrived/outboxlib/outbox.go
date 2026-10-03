// outbox.go
package outboxlib

import (
	"context"
	"log/slog"
	"sync"
	"time"
	"uuid"
)

type Option func(*Outbox)

func WithConfig(c Config) Option             { return func(o *Outbox) { o.cfg = c } }
func WithStore(s PipelineStore) Option       { return func(o *Outbox) { o.store = s } }
func WithLogStore(l TaskLogStore) Option     { return func(o *Outbox) { o.logs = l } }
func WithHandlerStore(h HandlerStore) Option { return func(o *Outbox) { o.handlers = h } }
func WithLogger(l *slog.Logger) Option       { return func(o *Outbox) { o.logger = l } }

type Outbox struct {
	cfg       Config
	store     PipelineStore
	logs      TaskLogStore
	handlers  HandlerStore
	logger    *slog.Logger
	pool      *WorkerPool
	startOnce sync.Once
}

func New(opts ...Option) *Outbox {
	o := &Outbox{
		cfg:      DefaultConfig(),
		handlers: def,
		logger:   slog.Default(),
		logs:     nil,
	}
	for _, opt := range opts {
		opt(o)
	}
	if o.logger == nil {
		o.logger = slog.Default()
	}
	o.pool = NewWorkerPool(o.cfg, o.store, o.logs, o.handlers, o.logger)
	return o
}

// Start запускает polling-loop на этом инстансе. Возвращается сразу.
func (o *Outbox) Start(ctx context.Context) {
	o.startOnce.Do(func() {
		go func() {
			o.logger.Info("outbox started", "instance", o.cfg.InstanceID)
			o.pool.Run(ctx)
			o.logger.Info("outbox stopped", "instance", o.cfg.InstanceID)
		}()
	})
}

// AddPipeline сохраняет pipeline в общую БД.
// Любой инстанс может его создать — выполнит тот, кто первый claim'нет.
func (o *Outbox) AddPipeline(ctx context.Context, p Pipeline) (uuid.UUID, error) {
	if p.ID == uuid.Nil() {
		p.ID = uuid.New()
	}
	p.State = Pending
	p.NextAttemptAt = time.Now()
	p.CreatedAt = time.Now()
	p.UpdatedAt = p.CreatedAt

	for si := range p.Stages {
		stage := &p.Stages[si]
		if stage.Number == 0 {
			stage.Number = si + 1
		}
		for ti := range stage.Tasks {
			t := &stage.Tasks[ti]

			if t.ID == uuid.Nil() {
				t.ID = uuid.New()
			}
			t.PipelineID = p.ID

			// Дефолт для MaxAttempts: если явно не задано —
			// берём конфиг. Но помним: 0 = «бесконечно».
			// Чтобы отличить «не задано» от «хочу бесконечно»,
			// используем отрицательное значение как маркер (см. ниже).
			if t.MaxAttempts == nil {
				*t.MaxAttempts = o.cfg.DefaultMaxAttempts
			}

			// Дефолт для Backoff: если пользователь создал Task{} руками.
			if t.Backoff.Kind == "" && t.Backoff.Base == 0 {
				t.Backoff = BackoffPolicy{
					Kind:       BackoffExponential,
					Base:       o.cfg.RetryBase,
					Max:        o.cfg.RetryMax,
					Factor:     2,
					JitterFrac: 0.25,
				}
			}

			// Первая попытка — сейчас.
			if t.NextAttemptAt.IsZero() {
				t.NextAttemptAt = p.CreatedAt
			}
			t.Attempt = 0
			t.Done = false
			t.Failed = false
		}
	}

	// Агрегат для claim'а.
	p.NextAttemptAt = minNextAttempt(p)

	return p.ID, o.store.Save(ctx, p)
}

func minNextAttempt(p Pipeline) time.Time {
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

// Backoff, config — из config.go.
func timeNow() time.Time { return time.Now() }
