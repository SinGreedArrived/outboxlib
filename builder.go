// builder.go
package outboxlib

import (
	"encoding/json"
	"time"

	"uuid"
)

type HandlerName string
type TaskOption func(*Task)


// Установить максимальное кол-во ретраев
func (t Task) WithMaxAttempts(n int) Task {
	t.MaxAttempts = new(n)

	return t
}

// Бесконечные попытки (например, для задач, которые обязаны доехать).
func (t Task) WithUnlimitedAttempts() Task {
	t.MaxAttempts = new(0)

	return t
}

// Фиксированная задержка — например, «пробовать каждые 5 секунд».
func (t Task) WithFixedDelay(d time.Duration) Task {
		t.Backoff = BackoffPolicy{Kind: BackoffFixed, Base: d, Max: d}

		return t
}

// Экспонента — «5s, 10s, 20s, ... до 30 минут».
func (t Task) WithExponentialBackoff(base, max time.Duration) Task {
		t.Backoff = BackoffPolicy{
			Kind: BackoffExponential, Base: base, Max: max,
			Factor: 2, JitterFrac: 0.25,
		}

		return t
}

// Линейный рост.
func (t Task) WithLinearBackoff(base time.Duration, factor float64, max time.Duration) Task{
		t.Backoff = BackoffPolicy{
			Kind: BackoffLinear, Base: base, Max: max,
			Factor: factor, JitterFrac: 0.25,
		}

		return t
}

// Payload задачи
func (t Task) WithPayload[T any](payload T) Task {
		raw, _ := json.Marshal(payload)
		t.Payload = raw

		return t
}

// Политика ретраев
func (t Task) WithBackoff(p BackoffPolicy) Task{
	t.Backoff = p 

	return t
}

// NewTask по умолчанию: 3 попытки, экспонента 5s → 30m.
func NewTask(handlerName HandlerName, opts ...TaskOption) Task {
	var raw json.RawMessage

	t := Task{
		ID:          uuid.New(),
		HandlerName: handlerName,
		Payload:     raw,
		MaxAttempts: new(3),
		Backoff: BackoffPolicy{
			Kind: BackoffExponential, Base: 5 * time.Second,
			Max: 30 * time.Minute, Factor: 2, JitterFrac: 0.25,
		},
	}
	for _, opt := range opts {
		opt(&t)
	}

	return t
}

type PipelineBuilder struct {
	p Pipeline
}

func NewPipelineBuilder() *PipelineBuilder {
	return &PipelineBuilder{p: Pipeline{ID: uuid.New(), State: Pending}}
}

func (b *PipelineBuilder) Stage(tasks ...Task) *PipelineBuilder {
	b.p.Stages = append(b.p.Stages, Stage{
		Number: len(b.p.Stages) + 1,
		Tasks:  tasks,
	})
	return b
}

func (b *PipelineBuilder) WithFilters[T any](v T) *PipelineBuilder {
	filterRaw,_ := json.Marshal(v)
	b.p.Filters = filterRaw

	return b
}

func (b *PipelineBuilder) Build() Pipeline {
	p := b.p
	p.NextAttemptAt = time.Now()
	for si := range p.Stages {
		for ti := range p.Stages[si].Tasks {
			p.Stages[si].Tasks[ti].PipelineID = p.ID
		}
	}
	return p
}
