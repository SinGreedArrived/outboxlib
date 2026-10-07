// types.go
package outboxlib

import (
	"encoding/json"
	"math"
	"math/rand"
	"time"
	"uuid"
)

type State string

const (
	Pending  State = "pending"
	Running  State = "running"
	Complete State = "complete"
	Failed   State = "failed"
)

// ---- Retry policy per task ----

type BackoffKind string

const (
	BackoffFixed       BackoffKind = "fixed"       // всегда одно и то же время
	BackoffLinear      BackoffKind = "linear"      // Base * (1 + Factor*(attempt-1))
	BackoffExponential BackoffKind = "exponential" // Base * Factor^(attempt-1)
)

type BackoffPolicy struct {
	Kind       BackoffKind   `json:"Kind"`
	Base       time.Duration `json:"Base"`
	Max        time.Duration `json:"Max"`        // 0 = без потолка
	Factor     float64       `json:"Factor"`     // для linear/exponential, по умолчанию 2
	JitterFrac float64       `json:"JitterFrac"` // 0.25 = ±25%
}

// Delay — пауза перед (attempt+1)-й попыткой.
// attempt = сколько попыток уже сделано, начиная с 1.
func (b BackoffPolicy) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	var d time.Duration
	switch b.Kind {
	case BackoffFixed, "":
		d = b.Base
	case BackoffLinear:
		f := b.Factor
		if f <= 0 {
			f = 1
		}
		d = time.Duration(float64(b.Base) * (1 + f*float64(attempt-1)))
	case BackoffExponential:
		f := b.Factor
		if f <= 1 {
			f = 2
		}

		shift := min(attempt-1, 30)
		d = time.Duration(float64(b.Base) * math.Pow(f, float64(shift)))
	}
	if b.Max > 0 && d > b.Max {
		d = b.Max
	}
	if b.JitterFrac > 0 {
		j := int64(float64(d) * b.JitterFrac)
		if j > 0 {
			d += time.Duration(rand.Int63n(2*j) - j)
		}
	}
	if d < 0 {
		d = b.Base
	}
	return d
}

// ---- Task ----

type Task struct {
	ID          uuid.UUID       `json:"ID"`
	PipelineID  uuid.UUID       `json:"PipelineID"`
	HandlerName HandlerName     `json:"HandlerName"`
	Payload     json.RawMessage `json:"Payload"`

	Done   bool `json:"Done"`   // успешно завершена
	Failed bool `json:"Failed"` // исчерпала попытки, терминально

	Attempt       int           `json:"Attempt"`     // сколько раз уже запускали
	MaxAttempts   *int          `json:"MaxAttempts"` // 0 = бесконечно
	Backoff       BackoffPolicy `json:"Backoff"`
	NextAttemptAt time.Time     `json:"NextAttemptAt"`
	LastError     string        `json:"LastError"`
}

// ---- Stage / Pipeline ----

type Stage struct {
	Number int    `json:"Number"`
	Tasks  []Task `json:"Tasks"`
}

type Pipeline struct {
	ID            uuid.UUID
	State         State
	Stages        []Stage
	NextAttemptAt time.Time // min по всем незавершённым Task
	LastError     string
	LockedBy      string
	LockedUntil   time.Time
	Filters       json.RawMessage
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// ---- TaskLog ----

type TaskLog struct {
	ID         uuid.UUID
	TaskID     uuid.UUID
	PipelineID uuid.UUID
	Attempt    int // == task.Attempt в момент запуска
	Response   json.RawMessage
	Error      string
	StartTime  time.Time
	EndTime    time.Time
}
