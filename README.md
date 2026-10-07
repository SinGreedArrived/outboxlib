# Outbox Library

[![Go Reference](https://pkg.go.dev/badge.svg)](https://pkg.go.dev/outbox_deepseek/pkg/outboxlib)

Go-библиотека для реализации паттерна **Outbox** — надёжного фонового выполнения задач с гарантией доставки даже при сбоях.

## Основные возможности

- **Гарантия доставки**: задачи сохраняются в PostgreSQL перед выполнением
- **Retry с backoff**: экспоненциальный, линейный или фиксированный
- **Распределённые инстансы**: несколько рабочих подов могут обрабатывать одну очередь
- **Stale lease recovery**: автоматический перехват задач у упавших подов
- **Task-level retry**: ретраи на уровне отдельных задач внутри pipeline
- **Task logging**: полная история всех попыток выполнения
- **Pipeline filters**: фильтрация pipeline по кастомным метаданным при создании

## Архитектура

```
┌─────────────────────────────────────────────────────────────┐
│                    Outbox Library                           │
├─────────────────────────────────────────────────────────────┤
│  Outbox (core)  →  WorkerPool  →  PipelineStore (Postgres) │
│                    ↓                                         │
│              HandlerStore (user handlers)                   │
│                    ↓                                         │
│              TaskLogStore (postgres)                        │
└─────────────────────────────────────────────────────────────┘
```

## Быстрый старт

```go
import (
    "context"
    "database/sql"
    "encoding/json"
    "log/slog"
    outbox "outbox_deepseek/pkg/outboxlib"
)

// 1. Регистрируем обработчики
reg := outbox.NewRegistrator()

reg.Register("send-email", func(ctx context.Context, req SendEmailRequest) (SendEmailResponse, error) {
    // ваша логика
    return SendEmailResponse{Sent: true}, nil
})

// 2. Инициализируем Outbox
outbox := outbox.New(
    outbox.WithConfig(outbox.DefaultConfig()),
    outbox.WithStore(outbox.NewPostgresStore(db)),
    outbox.WithLogStore(outbox.NewPostgresTaskLogStore(db)),
    outbox.WithHandlerStore(reg),
    outbox.WithLogger(slog.Default()),
)

// 3. Запускаем (неблокирующий)
outbox.Start(ctx)

// 4. Создаём pipeline
p := outbox.NewPipelineBuilder().
    Stage(outbox.NewTask("send-email").WithPayload(map[string]string{"to": "user@example.com"})),
    Stage(outbox.NewTask("notify-slack")),
    Build()

// 5. (опционально) Добавляем фильтры для pipeline
p := outbox.NewPipelineBuilder().
    Stage(outbox.NewTask("send-email").WithPayload(map[string]string{"to": "user@example.com"})),
    Stage(outbox.NewTask("notify-slack")),
    WithFilters(map[string]string{"env": "production", "priority": "high"}),
    Build()

id, err := outbox.AddPipeline(ctx, p)
```

## Основные типы

### Pipeline
Агрегат, который включает один или несколько **Stage**. Каждый Stage содержит набор **Task**.

```go
p := Pipeline{
    ID:        uuid.New(),
    State:     Pending,
    Stages:    []Stage{...},
    NextAttemptAt: time.Now(),
}
```

### Task
Отдельная задача с настраиваемой политикой ретраев:

```go
Task{
    HandlerName: "send-email",
    Payload:     json.RawMessage(...),
    MaxAttempts: 3,
    Backoff: BackoffPolicy{
        Kind:       BackoffExponential,
        Base:       5 * time.Second,
        Max:        30 * time.Minute,
        Factor:     2,
        JitterFrac: 0.25,
    },
}
```

### BackoffPolicy

| Kind | Описание |
|------|----------|
| `BackoffFixed` | Постоянная задержка |
| `BackoffLinear` | Линейный рост: `Base * (1 + Factor * (attempt-1))` |
| `BackoffExponential` | Экспонента: `Base * Factor^(attempt-1)` |

## Состояния

- `Pending` — ожидание
- `Running` — выполняется (lease взят)
- `Complete` — успешно завершён
- `Failed` — исчерпаны попытки (терминально)

## Конфигурация

```go
type Config struct {
    InstanceID         string        // ID инстанса (в K8s = $HOSTNAME)
    PollInterval       time.Duration // интервал опроса БД (default: 3s)
    LeaseDuration      time.Duration // TTL lease (default: 60s)
    HeartbeatInterval  time.Duration // интервал heartbeat (default: 20s)
    RetryBase          time.Duration // базовая задержка (default: 5s)
    RetryMax           time.Duration // потолок задержки (default: 30m)
    DefaultMaxAttempts int           // макс. попыток (default: 10)
    BatchSize          int           // pipeline за poll (default: 20)
    Concurrency        int           // параллельных pipeline (default: 8)
}
```

### Переменные окружения

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `OUTBOX_POLL_INTERVAL` | 3s | Интервал опроса |
| `OUTBOX_LEASE` | 60s | Длительность lease |
| `OUTBOX_HEARTBEAT` | 20s | Heartbeat интервал |
| `OUTBOX_RETRY_BASE` | 5s | Базовая задержка |
| `OUTBOX_RETRY_MAX` | 30m | Макс. задержка |
| `OUTBOX_MAX_ATTEMPTS` | 10 | Макс. попыток |
| `OUTBOX_BATCH_SIZE` | 20 | Batch size |
| `OUTBOX_CONCURRENCY` | 8 | Конкурентность |

## API

### Outbox

| Метод | Описание |
|-------|----------|
| `Start(ctx)` | Запуск worker pool (неблокирующий) |
| `AddPipeline(ctx, p)` | Добавление pipeline в очередь |

### PipelineBuilder

| Метод | Описание |
|-------|----------|
| `Stage(tasks...)` | Добавить stage с задачами |
| `WithFilters[T](v)` | Добавить фильтры (метаданные) для pipeline |
| `Build()` | Создать pipeline |

### Task Options

| Метод | Описание |
|-------|----------|
| `WithMaxAttempts(n)` | Установить макс. попытки |
| `WithUnlimitedAttempts()` | Бесконечные попытки (0) |
| `WithFixedDelay(d)` | Фиксированная задержка |
| `WithExponentialBackoff(base, max)` | Экспоненциальный backoff |
| `WithLinearBackoff(base, factor, max)` | Линейный backoff |
| `WithPayload[T](payload)` | Добавление payload(any)|
| `WithBackoff(p)` | Кастомная политика |

## Фильтры pipeline

Pipeline можно помечать кастомными метаданными (фильтрами) при создании. Фильтры сохраняются в БД как JSONB и могут быть использованы для последующей фильтрации:

```go
p := outbox.NewPipelineBuilder().
    Stage(outbox.NewTask("send-email")),
    WithFilters(map[string]string{
        "env": "production",
        "priority": "high",
        "tenant_id": "abc123",
    }),
    Build()
```

## Таблицы БД

### pipelines

| Поле | Тип | Описание |
|------|-----|----------|
| id | UUID | PK |
| state | TEXT | pending/running/complete/failed |
| stages | JSONB | Полное дерево задач |
| filter | JSONB | Фильтры (метаданные) pipeline |
| next_attempt_at | TIMESTAMPTZ | Когда следующая попытка |
| last_error | TEXT | Последняя ошибка |
| locked_by | TEXT | Кто держит lease |
| locked_until | TIMESTAMPTZ | Время истечения lease |
| created_at | TIMESTAMPTZ | — |
| updated_at | TIMESTAMPTZ | — |

### task_logs

| Поле | Тип | Описание |
|------|-----|----------|
| id | UUID | PK |
| task_id | UUID | Ссылка на task |
| pipeline_id | UUID | Ссылка на pipeline |
| attempt | INT | Номер попытки |
| response | JSONB | Ответ обработчика |
| error | TEXT | Ошибка (если была) |
| start_time | TIMESTAMPTZ | — |
| end_time | TIMESTAMPTZ | — |

## Миграции

См. `example/simple/migrations/`

```bash
# Применить миграции
goose -dir example/simple/migrations postgres "$DATABASE_URL" up

# Создать новую миграцию
goose create add_new_field sql
```

## Примеры

См. `example/simple/main.go` — полный пример приложения с HTTP API для создания pipelines.

## License

MIT
