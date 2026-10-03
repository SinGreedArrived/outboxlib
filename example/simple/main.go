package main

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	outbox "github.com/SinGreedArrived/outboxlib"

	"github.com/pressly/goose/v3"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	SendEmail   outbox.HandlerName = "send-email"
	NotifySlack outbox.HandlerName = "notify-slack"
	Flack       outbox.HandlerName = "flaky"
)

//go:embed migrations/*.sql
var embedMigrations embed.FS

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		logger.Error("DATABASE_URL is required")
		os.Exit(1)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		logger.Error("db open", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	// ...
	goose.SetBaseFS(embedMigrations)
	if err := goose.SetDialect("postgres"); err != nil {
		logger.Error("goose dialect", "err", err)
		os.Exit(1)
	}
	if err := goose.UpContext(ctx, db, "migrations"); err != nil {
		logger.Error("goose up", "err", err)
		os.Exit(1)
	}

	// Ждём, пока Postgres поднимется (на случай отсутствия healthcheck)
	if err := waitDB(db, 30*time.Second); err != nil {
		logger.Error("db not ready", "err", err)
		os.Exit(1)
	}
	registr := outbox.NewRegistrator()
	// Регистрируем хендлеры
	type SendEmailRequest struct {
		Body string
	}
	type SendEmailResponse struct {
		Sent bool
	}

	registr.Register(
		SendEmail,
		func(ctx context.Context, v SendEmailRequest) (SendEmailResponse, error) {
			logger.Info("send-email", "payload", v.Body)
			time.Sleep(2 * time.Second) // имитация работы
			return SendEmailResponse{Sent: true}, nil
		},
	)
	registr.Register(
		NotifySlack,
		func(ctx context.Context, v json.RawMessage) (json.RawMessage, error) {
			logger.Info("notify-slack")
			return json.RawMessage(`{"ok":true}`), nil
		},
	)
	// Хендлер, который иногда падает — для проверки retry
	registr.Register(
		Flack,
		func(ctx context.Context, v json.RawMessage) (json.RawMessage, error) {
			if time.Now().UnixNano()%2 == 0 {
				return nil, os.ErrDeadlineExceeded
			}
			return json.RawMessage(`{"ok":true}`), nil
		},
	)

	cfg := outbox.DefaultConfig()
	logger.Info("starting", "instance", cfg.InstanceID)

	mainOutbox := outbox.New(
		outbox.WithConfig(cfg),
		outbox.WithStore(outbox.NewPostgresStore(db)),
		outbox.WithLogStore(outbox.NewPostgresTaskLogStore(db)),
		outbox.WithHandlerStore(registr),
		outbox.WithLogger(logger),
	)

	mainOutbox.Start(ctx)

	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := db.PingContext(r.Context()); err != nil {
			http.Error(w, "db down", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})

	// Создать pipeline
	mux.HandleFunc("/pipelines", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}

		p := outbox.NewPipelineBuilder().
			Stage(outbox.NewTask(SendEmail).WithPayload(map[string]string{"to": "a@b.c"}).WithMaxAttempts(5)).
			Stage(outbox.NewTask(NotifySlack)).
			Build()

		id, err := mainOutbox.AddPipeline(r.Context(), p)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"id": id.String()})
	})

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("http listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http", "err", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
}

func waitDB(db *sql.DB, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := db.Ping(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	return lastErr
}
