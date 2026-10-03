// config.go
package outboxlib

import (
	"math/rand"
	"os"
	"strconv"
	"time"
)

// config.go

func envDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

type Config struct {
	InstanceID         string        // в K8s = $HOSTNAME (имя пода)
	PollInterval       time.Duration // как часто опрашивать БД
	LeaseDuration      time.Duration // TTL lease
	HeartbeatInterval  time.Duration // должен быть << LeaseDuration
	RetryBase          time.Duration // базовая задержка ретрая
	RetryMax           time.Duration // потолок задержки
	DefaultMaxAttempts int
	BatchSize          int // сколько pipeline забирать за один poll
	Concurrency        int // сколько pipeline параллельно на инстанс
}

func DefaultConfig() Config {
	return Config{
		InstanceID:         envOr("HOSTNAME", "local"),
		PollInterval:       envDur("OUTBOX_POLL_INTERVAL", 3*time.Second),
		LeaseDuration:      envDur("OUTBOX_LEASE", 60*time.Second),
		HeartbeatInterval:  envDur("OUTBOX_HEARTBEAT", 20*time.Second),
		RetryBase:          envDur("OUTBOX_RETRY_BASE", 5*time.Second),
		RetryMax:           envDur("OUTBOX_RETRY_MAX", 30*time.Minute),
		DefaultMaxAttempts: envInt("OUTBOX_MAX_ATTEMPTS", 10),
		BatchSize:          envInt("OUTBOX_BATCH_SIZE", 20),
		Concurrency:        envInt("OUTBOX_CONCURRENCY", 8),
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// Backoff — экспоненциальная задержка с потолком и jitter.
// attempt начинается с 1.
func Backoff(attempt int, base, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	// 2^(attempt-1): 1, 2, 4, 8, ...
	shift := min(attempt-1, 30)
	d := base * time.Duration(1<<shift)
	if d <= 0 || d > max {
		d = max
	}
	// jitter ±25%, чтобы избежать thundering herd
	j := int64(d) / 4
	if j > 0 {
		d += time.Duration(rand.Int63n(2*j) - j)
	}
	if d < 0 {
		d = base
	}
	return d
}
