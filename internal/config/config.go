// Package config loads and validates the process configuration from the
// environment. Every value has a local default documented in .env.example.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the validated configuration.
type Config struct {
	InstanceID string
	LogLevel   string
	HTTPAddr   string

	DatabaseURL    string
	DBMaxConns     int32
	MigrateOnStart bool

	AWSRegion          string
	AWSEndpoint        string
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	WagerQueueName     string
	WagerDLQName       string
	EventsQueueName    string

	OIDCIssuer   string
	OIDCJWKSURL  string
	OIDCAudience string

	ConsumerEnabled           bool
	ConsumerWaitTime          time.Duration
	ConsumerMaxMessages       int32
	ConsumerVisibilityTimeout time.Duration
	ConsumerDrainTimeout      time.Duration

	OutboxEnabled        bool
	OutboxPollInterval   time.Duration
	OutboxBatchSize      int
	OutboxLease          time.Duration
	OutboxInitialBackoff time.Duration
	OutboxMaxBackoff     time.Duration

	PendingWorkerEnabled  bool
	PendingPollInterval   time.Duration
	PendingBatchSize      int
	PendingInitialBackoff time.Duration
	PendingMaxBackoff     time.Duration
	PendingMaxAttempts    int
	PendingTTL            time.Duration

	ShutdownTimeout time.Duration
	FaultInject     string
}

// Load reads the environment.
func Load() (Config, error) {
	var errs []error
	host, _ := os.Hostname()
	c := Config{
		InstanceID:     getenv("INSTANCE_ID", host),
		LogLevel:       getenv("LOG_LEVEL", "info"),
		HTTPAddr:       getenv("HTTP_ADDR", ":8081"),
		DatabaseURL:    getenv("DATABASE_URL", ""),
		DBMaxConns:     int32(getint("DB_MAX_CONNS", 20, &errs)),
		MigrateOnStart: getbool("MIGRATE_ON_START", false, &errs),

		AWSRegion:          getenv("AWS_REGION", "us-east-1"),
		AWSEndpoint:        getenv("AWS_ENDPOINT_URL", ""),
		AWSAccessKeyID:     getenv("AWS_ACCESS_KEY_ID", ""),
		AWSSecretAccessKey: getenv("AWS_SECRET_ACCESS_KEY", ""),
		WagerQueueName:     getenv("SQS_WAGER_QUEUE_NAME", "wager-transactions.fifo"),
		WagerDLQName:       getenv("SQS_WAGER_DLQ_NAME", "wager-transactions-dlq.fifo"),
		EventsQueueName:    getenv("SQS_EVENTS_QUEUE_NAME", "wallet-events.fifo"),

		OIDCIssuer:   getenv("OIDC_ISSUER", ""),
		OIDCJWKSURL:  getenv("OIDC_JWKS_URL", ""),
		OIDCAudience: getenv("OIDC_AUDIENCE", "wager-api"),

		ConsumerEnabled:           getbool("CONSUMER_ENABLED", true, &errs),
		ConsumerWaitTime:          getdur("CONSUMER_WAIT_TIME", 10*time.Second, &errs),
		ConsumerMaxMessages:       int32(getint("CONSUMER_MAX_MESSAGES", 10, &errs)),
		ConsumerVisibilityTimeout: getdur("CONSUMER_VISIBILITY_TIMEOUT", 30*time.Second, &errs),
		ConsumerDrainTimeout:      getdur("CONSUMER_DRAIN_TIMEOUT", 15*time.Second, &errs),

		OutboxEnabled:        getbool("OUTBOX_PUBLISHER_ENABLED", true, &errs),
		OutboxPollInterval:   getdur("OUTBOX_POLL_INTERVAL", 500*time.Millisecond, &errs),
		OutboxBatchSize:      getint("OUTBOX_BATCH_SIZE", 50, &errs),
		OutboxLease:          getdur("OUTBOX_LEASE", 30*time.Second, &errs),
		OutboxInitialBackoff: getdur("OUTBOX_INITIAL_BACKOFF", time.Second, &errs),
		OutboxMaxBackoff:     getdur("OUTBOX_MAX_BACKOFF", time.Minute, &errs),

		PendingWorkerEnabled:  getbool("PENDING_WORKER_ENABLED", true, &errs),
		PendingPollInterval:   getdur("PENDING_POLL_INTERVAL", time.Second, &errs),
		PendingBatchSize:      getint("PENDING_BATCH_SIZE", 50, &errs),
		PendingInitialBackoff: getdur("PENDING_INITIAL_BACKOFF", 2*time.Second, &errs),
		PendingMaxBackoff:     getdur("PENDING_MAX_BACKOFF", time.Minute, &errs),
		PendingMaxAttempts:    getint("PENDING_MAX_ATTEMPTS", 10, &errs),
		PendingTTL:            getdur("PENDING_TTL", 15*time.Minute, &errs),

		ShutdownTimeout: getdur("SHUTDOWN_TIMEOUT", 25*time.Second, &errs),
		FaultInject:     getenv("FAULT_INJECT", ""),
	}
	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}
	if c.OIDCIssuer == "" || c.OIDCJWKSURL == "" {
		errs = append(errs, errors.New("OIDC_ISSUER and OIDC_JWKS_URL are required"))
	}
	if c.ConsumerVisibilityTimeout < 5*time.Second {
		errs = append(errs, errors.New("CONSUMER_VISIBILITY_TIMEOUT must be >= 5s"))
	}
	if c.PendingMaxAttempts < 1 || c.PendingTTL <= 0 {
		errs = append(errs, errors.New("PENDING_MAX_ATTEMPTS must be >= 1 and PENDING_TTL > 0"))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("SHUTDOWN_TIMEOUT must be > 0"))
	}
	return c, errors.Join(errs...)
}

func getenv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return def
}

func getint(key string, def int, errs *[]error) int {
	v := getenv(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return n
}

func getbool(key string, def bool, errs *[]error) bool {
	v := getenv(key, "")
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return b
}

func getdur(key string, def time.Duration, errs *[]error) time.Duration {
	v := getenv(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return d
}
