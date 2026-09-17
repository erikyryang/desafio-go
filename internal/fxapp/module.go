// Package fxapp composes the service with Uber Fx: configuration, connections,
// repositories, use cases, HTTP handlers and workers, plus lifecycle hooks.
package fxapp

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/erikyryan/desafio-go/internal/adapters/auth"
	"github.com/erikyryan/desafio-go/internal/adapters/httpapi"
	"github.com/erikyryan/desafio-go/internal/adapters/postgres"
	sqsadapter "github.com/erikyryan/desafio-go/internal/adapters/sqs"
	"github.com/erikyryan/desafio-go/internal/app"
	"github.com/erikyryan/desafio-go/internal/config"
	"github.com/erikyryan/desafio-go/internal/faults"
	"github.com/erikyryan/desafio-go/internal/observability"
	"github.com/erikyryan/desafio-go/internal/workers"
)

// Module is the full application graph; callers supply config.Config.
var Module = fx.Options(
	ObservabilityModule,
	InfraModule,
	AppModule,
	HTTPModule,
	WorkersModule,
	fx.Invoke(registerLifecycle),
	fx.WithLogger(func(log *slog.Logger) fxevent.Logger {
		return &fxevent.SlogLogger{Logger: log.With("component", "fx")}
	}),
)

// ObservabilityModule provides the logger and metrics.
var ObservabilityModule = fx.Module("observability",
	fx.Provide(
		func(cfg config.Config) *slog.Logger {
			return observability.NewLogger(os.Stdout, cfg.LogLevel, cfg.InstanceID)
		},
		fx.Annotate(observability.NewMetrics,
			fx.As(new(app.Metrics)), fx.As(new(sqsadapter.ConsumerMetrics)), fx.As(new(workers.OutboxMetrics)), fx.As(fx.Self())),
	),
)

// InfraModule provides PostgreSQL, SQS, the token verifier and fault injection.
var InfraModule = fx.Module("infra",
	fx.Provide(
		newPool,
		fx.Annotate(postgres.NewUnitOfWork, fx.As(new(app.UnitOfWork))),
		newSQSClient,
		newQueues,
		newVerifier,
		func(cfg config.Config, log *slog.Logger) *faults.Injector { return faults.New(cfg.FaultInject, log) },
	),
)

// AppModule provides the use cases.
var AppModule = fx.Module("app",
	fx.Provide(
		func() app.Clock { return func() time.Time { return time.Now().UTC() } },
		func() app.IDGenerator {
			return func() uuid.UUID {
				id, err := uuid.NewV7()
				if err != nil {
					return uuid.New()
				}
				return id
			}
		},
		func(cfg config.Config) app.PendingReferencePolicy {
			return app.PendingReferencePolicy{InitialBackoff: cfg.PendingInitialBackoff, MaxBackoff: cfg.PendingMaxBackoff,
				MaxAttempts: cfg.PendingMaxAttempts, TTL: cfg.PendingTTL}
		},
		app.NewWalletService,
		app.NewWagerService,
	),
)

// HTTPModule provides handlers, router and server.
var HTTPModule = fx.Module("http",
	fx.Provide(
		httpapi.NewHandlers,
		fx.Annotate(newReadinessChecks, fx.ResultTags(`name:"readiness"`)),
		fx.Annotate(newRouter, fx.ParamTags(``, ``, `name:"readiness"`)),
		func(cfg config.Config, h http.Handler, log *slog.Logger) *httpapi.Server {
			return httpapi.NewServer(cfg.HTTPAddr, h, log)
		},
	),
)

// WorkersModule provides the consumer, publishers and the supervisor.
var WorkersModule = fx.Module("workers",
	fx.Provide(
		newConsumer,
		newOutboxPublisher,
		newPendingWorker,
		func(log *slog.Logger) *workers.Supervisor { return workers.NewSupervisor(log) },
	),
)

func newPool(cfg config.Config, log *slog.Logger) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if cfg.MigrateOnStart {
		if err := postgres.MigrateUp(cfg.DatabaseURL); err != nil {
			return nil, err
		}
		log.Info("migrations applied")
	}
	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return nil, err
	}
	log.Info("postgres connected")
	return pool, nil
}

func newSQSClient(cfg config.Config) (*awssqs.Client, error) {
	return sqsadapter.NewClient(context.Background(), sqsadapter.Config{
		Region: cfg.AWSRegion, Endpoint: cfg.AWSEndpoint, AccessKeyID: cfg.AWSAccessKeyID, SecretAccessKey: cfg.AWSSecretAccessKey,
	})
}

func newQueues(cfg config.Config, client *awssqs.Client, log *slog.Logger) (sqsadapter.Queues, error) {
	q, err := sqsadapter.ResolveQueues(context.Background(), client, cfg.WagerQueueName, cfg.WagerDLQName, cfg.EventsQueueName)
	if err != nil {
		return q, err
	}
	log.Info("sqs queues resolved", "wagerQueue", q.WagerTransactions, "dlq", q.WagerDLQ, "eventsQueue", q.WalletEvents)
	return q, nil
}

func newVerifier(cfg config.Config) (httpapi.TokenVerifier, error) {
	return auth.NewVerifier(context.Background(), auth.Config{Issuer: cfg.OIDCIssuer, JWKSURL: cfg.OIDCJWKSURL, Audience: cfg.OIDCAudience},
		&http.Client{Timeout: 10 * time.Second})
}

type pgReadiness struct{ pool *pgxpool.Pool }

func (pgReadiness) Name() string                      { return "postgres" }
func (p pgReadiness) Check(ctx context.Context) error { return p.pool.Ping(ctx) }

func newReadinessChecks(pool *pgxpool.Pool, client *awssqs.Client, q sqsadapter.Queues) []httpapi.ReadinessCheck {
	return []httpapi.ReadinessCheck{pgReadiness{pool: pool}, sqsadapter.ReadinessCheck{Client: client, QueueURL: q.WagerTransactions}}
}

func newRouter(h *httpapi.Handlers, verifier httpapi.TokenVerifier, checks []httpapi.ReadinessCheck, metrics *observability.Metrics, log *slog.Logger) http.Handler {
	return httpapi.Router(h, verifier, checks, metrics.Handler(), log)
}

func newConsumer(cfg config.Config, client *awssqs.Client, q sqsadapter.Queues, uow app.UnitOfWork, wagers *app.WagerService,
	metrics sqsadapter.ConsumerMetrics, log *slog.Logger, inj *faults.Injector, clock app.Clock) *sqsadapter.Consumer {
	return sqsadapter.NewConsumer(client, q.WagerTransactions, q.WagerDLQ, uow, wagers, metrics, log, sqsadapter.ConsumerConfig{
		WaitTime: cfg.ConsumerWaitTime, MaxMessages: cfg.ConsumerMaxMessages, VisibilityTimeout: cfg.ConsumerVisibilityTimeout, DrainTimeout: cfg.ConsumerDrainTimeout,
	}, inj, clock)
}

func newOutboxPublisher(cfg config.Config, client *awssqs.Client, q sqsadapter.Queues, uow app.UnitOfWork, metrics workers.OutboxMetrics,
	log *slog.Logger, inj *faults.Injector, clock app.Clock) *workers.OutboxPublisher {
	return workers.NewOutboxPublisher(uow, sqsadapter.NewEventPublisher(client, q.WalletEvents), metrics, log, workers.OutboxConfig{
		Owner: cfg.InstanceID, PollInterval: cfg.OutboxPollInterval, BatchSize: cfg.OutboxBatchSize, Lease: cfg.OutboxLease,
		InitialBackoff: cfg.OutboxInitialBackoff, MaxBackoff: cfg.OutboxMaxBackoff,
	}, inj, clock)
}

func newPendingWorker(cfg config.Config, wagers *app.WagerService, log *slog.Logger) *workers.PendingReferenceWorker {
	return workers.NewPendingReferenceWorker(wagers, log, workers.PendingConfig{PollInterval: cfg.PendingPollInterval, BatchSize: cfg.PendingBatchSize})
}

// lifecycleDeps lists everything whose start/stop must be ordered.
type lifecycleDeps struct {
	fx.In
	Config     config.Config
	Log        *slog.Logger
	Pool       *pgxpool.Pool
	Server     *httpapi.Server
	Supervisor *workers.Supervisor
	Consumer   *sqsadapter.Consumer
	Outbox     *workers.OutboxPublisher
	Pending    *workers.PendingReferenceWorker
	Shutdowner fx.Shutdowner
}

// registerLifecycle appends hooks in dependency order. Fx runs OnStop hooks
// in reverse registration order, so the HTTP server (registered last) stops
// first, then the workers drain, and the pool closes last.
func registerLifecycle(lc fx.Lifecycle, d lifecycleDeps) {
	lc.Append(fx.Hook{OnStop: func(ctx context.Context) error {
		d.Pool.Close()
		d.Log.Info("postgres pool closed")
		return nil
	}})

	runners := map[string]workers.Runner{}
	if d.Config.ConsumerEnabled {
		runners["sqs-consumer"] = d.Consumer
	}
	if d.Config.OutboxEnabled {
		runners["outbox-publisher"] = d.Outbox
	}
	if d.Config.PendingWorkerEnabled {
		runners["pending-reference-worker"] = d.Pending
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			d.Supervisor.Start(ctx, runners)
			return nil
		},
		OnStop: func(ctx context.Context) error { return d.Supervisor.Stop(ctx) },
	})

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			errCh := make(chan error, 1)
			d.Server.Start(errCh)
			go func() {
				if err := <-errCh; err != nil {
					_ = d.Shutdowner.Shutdown(fx.ExitCode(1))
				}
			}()
			d.Log.Info("http server listening", "addr", d.Server.Addr())
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if err := d.Server.Stop(ctx); err != nil {
				return fmt.Errorf("http shutdown: %w", err)
			}
			d.Log.Info("http server stopped")
			return nil
		},
	})
}

// New builds the Fx application for the given configuration.
func New(cfg config.Config, extra ...fx.Option) *fx.App {
	opts := []fx.Option{fx.Supply(cfg), Module, fx.StartTimeout(60 * time.Second), fx.StopTimeout(cfg.ShutdownTimeout)}
	opts = append(opts, extra...)
	return fx.New(opts...)
}
