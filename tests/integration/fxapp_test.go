//go:build integration

package integration

import (
	"context"
	"net"
	"net/http"
	"path"
	"strings"
	"testing"
	"time"

	"go.uber.org/fx/fxtest"

	"github.com/erikyryan/desafio-go/internal/config"
)

// TestFxAppStartsAndStopsCleanly boots the full composition against the real
// dependencies, checks readiness and verifies an orderly shutdown (workers
// drained, pool closed) within the configured timeout.
func TestFxAppStartsAndStopsCleanly(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	cfg := config.Config{
		InstanceID: "fx-test", LogLevel: "warn", HTTPAddr: addr, DatabaseURL: dbURL, DBMaxConns: 5,
		AWSRegion: "us-east-1", AWSEndpoint: sqsEndpoint, AWSAccessKeyID: "test", AWSSecretAccessKey: "test",
		WagerQueueName: path.Base(queues.WagerTransactions), WagerDLQName: path.Base(queues.WagerDLQ), EventsQueueName: path.Base(queues.WalletEvents),
		OIDCIssuer: issuer, OIDCJWKSURL: keycloakURL + "/realms/wager/protocol/openid-connect/certs", OIDCAudience: "wager-api",
		ConsumerEnabled: true, ConsumerWaitTime: time.Second, ConsumerMaxMessages: 5, ConsumerVisibilityTimeout: 5 * time.Second, ConsumerDrainTimeout: 5 * time.Second,
		OutboxEnabled: true, OutboxPollInterval: 200 * time.Millisecond, OutboxBatchSize: 10, OutboxLease: 10 * time.Second, OutboxInitialBackoff: time.Second, OutboxMaxBackoff: time.Second,
		PendingWorkerEnabled: true, PendingPollInterval: 200 * time.Millisecond, PendingBatchSize: 10, PendingInitialBackoff: time.Second, PendingMaxBackoff: time.Second, PendingMaxAttempts: 3, PendingTTL: time.Minute,
		ShutdownTimeout: 20 * time.Second,
	}
	app := fxtest.New(t, fxappOptions(cfg)...)
	app.RequireStart()
	resp, err := http.Get("http://" + addr + "/health/ready")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("ready: %d", resp.StatusCode)
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := app.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("shutdown took %s", d)
	}
	if _, err := http.Get("http://" + addr + "/health/live"); err == nil || !strings.Contains(err.Error(), "connect") {
		t.Fatalf("server still accepting connections: %v", err)
	}
}
