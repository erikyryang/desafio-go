//go:build integration

// Package integration runs against real PostgreSQL, Keycloak and LocalStack
// (docker compose up postgres keycloak localstack). Each run creates its own
// database and SQS queues so it never interferes with running app instances.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/erikyryan/desafio-go/internal/adapters/postgres"
	sqsadapter "github.com/erikyryan/desafio-go/internal/adapters/sqs"
	"github.com/erikyryan/desafio-go/internal/app"
	"github.com/erikyryan/desafio-go/internal/domain/money"
	"github.com/erikyryan/desafio-go/internal/domain/wager"
)

var (
	adminURL    = envOr("IT_DATABASE_ADMIN_URL", "postgres://wager:wager@localhost:5432/wager?sslmode=disable")
	sqsEndpoint = envOr("IT_SQS_ENDPOINT", "http://localhost:4566")
	keycloakURL = envOr("IT_KEYCLOAK_URL", "http://localhost:8080")
	issuer      = envOr("IT_OIDC_ISSUER", "http://localhost:8080/realms/wager")

	dbURL     string
	pool      *pgxpool.Pool
	sqsClient *awssqs.Client
	queues    sqsadapter.Queues
	logger    = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	runID     = strings.ReplaceAll(uuid.NewString()[:8], "-", "")
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	dbURL, err = createDatabase(ctx, "wager_it_"+runID)
	if err != nil {
		fmt.Fprintln(os.Stderr, "create database:", err)
		os.Exit(1)
	}
	if err := postgres.MigrateUp(dbURL); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
	if pool, err = postgres.NewPool(ctx, dbURL, 10); err != nil {
		fmt.Fprintln(os.Stderr, "pool:", err)
		os.Exit(1)
	}
	if sqsClient, err = sqsadapter.NewClient(ctx, sqsadapter.Config{Region: "us-east-1", Endpoint: sqsEndpoint, AccessKeyID: "test", SecretAccessKey: "test"}); err != nil {
		fmt.Fprintln(os.Stderr, "sqs:", err)
		os.Exit(1)
	}
	if err := createQueues(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "queues:", err)
		os.Exit(1)
	}
	code := m.Run()
	pool.Close()
	for _, q := range []string{queues.WagerTransactions, queues.WagerDLQ, queues.WalletEvents} {
		_, _ = sqsClient.DeleteQueue(ctx, &awssqs.DeleteQueueInput{QueueUrl: aws.String(q)})
	}
	_ = dropDatabase(ctx, "wager_it_"+runID)
	os.Exit(code)
}

func createDatabase(ctx context.Context, name string) (string, error) {
	admin, err := pgxpool.New(ctx, adminURL)
	if err != nil {
		return "", err
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		return "", err
	}
	u, err := url.Parse(adminURL)
	if err != nil {
		return "", err
	}
	u.Path = "/" + name
	return u.String(), nil
}

func dropDatabase(ctx context.Context, name string) error {
	admin, err := pgxpool.New(ctx, adminURL)
	if err != nil {
		return err
	}
	defer admin.Close()
	_, err = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	return err
}

func createQueues(ctx context.Context) error {
	dlq, err := sqsClient.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String("it-" + runID + "-wager-dlq.fifo"),
		Attributes: map[string]string{"FifoQueue": "true"}})
	if err != nil {
		return err
	}
	attrs, err := sqsClient.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{QueueUrl: dlq.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	if err != nil {
		return err
	}
	redrive, _ := json.Marshal(map[string]string{"deadLetterTargetArn": attrs.Attributes["QueueArn"], "maxReceiveCount": "3"})
	main, err := sqsClient.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String("it-" + runID + "-wager.fifo"),
		Attributes: map[string]string{"FifoQueue": "true", "VisibilityTimeout": "5", "RedrivePolicy": string(redrive)}})
	if err != nil {
		return err
	}
	events, err := sqsClient.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String("it-" + runID + "-events.fifo"),
		Attributes: map[string]string{"FifoQueue": "true"}})
	if err != nil {
		return err
	}
	queues = sqsadapter.Queues{WagerTransactions: aws.ToString(main.QueueUrl), WagerDLQ: aws.ToString(dlq.QueueUrl), WalletEvents: aws.ToString(events.QueueUrl)}
	return nil
}

// services builds an independent "instance" (own pool = own connections).
type services struct {
	pool    *pgxpool.Pool
	uow     *postgres.UnitOfWork
	wallets *app.WalletService
	wagers  *app.WagerService
	clock   *fakeClock
}

type fakeClock struct{ offset time.Duration }

func (c *fakeClock) Now() time.Time { return time.Now().UTC().Add(c.offset) }

func newServices(t *testing.T, policy app.PendingReferencePolicy) *services {
	t.Helper()
	p, err := postgres.NewPool(context.Background(), dbURL, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return servicesFor(p, policy)
}

func servicesFor(p *pgxpool.Pool, policy app.PendingReferencePolicy) *services {
	if policy.MaxAttempts == 0 {
		policy = app.PendingReferencePolicy{InitialBackoff: 200 * time.Millisecond, MaxBackoff: time.Second, MaxAttempts: 5, TTL: time.Hour}
	}
	clock := &fakeClock{}
	uow := postgres.NewUnitOfWork(p)
	ids := func() uuid.UUID { id, _ := uuid.NewV7(); return id }
	return &services{pool: p, uow: uow, clock: clock,
		wallets: app.NewWalletService(uow, clock.Now, ids, nil, logger),
		wagers:  app.NewWagerService(uow, clock.Now, ids, nil, logger, policy)}
}

func (s *services) openWallet(t *testing.T, balance string) *wager.Wallet {
	t.Helper()
	w, err := s.wallets.OpenWallet(context.Background(), app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: money.MustParse(balance, "BRL"), CorrelationID: "it"})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func cmd(w *wager.Wallet, kind, ext, amount string) app.SubmitCommand {
	return app.SubmitCommand{IdempotencyKey: "provider-a:" + ext, ProviderID: "provider-a", ExternalTransactionID: ext,
		PlayerID: w.PlayerID().String(), WalletID: w.ID().String(), RoundID: "round-1", GameID: "game", Kind: kind,
		Amount: amount, Currency: "BRL", Source: app.SourceHTTP, CorrelationID: "it"}
}

func (s *services) submit(t *testing.T, c app.SubmitCommand) *app.SubmitResult {
	t.Helper()
	res, err := s.wagers.Submit(context.Background(), c)
	if err != nil {
		t.Fatalf("submit %s %s: %v", c.Kind, c.ExternalTransactionID, err)
	}
	return res
}

func (s *services) balance(t *testing.T, id uuid.UUID) *wager.Wallet {
	t.Helper()
	w, err := s.wallets.GetWallet(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// ledgerStats returns credits, debits, count for a wallet straight from SQL.
func ledgerStats(t *testing.T, walletID uuid.UUID) (credits, debits, count int64) {
	t.Helper()
	err := pool.QueryRow(context.Background(), `SELECT COALESCE(SUM(amount_minor) FILTER (WHERE direction='CREDIT'),0), COALESCE(SUM(amount_minor) FILTER (WHERE direction='DEBIT'),0), COUNT(*) FROM wallet_ledger_entries WHERE wallet_id=$1`, walletID).Scan(&credits, &debits, &count)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func assertReconciled(t *testing.T, s *services, walletID uuid.UUID) {
	t.Helper()
	rep, err := s.wallets.Reconcile(context.Background(), walletID)
	if err != nil || !rep.Consistent {
		t.Fatalf("reconciliation: %v %+v", err, rep)
	}
}

// token fetches a client_credentials token from Keycloak.
func token(t *testing.T, clientID, secret string) string {
	t.Helper()
	resp, err := http.PostForm(keycloakURL+"/realms/wager/protocol/openid-connect/token",
		url.Values{"grant_type": {"client_credentials"}, "client_id": {clientID}, "client_secret": {secret}})
	if err != nil {
		t.Fatalf("keycloak: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("keycloak %d: %s", resp.StatusCode, body)
	}
	var out struct {
		AccessToken string `json:"access_token"`
	}
	_ = json.Unmarshal(body, &out)
	return out.AccessToken
}

func sendWagerMessage(t *testing.T, queueURL, messageID, dedupID string, c app.SubmitCommand) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"messageId": messageID, "type": "WagerTransactionRequested", "occurredAt": time.Now().UTC().Format(time.RFC3339Nano),
		"data": map[string]any{
			"providerId": c.ProviderID, "externalTransactionId": c.ExternalTransactionID, "idempotencyKey": c.IdempotencyKey,
			"playerId": c.PlayerID, "walletId": c.WalletID, "roundId": c.RoundID, "gameId": c.GameID, "kind": c.Kind,
			"money":                          map[string]string{"amount": c.Amount, "currency": c.Currency},
			"referenceExternalTransactionId": c.ReferenceExternalTransactionID,
		},
	})
	sendRaw(t, queueURL, string(body), c.WalletID, dedupID)
}

func sendRaw(t *testing.T, queueURL, body, group, dedupID string) {
	t.Helper()
	_, err := sqsClient.SendMessage(context.Background(), &awssqs.SendMessageInput{QueueUrl: aws.String(queueURL), MessageBody: aws.String(body),
		MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(dedupID)})
	if err != nil {
		t.Fatal(err)
	}
}

func queueDepth(t *testing.T, queueURL string) int {
	t.Helper()
	out, err := sqsClient.GetQueueAttributes(context.Background(), &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible}})
	if err != nil {
		t.Fatal(err)
	}
	var n int
	fmt.Sscan(out.Attributes["ApproximateNumberOfMessages"], &n)
	var nv int
	fmt.Sscan(out.Attributes["ApproximateNumberOfMessagesNotVisible"], &nv)
	return n + nv
}

func eventually(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s: %s", timeout, msg)
}

func txStatus(t *testing.T, id uuid.UUID) (wager.Status, wager.FailureCode) {
	t.Helper()
	var s, c *string
	if err := pool.QueryRow(context.Background(), `SELECT status::text, failure_code FROM wager_transactions WHERE id=$1`, id).Scan(&s, &c); err != nil {
		t.Fatal(err)
	}
	var code string
	if c != nil {
		code = *c
	}
	return wager.Status(*s), wager.FailureCode(code)
}
