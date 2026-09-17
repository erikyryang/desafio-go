package sqs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/erikyryan/desafio-go/internal/app"
	"github.com/erikyryan/desafio-go/internal/faults"
)

// ConsumerName identifies this consumer in the inbox table.
const ConsumerName = "wager-transactions-consumer"

// Envelope is the message body accepted on wager-transactions.fifo.
type Envelope struct {
	MessageID     string          `json:"messageId"`
	Type          string          `json:"type"`
	OccurredAt    string          `json:"occurredAt"`
	CorrelationID string          `json:"correlationId,omitempty"`
	Data          json.RawMessage `json:"data"`
}

// EnvelopeData is the operation payload.
type EnvelopeData struct {
	ProviderID            string `json:"providerId"`
	ExternalTransactionID string `json:"externalTransactionId"`
	IdempotencyKey        string `json:"idempotencyKey"`
	PlayerID              string `json:"playerId"`
	WalletID              string `json:"walletId"`
	RoundID               string `json:"roundId"`
	GameID                string `json:"gameId"`
	Kind                  string `json:"kind"`
	Money                 *struct {
		Amount   *string `json:"amount"`
		Currency string  `json:"currency"`
	} `json:"money"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

// ConsumerMetrics is the instrumentation emitted by the consumer.
type ConsumerMetrics interface {
	MessageProcessed(outcome string)
	MessageDuplicate()
	MessageRetry()
	MessageDLQ(reason string)
}

// ConsumerConfig tunes polling and shutdown.
type ConsumerConfig struct {
	WaitTime          time.Duration // long polling wait
	MaxMessages       int32
	VisibilityTimeout time.Duration
	DrainTimeout      time.Duration // grace period for in-flight messages on shutdown
}

// Consumer processes wager operations from SQS with inbox deduplication.
type Consumer struct {
	client   *sqs.Client
	queueURL string
	dlqURL   string
	uow      app.UnitOfWork
	wagers   *app.WagerService
	metrics  ConsumerMetrics
	log      *slog.Logger
	cfg      ConsumerConfig
	faults   *faults.Injector
	clock    app.Clock

	// AfterCommit is a test hook invoked after the commit and before
	// DeleteMessage; returning an error simulates a crash at that point (the
	// message is left in the queue and must be deduplicated on redelivery).
	AfterCommit func(messageID string) error
}

// NewConsumer wires the consumer.
func NewConsumer(client *sqs.Client, queueURL, dlqURL string, uow app.UnitOfWork, wagers *app.WagerService,
	metrics ConsumerMetrics, log *slog.Logger, cfg ConsumerConfig, inj *faults.Injector, clock app.Clock) *Consumer {
	if cfg.WaitTime <= 0 {
		cfg.WaitTime = 10 * time.Second
	}
	if cfg.MaxMessages <= 0 {
		cfg.MaxMessages = 10
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 20 * time.Second
	}
	if clock == nil {
		clock = time.Now
	}
	return &Consumer{client: client, queueURL: queueURL, dlqURL: dlqURL, uow: uow, wagers: wagers, metrics: metrics,
		log: log.With("component", "sqs-consumer", "queue", queueURL), cfg: cfg, faults: inj, clock: clock}
}

// Run polls until ctx is cancelled, then drains in-flight messages within
// DrainTimeout; messages still unfinished get their visibility released.
func (c *Consumer) Run(ctx context.Context) {
	c.log.Info("consumer started")
	defer c.log.Info("consumer stopped")
	for ctx.Err() == nil {
		msgs, err := c.receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Warn("receive failed", "error", err.Error())
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}
		if len(msgs) == 0 {
			continue
		}
		c.handleBatch(ctx, msgs)
	}
}

func (c *Consumer) receive(ctx context.Context) ([]types.Message, error) {
	out, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(c.queueURL),
		MaxNumberOfMessages:         c.cfg.MaxMessages,
		WaitTimeSeconds:             int32(c.cfg.WaitTime / time.Second),
		VisibilityTimeout:           int32(c.cfg.VisibilityTimeout / time.Second),
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameMessageGroupId, types.MessageSystemAttributeNameApproximateReceiveCount},
	})
	if err != nil {
		return nil, err
	}
	return out.Messages, nil
}

// handleBatch processes message groups in parallel and messages of the same
// group in order. In-flight work survives ctx cancellation up to DrainTimeout.
func (c *Consumer) handleBatch(ctx context.Context, msgs []types.Message) {
	groups := map[string][]types.Message{}
	var order []string
	for _, m := range msgs {
		g := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
		if _, seen := groups[g]; !seen {
			order = append(order, g)
		}
		groups[g] = append(groups[g], m)
	}
	workCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	var wg sync.WaitGroup
	var mu sync.Mutex
	pending := map[string]types.Message{}
	for _, m := range msgs {
		pending[aws.ToString(m.ReceiptHandle)] = m
	}
	done := func(m types.Message) {
		mu.Lock()
		delete(pending, aws.ToString(m.ReceiptHandle))
		mu.Unlock()
	}
	for _, g := range order {
		wg.Add(1)
		go func(ms []types.Message) {
			defer wg.Done()
			for _, m := range ms {
				if workCtx.Err() != nil {
					return
				}
				c.handleMessage(workCtx, m)
				done(m)
			}
		}(groups[g])
	}
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
		return
	case <-ctx.Done():
	}
	select {
	case <-finished:
		return
	case <-time.After(c.cfg.DrainTimeout):
		cancel()
		<-finished
		mu.Lock()
		defer mu.Unlock()
		for _, m := range pending {
			c.releaseVisibility(m)
		}
	}
}

func (c *Consumer) releaseVisibility(m types.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(c.queueURL), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: 0,
	})
	if err != nil {
		c.log.Warn("release visibility failed", "error", err.Error(), "sqsMessageId", aws.ToString(m.MessageId))
	}
}

var (
	errDuplicate = errors.New("duplicate message")
	errPermanent = errors.New("permanent failure")
)

// handleMessage decides the fate of one message:
//
//	commit ok / business rejection / pending reference -> DeleteMessage
//	duplicate (inbox hit with same hash)                 -> DeleteMessage
//	malformed envelope, unknown wallet, conflicts        -> copy to DLQ, DeleteMessage
//	transient failure (DB/SQS down, timeouts, unknown)   -> leave in queue (visibility timeout + redrive to DLQ)
func (c *Consumer) handleMessage(ctx context.Context, m types.Message) {
	body := aws.ToString(m.Body)
	log := c.log.With("sqsMessageId", aws.ToString(m.MessageId), "receiveCount", m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])

	env, cmd, err := parseEnvelope(body)
	if err != nil {
		log.Warn("invalid message", "error", err.Error())
		c.deadLetter(ctx, m, "invalid-message")
		return
	}
	payloadHash := inboxHash(cmd)
	cmd.CorrelationID = env.CorrelationID
	if cmd.CorrelationID == "" {
		cmd.CorrelationID = env.MessageID
	}
	cmd.CausationID = env.MessageID
	log = log.With("messageId", env.MessageID, "correlationId", cmd.CorrelationID, "providerId", cmd.ProviderID, "walletId", cmd.WalletID)

	var res *app.SubmitResult
	err = c.uow.Do(ctx, func(ctx context.Context, st app.Store) error {
		inserted, stored, err := st.Inbox().Record(ctx, ConsumerName, env.MessageID, payloadHash, c.clock())
		if err != nil {
			return err
		}
		if !inserted {
			if stored != payloadHash {
				return fmt.Errorf("%w: messageId reused with a different payload", errPermanent)
			}
			return errDuplicate
		}
		r, err := c.wagers.SubmitIn(ctx, st, cmd)
		if err != nil {
			return err
		}
		res = r
		return nil
	})
	switch {
	case err == nil:
		c.metrics.MessageProcessed(string(res.Status))
		log.Info("message processed", "transactionId", res.TransactionID, "status", res.Status, "failureCode", res.FailureCode)
		if c.AfterCommit != nil {
			if err := c.AfterCommit(env.MessageID); err != nil {
				log.Warn("simulated crash after commit; skipping delete", "error", err.Error())
				return
			}
		}
		c.faults.Crash(faults.ConsumerAfterCommit)
		c.delete(ctx, m)
	case errors.Is(err, errDuplicate):
		c.metrics.MessageDuplicate()
		log.Info("duplicate message ignored")
		c.delete(ctx, m)
	case isPermanent(err):
		log.Warn("permanent failure, moving to DLQ", "error", err.Error())
		c.deadLetter(ctx, m, "permanent-failure")
	default:
		c.metrics.MessageRetry()
		log.Warn("transient failure, message will be redelivered", "error", err.Error())
	}
}

func isPermanent(err error) bool {
	return errors.Is(err, errPermanent) || errors.Is(err, app.ErrValidation) || errors.Is(err, app.ErrWalletNotFound) ||
		errors.Is(err, app.ErrIdempotencyConflict) || errors.Is(err, app.ErrExternalIDConflict)
}

// inboxHash is the SHA-256 of the canonical JSON of the business fields of
// data (idempotency key included, transport metadata such as occurredAt
// excluded), so a redelivery of the same operation always matches while a
// reused messageId carrying different content is detected.
func inboxHash(cmd app.SubmitCommand) string {
	b, _ := json.Marshal(map[string]string{
		"idempotencyKey": cmd.IdempotencyKey, "providerId": cmd.ProviderID, "externalTransactionId": cmd.ExternalTransactionID,
		"playerId": cmd.PlayerID, "walletId": cmd.WalletID, "roundId": cmd.RoundID, "gameId": cmd.GameID, "kind": cmd.Kind,
		"amount": cmd.Amount, "currency": cmd.Currency, "referenceExternalTransactionId": cmd.ReferenceExternalTransactionID,
	})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func parseEnvelope(body string) (Envelope, app.SubmitCommand, error) {
	var env Envelope
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		return env, app.SubmitCommand{}, fmt.Errorf("malformed envelope: %w", err)
	}
	if env.MessageID == "" {
		return env, app.SubmitCommand{}, errors.New("messageId is required")
	}
	if env.Type != "WagerTransactionRequested" {
		return env, app.SubmitCommand{}, fmt.Errorf("unsupported type %q", env.Type)
	}
	var data EnvelopeData
	if err := json.Unmarshal(env.Data, &data); err != nil {
		return env, app.SubmitCommand{}, fmt.Errorf("malformed data: %w", err)
	}
	if data.Money == nil || data.Money.Amount == nil {
		return env, app.SubmitCommand{}, errors.New("data.money.amount must be a decimal string")
	}
	if data.IdempotencyKey == "" {
		return env, app.SubmitCommand{}, errors.New("data.idempotencyKey is required")
	}
	return env, app.SubmitCommand{
		IdempotencyKey: data.IdempotencyKey, ProviderID: data.ProviderID, ExternalTransactionID: data.ExternalTransactionID,
		PlayerID: data.PlayerID, WalletID: data.WalletID, RoundID: data.RoundID, GameID: data.GameID, Kind: data.Kind,
		Amount: *data.Money.Amount, Currency: data.Money.Currency, ReferenceExternalTransactionID: data.ReferenceExternalTransactionID,
		Source: app.SourceSQS,
	}, nil
}

func (c *Consumer) delete(ctx context.Context, m types.Message) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if _, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(c.queueURL), ReceiptHandle: m.ReceiptHandle}); err != nil {
		// The inbox record makes the redelivery a harmless duplicate.
		c.log.Warn("delete message failed; redelivery will be deduplicated", "error", err.Error(), "sqsMessageId", aws.ToString(m.MessageId))
	}
}

// deadLetter copies the message to the DLQ and removes it from the source
// queue. If the copy fails the message stays and the redrive policy will
// eventually move it.
func (c *Consumer) deadLetter(ctx context.Context, m types.Message, reason string) {
	c.metrics.MessageDLQ(reason)
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	group := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
	if group == "" {
		group = "invalid"
	}
	_, err := c.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl: aws.String(c.dlqURL), MessageBody: m.Body,
		MessageGroupId: aws.String(group), MessageDeduplicationId: m.MessageId,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"reason": {DataType: aws.String("String"), StringValue: aws.String(reason)},
		},
	})
	if err != nil {
		c.log.Warn("send to DLQ failed; leaving message for redrive", "error", err.Error())
		return
	}
	c.delete(ctx, m)
}
