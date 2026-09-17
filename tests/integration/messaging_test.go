//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"

	sqsadapter "github.com/erikyryan/desafio-go/internal/adapters/sqs"
	"github.com/erikyryan/desafio-go/internal/app"
	"github.com/erikyryan/desafio-go/internal/domain/wager"
	"github.com/erikyryan/desafio-go/internal/faults"
	"github.com/erikyryan/desafio-go/internal/workers"
)

type countingMetrics struct {
	processed, duplicates, retries, dlq atomic.Int64
}

func (c *countingMetrics) MessageProcessed(string) { c.processed.Add(1) }
func (c *countingMetrics) MessageDuplicate()       { c.duplicates.Add(1) }
func (c *countingMetrics) MessageRetry()           { c.retries.Add(1) }
func (c *countingMetrics) MessageDLQ(string)       { c.dlq.Add(1) }
func (c *countingMetrics) OutboxPublished()        { c.processed.Add(1) }
func (c *countingMetrics) OutboxRetry()            { c.retries.Add(1) }
func (c *countingMetrics) OutboxLag(time.Duration) {}

// startConsumer runs a consumer over the given unit of work until the test ends.
func startConsumer(t *testing.T, s *services, uow app.UnitOfWork, m *countingMetrics, afterCommit func(string) error) (*sqsadapter.Consumer, context.CancelFunc) {
	t.Helper()
	c := sqsadapter.NewConsumer(sqsClient, queues.WagerTransactions, queues.WagerDLQ, uow, s.wagers, m, logger,
		sqsadapter.ConsumerConfig{WaitTime: time.Second, MaxMessages: 10, VisibilityTimeout: 5 * time.Second, DrainTimeout: 5 * time.Second}, faults.New("", logger), nil)
	c.AfterCommit = afterCommit
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return c, cancel
}

func inboxCount(t *testing.T, messageID string) int {
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM inbox_messages WHERE consumer_name=$1 AND message_id=$2`, sqsadapter.ConsumerName, messageID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestConsumerProcessesAndDeduplicatesRedelivery(t *testing.T) {
	s := newServices(t, app.PendingReferencePolicy{})
	m := &countingMetrics{}
	startConsumer(t, s, s.uow, m, nil)
	w := s.openWallet(t, "100.00")
	c := cmd(w, "BET", "sqs-"+runID, "30.00")
	c.Source = app.SourceSQS
	msgID := "msg-" + runID
	// The same envelope delivered twice (distinct broker dedup ids simulate a
	// producer retry outside the 5-minute FIFO window).
	sendWagerMessage(t, queues.WagerTransactions, msgID, uuid.NewString(), c)
	sendWagerMessage(t, queues.WagerTransactions, msgID, uuid.NewString(), c)
	eventually(t, 15*time.Second, func() bool { return m.duplicates.Load() == 1 && m.processed.Load() == 1 }, "one processed + one duplicate")
	eventually(t, 10*time.Second, func() bool { return queueDepth(t, queues.WagerTransactions) == 0 }, "queue drained")
	if got := s.balance(t, w.ID()); got.Balance().Amount() != "70.00" || got.Version() != 2 {
		t.Fatalf("balance %s v%d", got.Balance(), got.Version())
	}
	if inboxCount(t, msgID) != 1 {
		t.Fatal("inbox row missing")
	}
	// Same operation later via HTTP is a replay too.
	c.Source = app.SourceHTTP
	if res := s.submit(t, c); !res.IdempotentReplay || res.Balance.Amount() != "70.00" {
		t.Fatalf("http replay of sqs operation: %+v", res)
	}
	assertReconciled(t, s, w.ID())
}

func TestConsumerCrashAfterCommitBeforeDelete(t *testing.T) {
	s := newServices(t, app.PendingReferencePolicy{})
	m := &countingMetrics{}
	var crashed atomic.Bool
	startConsumer(t, s, s.uow, m, func(string) error {
		if crashed.CompareAndSwap(false, true) {
			return errors.New("simulated crash after commit")
		}
		return nil
	})
	w := s.openWallet(t, "100.00")
	c := cmd(w, "BET", "crash-"+runID, "25.00")
	sendWagerMessage(t, queues.WagerTransactions, "crash-msg-"+runID, uuid.NewString(), c)
	// First delivery commits and "crashes"; visibility timeout (5s) makes the
	// broker redeliver; the inbox turns it into a duplicate and it is deleted.
	eventually(t, 20*time.Second, func() bool { return m.duplicates.Load() == 1 }, "redelivery deduplicated")
	eventually(t, 10*time.Second, func() bool { return queueDepth(t, queues.WagerTransactions) == 0 }, "queue drained")
	if got := s.balance(t, w.ID()); got.Balance().Amount() != "75.00" {
		t.Fatalf("balance %s", got.Balance())
	}
	if _, debits, _ := ledgerStats(t, w.ID()); debits != 2500 {
		t.Fatalf("debits %d", debits)
	}
}

// flakyUoW fails the first n transactions with a transient error.
type flakyUoW struct {
	app.UnitOfWork
	remaining atomic.Int64
}

func (f *flakyUoW) Do(ctx context.Context, fn func(context.Context, app.Store) error) error {
	if f.remaining.Add(-1) >= 0 {
		return fmt.Errorf("%w: simulated outage", app.ErrUnavailable)
	}
	return f.UnitOfWork.Do(ctx, fn)
}

func TestConsumerRetriesTransientFailuresAndDeadLettersPermanentOnes(t *testing.T) {
	s := newServices(t, app.PendingReferencePolicy{})
	m := &countingMetrics{}
	flaky := &flakyUoW{UnitOfWork: s.uow}
	flaky.remaining.Store(1)
	startConsumer(t, s, flaky, m, nil)
	w := s.openWallet(t, "100.00")

	sendWagerMessage(t, queues.WagerTransactions, "transient-"+runID, uuid.NewString(), cmd(w, "BET", "transient-"+runID, "5.00"))
	eventually(t, 20*time.Second, func() bool { return m.retries.Load() >= 1 && m.processed.Load() == 1 }, "retry then success")
	if got := s.balance(t, w.ID()); got.Balance().Amount() != "95.00" {
		t.Fatalf("balance %s", got.Balance())
	}

	before := queueDepth(t, queues.WagerDLQ)
	sendRaw(t, queues.WagerTransactions, `{"not":"an envelope"}`, "bad", uuid.NewString())
	unknown := cmd(w, "BET", "no-wallet-"+runID, "5.00")
	unknown.WalletID = uuid.NewString()
	sendWagerMessage(t, queues.WagerTransactions, "no-wallet-"+runID, uuid.NewString(), unknown)
	eventually(t, 20*time.Second, func() bool { return m.dlq.Load() == 2 }, "two permanent failures dead-lettered")
	eventually(t, 10*time.Second, func() bool {
		return queueDepth(t, queues.WagerDLQ) == before+2 && queueDepth(t, queues.WagerTransactions) == 0
	}, "DLQ holds them")
}

func TestConsumerStopsReceivingOnShutdown(t *testing.T) {
	s := newServices(t, app.PendingReferencePolicy{})
	m := &countingMetrics{}
	_, cancel := startConsumer(t, s, s.uow, m, nil)
	cancel()
	w := s.openWallet(t, "100.00")
	sendWagerMessage(t, queues.WagerTransactions, "after-stop-"+runID, uuid.NewString(), cmd(w, "BET", "after-stop-"+runID, "5.00"))
	time.Sleep(2 * time.Second)
	if m.processed.Load() != 0 {
		t.Fatal("stopped consumer must not take new work")
	}
	// Another instance picks it up.
	startConsumer(t, s, s.uow, m, nil)
	eventually(t, 15*time.Second, func() bool { return m.processed.Load() == 1 }, "other instance processed")
}

// --- outbox ------------------------------------------------------------------

type recordingPublisher struct {
	mu      sync.Mutex
	seen    map[uuid.UUID]int
	failFor map[uuid.UUID]int
}

func newRecorder() *recordingPublisher {
	return &recordingPublisher{seen: map[uuid.UUID]int{}, failFor: map[uuid.UUID]int{}}
}

func (r *recordingPublisher) Publish(_ context.Context, rec app.OutboxRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failFor[rec.EventID] > 0 {
		r.failFor[rec.EventID]--
		return errors.New("broker down")
	}
	r.seen[rec.EventID]++
	return nil
}

func (r *recordingPublisher) total() (events, publications int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range r.seen {
		events++
		publications += n
	}
	return
}

func pendingOutbox(t *testing.T) int {
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func drainOutbox(t *testing.T) {
	t.Helper()
	p := workers.NewOutboxPublisher(servicesFor(pool, app.PendingReferencePolicy{}).uow, newRecorder(), &countingMetrics{}, logger,
		workers.OutboxConfig{Owner: "drain", BatchSize: 500}, faults.New("", logger), nil)
	for pendingOutbox(t) > 0 {
		if _, err := p.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOutboxTwoPublishersCompete(t *testing.T) {
	drainOutbox(t)
	s := newServices(t, app.PendingReferencePolicy{})
	const n = 60
	for i := 0; i < n; i++ {
		s.openWallet(t, "1.00") // 2 events each
	}
	rec := newRecorder()
	mk := func(owner string) *workers.OutboxPublisher {
		return workers.NewOutboxPublisher(newServices(t, app.PendingReferencePolicy{}).uow, rec, &countingMetrics{}, logger,
			workers.OutboxConfig{Owner: owner, BatchSize: 7, Lease: 30 * time.Second}, faults.New("", logger), nil)
	}
	a, b := mk("pub-a"), mk("pub-b")
	var wg sync.WaitGroup
	for _, p := range []*workers.OutboxPublisher{a, b} {
		wg.Add(1)
		go func(p *workers.OutboxPublisher) {
			defer wg.Done()
			for pendingOutbox(t) > 0 {
				if _, err := p.RunOnce(context.Background()); err != nil {
					t.Error(err)
					return
				}
			}
		}(p)
	}
	wg.Wait()
	events, pubs := rec.total()
	if events != 2*n || pubs != 2*n {
		t.Fatalf("events=%d publications=%d (want %d each)", events, pubs, 2*n)
	}
}

func TestOutboxRetryAndRecovery(t *testing.T) {
	drainOutbox(t)
	s := newServices(t, app.PendingReferencePolicy{})
	w := s.openWallet(t, "1.00")
	var eventID uuid.UUID
	if err := pool.QueryRow(context.Background(), `SELECT id FROM outbox_events WHERE aggregate_id=$1 AND event_type='WalletBalanceChanged'`, w.ID()).Scan(&eventID); err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	rec.failFor[eventID] = 1
	clock := &fakeClock{}
	pub := workers.NewOutboxPublisher(s.uow, rec, &countingMetrics{}, logger,
		workers.OutboxConfig{Owner: "retry", BatchSize: 10, Lease: 2 * time.Second, InitialBackoff: time.Second, MaxBackoff: time.Second}, faults.New("", logger), clock.Now)

	// 1) Publish failure: rescheduled with backoff, not published.
	if _, err := pub.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	var attempts int
	var next time.Time
	pool.QueryRow(context.Background(), `SELECT attempts, next_attempt_at FROM outbox_events WHERE id=$1`, eventID).Scan(&attempts, &next)
	if attempts != 1 || !next.After(time.Now()) {
		t.Fatalf("after failure: attempts=%d next=%s", attempts, next)
	}
	if _, err := pub.RunOnce(context.Background()); err != nil { // not due yet
		t.Fatal(err)
	}
	if rec.seen[eventID] != 0 {
		t.Fatal("published before backoff elapsed")
	}
	clock.offset = 2 * time.Second
	if _, err := pub.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec.seen[eventID] != 1 || pendingOutbox(t) != 0 {
		t.Fatalf("after backoff: seen=%d pending=%d", rec.seen[eventID], pendingOutbox(t))
	}

	// 2) Crash between claim and publish (abandoned lease): another publisher
	//    takes over once the lease expires.
	w2 := s.openWallet(t, "1.00")
	var ids []uuid.UUID
	rows, _ := pool.Query(context.Background(), `SELECT id FROM outbox_events WHERE aggregate_id=$1 OR aggregate_id IN (SELECT id FROM wager_transactions WHERE wallet_id=$1)`, w2.ID())
	for rows.Next() {
		var id uuid.UUID
		rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()
	var claimed int
	if err := s.uow.Do(context.Background(), func(ctx context.Context, st app.Store) error {
		recs, err := st.Outbox().Claim(ctx, "dead-instance", 2*time.Second, 10, time.Now())
		claimed = len(recs)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if claimed != len(ids) {
		t.Fatalf("claimed %d of %d", claimed, len(ids))
	}
	other := workers.NewOutboxPublisher(s.uow, rec, &countingMetrics{}, logger, workers.OutboxConfig{Owner: "survivor", BatchSize: 10, Lease: 30 * time.Second}, faults.New("", logger), nil)
	if _, err := other.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec.seen[ids[0]] != 0 {
		t.Fatal("leased row published by another publisher before lease expiry")
	}
	time.Sleep(2100 * time.Millisecond)
	if _, err := other.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if rec.seen[id] != 1 {
			t.Fatalf("event %s published %d times after lease expiry", id, rec.seen[id])
		}
	}

	// 3) Crash between publish and mark-published: republication keeps eventId.
	s.openWallet(t, "1.00")
	var id3 uuid.UUID // first due row: published, then the publisher "crashes" before marking it
	pool.QueryRow(context.Background(), `SELECT id FROM outbox_events WHERE published_at IS NULL ORDER BY sequence LIMIT 1`).Scan(&id3)
	crashing := faults.NewWithExit(faults.OutboxAfterPublish, logger, func(int) { panic("simulated crash") })
	crashPub := workers.NewOutboxPublisher(s.uow, rec, &countingMetrics{}, logger, workers.OutboxConfig{Owner: "crasher", BatchSize: 10, Lease: time.Second}, crashing, nil)
	func() {
		defer func() { recover() }()
		crashPub.RunOnce(context.Background())
	}()
	if rec.seen[id3] != 1 || pendingOutbox(t) == 0 {
		t.Fatalf("crash simulation: seen=%d pending=%d", rec.seen[id3], pendingOutbox(t))
	}
	time.Sleep(1100 * time.Millisecond)
	if _, err := other.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec.seen[id3] != 2 || pendingOutbox(t) != 0 {
		t.Fatalf("republication: seen=%d pending=%d", rec.seen[id3], pendingOutbox(t))
	}
}

func TestOutboxPublishesToRealQueueAfterCommit(t *testing.T) {
	drainOutbox(t)
	if _, err := sqsClient.PurgeQueue(context.Background(), &awssqs.PurgeQueueInput{QueueUrl: aws.String(queues.WalletEvents)}); err != nil {
		t.Fatal(err)
	}
	s := newServices(t, app.PendingReferencePolicy{})
	w := s.openWallet(t, "10.00")
	s.submit(t, cmd(w, "BET", "real-events-"+runID, "4.00"))
	// Nothing reaches the broker until a publisher runs (publication strictly after commit).
	pub := workers.NewOutboxPublisher(s.uow, sqsadapter.NewEventPublisher(sqsClient, queues.WalletEvents), &countingMetrics{}, logger,
		workers.OutboxConfig{Owner: "real", BatchSize: 10}, faults.New("", logger), nil)
	if _, err := pub.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	deadline := time.Now().Add(10 * time.Second)
	for len(seen) < 4 && time.Now().Before(deadline) {
		out, err := sqsClient.ReceiveMessage(context.Background(), &awssqs.ReceiveMessageInput{QueueUrl: aws.String(queues.WalletEvents), MaxNumberOfMessages: 10, WaitTimeSeconds: 2, MessageAttributeNames: []string{"All"}})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range out.Messages {
			seen[aws.ToString(m.MessageAttributes["eventType"].StringValue)]++
			sqsClient.DeleteMessage(context.Background(), &awssqs.DeleteMessageInput{QueueUrl: aws.String(queues.WalletEvents), ReceiptHandle: m.ReceiptHandle})
		}
	}
	if seen[wager.EventWagerTransactionProcessed] != 2 || seen[wager.EventWalletBalanceChanged] != 2 {
		t.Fatalf("events on broker: %v", seen)
	}
}
