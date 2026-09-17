//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/erikyryan/desafio-go/internal/adapters/auth"
	"github.com/erikyryan/desafio-go/internal/adapters/httpapi"
	"github.com/erikyryan/desafio-go/internal/app"
	"github.com/erikyryan/desafio-go/internal/domain/wager"
	"github.com/erikyryan/desafio-go/internal/observability"
)

// apiServer serves the real router with the real Keycloak verifier.
func apiServer(t *testing.T, s *services) *httptest.Server {
	t.Helper()
	verifier, err := auth.NewVerifier(context.Background(), auth.Config{Issuer: issuer, JWKSURL: keycloakURL + "/realms/wager/protocol/openid-connect/certs", Audience: "wager-api"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := httpapi.NewHandlers(s.wallets, s.wagers, logger)
	srv := httptest.NewServer(httpapi.Router(h, verifier, nil, observability.NewMetrics().Handler(), logger))
	t.Cleanup(srv.Close)
	return srv
}

type response struct {
	Status int
	Body   map[string]any
	Raw    []byte
}

func call(t *testing.T, srv *httptest.Server, method, path, tok string, body any, headers map[string]string) response {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if s, ok := body.(string); ok {
			buf.WriteString(s)
		} else {
			json.NewEncoder(&buf).Encode(body)
		}
	}
	req, _ := http.NewRequest(method, srv.URL+path, &buf)
	req.Header.Set("Content-Type", "application/json")
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return response{Status: resp.StatusCode, Body: m, Raw: raw}
}

func betBody(w *wager.Wallet, provider, ext, amount string) map[string]any {
	return map[string]any{"providerId": provider, "externalTransactionId": ext, "playerId": w.PlayerID().String(), "walletId": w.ID().String(),
		"roundId": "round-1", "gameId": "game", "kind": "BET", "money": map[string]string{"amount": amount, "currency": "BRL"}}
}

func TestAuthenticationAndAuthorization(t *testing.T) {
	s := newServices(t, app.PendingReferencePolicy{})
	srv := apiServer(t, s)
	internal := token(t, "internal-service", "internal-service-secret")
	providerA := token(t, "provider-a", "provider-a-secret")
	providerB := token(t, "provider-b", "provider-b-secret")
	noRole := token(t, "no-role-client", "no-role-client-secret")

	open := map[string]any{"playerId": uuid.NewString(), "initialBalance": map[string]string{"amount": "100.00", "currency": "BRL"}}
	t.Run("missing, malformed and expired credentials", func(t *testing.T) {
		if r := call(t, srv, "POST", "/wallets", "", open, nil); r.Status != 401 {
			t.Fatalf("no token: %d %s", r.Status, r.Raw)
		}
		if r := call(t, srv, "POST", "/wallets", "not.a.jwt", open, nil); r.Status != 401 {
			t.Fatalf("garbage token: %d", r.Status)
		}
		tampered := internal[:len(internal)-6] + "AAAAAA"
		if r := call(t, srv, "POST", "/wallets", tampered, open, nil); r.Status != 401 {
			t.Fatalf("tampered signature: %d", r.Status)
		}
		short := token(t, "provider-a-short-lived", "provider-a-short-lived-secret")
		time.Sleep(2500 * time.Millisecond)
		if r := call(t, srv, "GET", "/wagering/transactions/"+uuid.NewString(), short, nil, nil); r.Status != 401 {
			t.Fatalf("expired token: %d %s", r.Status, r.Raw)
		}
		var wallets int
		pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM wallets WHERE player_id=$1`, open["playerId"]).Scan(&wallets)
		if wallets != 0 {
			t.Fatal("unauthenticated request had financial effect")
		}
	})

	r := call(t, srv, "POST", "/wallets", internal, open, nil)
	if r.Status != 201 {
		t.Fatalf("open wallet: %d %s", r.Status, r.Raw)
	}
	walletID := r.Body["id"].(string)
	w, _ := s.wallets.GetWallet(context.Background(), uuid.MustParse(walletID))

	t.Run("wallet operations are internal only", func(t *testing.T) {
		for _, tok := range []string{providerA, noRole} {
			for _, c := range []struct{ m, p string }{{"POST", "/wallets"}, {"GET", "/wallets/" + walletID}, {"GET", "/wallets/" + walletID + "/ledger"}, {"POST", "/wallets/" + walletID + "/reconciliation"}} {
				if r := call(t, srv, c.m, c.p, tok, open, nil); r.Status != 403 {
					t.Fatalf("%s %s: %d", c.m, c.p, r.Status)
				}
			}
		}
	})
	t.Run("submission requires the provider role and a matching providerId", func(t *testing.T) {
		hdr := map[string]string{"Idempotency-Key": "provider-b:spoof-" + runID}
		if r := call(t, srv, "POST", "/wagering/transactions", providerB, betBody(w, "provider-a", "spoof-"+runID, "10.00"), hdr); r.Status != 403 {
			t.Fatalf("spoofed providerId: %d %s", r.Status, r.Raw)
		}
		if r := call(t, srv, "POST", "/wagering/transactions", internal, betBody(w, "provider-a", "spoof-"+runID, "10.00"), hdr); r.Status != 403 {
			t.Fatalf("internal submitting: %d", r.Status)
		}
		if r := call(t, srv, "POST", "/wagering/transactions", noRole, betBody(w, "provider-a", "spoof-"+runID, "10.00"), hdr); r.Status != 403 {
			t.Fatalf("no role: %d", r.Status)
		}
		if got := s.balance(t, w.ID()); got.Balance().Amount() != "100.00" {
			t.Fatal("forbidden submission moved money")
		}
	})

	r = call(t, srv, "POST", "/wagering/transactions", providerA, betBody(w, "provider-a", "auth-bet-"+runID, "10.00"), map[string]string{"Idempotency-Key": "provider-a:auth-bet-" + runID})
	if r.Status != 200 || r.Body["status"] != "PROCESSED" {
		t.Fatalf("bet: %d %s", r.Status, r.Raw)
	}
	txID := r.Body["transactionId"].(string)

	t.Run("providers are isolated on reads and replays", func(t *testing.T) {
		if r := call(t, srv, "GET", "/wagering/transactions/"+txID, providerB, nil, nil); r.Status != 404 {
			t.Fatalf("provider-b read by id: %d", r.Status)
		}
		if r := call(t, srv, "GET", "/providers/provider-a/wagering/transactions/auth-bet-"+runID, providerB, nil, nil); r.Status != 403 {
			t.Fatalf("provider-b read by external id: %d", r.Status)
		}
		if r := call(t, srv, "GET", "/providers/provider-a/wagering/transactions/auth-bet-"+runID, providerA, nil, nil); r.Status != 200 {
			t.Fatalf("owner read: %d", r.Status)
		}
		if r := call(t, srv, "GET", "/wagering/transactions/"+txID, internal, nil, nil); r.Status != 200 {
			t.Fatalf("internal read: %d", r.Status)
		}
		// provider-b replaying provider-a's operation with its own providerId is a different operation
		// (namespace is per provider) and cannot touch provider-a's record.
		body := betBody(w, "provider-b", "auth-bet-"+runID, "10.00")
		body["playerId"] = uuid.NewString()
		if r := call(t, srv, "POST", "/wagering/transactions", providerB, body, map[string]string{"Idempotency-Key": "provider-a:auth-bet-" + runID}); r.Status != 409 {
			t.Fatalf("cross-provider key reuse: %d %s", r.Status, r.Raw)
		}
		if r := call(t, srv, "GET", "/wagering/transactions/"+txID, providerA, nil, nil); r.Body["status"] != "PROCESSED" || r.Body["providerId"] != "provider-a" {
			t.Fatalf("record altered: %s", r.Raw)
		}
	})

	t.Run("contract statuses", func(t *testing.T) {
		hdr := func(k string) map[string]string { return map[string]string{"Idempotency-Key": k} }
		if r := call(t, srv, "POST", "/wagering/transactions", providerA, betBody(w, "provider-a", "x", "10.00"), nil); r.Status != 400 || r.Body["code"] != "INVALID_INPUT" {
			t.Fatalf("missing key: %d %s", r.Status, r.Raw)
		}
		if r := call(t, srv, "POST", "/wagering/transactions", providerA, `{"providerId":"provider-a","money":{"amount":25.00,"currency":"BRL"}}`, hdr("k1-"+runID)); r.Status != 400 {
			t.Fatalf("numeric amount: %d %s", r.Status, r.Raw)
		}
		if r := call(t, srv, "POST", "/wagering/transactions", providerA, betBody(w, "provider-a", "sci-"+runID, "1e2"), hdr("sci-"+runID)); r.Status != 400 {
			t.Fatalf("scientific: %d", r.Status)
		}
		if r := call(t, srv, "POST", "/wagering/transactions", providerA, betBody(w, "provider-a", "auth-bet-"+runID, "11.00"), hdr("provider-a:auth-bet-"+runID)); r.Status != 409 || r.Body["code"] != "IDEMPOTENCY_CONFLICT" {
			t.Fatalf("conflict: %d %s", r.Status, r.Raw)
		}
		if r := call(t, srv, "POST", "/wagering/transactions", providerA, betBody(w, "provider-a", "auth-bet-"+runID, "10.00"), hdr("other-"+runID)); r.Status != 409 || r.Body["code"] != "EXTERNAL_ID_CONFLICT" {
			t.Fatalf("external id conflict: %d %s", r.Status, r.Raw)
		}
		if r := call(t, srv, "POST", "/wagering/transactions", providerA, betBody(w, "provider-a", "big-"+runID, "1000.00"), hdr("big-"+runID)); r.Status != 422 || r.Body["failureCode"] != "INSUFFICIENT_BALANCE" {
			t.Fatalf("rejection: %d %s", r.Status, r.Raw)
		}
		rb := betBody(w, "provider-a", "rb-"+runID, "10.00")
		rb["kind"], rb["referenceExternalTransactionId"] = "ROLLBACK", "future-"+runID
		if r := call(t, srv, "POST", "/wagering/transactions", providerA, rb, hdr("rb-"+runID)); r.Status != 202 || r.Body["status"] != "PENDING_REFERENCE" {
			t.Fatalf("pending: %d %s", r.Status, r.Raw)
		}
		if r := call(t, srv, "GET", "/providers/provider-a/wagering/transactions/rb-"+runID, providerA, nil, nil); r.Status != 200 || r.Body["status"] != "PENDING_REFERENCE" || r.Body["nextAttemptAt"] == nil {
			t.Fatalf("follow pending: %d %s", r.Status, r.Raw)
		}
		if r := call(t, srv, "POST", "/wagering/transactions", providerA, betBody(w, "provider-a", "auth-bet-"+runID, "10.00"), hdr("provider-a:auth-bet-"+runID)); r.Status != 200 || r.Body["idempotentReplay"] != true {
			t.Fatalf("replay: %d %s", r.Status, r.Raw)
		}
		open["playerId"] = w.PlayerID().String()
		if r := call(t, srv, "POST", "/wallets", internal, open, nil); r.Status != 409 {
			t.Fatalf("duplicate wallet: %d", r.Status)
		}
		if r := call(t, srv, "GET", "/wallets/"+uuid.NewString(), internal, nil, nil); r.Status != 404 {
			t.Fatalf("unknown wallet: %d", r.Status)
		}
		if r := call(t, srv, "POST", "/wallets/"+walletID+"/reconciliation", internal, nil, nil); r.Status != 200 || r.Body["consistent"] != true || r.Body["checkedEntries"].(float64) != 2 {
			t.Fatalf("reconciliation: %d %s", r.Status, r.Raw)
		}
	})

	t.Run("ledger pagination with opaque cursor", func(t *testing.T) {
		r := call(t, srv, "GET", "/wallets/"+walletID+"/ledger?limit=1", internal, nil, nil)
		if r.Status != 200 || len(r.Body["entries"].([]any)) != 1 || r.Body["nextCursor"] == nil {
			t.Fatalf("page 1: %d %s", r.Status, r.Raw)
		}
		r2 := call(t, srv, "GET", "/wallets/"+walletID+"/ledger?limit=1&cursor="+r.Body["nextCursor"].(string), internal, nil, nil)
		if r2.Status != 200 || len(r2.Body["entries"].([]any)) != 1 || r2.Body["nextCursor"] != nil {
			t.Fatalf("page 2: %d %s", r2.Status, r2.Raw)
		}
		if r.Body["entries"].([]any)[0].(map[string]any)["direction"] != "CREDIT" || r2.Body["entries"].([]any)[0].(map[string]any)["direction"] != "DEBIT" {
			t.Fatal("ordering")
		}
		if r := call(t, srv, "GET", "/wallets/"+walletID+"/ledger?cursor=garbage", internal, nil, nil); r.Status != 400 {
			t.Fatalf("bad cursor: %d", r.Status)
		}
	})

	t.Run("public endpoints", func(t *testing.T) {
		if r := call(t, srv, "GET", "/health/live", "", nil, nil); r.Status != 200 {
			t.Fatal("live")
		}
		if r := call(t, srv, "GET", "/metrics", "", nil, nil); r.Status != 200 || !bytes.Contains(r.Raw, []byte("outbox_lag_seconds")) {
			t.Fatalf("metrics: %d", r.Status)
		}
	})
}

func TestSameOperationViaHTTPAndSQSConcurrently(t *testing.T) {
	s := newServices(t, app.PendingReferencePolicy{})
	srv := apiServer(t, s)
	providerA := token(t, "provider-a", "provider-a-secret")
	m := &countingMetrics{}
	startConsumer(t, s, s.uow, m, nil)
	w := s.openWallet(t, "100.00")
	c := cmd(w, "BET", "cross-"+runID, "60.00")
	var wg sync.WaitGroup
	statuses := make([]int, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				r := call(t, srv, "POST", "/wagering/transactions", providerA, betBody(w, "provider-a", "cross-"+runID, "60.00"), map[string]string{"Idempotency-Key": c.IdempotencyKey})
				statuses[i] = r.Status
			} else {
				sendWagerMessage(t, queues.WagerTransactions, fmt.Sprintf("cross-%s-%d", runID, i), uuid.NewString(), c)
				statuses[i] = 200
			}
		}(i)
	}
	wg.Wait()
	eventually(t, 15*time.Second, func() bool { return m.processed.Load()+m.duplicates.Load() == 2 }, "sqs deliveries handled")
	for i, st := range statuses {
		if st != 200 {
			t.Fatalf("http %d: %d", i, st)
		}
	}
	if got := s.balance(t, w.ID()); got.Balance().Amount() != "40.00" || got.Version() != 2 {
		t.Fatalf("balance %s v%d", got.Balance(), got.Version())
	}
	if _, debits, n := ledgerStats(t, w.ID()); debits != 6000 || n != 2 {
		t.Fatalf("ledger debits=%d entries=%d", debits, n)
	}
	assertReconciled(t, s, w.ID())
}
