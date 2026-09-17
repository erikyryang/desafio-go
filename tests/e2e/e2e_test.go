//go:build e2e

// Package e2e exercises three independent processes started by docker
// compose (app1/app2/app3), each with its own connections and memory.
// Configure with E2E_BASE_URLS (comma separated), KEYCLOAK_URL and
// AWS_ENDPOINT_URL; defaults match docker-compose.yml.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"

	sqsadapter "github.com/erikyryan/desafio-go/internal/adapters/sqs"
)

var (
	baseURLs    = strings.Split(envOr("E2E_BASE_URLS", "http://localhost:8081,http://localhost:8082,http://localhost:8083"), ",")
	keycloakURL = envOr("KEYCLOAK_URL", "http://localhost:8080")
	sqsEndpoint = envOr("AWS_ENDPOINT_URL", "http://localhost:4566")
	client      = &http.Client{Timeout: 15 * time.Second}
	runID       = uuid.NewString()[:8]
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func instance(i int) string { return baseURLs[i%len(baseURLs)] }

func token(t *testing.T, id, secret string) string {
	t.Helper()
	resp, err := http.PostForm(keycloakURL+"/realms/wager/protocol/openid-connect/token", url.Values{"grant_type": {"client_credentials"}, "client_id": {id}, "client_secret": {secret}})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || out.AccessToken == "" {
		t.Fatalf("token for %s: %v", id, err)
	}
	return out.AccessToken
}

type resp struct {
	Status int
	Body   map[string]any
	Raw    []byte
}

func call(t *testing.T, base, method, path, tok string, body any, key string) resp {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		json.NewEncoder(&buf).Encode(body)
	}
	req, _ := http.NewRequest(method, base+path, &buf)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+tok)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	r, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	raw, _ := io.ReadAll(r.Body)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return resp{Status: r.StatusCode, Body: m, Raw: raw}
}

type wallet struct{ ID, PlayerID string }

func openWallet(t *testing.T, internal, amount string) wallet {
	t.Helper()
	r := call(t, instance(0), "POST", "/wallets", internal, map[string]any{"playerId": uuid.NewString(), "initialBalance": map[string]string{"amount": amount, "currency": "BRL"}}, "")
	if r.Status != 201 {
		t.Fatalf("open wallet: %d %s", r.Status, r.Raw)
	}
	return wallet{ID: r.Body["id"].(string), PlayerID: r.Body["playerId"].(string)}
}

func bet(w wallet, ext, amount string) map[string]any {
	return map[string]any{"providerId": "provider-a", "externalTransactionId": ext, "playerId": w.PlayerID, "walletId": w.ID,
		"roundId": "round-1", "gameId": "game", "kind": "BET", "money": map[string]string{"amount": amount, "currency": "BRL"}}
}

func balance(t *testing.T, internal string, w wallet) (amount string, version float64) {
	t.Helper()
	r := call(t, instance(1), "GET", "/wallets/"+w.ID, internal, nil, "")
	if r.Status != 200 {
		t.Fatalf("get wallet: %d %s", r.Status, r.Raw)
	}
	return r.Body["balance"].(map[string]any)["amount"].(string), r.Body["version"].(float64)
}

func reconcile(t *testing.T, internal string, w wallet) map[string]any {
	t.Helper()
	r := call(t, instance(2), "POST", "/wallets/"+w.ID+"/reconciliation", internal, nil, "")
	if r.Status != 200 || r.Body["consistent"] != true {
		t.Fatalf("reconciliation: %d %s", r.Status, r.Raw)
	}
	return r.Body
}

func ledgerDebits(t *testing.T, internal string, w wallet) (debits int, entries int) {
	t.Helper()
	r := call(t, instance(0), "GET", "/wallets/"+w.ID+"/ledger?limit=200", internal, nil, "")
	for _, e := range r.Body["entries"].([]any) {
		entries++
		if e.(map[string]any)["direction"] == "DEBIT" {
			debits++
		}
	}
	return
}

func TestSameBetFiftyTimesAcrossInstances(t *testing.T) {
	internal, provider := token(t, "internal-service", "internal-service-secret"), token(t, "provider-a", "provider-a-secret")
	w := openWallet(t, internal, "100.00")
	ext := "dup-" + runID
	var wg sync.WaitGroup
	results := make([]resp, 50)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = call(t, instance(i), "POST", "/wagering/transactions", provider, bet(w, ext, "10.00"), "provider-a:"+ext)
		}(i)
	}
	wg.Wait()
	replays := 0
	for i, r := range results {
		if r.Status != 200 || r.Body["status"] != "PROCESSED" || r.Body["balance"].(map[string]any)["amount"] != "90.00" {
			t.Fatalf("result %d: %d %s", i, r.Status, r.Raw)
		}
		if r.Body["idempotentReplay"] == true {
			replays++
		}
	}
	if replays != 49 {
		t.Fatalf("replays %d", replays)
	}
	if amt, v := balance(t, internal, w); amt != "90.00" || v != 2 {
		t.Fatalf("balance %s v%v", amt, v)
	}
	if d, n := ledgerDebits(t, internal, w); d != 1 || n != 2 {
		t.Fatalf("ledger debits=%d entries=%d", d, n)
	}
	reconcile(t, internal, w)
}

func TestTwoBetsOfEightyAcrossInstances(t *testing.T) {
	internal, provider := token(t, "internal-service", "internal-service-secret"), token(t, "provider-a", "provider-a-secret")
	w := openWallet(t, internal, "100.00")
	run := func() (a, b resp) {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			a = call(t, instance(1), "POST", "/wagering/transactions", provider, bet(w, "a-"+runID, "80.00"), "provider-a:a-"+runID)
		}()
		go func() {
			defer wg.Done()
			b = call(t, instance(2), "POST", "/wagering/transactions", provider, bet(w, "b-"+runID, "80.00"), "provider-a:b-"+runID)
		}()
		wg.Wait()
		return
	}
	a, b := run()
	codes := map[int]int{a.Status: 1}
	codes[b.Status]++
	if codes[200] != 1 || codes[422] != 1 {
		t.Fatalf("outcomes: %d %s / %d %s", a.Status, a.Raw, b.Status, b.Raw)
	}
	for _, r := range []resp{a, b} {
		if r.Status == 422 && r.Body["failureCode"] != "INSUFFICIENT_BALANCE" {
			t.Fatalf("code %s", r.Raw)
		}
	}
	a2, b2 := run()
	if a2.Status != a.Status || b2.Status != b.Status || a2.Body["idempotentReplay"] != true || b2.Body["idempotentReplay"] != true {
		t.Fatalf("resend changed outcome: %s %s", a2.Raw, b2.Raw)
	}
	if amt, _ := balance(t, internal, w); amt != "20.00" {
		t.Fatalf("balance %s", amt)
	}
	if d, _ := ledgerDebits(t, internal, w); d != 1 {
		t.Fatalf("debits %d", d)
	}
	reconcile(t, internal, w)
}

func TestDistinctWalletsAcrossInstances(t *testing.T) {
	internal, provider := token(t, "internal-service", "internal-service-secret"), token(t, "provider-a", "provider-a-secret")
	const wallets, bets = 9, 6
	ws := make([]wallet, wallets)
	for i := range ws {
		ws[i] = openWallet(t, internal, "600.00")
	}
	var wg sync.WaitGroup
	for i, w := range ws {
		for j := 0; j < bets; j++ {
			wg.Add(1)
			go func(i, j int, w wallet) {
				defer wg.Done()
				ext := fmt.Sprintf("par-%s-%d-%d", runID, i, j)
				if r := call(t, instance(i+j), "POST", "/wagering/transactions", provider, bet(w, ext, "50.00"), "provider-a:"+ext); r.Status != 200 {
					t.Errorf("wallet %d bet %d: %d %s", i, j, r.Status, r.Raw)
				}
			}(i, j, w)
		}
	}
	wg.Wait()
	for _, w := range ws {
		if amt, v := balance(t, internal, w); amt != "300.00" || v != 1+bets {
			t.Fatalf("wallet %s: %s v%v", w.ID, amt, v)
		}
		reconcile(t, internal, w)
	}
}

func TestHTTPAndSQSSameOperationAcrossInstances(t *testing.T) {
	internal, provider := token(t, "internal-service", "internal-service-secret"), token(t, "provider-a", "provider-a-secret")
	sqsClient, err := sqsadapter.NewClient(context.Background(), sqsadapter.Config{Region: "us-east-1", Endpoint: sqsEndpoint, AccessKeyID: "test", SecretAccessKey: "test"})
	if err != nil {
		t.Fatal(err)
	}
	q, err := sqsClient.GetQueueUrl(context.Background(), &awssqs.GetQueueUrlInput{QueueName: aws.String("wager-transactions.fifo")})
	if err != nil {
		t.Fatal(err)
	}
	w := openWallet(t, internal, "100.00")
	ext := "cross-" + runID
	body := bet(w, ext, "70.00")
	body["idempotencyKey"] = "provider-a:" + ext
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				if r := call(t, instance(i), "POST", "/wagering/transactions", provider, bet(w, ext, "70.00"), "provider-a:"+ext); r.Status != 200 {
					t.Errorf("http: %d %s", r.Status, r.Raw)
				}
				return
			}
			env, _ := json.Marshal(map[string]any{"messageId": fmt.Sprintf("%s-%d", ext, i), "type": "WagerTransactionRequested", "occurredAt": time.Now().UTC().Format(time.RFC3339), "data": body})
			if _, err := sqsClient.SendMessage(context.Background(), &awssqs.SendMessageInput{QueueUrl: q.QueueUrl, MessageBody: aws.String(string(env)),
				MessageGroupId: aws.String(w.ID), MessageDeduplicationId: aws.String(uuid.NewString())}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		r := call(t, instance(0), "GET", "/providers/provider-a/wagering/transactions/"+ext, provider, nil, "")
		if r.Status == 200 && r.Body["status"] == "PROCESSED" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(2 * time.Second) // let the SQS copies be consumed and deduplicated
	if amt, v := balance(t, internal, w); amt != "30.00" || v != 2 {
		t.Fatalf("balance %s v%v", amt, v)
	}
	if d, _ := ledgerDebits(t, internal, w); d != 1 {
		t.Fatalf("debits %d", d)
	}
	reconcile(t, internal, w)
}
