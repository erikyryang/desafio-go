.PHONY: up down logs build vet fmt test test-race test-integration test-integration-race test-e2e test-all \
        migrate-up migrate-down migrate-version chaos-consumer chaos-outbox chaos-pending chaos-sigterm token

DATABASE_URL ?= postgres://wager:wager@localhost:5432/wager?sslmode=disable

up:            ## build images and start postgres, keycloak, localstack, migrations and app1/app2/app3
	docker compose up --build -d
	docker compose ps
down:          ## stop everything and drop volumes
	docker compose down -v --remove-orphans
logs:
	docker compose logs -f app1 app2 app3

build:
	go build ./...
vet:
	go vet ./... && go vet -tags integration ./tests/integration/ && go vet -tags e2e ./tests/e2e/
fmt:
	gofmt -l . && test -z "$$(gofmt -l .)"

test:          ## unit tests (no infrastructure needed)
	go test ./...
test-race:
	go test -race ./...
test-integration:      ## needs: docker compose up postgres keycloak localstack
	go test -tags integration -count=1 ./tests/integration/
test-integration-race:
	go test -race -tags integration -count=1 ./tests/integration/
test-e2e:      ## needs the full stack (three instances) running
	go test -tags e2e -count=1 -v ./tests/e2e/
test-all: vet test-race test-integration-race test-e2e

migrate-up:
	DATABASE_URL="$(DATABASE_URL)" go run ./cmd/wager migrate up
migrate-down:  ## reverts every migration (use STEPS=1 for the last one)
	DATABASE_URL="$(DATABASE_URL)" go run ./cmd/wager migrate down $(STEPS)
migrate-version:
	DATABASE_URL="$(DATABASE_URL)" go run ./cmd/wager migrate version

chaos-consumer:  ## crash after commit, before DeleteMessage
	./scripts/chaos-consumer-crash.sh
chaos-outbox:    ## crash after publish, before marking the outbox row
	./scripts/chaos-outbox-crash.sh
chaos-pending:   ## SIGKILL with a PENDING_REFERENCE; another instance resumes
	./scripts/chaos-pending-restart.sh
chaos-sigterm:   ## graceful shutdown while consuming
	./scripts/chaos-sigterm.sh

token:         ## make token CLIENT=provider-a SECRET=provider-a-secret
	@./scripts/token.sh $(CLIENT) $(SECRET)

chaos-postgres:  ## pause PostgreSQL: 503 + readiness DOWN, then recovery
	./scripts/chaos-postgres-outage.sh
