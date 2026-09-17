# Wager Service — processamento distribuído de apostas em Go

Serviço HTTP + consumidor SQS que movimenta carteiras de jogadores com
idempotência persistente, ledger append-only, transactional outbox e
autenticação OAuth 2.0/OIDC (Keycloak). Construído com Go 1.27, Uber Fx, pgx,
PostgreSQL, AWS SQS (LocalStack) e Docker Compose.

As decisões de arquitetura estão em [ARCHITECTURE.md](ARCHITECTURE.md).

## Pré-requisitos

- Docker 24+ com Docker Compose v2
- Go 1.27 (apenas para rodar os testes / o binário fora do container)
- `curl` e `python3` para os scripts de demonstração

## Subindo o ambiente

```sh
docker compose up --build          # ou: make up (em background)
```

Sobe, nesta ordem: PostgreSQL 17, Keycloak 26 (realm `wager` importado
automaticamente), LocalStack (filas FIFO provisionadas), o job `migrate`
(aplica as migrations) e três instâncias independentes do serviço:

| Instância | URL                     |
| --------- | ----------------------- |
| app1      | http://localhost:8081   |
| app2      | http://localhost:8082   |
| app3      | http://localhost:8083   |
| Keycloak  | http://localhost:8080 (admin/admin) |
| LocalStack| http://localhost:4566   |
| PostgreSQL| localhost:5432 (wager/wager, db `wager`) |

Health checks públicos: `GET /health/live` e `GET /health/ready` (verifica
PostgreSQL e SQS). Métricas Prometheus em `GET /metrics`.

Para parar e limpar volumes: `docker compose down -v` (ou `make down`).

## Variáveis de ambiente

Todas estão documentadas com valores locais em [`.env.example`](.env.example).
As principais:

| Variável | Descrição |
| --- | --- |
| `DATABASE_URL` | DSN do PostgreSQL |
| `MIGRATE_ON_START` | aplica migrations no boot (o compose usa o job `migrate` em vez disso) |
| `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | acesso ao SQS |
| `SQS_WAGER_QUEUE_NAME`, `SQS_WAGER_DLQ_NAME`, `SQS_EVENTS_QUEUE_NAME` | nomes das filas (resolvidas no boot) |
| `OIDC_ISSUER`, `OIDC_JWKS_URL`, `OIDC_AUDIENCE` | validação de tokens |
| `CONSUMER_*`, `OUTBOX_*`, `PENDING_*` | parâmetros dos workers (polling, backoff, lease, TTL) |
| `SHUTDOWN_TIMEOUT` | prazo do desligamento gracioso |
| `FAULT_INJECT` | pontos de crash para demonstração (`consumer-after-commit`, `outbox-after-publish`) |

Para rodar o binário no host: `cp .env.example .env`, exporte as variáveis
(`set -a; source .env; set +a`) e execute `go run ./cmd/wager`.

## Migrations

As migrations ficam em `internal/adapters/postgres/migrations` (embutidas no
binário, aplicadas com golang-migrate).

```sh
make migrate-up                 # aplica
make migrate-down               # reverte todas
make migrate-down STEPS=1       # reverte a última
make migrate-version            # versão atual
# equivalente: DATABASE_URL=... go run ./cmd/wager migrate up|down [n]|version
# no container: docker compose run --rm migrate migrate down
```

## Filas SQS

Provisionadas por `deploy/localstack/init-queues.sh` na inicialização do
LocalStack:

| Fila | Uso |
| --- | --- |
| `wager-transactions.fifo` | entrada de operações (`WagerTransactionRequested`); `VisibilityTimeout=30s`, redrive para a DLQ após 5 recebimentos |
| `wager-transactions-dlq.fifo` | mensagens inválidas ou esgotadas |
| `wallet-events.fifo` | destino da outbox (`WagerTransactionProcessed`, `WagerTransactionRejected`, `WagerTransactionPendingReference`, `WalletBalanceChanged`) |

Ao publicar na fila de entrada use `MessageGroupId = walletId` (ordem por
carteira) e `MessageDeduplicationId = messageId`. Detalhes em ARCHITECTURE.md.

## Identidades de teste (Keycloak, realm `wager`)

Provisionadas automaticamente por `deploy/keycloak/wager-realm.json`
(`client_credentials`):

| client_id | secret | papel | claims |
| --- | --- | --- | --- |
| `provider-a` | `provider-a-secret` | `wager:provider` | `providerId=provider-a` |
| `provider-b` | `provider-b-secret` | `wager:provider` | `providerId=provider-b` |
| `internal-service` | `internal-service-secret` | `wager:internal` | — |
| `provider-a-short-lived` | `provider-a-short-lived-secret` | `wager:provider` | token expira em 1s (testes) |
| `no-role-client` | `no-role-client-secret` | nenhum | usado para provar o 403 |

```sh
INT=$(scripts/token.sh internal-service internal-service-secret)
PA=$(scripts/token.sh provider-a provider-a-secret)
```

## Exemplos de chamadas

```sh
# abrir carteira (papel interno)
curl -s -X POST localhost:8081/wallets -H "Authorization: Bearer $INT" -H 'Content-Type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}'
# -> 201 {"id":"<walletId>","playerId":"...","balance":{"amount":"1000.00","currency":"BRL"},"version":1,...}

# aposta (papel provider; providerId do corpo deve ser o do token)
curl -s -X POST localhost:8082/wagering/transactions -H "Authorization: Bearer $PA" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: provider-a:transaction-123' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
       "walletId":"<walletId>","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
# -> 200 {"transactionId":"...","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}
# reenvio idêntico (em qualquer instância) -> 200 ... "idempotentReplay":true

# reversão (REFUND/ROLLBACK): acrescente "referenceExternalTransactionId":"transaction-123"

# consultas
curl -s localhost:8081/wagering/transactions/<transactionId> -H "Authorization: Bearer $PA"
curl -s localhost:8081/providers/provider-a/wagering/transactions/transaction-123 -H "Authorization: Bearer $PA"
curl -s localhost:8081/wallets/<walletId> -H "Authorization: Bearer $INT"
curl -s "localhost:8081/wallets/<walletId>/ledger?limit=50&cursor=" -H "Authorization: Bearer $INT"
curl -s -X POST localhost:8081/wallets/<walletId>/reconciliation -H "Authorization: Bearer $INT"

# envio por SQS
docker compose exec localstack awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id <walletId> --message-deduplication-id msg-123 \
  --message-body '{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z",
    "data":{"providerId":"provider-a","externalTransactionId":"transaction-124","idempotencyKey":"provider-a:transaction-124",
    "playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"<walletId>","roundId":"round-987","gameId":"fortune-chimp",
    "kind":"BET","money":{"amount":"25.00","currency":"BRL"}}}'
```

### Códigos HTTP do contrato

| Situação | Status | Corpo |
| --- | --- | --- |
| Operação processada | `200` | `{"transactionId","status":"PROCESSED","balance","idempotentReplay"}` |
| Aguardando referência (REFUND/ROLLBACK antes da transação referenciada) | `202` | `{"transactionId","status":"PENDING_REFERENCE","idempotentReplay"}` — acompanhe por `GET` |
| Rejeição por regra de negócio | `422` | `{"transactionId","status":"REJECTED","failureCode","balance","idempotentReplay"}` |
| Entrada inválida (JSON, dinheiro, UUID, `Idempotency-Key` ausente, `OPENING`, `LOSS` ≠ 0.00, valor ≤ 0) | `400` | `{"code":"INVALID_INPUT","message"}` |
| Token ausente/inválido/expirado | `401` | `{"code":"UNAUTHENTICATED"}` + `WWW-Authenticate` |
| Identidade sem permissão (papel errado, `providerId` de outro provedor) | `403` | `{"code":"FORBIDDEN"}` |
| Carteira/transação inexistente ou invisível para o provedor | `404` | `{"code":"WALLET_NOT_FOUND" \| "TRANSACTION_NOT_FOUND"}` |
| Chave reutilizada com conteúdo diferente | `409` | `{"code":"IDEMPOTENCY_CONFLICT"}` |
| `(providerId, externalTransactionId)` já usado com outra chave | `409` | `{"code":"EXTERNAL_ID_CONFLICT"}` |
| Carteira já existe para `(playerId, currency)` | `409` | `{"code":"WALLET_ALREADY_EXISTS"}` |
| PostgreSQL/SQS indisponíveis, timeout, deadlock/serialização | `503` + `Retry-After: 1` | `{"code":"UNAVAILABLE"}` |
| Erro inesperado | `500` | `{"code":"INTERNAL_ERROR"}` |

Replays retornam o mesmo status do resultado original (`200`/`202`/`422`) com
`idempotentReplay: true` e o saldo observado no processamento original.

### Códigos de rejeição (`failureCode`)

| Código | Significado | Corrigível pelo provedor? |
| --- | --- | --- |
| `INSUFFICIENT_BALANCE` | BET sem saldo | não (resultado definitivo) |
| `REVERSAL_INSUFFICIENT_BALANCE` | ROLLBACK de WIN/REFUND debitaria além do saldo | não |
| `REFERENCE_NOT_FOUND` | referência nunca chegou (tentativas/TTL esgotados) | sim (reenviar com novo id após a referência) |
| `REFERENCE_NOT_PROCESSED` | referência terminou `REJECTED`/`FAILED` | não |
| `REFERENCE_MISMATCH` | provedor/jogador/carteira/moeda/rodada divergem | sim |
| `REFERENCE_KIND_MISMATCH` | ex.: REFUND de um WIN | sim |
| `REFERENCE_AMOUNT_MISMATCH` | reversão parcial | sim |
| `REFERENCE_ALREADY_REVERSED` | a referência já tem uma reversão bem-sucedida | não |
| `CURRENCY_MISMATCH` | moeda do valor ≠ moeda da carteira | sim |
| `WALLET_PLAYER_MISMATCH` | carteira não pertence ao jogador | sim |

## Testes

```sh
go vet ./...
go test ./...                 # unitários (sem infraestrutura)
go test -race ./...
```

Testes de integração (PostgreSQL, Keycloak e LocalStack reais; a suíte cria
um banco e filas próprios por execução, sem interferir nas instâncias em
execução):

```sh
docker compose up -d postgres keycloak localstack   # ou o stack completo
go test -tags integration -count=1 ./tests/integration/
go test -race -tags integration -count=1 ./tests/integration/
```

Variáveis opcionais: `IT_DATABASE_ADMIN_URL`, `IT_SQS_ENDPOINT`,
`IT_KEYCLOAK_URL`, `IT_OIDC_ISSUER` (padrões apontam para o compose).

Cobertura da suíte de integração: migrations up/down, constraints e triggers
(saldo negativo, ledger imutável, unicidades, uma reversão por referência,
imutabilidade de transações terminais e da outbox), atomicidade financeira,
50 envios paralelos da mesma aposta, disputa 80+80 sobre 100, carteiras
distintas em paralelo (três "instâncias" com pools próprios), consumidor SQS
(dedup por inbox, crash após commit antes do delete, retry transitório, DLQ,
parada em shutdown), outbox com dois publishers concorrentes, retry com
backoff, lease abandonado e republicação com o mesmo `eventId`, publicação
real no broker, referência pendente resolvida por outra instância, expiração
como `REJECTED`, reinício com preservação de idempotência/pendências,
autenticação e autorização reais (401/403/404, isolamento entre provedores,
ausência de efeitos financeiros), mesma operação por HTTP e SQS simultâneos e
o ciclo de vida completo da aplicação Fx (start, readiness, stop).

Múltiplas instâncias reais (três processos do compose):

```sh
docker compose up --build -d
go test -tags e2e -count=1 -v ./tests/e2e/        # ou: make test-e2e
```

Simulações de falha reproduzíveis (stack completo em execução):

```sh
make chaos-consumer   # crash após commit e antes do DeleteMessage -> reentrega deduplicada
make chaos-outbox     # crash após publicar e antes de marcar a outbox -> republicação com o mesmo eventId
make chaos-pending    # SIGKILL com REFUND pendente -> outra instância resolve quando a BET chega
make chaos-sigterm    # SIGTERM durante consumo -> drenagem e continuidade pelas outras instâncias
make chaos-postgres   # PostgreSQL pausado -> 503 com Retry-After e readiness DOWN; recuperação sem perda
```

`make test-all` executa vet, unitários com `-race`, integração com `-race` e e2e.

## Layout

```
cmd/wager                    binário (serve | migrate)
internal/domain/money        value object Money (int64 em centavos)
internal/domain/wager        Wallet, LedgerEntry, WagerTransaction, reversões, eventos
internal/app                 casos de uso, ports, hash de idempotência
internal/adapters/postgres   pgx, SQL explícito, migrations
internal/adapters/httpapi    net/http, middlewares de auth, contratos
internal/adapters/sqs        cliente, consumidor (inbox), publisher
internal/adapters/auth       verificação OIDC (go-oidc + JWKS)
internal/workers             outbox publisher, worker de referências, supervisor
internal/fxapp               composição Fx e lifecycle
internal/observability       logs JSON e métricas Prometheus
deploy/                      realm do Keycloak e provisionamento das filas
scripts/                     tokens e simulações de falha
tests/integration, tests/e2e suítes com infraestrutura real
```
