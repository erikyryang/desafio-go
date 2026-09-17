# Arquitetura

Este documento registra as decisões técnicas, interpretações adotadas e
limitações. A organização segue os temas pedidos no desafio.

## 1. Visão geral

```
                 ┌──────────────┐        ┌───────────────────────┐
  provedores ───▶│  HTTP (net/  │        │  SQS wager-transactions│◀── provedores
  (Bearer JWT)   │  http)       │        │  .fifo  (+ DLQ)        │
                 └──────┬───────┘        └───────────┬───────────┘
                        │ SubmitCommand               │ Consumer (inbox)
                        ▼                             ▼
                 ┌──────────────────────────────────────────────┐
                 │ app.WagerService / app.WalletService         │
                 │ (uma transação SQL por operação)             │
                 └──────────────────────┬───────────────────────┘
                                        ▼
        PostgreSQL: wallets · wager_transactions · wallet_ledger_entries
                    inbox_messages · outbox_events
                                        │
      pending-reference worker ◀────────┼────────▶ outbox publisher ──▶ SQS wallet-events.fifo
```

Camadas (`internal/`):

- `domain/money`, `domain/wager`: domínio puro, sem Fx/HTTP/SQS/pgx.
- `app`: casos de uso e *ports* (`UnitOfWork`, repositórios, `Metrics`).
- `adapters/*`: PostgreSQL (pgx), HTTP, SQS, OIDC.
- `workers`: loops de fundo. `fxapp`: composição e ciclo de vida.

Três instâncias (`app1..3`) rodam no Compose; nada depende de estado em
memória, então qualquer instância pode atender qualquer requisição, mensagem,
evento pendente ou reversão parada.

## 2. Dinheiro

`money.Money` é um value object imutável: `int64` em **unidades mínimas**
(centavos) + `Currency` (ISO 4217, 3 letras maiúsculas). Escala fixa de 2.

- Parsing aceita `^-?[0-9]{1,17}(\.[0-9]{1,2})?$`; rejeita vazio, espaços,
  `+`, `NaN`, `Infinity`, notação científica, separador de milhar, mais de
  duas casas (`ErrScaleExceeded`) e overflow (`ErrOverflow`). O contrato
  externo usa `ParseNonNegative`, que também rejeita negativos.
- **Normalização** (documentada por causa do hash de idempotência): `"25"` e
  `"25.5"` são aceitos e normalizados para `"25.00"`/`"25.50"` antes de
  qualquer uso; o hash é calculado sobre a forma normalizada, portanto
  `"25"` e `"25.00"` são a mesma operação. Nunca há arredondamento.
- Limites: `[-92233720368547758.08, 92233720368547758.07]`. Soma, subtração e
  negação verificam overflow. Aritmética/comparação exigem a mesma moeda
  (`ErrCurrencyMismatch`); o zero-value de `Money` é inválido
  (`ErrUninitialized`).
- JSON: `{"amount":"25.00","currency":"BRL"}`; `amount` numérico é recusado
  (o decoder nunca passa por `float64`).
- Persistência: `BIGINT` (minor units) + `CHAR(3)` em todas as tabelas; não
  existe coluna de ponto flutuante. Só BRL é usado nos cenários; o tipo
  carrega a moeda e há testes de incompatibilidade.

## 3. Modelo de domínio

- **Wallet** (raiz do agregado): `id`, `playerId`, `currency`, `balance`,
  `version`, timestamps. `NewWallet` (versão 1, saldo 0) e `RehydrateWallet`
  (sem reaplicar nada). `Credit`/`Debit` produzem o `LedgerEntry`
  correspondente, incrementam a versão e recusam débitos que deixariam o
  saldo negativo. `Open` aplica o crédito inicial mantendo a versão 1 (a
  abertura faz parte da criação, como exige o contrato).
- **LedgerEntry**: imutável; o construtor valida
  `balanceAfter = balanceBefore ± amount`, moedas e não negatividade.
- **WagerTransaction**: `NewExternalTransaction` (valida ids, política de
  valor por tipo, presença de referência só em reversões, recusa `OPENING`),
  `NewOpeningTransaction` (interna, sem metadados externos) e
  `RehydrateTransaction`. Transições (`MarkProcessed`, `MarkRejected`,
  `MarkFailed`, `MarkPendingReference`) validam a máquina de estados; estados
  terminais não aceitam novas transições.
- Erros são sentinelas (`errors.Is`) — nenhum `panic` representa rejeição.
  Toda I/O recebe `context.Context`.

### Máquina de estados

```
PENDING ──▶ PROCESSED   (terminal)
   │  ├───▶ REJECTED    (terminal, failureCode)
   │  ├───▶ FAILED      (terminal, falha permanente de infraestrutura)
   │  └───▶ PENDING_REFERENCE ─┬─▶ PENDING_REFERENCE (reagendado, attempts++)
   │                           ├─▶ PROCESSED
   └───────────────────────────┴─▶ REJECTED
```

`PENDING` existe em memória durante o processamento síncrono; como a
operação é concluída (ou parada como `PENDING_REFERENCE`) na mesma transação
em que é inserida, **nunca há `PENDING` confirmado no banco** — não existe
aceite assíncrono a retomar. O único estado não terminal durável é
`PENDING_REFERENCE`, cuja retomada é feita pelo worker de qualquer instância
(§7). `FAILED` está modelado (domínio, schema, transições) para auditoria de
falhas permanentes de infraestrutura; no fluxo atual falhas de
infraestrutura são sempre transitórias (a transação é abortada e a origem
reenvia), então nenhum caminho o grava automaticamente.

**Transitório vs permanente.** Transitório: erros de conexão/timeout,
`40001`/`40P01` (serialização/deadlock), classe `08`, `57P0x`, pool fechado —
mapeados para `app.ErrUnavailable` → HTTP `503` / mensagem deixada na fila
para reentrega. Permanente: validação, carteira inexistente, conflitos de
idempotência → HTTP `4xx` / mensagem copiada para a DLQ.

## 4. Persistência e transações

Biblioteca: **pgx v5** (`pgxpool`) com SQL explícito; migrations com
golang-migrate embutidas no binário (`wager migrate up|down|version`).

`Money` ↔ `amount_minor BIGINT` + `currency CHAR(3)`; `balance_after_minor`
guarda o saldo observado no resultado devolvido ao provedor.

**Delimitação da transação SQL.** `app.UnitOfWork.Do` abre uma transação
(`READ COMMITTED`) e entrega um `Store` cujos repositórios compartilham o
mesmo `pgx.Tx`. Um caso de uso é uma única transação: lookup de idempotência
→ `SELECT ... FOR UPDATE` da carteira → regras → `INSERT` da transação →
`INSERT` do ledger → `UPDATE` da carteira com verificação de versão →
`INSERT` na outbox → `COMMIT`. O consumidor SQS chama `SubmitIn` dentro da
sua própria transação para incluir a inbox no mesmo commit. A reconciliação
usa `REPEATABLE READ, READ ONLY` para ler saldo e ledger na mesma foto.

**Invariantes no schema** (`000001_init.up.sql`):

- `wallets.balance_minor >= 0`, `version >= 1`, `UNIQUE (player_id, currency)`.
- `wager_transactions`: `UNIQUE (idempotency_key)`, `UNIQUE (provider_id,
  external_transaction_id)`, um `OPENING` por carteira (índice parcial), uma
  reversão `PROCESSED` por referência (índice parcial), CHECKs que separam
  origem interna/externa, exigem referência só em reversões, política de
  valor por tipo (`LOSS = 0`, demais `> 0`), `failure_code` ⇔
  `REJECTED/FAILED`, `balance_after` em `PROCESSED`, agenda em
  `PENDING_REFERENCE`. Trigger impede `DELETE` e qualquer `UPDATE` em linha
  terminal ou nos campos de identidade.
- `wallet_ledger_entries`: `UNIQUE (wallet_id, transaction_id)`, CHECK da
  aritmética, valores positivos, saldos não negativos, `sequence BIGSERIAL`
  para ordenação estável; trigger recusa `UPDATE`/`DELETE` (append-only).
- `outbox_events`: trigger recusa alteração do payload/tipo/ocorrência.
- `inbox_messages`: `PRIMARY KEY (consumer_name, message_id)`.

## 5. Concorrência

Coordenação **por carteira**, combinando:

1. **Lock pessimista**: `SELECT ... FOR UPDATE` na linha da carteira
   serializa escritores da mesma carteira; carteiras diferentes não se
   bloqueiam (não há lock global).
2. **Verificação otimista**: `UPDATE wallets ... WHERE id = $1 AND version =
   $expected`; zero linhas → `ErrConcurrentModification` (métrica
   `wager_concurrency_conflicts_total`). É redundante com o lock, mas
   garante ausência de *lost update* mesmo que algum caminho esqueça o lock.
3. **Constraints**: `balance_minor >= 0` e os índices únicos são a última
   linha de defesa, independentes de locks locais e da deduplicação FIFO.

Ordem de locks: caminho síncrono bloqueia só a carteira; o worker de
referências bloqueia a linha da transação pendente (`FOR UPDATE SKIP
LOCKED`) e depois a carteira. Não há ciclo; um deadlock eventual seria
detectado pelo PostgreSQL e devolvido como transitório.

Cenário obrigatório (100.00 com duas apostas de 80.00): o segundo escritor
espera o lock, relê o saldo (20.00), é rejeitado com
`INSUFFICIENT_BALANCE` e sua rejeição fica persistida; reenvios reproduzem
o mesmo resultado. Testes: `TestTwoBetsOfEightyOnOneHundred` (3 pools) e
`TestTwoBetsOfEightyAcrossInstances` (3 processos).

## 6. Idempotência

Persistida em `wager_transactions` (sobrevive a reinícios; testado em
`TestRestartPreservesIdempotencyPendingAndConsistency`).

- Chave: header `Idempotency-Key` (HTTP) ou `data.idempotencyKey` (SQS). O
  servidor nunca substitui a chave recebida.
- **Hash do payload**: SHA-256 do JSON canônico (chaves ordenadas, sem
  espaços) dos campos `externalTransactionId, gameId, kind,
  money{amount,currency}, playerId, providerId, roundId, walletId` e, em
  reversões, `referenceExternalTransactionId`. Normalizações: UUIDs em
  minúsculas canônicas, `amount` com exatamente duas casas. Chave de
  idempotência, `messageId`, `correlationId` e demais metadados de
  transporte ficam fora, logo HTTP e SQS produzem o mesmo hash
  (`TestSameOperationViaHTTPAndSQSConcurrently`).
- Decisão: existe linha com a mesma chave → hash igual: replay
  (`idempotentReplay: true`, resultado e saldo originais); hash diferente:
  `409 IDEMPOTENCY_CONFLICT`. Não existe pela chave mas existe por
  `(providerId, externalTransactionId)` → `409 EXTERNAL_ID_CONFLICT` (a
  operação nunca é reaplicada com outra chave).
- O lookup é feito antes (caminho rápido, sem esperar o lock) e **de novo
  sob o lock da carteira**, o que elimina a corrida entre duplicatas
  simultâneas; se ainda assim um `INSERT` violar um índice único (chave
  igual com carteiras diferentes), o erro vira `409`.
- Replays de operações concluídas devolvem `balance_after_minor` gravado na
  época, mesmo que a carteira tenha se movido depois.

## 7. Reversões e referências pendentes

| Tipo | Referência aceita | Movimento |
| --- | --- | --- |
| `REFUND` | `BET` | crédito |
| `ROLLBACK` | `BET` → crédito; `WIN` ou `REFUND` → débito |

Validação (`wager.ValidateReference`): referência `PROCESSED`; tipo
compatível; mesmo provedor, jogador, carteira, moeda e rodada; valor igual
(sem parciais). Referência `REJECTED`/`FAILED` → `REFERENCE_NOT_PROCESSED`.
Referência ainda `PENDING_REFERENCE` (ex.: ROLLBACK de um REFUND que espera
sua BET) → a nova reversão também espera.

**REFUND + ROLLBACK sobre a mesma aposta.** Regra adotada: *cada transação
recebe no máximo uma reversão bem-sucedida, de qualquer tipo*. A segunda
(REFUND ou ROLLBACK) é rejeitada com `REFERENCE_ALREADY_REVERSED`. Isso
impede devolver o mesmo débito duas vezes. O ROLLBACK de um REFUND
referencia o próprio REFUND (debita de volta) e também só pode ocorrer uma
vez. Limitação assumida: depois de `BET → REFUND → ROLLBACK(REFUND)`, um novo
REFUND da BET é recusado (a BET já tem uma reversão registrada); a cadeia
seria resolvida com um novo lançamento explícito, fora do escopo. A regra é
imposta no banco pelo índice parcial `one_reversal_per_reference`
(`TestSchemaConstraints`) e, sob o lock da carteira, pela consulta
`HasProcessedReversal`.

Reversão que debitaria além do saldo → `REVERSAL_INSUFFICIENT_BALANCE`
(código distinto de `INSUFFICIENT_BALANCE`), persistida e auditável.

**Referência indisponível.** A operação é gravada como `PENDING_REFERENCE`
com `attempts = 1` e `next_attempt_at = now + backoff`, e o evento
`WagerTransactionPendingReference` vai para a outbox no mesmo commit (HTTP
`202`; no SQS a mensagem é concluída e removida — a continuidade é do
worker). O `PendingReferenceWorker` de cada instância lista linhas
vencidas, trava cada uma com `FOR UPDATE SKIP LOCKED` (uma instância por
linha), trava a carteira e reavalia. Backoff exponencial
`PENDING_INITIAL_BACKOFF · 2^(attempts-1)` limitado a `PENDING_MAX_BACKOFF`;
o agendamento vive no banco, logo sobrevive a reinícios
(`TestReversalBeforeReferenceIsResolvedByAnotherInstance`). Esgotados
`PENDING_MAX_ATTEMPTS` ou `PENDING_TTL`, a transação vira `REJECTED` com
`REFERENCE_NOT_FOUND` e `WagerTransactionRejected` é emitido; uma referência
que chegue depois não reabre o estado terminal.

## 8. Inbox e outbox

**Inbox** (`inbox_messages`): `messageId` do envelope é a identidade durável;
o hash é o SHA-256 canônico dos campos de negócio de `data` (inclui
`idempotencyKey`, exclui `occurredAt`). O `INSERT ... ON CONFLICT DO
NOTHING` acontece na mesma transação da inbox, do domínio, do ledger e da
outbox; `received_at = completed_at` porque só há commit quando o
tratamento termina. Reentrega com o mesmo hash → duplicata (removida da
fila); mesmo `messageId` com conteúdo diferente → DLQ.

**Outbox** (`outbox_events`): `id` = `eventId` estável, agregado, tipo,
versão, payload JSONB imutável (snapshot do envelope), `attempts`,
`next_attempt_at`, `locked_by/locked_until` (lease), `published_at`.

Publisher (`workers.OutboxPublisher`, um por instância):

1. `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED)` reivindica
   até `OUTBOX_BATCH_SIZE` linhas vencidas e sem lease válido, gravando o
   lease (`OUTBOX_LEASE`, 30s). Vários publishers disputam sem colisão
   (`TestOutboxTwoPublishersCompete`).
2. `SendMessage` em `wallet-events.fifo` com `MessageGroupId = aggregateId`,
   `MessageDeduplicationId = eventId`, atributos `eventType`,
   `aggregateType`, `correlationId`; corpo = envelope.
3. Sucesso → `published_at`; falha → `attempts++`, `next_attempt_at` com
   backoff exponencial e lease liberado.

Recuperação: crash entre commit e publicação → a linha continua sem
`published_at` e qualquer instância a publica; crash entre `SendMessage` e
`published_at` → o lease expira e outra instância **republica com o mesmo
`eventId`** (o broker FIFO deduplica por 5 min; consumidores devem ser
idempotentes por `eventId`). Demonstrado em `TestOutboxRetryAndRecovery` e
em `make chaos-outbox`. A publicação só ocorre depois do commit por
construção: o publisher lê apenas linhas já confirmadas.

Envelope: `eventId, eventType, aggregateType, aggregateId, correlationId,
causationId?, occurredAt (RFC 3339 UTC), version (1), data`. Tipos e versão
são fixados pelos construtores em `domain/wager/events.go`. Payload de
`WalletBalanceChanged`: `walletId, playerId, transactionId, direction,
money, balanceBefore, balanceAfter, walletVersion, occurredAt`.
`LOSS` produz `WagerTransactionProcessed` sem `WalletBalanceChanged`.

## 9. Consumidor SQS

- Filas FIFO. Produtor: `MessageGroupId = walletId` (ordem por carteira,
  paralelismo entre carteiras), `MessageDeduplicationId = messageId` (o
  broker suprime duplicatas em 5 min; a inbox cobre o resto).
- `ReceiveMessage` com long polling (`CONSUMER_WAIT_TIME` 10s), até 10
  mensagens, `VisibilityTimeout` 30s. Mensagens do mesmo grupo são tratadas
  em ordem; grupos diferentes em paralelo.
- Resultado: commit (processada, rejeitada ou pendente) → `DeleteMessage`;
  duplicata → `DeleteMessage`; permanente (envelope inválido, dinheiro
  inválido, carteira inexistente, conflito de idempotência) → cópia para a
  DLQ com atributo `reason` e `DeleteMessage`; transitório → a mensagem não é
  removida, volta após o visibility timeout e, após `maxReceiveCount = 5`,
  o redrive a leva à DLQ.
- **Ordem de deleção**: nunca antes do commit. Se o delete falhar após o
  commit, a reentrega é deduplicada pela inbox (`make chaos-consumer`).
- `SIGTERM`: o contexto do consumidor é cancelado; ele para de receber,
  termina o lote em andamento em até `CONSUMER_DRAIN_TIMEOUT` e, se
  estourar, chama `ChangeMessageVisibility(0)` nas mensagens não
  concluídas para reentrega imediata.
- Controle de acesso ao broker: credenciais estáticas por configuração e
  *queue policy* que restringe as ações ao principal `wager-service`
  (`deploy/localstack/init-queues.sh`). O LocalStack Community não aplica IAM;
  em produção a mesma policy + IAM role da instância fazem o enforcement.
  As validações de domínio permanecem no consumidor.

## 10. Autenticação e autorização

IdP: **Keycloak** (recomendado no desafio; open source, importa realm
declarativo, suporta `client_credentials` e mappers de claims). O serviço
não emite tokens nem guarda senhas.

Validação: `go-oidc` com JWKS remoto (`OIDC_JWKS_URL`, cache com refresh ao
ver `kid` desconhecido), verificando assinatura RS256, `iss ==
OIDC_ISSUER`, `aud` contém `wager-api` (audience mapper em cada client) e
`exp`. O issuer é fixo (`KC_HOSTNAME=http://localhost:8080`) para que tokens
obtidos pelo host ou pela rede do Compose tenham o mesmo `iss`, enquanto o
serviço busca as chaves pelo endereço interno.

Modelo de permissões (realm roles em `realm_access.roles`):

| Papel | Claims | Pode |
| --- | --- | --- |
| `wager:provider` | `providerId` (hardcoded claim mapper por client) | `POST /wagering/transactions` (o `providerId` do corpo **deve** ser o do token — a identidade decide o provedor), ler transações do próprio provedor por id ou por `(providerId, externalTransactionId)` |
| `wager:internal` | — | `POST /wallets`, `GET /wallets/*`, reconciliação, leitura de qualquer transação |

Isolamento: `GET /wagering/transactions/:id` devolve `404` a outro provedor
(não revela existência); `GET /providers/:providerId/...` devolve `403` se o
`providerId` não for o do token; transações internas (`OPENING`) são
invisíveis a provedores. A checagem existe no handler e também no caso de
uso (`app.Actor`), como defesa em profundidade. Testado com tokens reais em
`TestAuthenticationAndAuthorization` (ausente, malformado, adulterado,
expirado, papéis errados, spoof de `providerId`, sem efeito financeiro).

## 11. Uso do Fx e ciclo de vida

Módulos (`internal/fxapp`): `observability` (logger slog JSON, métricas),
`infra` (pool pgx, `UnitOfWork`, cliente SQS, resolução de filas, verifier
OIDC, fault injector), `app` (clock, gerador de UUID v7, política de
pendências, serviços), `http` (handlers, readiness checks, router, server)
e `workers` (consumer, outbox publisher, pending worker, supervisor).
Injeção por construtores com `fx.Provide`/`fx.Annotate(fx.As(...))` para as
interfaces; `fx.Invoke(registerLifecycle)` registra os hooks. `fx.ValidateApp`
é executado em teste unitário; start/stop real em `TestFxAppStartsAndStopsCleanly`.

Inicialização: `config.Load` valida variáveis e falha cedo; os construtores
pingam o PostgreSQL e resolvem as três filas (erro = a app não sobe).

Encerramento (`fx.StopTimeout = SHUTDOWN_TIMEOUT`, `SIGINT/SIGTERM` via
`app.Run`), em ordem inversa ao registro:

1. `http.Server.Shutdown` — deixa de aceitar conexões e espera requisições
   em andamento.
2. `Supervisor.Stop` — cancela o contexto compartilhado dos workers e espera
   o `WaitGroup` (consumidor drena o lote; publisher e pending worker
   terminam a iteração). Bookkeeping da outbox usa `context.WithoutCancel`
   para não perder um `published_at` na parada.
3. Fechamento do pool pgx — só depois de todos os usuários pararem.

O supervisor reporta os workers encerrados e a duração; estouro do prazo
devolve erro no `Stop`.

## 12. Observabilidade

Logs JSON (`log/slog`) com `instanceId`, `correlationId`, `messageId`,
`transactionId`, `walletId`, `providerId`, `eventId`; sem tokens nem
payloads completos. Métricas Prometheus em `/metrics`:
`wager_transactions_total{source,kind,status,failure_code}`,
`wager_idempotent_replays_total`, `wager_idempotency_conflicts_total`,
`wager_concurrency_conflicts_total`, `wager_processing_duration_seconds`,
`wager_pending_reference_retries_total`,
`wallet_reconciliation_divergences_total`, `sqs_messages_processed_total`,
`sqs_messages_duplicate_total`, `sqs_messages_retry_total`,
`sqs_messages_dlq_total{reason}`, `outbox_published_total`,
`outbox_publish_retries_total`, `outbox_lag_seconds`. Divergências de
reconciliação aparecem na resposta, em log `ERROR` e na métrica.

## 13. Interpretações adotadas

- `LOSS` com valor ≠ `0.00`, `BET/WIN/REFUND/ROLLBACK` com valor ≤ 0 e
  `OPENING` externo são erros de entrada (`400`), não rejeições persistidas.
- Carteira inexistente é `404` (HTTP) / DLQ (SQS), sem registro de
  transação, porque não há agregado ao qual associar a rejeição.
- Rejeições de negócio persistidas (`422`) guardam o saldo observado para
  que replays devolvam a mesma resposta.
- O `providerId` do corpo é obrigatório e precisa coincidir com o token;
  divergência é `403` e não gera efeito.
- A abertura com saldo positivo grava `OPENING` `PROCESSED`, ledger e os
  dois eventos no mesmo commit da carteira, com `version = 1`.
- O worker de referências usa `correlationId = transactionId` nos eventos
  gerados de forma assíncrona.

## 14. Limitações e trabalho não concluído

- Tracing OpenTelemetry, dashboards e testes de carga (diferenciais) não
  foram implementados.
- Ledger de partidas dobradas (opcional) não foi implementado; o ledger é de
  entrada única por movimento com saldo antes/depois.
- Retenção/limpeza de `outbox_events` e `inbox_messages` não é automática
  (a trigger permite `DELETE` na outbox para um job futuro).
- O IAM do LocalStack Community não é aplicado; a policy está provisionada
  como documentação executável.
- Cadeias de reversão profundas (§7) são deliberadamente recusadas.
- `FAILED` não é gravado automaticamente por nenhum caminho do fluxo atual
  (§3).
