-- Schema for the wager service. Money is stored as BIGINT minor units plus a
-- CHAR(3) ISO 4217 currency; no floating point column exists anywhere.

CREATE TABLE wallets (
    id             UUID PRIMARY KEY,
    player_id      UUID        NOT NULL,
    currency       CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_minor  BIGINT      NOT NULL CHECK (balance_minor >= 0),
    version        BIGINT      NOT NULL CHECK (version >= 1),
    created_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL,
    CONSTRAINT wallets_player_currency_unique UNIQUE (player_id, currency)
);

CREATE TYPE wager_origin AS ENUM ('INTERNAL', 'EXTERNAL');
CREATE TYPE wager_kind   AS ENUM ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK');
CREATE TYPE wager_status AS ENUM ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED');

CREATE TABLE wager_transactions (
    id                                UUID PRIMARY KEY,
    origin                            wager_origin NOT NULL,
    kind                              wager_kind   NOT NULL,
    status                            wager_status NOT NULL,
    wallet_id                         UUID         NOT NULL REFERENCES wallets (id),
    player_id                         UUID         NOT NULL,
    amount_minor                      BIGINT       NOT NULL CHECK (amount_minor >= 0),
    currency                          CHAR(3)      NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    provider_id                       TEXT,
    external_transaction_id           TEXT,
    idempotency_key                   TEXT,
    payload_hash                      TEXT,
    round_id                          TEXT,
    game_id                           TEXT,
    reference_external_transaction_id TEXT,
    reference_transaction_id          UUID REFERENCES wager_transactions (id),
    failure_code                      TEXT,
    balance_after_minor               BIGINT CHECK (balance_after_minor IS NULL OR balance_after_minor >= 0),
    attempts                          INT          NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at                   TIMESTAMPTZ,
    created_at                        TIMESTAMPTZ  NOT NULL,
    updated_at                        TIMESTAMPTZ  NOT NULL,
    processed_at                      TIMESTAMPTZ,

    -- Internal operations (OPENING) carry no provider metadata; external ones require it.
    CONSTRAINT wager_transactions_origin_fields CHECK (
        (origin = 'INTERNAL' AND kind = 'OPENING'
            AND provider_id IS NULL AND external_transaction_id IS NULL
            AND idempotency_key IS NULL AND payload_hash IS NULL
            AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL AND reference_transaction_id IS NULL
            AND amount_minor > 0)
        OR
        (origin = 'EXTERNAL' AND kind <> 'OPENING'
            AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL
            AND idempotency_key IS NOT NULL AND payload_hash IS NOT NULL
            AND round_id IS NOT NULL AND game_id IS NOT NULL)
    ),
    -- Reversals must carry a reference; other kinds must not.
    CONSTRAINT wager_transactions_reference_fields CHECK (
        (kind IN ('REFUND', 'ROLLBACK')) = (reference_external_transaction_id IS NOT NULL)
    ),
    CONSTRAINT wager_transactions_resolved_reference CHECK (
        reference_transaction_id IS NULL OR kind IN ('REFUND', 'ROLLBACK')
    ),
    -- Amount policy per kind.
    CONSTRAINT wager_transactions_amount_policy CHECK (
        (kind = 'LOSS' AND amount_minor = 0) OR (kind <> 'LOSS' AND amount_minor > 0)
    ),
    -- Rejections and failures always carry a code; other statuses never do.
    CONSTRAINT wager_transactions_failure_code CHECK (
        (status IN ('REJECTED', 'FAILED')) = (failure_code IS NOT NULL)
    ),
    CONSTRAINT wager_transactions_processed_balance CHECK (
        status <> 'PROCESSED' OR balance_after_minor IS NOT NULL
    ),
    CONSTRAINT wager_transactions_pending_schedule CHECK (
        status <> 'PENDING_REFERENCE' OR next_attempt_at IS NOT NULL
    ),
    CONSTRAINT wager_transactions_processed_reference CHECK (
        NOT (status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK')) OR reference_transaction_id IS NOT NULL
    )
);

-- Idempotency: one row per key and one row per provider operation.
CREATE UNIQUE INDEX wager_transactions_idempotency_key_unique
    ON wager_transactions (idempotency_key);
CREATE UNIQUE INDEX wager_transactions_provider_external_unique
    ON wager_transactions (provider_id, external_transaction_id);
-- A wallet receives at most one OPENING credit.
CREATE UNIQUE INDEX wager_transactions_one_opening_per_wallet
    ON wager_transactions (wallet_id) WHERE kind = 'OPENING';
-- A reference receives at most one successful reversal (REFUND or ROLLBACK).
CREATE UNIQUE INDEX wager_transactions_one_reversal_per_reference
    ON wager_transactions (reference_transaction_id)
    WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');
CREATE INDEX wager_transactions_pending_reference_due
    ON wager_transactions (next_attempt_at) WHERE status = 'PENDING_REFERENCE';
CREATE INDEX wager_transactions_wallet_idx ON wager_transactions (wallet_id);

-- Terminal transactions are immutable; nothing is ever deleted.
CREATE FUNCTION wager_transactions_guard() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'wager_transactions is append-only' USING ERRCODE = 'restrict_violation';
    END IF;
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'wager transaction % is terminal (%)', OLD.id, OLD.status USING ERRCODE = 'restrict_violation';
    END IF;
    IF NEW.id <> OLD.id OR NEW.origin <> OLD.origin OR NEW.kind <> OLD.kind OR NEW.wallet_id <> OLD.wallet_id
       OR NEW.player_id <> OLD.player_id OR NEW.amount_minor <> OLD.amount_minor OR NEW.currency <> OLD.currency
       OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'wager transaction identity fields are immutable' USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER wager_transactions_guard
    BEFORE UPDATE OR DELETE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION wager_transactions_guard();

CREATE TYPE ledger_direction AS ENUM ('DEBIT', 'CREDIT');

CREATE TABLE wallet_ledger_entries (
    id                   UUID PRIMARY KEY,
    sequence             BIGSERIAL        NOT NULL UNIQUE, -- stable cursor ordering
    wallet_id            UUID             NOT NULL REFERENCES wallets (id),
    transaction_id       UUID             NOT NULL REFERENCES wager_transactions (id),
    direction            ledger_direction NOT NULL,
    amount_minor         BIGINT           NOT NULL CHECK (amount_minor > 0),
    currency             CHAR(3)          NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_before_minor BIGINT           NOT NULL CHECK (balance_before_minor >= 0),
    balance_after_minor  BIGINT           NOT NULL CHECK (balance_after_minor >= 0),
    created_at           TIMESTAMPTZ      NOT NULL,
    CONSTRAINT wallet_ledger_entries_wallet_transaction_unique UNIQUE (wallet_id, transaction_id),
    CONSTRAINT wallet_ledger_entries_arithmetic CHECK (
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor) OR
        (direction = 'DEBIT'  AND balance_after_minor = balance_before_minor - amount_minor)
    )
);
CREATE INDEX wallet_ledger_entries_wallet_seq ON wallet_ledger_entries (wallet_id, sequence);

CREATE FUNCTION wallet_ledger_entries_guard() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append-only (% not allowed)', TG_OP USING ERRCODE = 'restrict_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER wallet_ledger_entries_guard
    BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION wallet_ledger_entries_guard();

CREATE TABLE inbox_messages (
    consumer_name TEXT        NOT NULL,
    message_id    TEXT        NOT NULL,
    payload_hash  TEXT        NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL,
    completed_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (consumer_name, message_id)
);

CREATE TABLE outbox_events (
    id              UUID PRIMARY KEY,               -- eventId, stable across republications
    sequence        BIGSERIAL   NOT NULL UNIQUE,
    aggregate_type  TEXT        NOT NULL,
    aggregate_id    UUID        NOT NULL,
    event_type      TEXT        NOT NULL,
    event_version   INT         NOT NULL,
    correlation_id  TEXT        NOT NULL DEFAULT '',
    causation_id    TEXT,
    payload         JSONB       NOT NULL,            -- immutable snapshot of the envelope
    occurred_at     TIMESTAMPTZ NOT NULL,
    attempts        INT         NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    locked_by       TEXT,
    locked_until    TIMESTAMPTZ,
    published_at    TIMESTAMPTZ,
    last_error      TEXT
);
CREATE INDEX outbox_events_pending
    ON outbox_events (next_attempt_at, sequence) WHERE published_at IS NULL;

CREATE FUNCTION outbox_events_guard() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RETURN OLD; -- retention jobs may purge published rows
    END IF;
    IF NEW.payload <> OLD.payload OR NEW.event_type <> OLD.event_type OR NEW.occurred_at <> OLD.occurred_at THEN
        RAISE EXCEPTION 'outbox payload is immutable' USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outbox_events_guard
    BEFORE UPDATE OR DELETE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION outbox_events_guard();
