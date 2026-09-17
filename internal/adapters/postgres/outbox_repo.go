package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/erikyryan/desafio-go/internal/app"
	"github.com/erikyryan/desafio-go/internal/domain/wager"
)

type outboxRepo struct{ tx pgx.Tx }

// envelope is the published JSON form of an event.
type envelope struct {
	EventID       uuid.UUID `json:"eventId"`
	EventType     string    `json:"eventType"`
	AggregateType string    `json:"aggregateType"`
	AggregateID   uuid.UUID `json:"aggregateId"`
	CorrelationID string    `json:"correlationId"`
	CausationID   string    `json:"causationId,omitempty"`
	OccurredAt    time.Time `json:"occurredAt"`
	Version       int       `json:"version"`
	Data          any       `json:"data"`
}

// EncodeEnvelope renders the immutable payload stored in the outbox.
func EncodeEnvelope(ev wager.Event) ([]byte, error) {
	return json.Marshal(envelope{
		EventID: ev.EventID, EventType: ev.EventType, AggregateType: ev.AggregateType, AggregateID: ev.AggregateID,
		CorrelationID: ev.CorrelationID, CausationID: ev.CausationID, OccurredAt: ev.OccurredAt.UTC(), Version: ev.Version, Data: ev.Data,
	})
}

func (r *outboxRepo) Insert(ctx context.Context, events ...wager.Event) error {
	for _, ev := range events {
		payload, err := EncodeEnvelope(ev)
		if err != nil {
			return fmt.Errorf("encode event %s: %w", ev.EventType, err)
		}
		_, err = r.tx.Exec(ctx, `INSERT INTO outbox_events
			(id, aggregate_type, aggregate_id, event_type, event_version, correlation_id, causation_id, payload, occurred_at, next_attempt_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9)`,
			ev.EventID, ev.AggregateType, ev.AggregateID, ev.EventType, ev.Version, ev.CorrelationID, nullString(ev.CausationID), payload, ev.OccurredAt.UTC())
		if err != nil {
			return translate(err)
		}
	}
	return nil
}

// Claim leases due rows with FOR UPDATE SKIP LOCKED so that concurrent
// publishers never pick the same row; expired leases are reclaimed.
func (r *outboxRepo) Claim(ctx context.Context, owner string, lease time.Duration, limit int, now time.Time) ([]app.OutboxRecord, error) {
	// UPDATE ... RETURNING has no ordering guarantee, so the CTE re-sorts by
	// sequence to publish in commit order.
	rows, err := r.tx.Query(ctx, `WITH claimed AS (
			UPDATE outbox_events SET locked_by = $1, locked_until = $2
			WHERE id IN (
				SELECT id FROM outbox_events
				WHERE published_at IS NULL AND next_attempt_at <= $3 AND (locked_until IS NULL OR locked_until < $3)
				ORDER BY sequence LIMIT $4 FOR UPDATE SKIP LOCKED
			)
			RETURNING id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, attempts, sequence
		)
		SELECT id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, attempts FROM claimed ORDER BY sequence`,
		owner, now.Add(lease), now, limit)
	if err != nil {
		return nil, translate(err)
	}
	defer rows.Close()
	var out []app.OutboxRecord
	for rows.Next() {
		var rec app.OutboxRecord
		if err := rows.Scan(&rec.EventID, &rec.EventType, &rec.AggregateType, &rec.AggregateID, &rec.CorrelationID, &rec.Payload, &rec.OccurredAt, &rec.Attempts); err != nil {
			return nil, translate(err)
		}
		out = append(out, rec)
	}
	return out, translate(rows.Err())
}

func (r *outboxRepo) MarkPublished(ctx context.Context, eventID uuid.UUID, now time.Time) error {
	_, err := r.tx.Exec(ctx, `UPDATE outbox_events SET published_at = $2, locked_by = NULL, locked_until = NULL, attempts = attempts + 1
		WHERE id = $1 AND published_at IS NULL`, eventID, now)
	return translate(err)
}

func (r *outboxRepo) Reschedule(ctx context.Context, eventID uuid.UUID, nextAttemptAt time.Time, lastError string) error {
	_, err := r.tx.Exec(ctx, `UPDATE outbox_events SET attempts = attempts + 1, next_attempt_at = $2, last_error = $3,
		locked_by = NULL, locked_until = NULL WHERE id = $1 AND published_at IS NULL`, eventID, nextAttemptAt, lastError)
	return translate(err)
}

func (r *outboxRepo) OldestPendingAge(ctx context.Context, now time.Time) (time.Duration, error) {
	var oldest *time.Time
	err := r.tx.QueryRow(ctx, `SELECT MIN(occurred_at) FROM outbox_events WHERE published_at IS NULL`).Scan(&oldest)
	if err != nil {
		return 0, translate(err)
	}
	if oldest == nil {
		return 0, nil
	}
	return now.Sub(*oldest), nil
}
