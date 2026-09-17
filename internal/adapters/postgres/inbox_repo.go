package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

type inboxRepo struct{ tx pgx.Tx }

// Record inserts the inbox row; the row becomes visible only when the
// surrounding transaction (domain changes included) commits.
func (r *inboxRepo) Record(ctx context.Context, consumer, messageID, payloadHash string, now time.Time) (bool, string, error) {
	tag, err := r.tx.Exec(ctx, `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at, completed_at)
		VALUES ($1,$2,$3,$4,$4) ON CONFLICT (consumer_name, message_id) DO NOTHING`, consumer, messageID, payloadHash, now)
	if err != nil {
		return false, "", translate(err)
	}
	if tag.RowsAffected() == 1 {
		return true, payloadHash, nil
	}
	var stored string
	err = r.tx.QueryRow(ctx, `SELECT payload_hash FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID).Scan(&stored)
	if err != nil {
		return false, "", translate(err)
	}
	return false, stored, nil
}
