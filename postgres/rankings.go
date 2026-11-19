package postgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	s "github.com/StuHelper/stuhelper-search"
	"time"
)

func (r *Repository) SaveRanking(ctx context.Context, targets []s.Target, now time.Time, ttl time.Duration) (string, error) {
	data, err := json.Marshal(targets)
	if err != nil {
		return "", s.ErrUnavailable
	}
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return "", s.ErrUnavailable
	}
	token := hex.EncodeToString(nonce[:])
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return "", safe(err)
	}
	defer tx.Rollback()
	// Serialize slot allocation, independently of projection updates. An active slot is never evicted.
	if _, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(726391840132)"); err != nil {
		return "", safe(err)
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO search.rankings(slot,token,targets,expires_at)
 SELECT n,$1,$2,$3 FROM generate_series(1,1024) n LEFT JOIN search.rankings r ON r.slot=n
 WHERE r.slot IS NULL OR r.expires_at <= $4 ORDER BY n LIMIT 1
 ON CONFLICT(slot) DO UPDATE SET token=EXCLUDED.token,targets=EXCLUDED.targets,expires_at=EXCLUDED.expires_at WHERE search.rankings.expires_at <= $4`, token, data, now.Add(ttl), now)
	if err != nil {
		return "", safe(err)
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return "", s.ErrUnavailable
	}
	return token, safe(tx.Commit())
}
func (r *Repository) ResumeRanking(ctx context.Context, token string, now time.Time, ttl time.Duration) ([]s.Target, error) {
	var data []byte
	err := r.DB.QueryRowContext(ctx, `UPDATE search.rankings SET expires_at=greatest(expires_at,$1) WHERE token=$2 AND expires_at>$3 RETURNING targets`, now.Add(ttl), token, now).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, s.ErrSnapshotExpired
	}
	if err != nil {
		return nil, safe(err)
	}
	var targets []s.Target
	if err = json.Unmarshal(data, &targets); err != nil {
		return nil, s.ErrUnavailable
	}
	return targets, nil
}
