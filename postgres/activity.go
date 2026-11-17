package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	s "github.com/StuHelper/stuhelper-search"
)

func putActivity(ctx context.Context, tx *sql.Tx, d s.ActivitySnapshot, equal bool) error {
	b, e := json.Marshal(d)
	if e != nil {
		return e
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO search.activities(source,id,kind,object_id,day,revision,data) VALUES($1,$2,$3,$4,$5,$6,$7)
 ON CONFLICT(source,id) DO UPDATE SET revision=EXCLUDED.revision,data=EXCLUDED.data
 WHERE search.activities.revision<EXCLUDED.revision OR ($8 AND search.activities.revision=EXCLUDED.revision)`, d.Source, d.ID, d.Target.Kind, d.Target.ID, d.Day, d.Revision, b, equal)
	return e
}
func (r *Repository) ApplyActivity(ctx context.Context, event s.ActivityEvent, digest string) error {
	return r.withInboxTx(ctx, event.Source, event.ID, digest, func(tx *sql.Tx) error {
		return putActivity(ctx, tx, event.Data, false)
	})
}
func (r *Repository) Activities(ctx context.Context, f s.ActivityFilter) ([]s.ActivitySnapshot, error) {
	if f.Limit < 1 || f.Limit > 100 {
		return nil, s.ErrInput
	}
	var ready bool
	if e := r.DB.QueryRowContext(ctx, "SELECT ready FROM search.state WHERE id").Scan(&ready); e != nil || !ready {
		return nil, s.ErrUnavailable
	}
	rows, e := r.DB.QueryContext(ctx, `SELECT data FROM search.activities WHERE kind=$1 AND day>=$2 AND day<$3 AND source||'/'||id>$4 AND (data->>'visible')::boolean ORDER BY source||'/'||id LIMIT $5`, f.Kind, f.From, f.To, f.After, f.Limit)
	if e != nil {
		return nil, safe(e)
	}
	defer rows.Close()
	out := []s.ActivitySnapshot{}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			return nil, safe(e)
		}
		var d s.ActivitySnapshot
		if e = json.Unmarshal(b, &d); e != nil {
			return nil, s.ErrUnavailable
		}
		out = append(out, d)
	}
	return out, safe(rows.Err())
}
