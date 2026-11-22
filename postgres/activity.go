package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	s "github.com/StuHelper/stuhelper-search"
	"github.com/StuHelper/stuhelper-search/internal/activityday"
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
	return r.withInboxTx(ctx, event.Source, event.Type, event.ID, digest, event, func(tx *sql.Tx) error {
		if event.Type == s.LegacyActivityEventType {
			// A legacy UTC snapshot may share a key with a different Beijing day bucket.
			return nil
		}
		return putActivity(ctx, tx, event.Data, false)
	})
}
func (r *Repository) Activities(ctx context.Context, f s.ActivityFilter) ([]s.ActivitySnapshot, error) {
	afterSource, afterID, ok := splitAfter(f.After)
	if !ok || f.Limit < 1 || f.Limit > 500 || !validDBString(f.Kind) || !validDBString(f.After) {
		return nil, s.ErrInput
	}
	var ready bool
	if e := r.DB.QueryRowContext(ctx, "SELECT ready FROM search.state WHERE id").Scan(&ready); e != nil {
		return nil, safe(e)
	}
	if !ready {
		return nil, s.ErrIndexBuilding
	}
	var rows *sql.Rows
	var e error
	excluded := f.ExcludeSources
	if excluded == nil {
		excluded = []string{}
	}
	firstDay, afterLastDay := activityday.Date(f.From), activityday.Date(f.To)
	if f.AfterDay == "" {
		rows, e = r.DB.QueryContext(ctx, `SELECT data FROM search.activities WHERE kind=$1 AND day>=$2::date AND day<$3::date AND (source COLLATE "C", id COLLATE "C") > ($4 COLLATE "C", $5 COLLATE "C") AND (data->>'visible')::boolean AND NOT (source=ANY($7::text[])) ORDER BY source COLLATE "C", id COLLATE "C" LIMIT $6`, f.Kind, firstDay, afterLastDay, afterSource, afterID, f.Limit, excluded)
	} else {
		day, err := time.Parse(time.DateOnly, f.AfterDay)
		if err != nil || day.Format(time.DateOnly) != f.AfterDay || f.AfterDay < firstDay || f.AfterDay >= afterLastDay {
			return nil, s.ErrInput
		}
		rows, e = r.DB.QueryContext(ctx, `SELECT data FROM search.activities
 WHERE kind=$1 AND day >= $2::date AND day < $3::date AND (data->>'visible')::boolean
 AND (day,source COLLATE "C",id COLLATE "C") > ($4::date,$5 COLLATE "C",$6 COLLATE "C")
 AND NOT (source=ANY($8::text[])) ORDER BY day,source COLLATE "C",id COLLATE "C" LIMIT $7`, f.Kind, firstDay, afterLastDay, f.AfterDay, afterSource, afterID, f.Limit, excluded)
	}
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
