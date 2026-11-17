package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	s "github.com/StuHelper/stuhelper-search"
	_ "github.com/jackc/pgx/v5/stdlib"
	"time"
)

//go:embed schema.sql
var Schema string

type Repository struct{ DB *sql.DB }

func safe(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, s.ErrConflict) {
		return err
	}
	return s.ErrUnavailable
}
func Migrate(ctx context.Context, db *sql.DB) error {
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if _, e = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(726391840129)"); e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, Schema); e != nil {
		return e
	}
	return tx.Commit()
}

// Every writer takes this lock. Rebuild is atomic and relay deliveries wait until publication.
func lock(ctx context.Context, tx *sql.Tx) error {
	_, e := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(726391840130)")
	return e
}
func put(ctx context.Context, tx *sql.Tx, d s.Document, equal bool) error {
	b, e := json.Marshal(d)
	if e != nil {
		return e
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO search.documents(source,id,revision,data) VALUES($1,$2,$3,$4)
 ON CONFLICT(source,id) DO UPDATE SET revision=EXCLUDED.revision,data=EXCLUDED.data
 WHERE search.documents.revision < EXCLUDED.revision OR ($5 AND search.documents.revision=EXCLUDED.revision)`, d.Target.Source, d.Target.ID, d.Revision, b, equal)
	return e
}
func (r *Repository) Apply(ctx context.Context, event s.Event, digest string) error {
	tx, e := r.DB.BeginTx(ctx, nil)
	if e != nil {
		return safe(e)
	}
	defer tx.Rollback()
	if e = lock(ctx, tx); e != nil {
		return safe(e)
	}
	var old string
	e = tx.QueryRowContext(ctx, "SELECT digest FROM search.inbox WHERE source=$1 AND id=$2", event.Source, event.ID).Scan(&old)
	if e == nil {
		if old != digest {
			return s.ErrConflict
		}
		return nil
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return safe(e)
	}
	if e = put(ctx, tx, event.Data, false); e != nil {
		return safe(e)
	}
	if _, e = tx.ExecContext(ctx, "INSERT INTO search.inbox(source,id,digest) VALUES($1,$2,$3)", event.Source, event.ID, digest); e != nil {
		return safe(e)
	}
	return safe(tx.Commit())
}
func (r *Repository) Candidates(ctx context.Context, f s.Filter) ([]s.Document, error) {
	var ready bool
	if e := r.DB.QueryRowContext(ctx, "SELECT ready FROM search.state WHERE id").Scan(&ready); e != nil || !ready {
		return nil, s.ErrUnavailable
	}
	if f.Limit < 1 || f.Limit > 100 {
		return nil, s.ErrInput
	}
	// Parameterized substring search gives literal semantics for %, _ and backslashes.
	rows, e := r.DB.QueryContext(ctx, `SELECT data FROM search.documents WHERE (data->>'visible')::boolean
 AND source||'/'||id > $1 AND ($2='' OR CASE WHEN data->'target'->>'kind'='school' AND $2 ~ '^[0-9]+$' THEN
 id=$2 OR data->>'code'=$2
 OR (length($2)=5 AND ((id ~ '^[0-9]{10}$' AND right(id,5)=$2) OR (data->>'code' ~ '^[0-9]{10}$' AND right(data->>'code',5)=$2)))
 ELSE position(lower($2) in lower(data->>'title'))>0
 OR (data->'target'->>'kind' IN ('school','course') AND (position(lower($2) in lower(data->>'code'))>0 OR EXISTS(SELECT 1 FROM jsonb_array_elements_text(COALESCE(NULLIF(data->'aliases','null'::jsonb),'[]'::jsonb)) alias WHERE position(lower($2) in lower(alias))>0)))
 OR (data->'target'->>'kind'='course' AND (position(lower($2) in lower(data->>'english_name'))>0 OR EXISTS(SELECT 1 FROM jsonb_array_elements_text(COALESCE(NULLIF(data->'former_codes','null'::jsonb),'[]'::jsonb)||COALESCE(NULLIF(data->'former_names','null'::jsonb),'[]'::jsonb)) value WHERE position(lower($2) in lower(value))>0))) END)
 AND ($3='' OR data->'target'->>'kind'=$3)
 ORDER BY source||'/'||id LIMIT $4`, f.After, f.Query, f.Kind, f.Limit)
	if e != nil {
		return nil, safe(e)
	}
	defer rows.Close()
	out := []s.Document{}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			return nil, safe(e)
		}
		var d s.Document
		if e = json.Unmarshal(b, &d); e != nil {
			return nil, s.ErrUnavailable
		}
		out = append(out, d)
	}
	return out, safe(rows.Err())
}
func (r *Repository) Rebuild(ctx context.Context, load func(func([]s.Document) error, func([]s.ActivitySnapshot) error) error) error {
	return r.RebuildWindow(ctx, time.Time{}, time.Time{}, time.Time{}, load)
}

func (r *Repository) RebuildWindow(ctx context.Context, rebuiltAt, from, to time.Time, load func(func([]s.Document) error, func([]s.ActivitySnapshot) error) error) error {
	tx, e := r.DB.BeginTx(ctx, nil)
	if e != nil {
		return safe(e)
	}
	defer tx.Rollback()
	if e = lock(ctx, tx); e != nil {
		return safe(e)
	}
	// Preserve revision tombstones for objects absent from the snapshot, so late events cannot resurrect them.
	if _, e = tx.ExecContext(ctx, `UPDATE search.documents SET data=jsonb_set(data,'{visible}','false')`); e != nil {
		return safe(e)
	}
	if _, e = tx.ExecContext(ctx, `UPDATE search.activities SET data=jsonb_set(data,'{visible}','false')`); e != nil {
		return safe(e)
	}
	e = load(func(docs []s.Document) error {
		for _, d := range docs {
			var rev int64
			e := tx.QueryRowContext(ctx, "SELECT revision FROM search.documents WHERE source=$1 AND id=$2", d.Target.Source, d.Target.ID).Scan(&rev)
			if e != nil && !errors.Is(e, sql.ErrNoRows) {
				return e
			}
			if rev > d.Revision {
				return s.ErrConflict
			}
			if e = put(ctx, tx, d, true); e != nil {
				return e
			}
		}
		return nil
	}, func(docs []s.ActivitySnapshot) error {
		for _, d := range docs {
			var rev int64
			err := tx.QueryRowContext(ctx, "SELECT revision FROM search.activities WHERE source=$1 AND id=$2", d.Source, d.ID).Scan(&rev)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if rev > d.Revision {
				return s.ErrConflict
			}
			if err = putActivity(ctx, tx, d, true); err != nil {
				return err
			}
		}
		return nil
	})
	if e != nil {
		return safe(e)
	}
	if rebuiltAt.IsZero() {
		_, e = tx.ExecContext(ctx, "UPDATE search.state SET ready=true, rebuilt_at=now() WHERE id")
	} else {
		_, e = tx.ExecContext(ctx, "UPDATE search.state SET ready=true, rebuilt_at=$1, activity_from=$2, activity_to=$3 WHERE id", rebuiltAt.UTC(), from.UTC().Format(time.DateOnly), to.UTC().Format(time.DateOnly))
	}
	if e != nil {
		return safe(e)
	}
	return safe(tx.Commit())
}

func (r *Repository) Status(ctx context.Context) (s.Status, error) {
	out := s.Status{
		Documents:  map[string]s.SourceCounts{},
		Activities: map[string]s.SourceCounts{},
	}
	var rebuiltAt sql.NullTime
	var actFrom, actTo sql.NullString
	e := r.DB.QueryRowContext(ctx, `SELECT ready, rebuilt_at, activity_from::text, activity_to::text FROM search.state WHERE id`).Scan(&out.Ready, &rebuiltAt, &actFrom, &actTo)
	if e != nil {
		return s.Status{}, safe(e)
	}
	if rebuiltAt.Valid {
		t := rebuiltAt.Time.UTC()
		out.RebuiltAt = &t
	}
	if actFrom.Valid {
		out.ActivityWindowFrom = actFrom.String
	}
	if actTo.Valid {
		out.ActivityWindowTo = actTo.String
	}
	docRows, e := r.DB.QueryContext(ctx, `SELECT source, count(*), count(*) FILTER (WHERE (data->>'visible')::boolean) FROM search.documents GROUP BY source`)
	if e != nil {
		return s.Status{}, safe(e)
	}
	defer docRows.Close()
	for docRows.Next() {
		var src string
		var c s.SourceCounts
		if e = docRows.Scan(&src, &c.Total, &c.Visible); e != nil {
			return s.Status{}, safe(e)
		}
		out.Documents[src] = c
	}
	if e = docRows.Err(); e != nil {
		return s.Status{}, safe(e)
	}
	actRows, e := r.DB.QueryContext(ctx, `SELECT source, count(*), count(*) FILTER (WHERE (data->>'visible')::boolean) FROM search.activities GROUP BY source`)
	if e != nil {
		return s.Status{}, safe(e)
	}
	defer actRows.Close()
	for actRows.Next() {
		var src string
		var c s.SourceCounts
		if e = actRows.Scan(&src, &c.Total, &c.Visible); e != nil {
			return s.Status{}, safe(e)
		}
		out.Activities[src] = c
	}
	if e = actRows.Err(); e != nil {
		return s.Status{}, safe(e)
	}
	if e = r.DB.QueryRowContext(ctx, `SELECT count(*) FROM search.inbox`).Scan(&out.InboxCount); e != nil {
		return s.Status{}, safe(e)
	}
	return out, nil
}
func (r *Repository) CheckRuntime(ctx context.Context) error {
	var unsafe bool
	e := r.DB.QueryRowContext(ctx, `SELECT rolsuper OR rolcreatedb OR rolcreaterole OR has_schema_privilege(current_user,'search','CREATE') FROM pg_roles WHERE rolname=current_user`).Scan(&unsafe)
	if e != nil || unsafe {
		return s.ErrUnavailable
	}
	for _, table := range []string{"documents", "activities", "inbox", "state"} {
		var ok bool
		e = r.DB.QueryRowContext(ctx, "SELECT has_table_privilege(current_user,$1,'SELECT') AND has_table_privilege(current_user,$1,'INSERT')", "search."+table).Scan(&ok)
		if e != nil || !ok {
			return s.ErrUnavailable
		}
	}
	var forbidden bool
	e = r.DB.QueryRowContext(ctx, "SELECT has_table_privilege(current_user,'search.inbox','UPDATE,DELETE')").Scan(&forbidden)
	if e != nil || forbidden {
		return s.ErrUnavailable
	}
	return nil
}
