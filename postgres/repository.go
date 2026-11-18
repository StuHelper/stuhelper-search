package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	s "github.com/StuHelper/stuhelper-search"
	_ "github.com/jackc/pgx/v5/stdlib"
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
	normQ, terms := s.NormalizeQuery(f.Query)
	if terms == nil {
		terms = []string{}
	}
	afterBucket, afterKindRank, afterSource, afterID, hasCursor, e := s.ParseSearchAfter(f.After, normQ != "")
	if e != nil {
		return nil, e
	}
	// Parameterized substring search gives literal semantics for %, _ and backslashes.
	rows, e := r.DB.QueryContext(ctx, `WITH base AS (
  SELECT
    source,
    id,
    data,
    data->'target'->>'kind' AS kind,
    normalize(COALESCE(data->>'code', ''), NFKC) AS norm_code,
    lower(normalize(COALESCE(data->>'title', ''), NFKC) COLLATE "pg_c_utf8") AS l_title,
    lower(normalize(COALESCE(data->>'code', ''), NFKC) COLLATE "pg_c_utf8") AS l_code,
    lower(normalize(COALESCE(data->>'english_name', ''), NFKC) COLLATE "pg_c_utf8") AS l_eng,
    lower($1 COLLATE "pg_c_utf8") AS l_q
  FROM search.documents
  WHERE (data->>'visible')::boolean
    AND ($3 = '' OR data->'target'->>'kind' = $3)
),
matched AS (
  SELECT
    source,
    id,
    data,
    CASE
      WHEN $1 = '' THEN 0
      WHEN (kind = 'school' AND $1 ~ '^[0-9]+$' AND (
        id = $1 OR norm_code = $1
        OR (length($1) = 5 AND ((id ~ '^[0-9]{10}$' AND right(id, 5) = $1) OR (norm_code ~ '^[0-9]{10}$' AND right(norm_code, 5) = $1)))
      ))
      OR l_title = l_q
      OR (kind IN ('school', 'course') AND l_code <> '' AND l_code = l_q)
      OR (kind = 'course' AND (
        (l_eng <> '' AND l_eng = l_q)
        OR EXISTS (
          SELECT 1 FROM jsonb_array_elements_text(COALESCE(NULLIF(data->'former_codes', 'null'::jsonb), '[]'::jsonb)) fc
          WHERE lower(normalize(fc, NFKC) COLLATE "pg_c_utf8") = l_q
        )
      )) THEN 0
      WHEN position(l_q in l_title) = 1
        OR (kind IN ('school', 'course') AND l_code <> '' AND position(l_q in l_code) = 1)
        OR (kind = 'course' AND l_eng <> '' AND position(l_q in l_eng) = 1) THEN 1
      WHEN NOT EXISTS (
        SELECT 1 FROM unnest($2::text[]) AS t
        WHERE position(lower(t COLLATE "pg_c_utf8") in l_title) = 0
      )
      OR (kind = 'course' AND l_eng <> '' AND NOT EXISTS (
        SELECT 1 FROM unnest($2::text[]) AS t
        WHERE position(lower(t COLLATE "pg_c_utf8") in l_eng) = 0
      )) THEN 2
      ELSE 3
    END AS bucket,
    CASE
      WHEN $1 = '' THEN 0
      WHEN kind = 'school' THEN 1
      WHEN kind = 'course' THEN 2
      WHEN kind = 'teacher' THEN 3
      WHEN kind = 'resource' THEN 4
      WHEN kind = 'review' THEN 5
      ELSE 6
    END AS kind_rank
  FROM base
  WHERE $1 = '' OR NOT EXISTS (
    SELECT 1 FROM unnest($2::text[]) AS t
    WHERE NOT (
      CASE
        WHEN kind = 'school' AND t ~ '^[0-9]+$' THEN
          id = t OR norm_code = t
          OR (length(t) = 5 AND ((id ~ '^[0-9]{10}$' AND right(id, 5) = t) OR (norm_code ~ '^[0-9]{10}$' AND right(norm_code, 5) = t)))
          OR position(t in l_title) > 0
          OR EXISTS (
            SELECT 1 FROM jsonb_array_elements_text(COALESCE(NULLIF(data->'aliases', 'null'::jsonb), '[]'::jsonb)) alias
            WHERE position(t in lower(normalize(alias, NFKC) COLLATE "pg_c_utf8")) > 0
          )
        ELSE
          position(lower(t COLLATE "pg_c_utf8") in l_title) > 0
          OR (kind = 'course' AND position(lower(t COLLATE "pg_c_utf8") in l_code) > 0)
          OR (kind = 'school' AND (l_code = lower(t COLLATE "pg_c_utf8") OR id = t))
          OR (kind IN ('school', 'course', 'teacher') AND EXISTS (
            SELECT 1 FROM jsonb_array_elements_text(COALESCE(NULLIF(data->'aliases', 'null'::jsonb), '[]'::jsonb)) alias
            WHERE position(lower(t COLLATE "pg_c_utf8") in lower(normalize(alias, NFKC) COLLATE "pg_c_utf8")) > 0
          ))
          OR (kind = 'course' AND (
            position(lower(t COLLATE "pg_c_utf8") in l_eng) > 0
            OR EXISTS (
              SELECT 1 FROM jsonb_array_elements_text(COALESCE(NULLIF(data->'former_codes', 'null'::jsonb), '[]'::jsonb) || COALESCE(NULLIF(data->'former_names', 'null'::jsonb), '[]'::jsonb)) val
              WHERE position(lower(t COLLATE "pg_c_utf8") in lower(normalize(val, NFKC) COLLATE "pg_c_utf8")) > 0
            )
          ))
      END
    )
  )
)
SELECT data FROM matched
WHERE (NOT $4 OR (bucket, kind_rank, source COLLATE "C", id COLLATE "C") > ($5, $6, $7 COLLATE "C", $8 COLLATE "C"))
ORDER BY bucket ASC, kind_rank ASC, source COLLATE "C" ASC, id COLLATE "C" ASC
LIMIT $9`, normQ, terms, f.Kind, hasCursor, afterBucket, afterKindRank, afterSource, afterID, f.Limit)
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
	if _, e = tx.ExecContext(ctx, "UPDATE search.state SET ready=true WHERE id"); e != nil {
		return safe(e)
	}
	return safe(tx.Commit())
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
