package postgres

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	s "github.com/StuHelper/stuhelper-search"
	_ "github.com/jackc/pgx/v5/stdlib"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

func validDBString(v string) bool {
	return utf8.ValidString(v) && !strings.ContainsRune(v, 0)
}

//go:embed schema.sql
var Schema string

type Repository struct {
	DB        *sql.DB
	mu        sync.Mutex
	cursorKey []byte
}

func splitAfter(after string) (string, string, bool) {
	if after == "" {
		return "", "", true
	}
	src, id, ok := strings.Cut(after, "/")
	if !ok || src == "" || id == "" {
		return "", "", false
	}
	return src, id, true
}

func (r *Repository) CursorKey(ctx context.Context) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.cursorKey) == 32 {
		return r.cursorKey, nil
	}
	var key []byte
	if err := r.DB.QueryRowContext(ctx, "SELECT cursor_key FROM search.state WHERE id").Scan(&key); err != nil {
		return nil, safe(err)
	}
	if len(key) != 32 {
		return nil, s.ErrUnavailable
	}
	r.cursorKey = append([]byte(nil), key...)
	return r.cursorKey, nil
}

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
	key := make([]byte, 32)
	if _, e = rand.Read(key); e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, "UPDATE search.state SET cursor_key=$1 WHERE id AND cursor_key IS NULL", key); e != nil {
		return e
	}
	return tx.Commit()
}

// Rebuild takes the exclusive advisory lock; daily event consumption uses the shared advisory lock.
func lock(ctx context.Context, tx *sql.Tx) error {
	_, e := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(726391840130)")
	return e
}
func lockShared(ctx context.Context, tx *sql.Tx) error {
	_, e := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock_shared(726391840130)")
	return e
}
func (r *Repository) withInboxTx(ctx context.Context, src, eventType, id, digest string, original any, apply func(*sql.Tx) error) error {
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return safe(err)
	}
	defer tx.Rollback()
	if err = lockShared(ctx, tx); err != nil {
		return safe(err)
	}
	if seen, err := replayed(ctx, tx, src, eventType, id, digest, original); seen || err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO search.inbox(source,event_type,id,digest) VALUES($1,$2,$3,$4)
 ON CONFLICT(source,event_type,id) DO NOTHING`, src, eventType, id, digest)
	if err != nil {
		return safe(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return safe(err)
	}
	if n == 0 {
		seen, err := replayed(ctx, tx, src, eventType, id, digest, original)
		if err != nil || seen {
			return err
		}
		return s.ErrUnavailable
	}
	if err = apply(tx); err != nil {
		return safe(err)
	}
	return safe(tx.Commit())
}
func put(ctx context.Context, tx *sql.Tx, d s.Document, equal bool) error {
	b, e := json.Marshal(d)
	if e != nil {
		return e
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO search.documents(source,id,revision,at_utc,data) VALUES($1,$2,$3,$4,$5)
 ON CONFLICT(source,id) DO UPDATE SET revision=EXCLUDED.revision,at_utc=EXCLUDED.at_utc,data=EXCLUDED.data
 WHERE search.documents.revision < EXCLUDED.revision OR ($6 AND search.documents.revision=EXCLUDED.revision)`, d.Target.Source, d.Target.ID, d.Revision, d.At.UTC(), b, equal)
	return e
}

// Before event types were stored, one receipt covered every event with the same
// source and ID. Its digest was computed over the original, unnormalized event.
func replayed(ctx context.Context, tx *sql.Tx, source, eventType, id, digest string, original any) (bool, error) {
	var storedType, storedDigest string
	err := tx.QueryRowContext(ctx, `SELECT event_type, digest FROM search.inbox
 WHERE source=$1 AND id=$2 AND event_type IN ('',$3)
 ORDER BY event_type LIMIT 1`, source, id, eventType).Scan(&storedType, &storedDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, safe(err)
	}
	if storedType == "" {
		raw, err := json.Marshal(original)
		if err != nil {
			return false, safe(err)
		}
		sum := sha256.Sum256(raw)
		digest = hex.EncodeToString(sum[:])
	}
	if storedDigest != digest {
		return false, s.ErrConflict
	}
	return true, nil
}
func (r *Repository) Apply(ctx context.Context, event s.Event, digest string) error {
	return r.withInboxTx(ctx, event.Source, event.Type, event.ID, digest, event, func(tx *sql.Tx) error {
		return put(ctx, tx, event.Data, false)
	})
}
func (r *Repository) Candidates(ctx context.Context, f s.Filter) ([]s.Document, error) {
	var ready bool
	if e := r.DB.QueryRowContext(ctx, "SELECT ready FROM search.state WHERE id").Scan(&ready); e != nil {
		return nil, safe(e)
	}
	if !ready {
		return nil, s.ErrIndexBuilding
	}
	if (f.Sort != "" && f.Sort != "relevance" && f.Sort != "recent") || f.Limit < 1 || f.Limit > 100 || !validDBString(f.Query) || !validDBString(f.After) || !validDBString(f.Kind) || !validDBString(f.SchoolID) || !validDBString(f.CourseID) || !validDBString(f.TeacherID) {
		return nil, s.ErrInput
	}
	normQ, terms := s.NormalizeQuery(f.Query)
	if terms == nil {
		terms = []string{}
	}
	var afterBucket, afterKindRank int
	var afterSource, afterID string
	var afterAt time.Time
	var hasCursor bool
	var e error
	if f.Sort == "recent" {
		afterAt, afterSource, afterID, hasCursor, e = s.ParseRecentAfter(f.After)
	} else {
		afterBucket, afterKindRank, afterSource, afterID, hasCursor, e = s.ParseSearchAfter(f.After, normQ != "")
	}
	if e != nil {
		return nil, e
	}
	// Parameterized substring search gives literal semantics for %, _ and backslashes.
	var rows *sql.Rows
	if normQ == "" && f.Sort == "recent" {
		rows, e = r.DB.QueryContext(ctx, `SELECT data || jsonb_build_object('at',at_utc) FROM search.documents
 WHERE (data->>'visible')::boolean AND at_utc IS NOT NULL
 AND (NOT $1 OR at_utc < $2 OR (at_utc=$2 AND (source COLLATE "C",id COLLATE "C")>($3 COLLATE "C",$4 COLLATE "C")))
 AND ($5='' OR data->'target'->>'kind'=$5) AND ($6='' OR data->>'school_id'=$6)
 AND ($7='' OR data->>'course_id'=$7) AND ($8='' OR jsonb_exists(COALESCE(NULLIF(data->'teacher_ids','null'::jsonb),'[]'::jsonb),$8))
 ORDER BY at_utc DESC,source COLLATE "C",id COLLATE "C" LIMIT $9`, hasCursor, afterAt, afterSource, afterID, f.Kind, f.SchoolID, f.CourseID, f.TeacherID, f.Limit)
	} else if normQ == "" {
		rows, e = r.DB.QueryContext(ctx, `SELECT data FROM search.documents
 WHERE (data->>'visible')::boolean AND (source COLLATE "C",id COLLATE "C") > ($1 COLLATE "C",$2 COLLATE "C")
 AND ($3='' OR data->'target'->>'kind'=$3) AND ($4='' OR data->>'school_id'=$4)
 AND ($5='' OR data->>'course_id'=$5) AND ($6='' OR jsonb_exists(COALESCE(NULLIF(data->'teacher_ids','null'::jsonb),'[]'::jsonb),$6))
 ORDER BY source COLLATE "C",id COLLATE "C" LIMIT $7`, afterSource, afterID, f.Kind, f.SchoolID, f.CourseID, f.TeacherID, f.Limit)
	} else {
		const matching = `WITH base AS (
  SELECT
    source,
    id,
    data, at_utc,
    data->'target'->>'kind' AS kind,
    normalize(COALESCE(data->>'code', ''), NFKC) AS norm_code,
    lower(normalize(COALESCE(data->>'title', ''), NFKC) COLLATE "pg_c_utf8") AS l_title,
    lower(normalize(COALESCE(data->>'code', ''), NFKC) COLLATE "pg_c_utf8") AS l_code,
    lower(normalize(COALESCE(data->>'english_name', ''), NFKC) COLLATE "pg_c_utf8") AS l_eng,
    lower($1 COLLATE "pg_c_utf8") AS l_q
  FROM search.documents
  WHERE (data->>'visible')::boolean
    AND ($3 = '' OR data->'target'->>'kind' = $3)
    AND ($4='' OR data->>'school_id'=$4)
    AND ($5='' OR data->>'course_id'=$5)
    AND ($6='' OR jsonb_exists(COALESCE(NULLIF(data->'teacher_ids','null'::jsonb),'[]'::jsonb),$6))
),
matched AS (
  SELECT
    source,
    id,
    data, at_utc,
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
`
		args := []any{normQ, terms, f.Kind, f.SchoolID, f.CourseID, f.TeacherID, hasCursor}
		if f.Sort == "recent" {
			rows, e = r.DB.QueryContext(ctx, matching+`SELECT data || jsonb_build_object('at',at_utc) FROM matched WHERE at_utc IS NOT NULL AND (NOT $7 OR at_utc < $8 OR (at_utc=$8 AND (source COLLATE "C",id COLLATE "C")>($9 COLLATE "C",$10 COLLATE "C"))) ORDER BY at_utc DESC,source COLLATE "C",id COLLATE "C" LIMIT $11`, append(args, afterAt, afterSource, afterID, f.Limit)...)
		} else {
			rows, e = r.DB.QueryContext(ctx, matching+`SELECT data FROM matched
WHERE (NOT $7 OR (bucket, kind_rank, source COLLATE "C", id COLLATE "C") > ($8, $9, $10 COLLATE "C", $11 COLLATE "C"))
ORDER BY bucket ASC, kind_rank ASC, source COLLATE "C" ASC, id COLLATE "C" ASC
LIMIT $12`, append(args, afterBucket, afterKindRank, afterSource, afterID, f.Limit)...)
		}
	}
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
func putDocumentBatch(ctx context.Context, tx *sql.Tx, docs []s.Document) error {
	if len(docs) == 0 {
		return nil
	}
	sources := make([]string, len(docs))
	ids := make([]string, len(docs))
	revisions := make([]int64, len(docs))
	payloads := make([]string, len(docs))
	seen := make(map[string]bool, len(docs))
	for i, d := range docs {
		k := d.Target.Source + "/" + d.Target.ID
		if seen[k] {
			return s.ErrUnavailable
		}
		seen[k] = true
		b, err := json.Marshal(d)
		if err != nil {
			return err
		}
		sources[i] = d.Target.Source
		ids[i] = d.Target.ID
		revisions[i] = d.Revision
		payloads[i] = string(b)
	}
	var stale bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM search.documents cur
		JOIN UNNEST($1::text[], $2::text[], $3::bigint[]) AS inc(source, id, revision)
		  ON cur.source = inc.source AND cur.id = inc.id
		WHERE cur.revision > inc.revision
	)`, sources, ids, revisions).Scan(&stale); err != nil {
		return err
	}
	if stale {
		return s.ErrConflict
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO search.documents(source,id,revision,data,at_utc)
		SELECT src, id, rev, payload::jsonb, (payload::jsonb->>'at')::timestamptz
		FROM UNNEST($1::text[], $2::text[], $3::bigint[], $4::text[]) AS inc(src, id, rev, payload)
		ON CONFLICT (source,id) DO UPDATE SET revision=EXCLUDED.revision, data=EXCLUDED.data, at_utc=EXCLUDED.at_utc
		WHERE search.documents.revision <= EXCLUDED.revision`, sources, ids, revisions, payloads)
	return err
}

func putActivityBatch(ctx context.Context, tx *sql.Tx, docs []s.ActivitySnapshot) error {
	if len(docs) == 0 {
		return nil
	}
	sources := make([]string, len(docs))
	ids := make([]string, len(docs))
	kinds := make([]string, len(docs))
	objectIDs := make([]string, len(docs))
	days := make([]string, len(docs))
	revisions := make([]int64, len(docs))
	payloads := make([]string, len(docs))
	seen := make(map[string]bool, len(docs))
	for i, d := range docs {
		k := d.Source + "/" + d.ID
		if seen[k] {
			return s.ErrUnavailable
		}
		seen[k] = true
		b, err := json.Marshal(d)
		if err != nil {
			return err
		}
		sources[i] = d.Source
		ids[i] = d.ID
		kinds[i] = d.Target.Kind
		objectIDs[i] = d.Target.ID
		days[i] = d.Day
		revisions[i] = d.Revision
		payloads[i] = string(b)
	}
	var stale bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM search.activities cur
		JOIN UNNEST($1::text[], $2::text[], $3::bigint[]) AS inc(source, id, revision)
		  ON cur.source = inc.source AND cur.id = inc.id
		WHERE cur.revision > inc.revision
	)`, sources, ids, revisions).Scan(&stale); err != nil {
		return err
	}
	if stale {
		return s.ErrConflict
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO search.activities(source,id,kind,object_id,day,revision,data)
		SELECT src, id, kind, obj, day::date, rev, payload::jsonb
		FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::bigint[], $7::text[]) AS inc(src, id, kind, obj, day, rev, payload)
		ON CONFLICT (source,id) DO UPDATE SET revision=EXCLUDED.revision, data=EXCLUDED.data
		WHERE search.activities.revision <= EXCLUDED.revision`, sources, ids, kinds, objectIDs, days, revisions, payloads)
	return err
}

func (r *Repository) Rebuild(ctx context.Context, load func(func([]s.Document) error, func([]s.ActivitySnapshot) error) error) error {
	return r.RebuildWindow(ctx, time.Time{}, time.Time{}, load)
}

func (r *Repository) RebuildWindow(ctx context.Context, from, to time.Time, load func(func([]s.Document) error, func([]s.ActivitySnapshot) error) error) error {
	return r.RebuildWindowAt(ctx, time.Time{}, from, to, load)
}

func (r *Repository) RebuildWindowAt(ctx context.Context, rebuiltAt, from, to time.Time, load func(func([]s.Document) error, func([]s.ActivitySnapshot) error) error) error {
	tx, e := r.DB.BeginTx(ctx, nil)
	if e != nil {
		return safe(e)
	}
	defer tx.Rollback()
	if e = lock(ctx, tx); e != nil {
		return safe(e)
	}
	// Preserve revision tombstones for objects absent from the snapshot without rewriting already-hidden rows or wiping activities outside [from, to).
	if _, e = tx.ExecContext(ctx, `UPDATE search.documents SET data=jsonb_set(data,'{visible}','false') WHERE (data->>'visible')::boolean`); e != nil {
		return safe(e)
	}
	if !from.IsZero() && !to.IsZero() {
		_, e = tx.ExecContext(ctx, `UPDATE search.activities SET data=jsonb_set(data,'{visible}','false') WHERE day>=$1 AND day<$2 AND (data->>'visible')::boolean`, from.Format(time.DateOnly), to.Format(time.DateOnly))
	} else {
		_, e = tx.ExecContext(ctx, `UPDATE search.activities SET data=jsonb_set(data,'{visible}','false') WHERE (data->>'visible')::boolean`)
	}
	if e != nil {
		return safe(e)
	}
	e = load(func(docs []s.Document) error {
		return putDocumentBatch(ctx, tx, docs)
	}, func(docs []s.ActivitySnapshot) error {
		return putActivityBatch(ctx, tx, docs)
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
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var unsafe bool
	e := r.DB.QueryRowContext(ctx, `WITH owners AS (
			SELECT nspowner AS owner FROM pg_namespace WHERE nspname = 'search'
			UNION
			SELECT c.relowner FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'search'
		) SELECT rolsuper OR rolcreatedb OR rolcreaterole OR rolbypassrls OR rolreplication
		OR has_schema_privilege(current_user, 'search', 'CREATE')
		OR EXISTS (SELECT 1 FROM owners WHERE pg_has_role(current_user, owner,
			CASE WHEN current_setting('server_version_num')::int >= 160000 THEN 'USAGE,SET' ELSE 'MEMBER' END))
		FROM pg_roles WHERE rolname = current_user`).Scan(&unsafe)
	if e != nil || unsafe {
		return s.ErrUnavailable
	}
	for _, req := range []struct {
		table string
		privs []string
	}{
		{"search.documents", []string{"SELECT", "INSERT", "UPDATE"}},
		{"search.activities", []string{"SELECT", "INSERT", "UPDATE"}},
		{"search.state", []string{"SELECT", "UPDATE"}},
		{"search.rankings", []string{"SELECT", "INSERT", "UPDATE"}},
		{"search.inbox", []string{"SELECT", "INSERT"}},
	} {
		for _, priv := range req.privs {
			var ok bool
			if e = r.DB.QueryRowContext(ctx, "SELECT has_table_privilege(current_user, $1, $2)", req.table, priv).Scan(&ok); e != nil || !ok {
				return s.ErrUnavailable
			}
		}
	}
	var forbidden bool
	e = r.DB.QueryRowContext(ctx, `SELECT
		has_table_privilege(current_user, 'search.inbox', 'UPDATE,DELETE,TRUNCATE')
		OR has_table_privilege(current_user, 'search.documents', 'TRUNCATE')
		OR has_table_privilege(current_user, 'search.activities', 'TRUNCATE')
		OR has_table_privilege(current_user, 'search.state', 'TRUNCATE')
 OR has_table_privilege(current_user, 'search.rankings', 'TRUNCATE')
		OR EXISTS (
			SELECT 1 FROM pg_namespace
			WHERE nspname NOT LIKE 'pg_%' AND nspname <> 'information_schema'
			  AND (
				has_schema_privilege(current_user, oid, 'CREATE')
				OR (nspname NOT IN ('search', 'public') AND has_schema_privilege(current_user, oid, 'USAGE'))
			  )
		)
		OR EXISTS (
			SELECT 1 FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname NOT IN ('search', 'information_schema')
			  AND (
				(c.relkind IN ('r', 'p', 'v', 'm', 'f') AND has_table_privilege(current_user, c.oid, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
				OR (c.relkind = 'S' AND has_sequence_privilege(current_user, c.oid, 'USAGE,SELECT,UPDATE'))
			  )
		)`).Scan(&forbidden)
	if e != nil || forbidden {
		return s.ErrUnavailable
	}
	return nil
}
