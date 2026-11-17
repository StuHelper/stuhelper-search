package postgres

import (
	"context"
	"database/sql"
	s "github.com/StuHelper/stuhelper-search"
	"time"
)

const (
	defaultCleanupLimit   = 1000
	maxCleanupLimit       = 1000
	defaultCleanupTimeout = 3 * time.Second
	maxCleanupTimeout     = 3 * time.Second
	minInboxRetention     = 24 * time.Hour
	maxHeatWindowDays     = 366
)

// Maintenance executes bounded retention cleanup using a dedicated least-privilege database role.
type Maintenance struct {
	DB  *sql.DB
	Now func() time.Time
}

func (m *Maintenance) nowUTC() time.Time {
	if m != nil && m.Now != nil {
		return m.Now().UTC()
	}
	return time.Now().UTC()
}

// CheckPermissions verifies that the maintenance connection holds only SELECT/DELETE on inbox and activities.
func (m *Maintenance) CheckPermissions(ctx context.Context) error {
	if m == nil || m.DB == nil {
		return s.ErrUnavailable
	}
	var unsafe bool
	e := m.DB.QueryRowContext(ctx, `SELECT rolsuper OR rolcreatedb OR rolcreaterole OR NOT has_schema_privilege(current_user,'search','USAGE') OR has_schema_privilege(current_user,'search','CREATE') FROM pg_roles WHERE rolname=current_user`).Scan(&unsafe)
	if e != nil || unsafe {
		return s.ErrUnavailable
	}
	for _, table := range []string{"search.inbox", "search.activities"} {
		var allowed, forbidden bool
		e = m.DB.QueryRowContext(ctx, `SELECT has_table_privilege(current_user,$1,'SELECT') AND has_table_privilege(current_user,$1,'DELETE'), has_table_privilege(current_user,$1,'INSERT,UPDATE,TRUNCATE')`, table).Scan(&allowed, &forbidden)
		if e != nil || !allowed || forbidden {
			return s.ErrUnavailable
		}
	}
	for _, table := range []string{"search.documents", "search.state"} {
		var anyPriv bool
		e = m.DB.QueryRowContext(ctx, `SELECT has_table_privilege(current_user,$1,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE')`, table).Scan(&anyPriv)
		if e != nil || anyPriv {
			return s.ErrUnavailable
		}
	}
	var crossSchema bool
	e = m.DB.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind IN ('r','p','v','m')
		  AND n.nspname NOT IN ('search','pg_catalog','information_schema')
		  AND n.nspname NOT LIKE 'pg_%'
		  AND has_schema_privilege(current_user, n.oid, 'USAGE')
		  AND has_table_privilege(current_user, c.oid, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE')
	)`).Scan(&crossSchema)
	if e != nil || crossSchema {
		return s.ErrUnavailable
	}
	return nil
}

// Cleanup deletes expired inbox receipts and historical activity rows strictly before the cutoffs in bounded batches.
func (m *Maintenance) Cleanup(ctx context.Context, opts s.CleanupOptions) (s.CleanupResult, error) {
	if m == nil || m.DB == nil {
		return s.CleanupResult{}, s.ErrUnavailable
	}
	if opts.InboxBefore.IsZero() && opts.ActivityBeforeDay == "" {
		return s.CleanupResult{}, s.ErrInput
	}
	limit := opts.Limit
	if limit == 0 {
		limit = defaultCleanupLimit
	}
	if limit < 1 || limit > maxCleanupLimit {
		return s.CleanupResult{}, s.ErrInput
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = defaultCleanupTimeout
	}
	if timeout <= 0 || timeout > maxCleanupTimeout {
		return s.CleanupResult{}, s.ErrInput
	}
	now := m.nowUTC()
	if !opts.InboxBefore.IsZero() && opts.InboxBefore.UTC().After(now.Add(-minInboxRetention)) {
		return s.CleanupResult{}, s.ErrInput
	}
	if opts.ActivityBeforeDay != "" {
		day, err := time.Parse(time.DateOnly, opts.ActivityBeforeDay)
		if err != nil || day.Format(time.DateOnly) != opts.ActivityBeforeDay {
			return s.CleanupResult{}, s.ErrInput
		}
		earliestWindowStart := now.Add(-maxHeatWindowDays * 24 * time.Hour).Truncate(24 * time.Hour)
		if day.After(earliestWindowStart) {
			return s.CleanupResult{}, s.ErrInput
		}
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	tx, e := m.DB.BeginTx(ctx, nil)
	if e != nil {
		return s.CleanupResult{}, safe(e)
	}
	defer tx.Rollback()
	if _, e = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock_shared(726391840130)"); e != nil {
		return s.CleanupResult{}, safe(e)
	}
	var res s.CleanupResult
	if !opts.InboxBefore.IsZero() {
		r, err := tx.ExecContext(ctx, `DELETE FROM search.inbox WHERE (source, id) IN (
			SELECT source, id FROM search.inbox
			WHERE received_at < $1
			ORDER BY received_at ASC, source COLLATE "C" ASC, id COLLATE "C" ASC
			LIMIT $2
		)`, opts.InboxBefore.UTC(), limit)
		if err != nil {
			return s.CleanupResult{}, safe(err)
		}
		res.DeletedInbox, _ = r.RowsAffected()
	}
	if opts.ActivityBeforeDay != "" {
		r, err := tx.ExecContext(ctx, `DELETE FROM search.activities WHERE (source, id) IN (
			SELECT source, id FROM search.activities
			WHERE day < $1::date
			ORDER BY day ASC, source COLLATE "C" ASC, id COLLATE "C" ASC
			LIMIT $2
		)`, opts.ActivityBeforeDay, limit)
		if err != nil {
			return s.CleanupResult{}, safe(err)
		}
		res.DeletedActivities, _ = r.RowsAffected()
	}
	if e = tx.Commit(); e != nil {
		return s.CleanupResult{}, safe(e)
	}
	return res, nil
}
