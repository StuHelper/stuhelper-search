package postgres

import (
	"context"
	"database/sql"
)

// RollbackActivityCalendar prepares search for a release that uses UTC activity dates.
// After the providers restore their UTC projections, the old release must rebuild its
// index before serving requests.
func RollbackActivityCalendar(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(726391840129)"); err != nil {
		return err
	}
	var migrated bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_attribute
 WHERE attrelid=to_regclass('search.state') AND attname='activity_calendar' AND NOT attisdropped)`).Scan(&migrated); err != nil {
		return err
	}
	if !migrated {
		return nil
	}
	if err = switchActivityCalendar(ctx, tx, "UTC"); err != nil {
		return err
	}
	return tx.Commit()
}

func switchActivityCalendar(ctx context.Context, tx *sql.Tx, calendar string) error {
	var current string
	if err := tx.QueryRowContext(ctx, "SELECT activity_calendar FROM search.state WHERE id").Scan(&current); err != nil {
		return err
	}
	if current == calendar {
		return nil
	}
	if err := lock(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO search.activity_calendar_archive(calendar,source,id,kind,object_id,day,revision,data)
 SELECT $1,source,id,kind,object_id,day,revision,data FROM search.activities
 ON CONFLICT(calendar,source,id) DO UPDATE
 SET kind=EXCLUDED.kind,object_id=EXCLUDED.object_id,day=EXCLUDED.day,revision=EXCLUDED.revision,data=EXCLUDED.data`, current); err != nil {
		return err
	}
	// Source snapshots are authoritative after the switch. Previous rankings use the old calendar.
	if _, err := tx.ExecContext(ctx, "DELETE FROM search.activities; DELETE FROM search.rankings"); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE search.state
 SET ready=false,rebuilt_at=NULL,activity_from=NULL,activity_to=NULL,activity_calendar=$1 WHERE id`, calendar)
	return err
}
