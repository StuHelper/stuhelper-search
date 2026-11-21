package search_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	s "github.com/StuHelper/stuhelper-search"
	"github.com/StuHelper/stuhelper-search/postgres"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestActivityCalendarMigrationRollbackAndRebuild(t *testing.T) {
	svc, repo, f, db := setup(t)
	ctx := context.Background()
	// Recreate the pre-migration schema before seeding the legacy activity and ranking.
	if _, err := db.Exec(`ALTER TABLE search.state DROP COLUMN activity_calendar;
		DROP TABLE search.activity_calendar_archive`); err != nil {
		t.Fatal(err)
	}
	if err := postgres.RollbackActivityCalendar(ctx, db); err != nil {
		t.Fatal(err)
	}
	course := doc("catalog", "course", "migration-course")
	add(t, svc, f, course)
	old := activity("catalog", "course", course.Target.ID, "2026-11-09", map[string]int64{"views": 9})
	old.Revision = 99
	payload, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`WITH legacy_activity AS (
			INSERT INTO search.activities(source,id,kind,object_id,day,revision,data)
			VALUES($1,$2,$3,$4,$5::date,$6,$7) RETURNING source
		)
		INSERT INTO search.inbox(source,event_type,id,digest)
		SELECT source,$8,'utc-event','utc-event-digest' FROM legacy_activity`,
		old.Source, old.ID, old.Target.Kind, old.Target.ID, old.Day, old.Revision, payload, s.LegacyActivityEventType); err != nil {
		t.Fatal(err)
	}
	oldToken, err := repo.SaveRanking(ctx, []s.Target{course.Target}, now, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	assertState := func(calendar string, ready bool, activities int) {
		t.Helper()
		var mode string
		var published bool
		var count int
		if err := db.QueryRow(`SELECT activity_calendar,ready,(SELECT count(*) FROM search.activities)
			FROM search.state WHERE id`).Scan(&mode, &published, &count); err != nil {
			t.Fatal(err)
		}
		if mode != calendar || published != ready || count != activities {
			t.Fatalf("state: calendar=%s ready=%t active=%d, want %s %t %d", mode, published, count, calendar, ready, activities)
		}
	}
	assertArchive := func(calendar, id string, views int64) {
		t.Helper()
		var count int64
		if err := db.QueryRow(`SELECT (data #>> '{metrics,views}')::bigint FROM search.activity_calendar_archive
			WHERE calendar=$1 AND source='catalog' AND id=$2`, calendar, id).Scan(&count); err != nil || count != views {
			t.Fatalf("archive %s %s: views=%d err=%v, want %d", calendar, id, count, err, views)
		}
	}

	if err := postgres.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	assertState("Asia/Shanghai", false, 0)
	assertArchive("UTC", old.ID, 9)
	if _, err := repo.ResumeRanking(ctx, oldToken, now, 24*time.Hour); !errors.Is(err, s.ErrSnapshotExpired) {
		t.Fatalf("legacy ranking: %v", err)
	}
	var retained bool
	if err := db.QueryRow(`SELECT
		EXISTS (SELECT 1 FROM search.documents WHERE id = $1) AND
		EXISTS (SELECT 1 FROM search.inbox WHERE id='utc-event' AND digest='utc-event-digest')`,
		course.Target.ID).Scan(&retained); err != nil || !retained {
		t.Fatalf("unrelated rows retained=%t err=%v", retained, err)
	}

	// The provider's relocated snapshot can have a lower revision than the archived projection.
	shanghai := activity("catalog", "course", course.Target.ID, old.Day, map[string]int64{"views": 4})
	f.activities[shanghai.Source+"/"+shanghai.ID] = shanghai
	historicCourse := doc("catalog", "course", "historical-course")
	f.docs[historicCourse.Target] = historicCourse
	historic := activity("catalog", "course", historicCourse.Target.ID, "2025-11-09", map[string]int64{"views": 12})
	f.activities[historic.Source+"/"+historic.ID] = historic
	f.activityFail = "catalog"
	if err := svc.Rebuild(ctx); err == nil {
		t.Fatal("expected rebuild failure")
	}
	assertState("Asia/Shanghai", false, 0)
	f.activityFail = ""
	if err := svc.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	assertState("Asia/Shanghai", true, 2)
	expectPopular(t, svc, "course", course.Target.ID)
	wider, err := s.New(repo, f, s.Config{
		QueryTimeout:       time.Second,
		HeatWindow:         366 * 24 * time.Hour,
		HeatWeights:        [4]float64{1},
		MinimumHeatSamples: 1,
		Now:                func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	// Widening the window can use the backfilled row without another rebuild or event.
	expectPopular(t, wider, "course", historicCourse.Target.ID, course.Target.ID)

	shanghaiNext := activity("catalog", "course", course.Target.ID, "2026-11-08", map[string]int64{"views": 7})
	addActivity(t, svc, f, shanghaiNext)
	if err := postgres.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	assertState("Asia/Shanghai", true, 3)
	assertArchive("UTC", old.ID, 9)
	if err := postgres.RollbackActivityCalendar(ctx, db); err != nil {
		t.Fatal(err)
	}
	assertState("UTC", false, 0)
	assertArchive("Asia/Shanghai", shanghaiNext.ID, 7)

	// On rollback the UTC release rebuilds from the current provider snapshots, which include new writes.
	currentUTC := old
	currentUTC.Revision++
	currentUTC.Metrics = map[string]int64{"views": 16}
	if err := repo.Rebuild(ctx, func(putDocs func([]s.Document) error, putActivities func([]s.ActivitySnapshot) error) error {
		if err := putDocs([]s.Document{course}); err != nil {
			return err
		}
		return putActivities([]s.ActivitySnapshot{currentUTC})
	}); err != nil {
		t.Fatal(err)
	}
	if err := postgres.RollbackActivityCalendar(ctx, db); err != nil {
		t.Fatal(err)
	}
	assertState("UTC", true, 1)
	if err := postgres.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	assertState("Asia/Shanghai", false, 0)
	assertArchive("UTC", old.ID, 16)
	assertArchive("Asia/Shanghai", shanghaiNext.ID, 7)
}

func TestActivityCalendarArchiveDeniesRuntimeCRUD(t *testing.T) {
	_, _, _, db := setup(t)
	ctx := context.Background()
	role := fmt.Sprintf("archive_runtime_%d", time.Now().UnixNano())
	if _, err := db.Exec("CREATE ROLE " + role + " NOLOGIN NOSUPERUSER NOBYPASSRLS"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec("DROP OWNED BY " + role + "; DROP ROLE " + role)
	})
	if _, err := db.Exec(fmt.Sprintf(`
		GRANT USAGE ON SCHEMA search TO %[1]s;
		GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA search TO %[1]s;
		INSERT INTO search.activity_calendar_archive VALUES
			('UTC','catalog','activity','course','c','2026-11-09',1,'{}');
	`, role)); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec("SET LOCAL ROLE " + role); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := tx.QueryRow("SELECT count(*) FROM search.activity_calendar_archive").Scan(&count); err != nil || count != 0 {
		t.Fatalf("runtime saw %d archived rows: %v", count, err)
	}
	result, err := tx.Exec("DELETE FROM search.activity_calendar_archive")
	if err != nil {
		t.Fatal(err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 0 {
		t.Fatalf("runtime deleted %d archived rows: %v", count, err)
	}
	_, err = tx.Exec(`INSERT INTO search.activity_calendar_archive VALUES
		('UTC','catalog','injected','course','c','2026-11-09',1,'{}')`)
	var dbErr *pgconn.PgError
	if !errors.As(err, &dbErr) || dbErr.Code != "42501" {
		t.Fatalf("runtime archive insert: %v", err)
	}
}
