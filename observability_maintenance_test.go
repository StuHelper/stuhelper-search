package search_test

import (
	"context"
	"errors"
	"fmt"
	s "github.com/StuHelper/stuhelper-search"
	"github.com/StuHelper/stuhelper-search/postgres"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestObserverStatusAndBoundedMaintenanceCleanup(t *testing.T) {
	_, repo, f, db := setup(t)
	ctx := context.Background()

	var mu sync.Mutex
	var obs []s.Observation
	svc, err := s.New(repo, f, s.Config{
		QueryTimeout:       time.Second,
		HeatWindow:         7 * 24 * time.Hour,
		HeatWeights:        [4]float64{1, 2, 3, 4},
		MinimumHeatSamples: 1,
		Now:                func() time.Time { return now },
		Observer: s.ObserverFunc(func(o s.Observation) {
			mu.Lock()
			defer mu.Unlock()
			obs = append(obs, o)
		}),
	})
	if err != nil {
		t.Fatal(err)
	}

	course := doc("catalog", "course", "obs-course")
	course.Title = "操作系统敏感查询词课程"
	add(t, svc, f, course)
	addActivity(t, svc, f, activity("catalog", "course", "obs-course", "2026-11-09", map[string]int64{"views": 5}))

	if err := svc.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}

	// Verify Status reports ready, rebuild window, per-source counts, and inbox count.
	st, err := svc.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Ready || st.RebuiltAt == nil || !st.RebuiltAt.Equal(now) {
		t.Fatalf("unexpected status ready/rebuilt_at: %+v", st)
	}
	if st.ActivityWindowFrom != "2026-11-02" || st.ActivityWindowTo != "2026-11-10" {
		t.Fatalf("unexpected rebuild window: %+v", st)
	}
	if st.Documents["catalog"].Visible != 1 || st.Activities["catalog"].Visible != 1 || st.InboxCount != 2 {
		t.Fatalf("unexpected projection/inbox counts: %+v", st)
	}

	// Advance live revision without projection update: Search should record RevisionSkips and never leak query text.
	liveAhead := course
	liveAhead.Revision = 2
	f.docs[course.Target] = liveAhead
	secretQuery := "操作系统敏感查询词"
	page, err := svc.Search(ctx, "secret-principal", secretQuery, "course", "", 20)
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("expected revision mismatch skip, got %+v %v", page, err)
	}

	// Trigger upstream failure on reviews to verify source attribution in Observer.
	f.activityFail = "reviews"
	addActivity(t, svc, f, activity("reviews", "course", "obs-course", "2026-11-09", map[string]int64{"reviews": 1, "interactions": 0}))
	if _, err := svc.Popular(ctx, "secret-principal", "course", "", 20); !errors.Is(err, s.ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
	f.activityFail = ""

	mu.Lock()
	recorded := append([]s.Observation(nil), obs...)
	mu.Unlock()
	var sawSearchSkip, sawPopularSource bool
	for _, o := range recorded {
		if strings.Contains(o.Operation+o.Source+o.ErrorCode, secretQuery) || strings.Contains(o.Operation+o.Source+o.ErrorCode, "secret-principal") {
			t.Fatalf("observer leaked sensitive query or principal: %+v", o)
		}
		if o.Operation == "search" && o.ErrorCode == "ok" && o.Scanned == 1 && o.RevisionSkips == 1 {
			sawSearchSkip = true
		}
		if o.Operation == "popular" && o.Source == "reviews" && o.ErrorCode == "upstream_unavailable" {
			sawPopularSource = true
		}
	}
	if !sawSearchSkip || !sawPopularSource {
		t.Fatalf("missing expected observations: %+v", recorded)
	}

	// Prepare aged inbox receipts and >366d historical activities, including exact boundary records.
	inboxCutoff := now.Add(-90 * 24 * time.Hour)
	for i := 0; i < 4; i++ {
		at := inboxCutoff.Add(-time.Duration(i+1) * time.Hour)
		if _, err := db.Exec(`INSERT INTO search.inbox(source,id,digest,received_at) VALUES('catalog',$1,'d',$2)`, fmt.Sprintf("expired-%d", i), at); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO search.inbox(source,id,digest,received_at) VALUES('catalog','boundary','d',$1)`, inboxCutoff); err != nil {
		t.Fatal(err)
	}

	activityCutoff := "2025-11-08" // now (2026-11-09) - 366d is 2025-11-08
	for i, day := range []string{"2025-10-01", "2025-10-02", "2025-10-03", activityCutoff} {
		act := activity("catalog", "course", fmt.Sprintf("hist-%d", i), day, map[string]int64{"views": 1})
		if err := svc.ConsumeActivity(ctx, activityEvent(act, fmt.Sprintf("hist-ev-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// Verify least-privilege maintenance role isolation.
	maint := &postgres.Maintenance{DB: db, Now: func() time.Time { return now }}
	if err := maint.CheckPermissions(ctx); err == nil {
		t.Fatal("superuser/migration owner should be rejected as maintenance identity")
	}

	roleName := fmt.Sprintf("search_maint_%d", time.Now().UnixNano())
	if _, err := db.Exec(fmt.Sprintf(`
		CREATE SCHEMA other_mod;
		CREATE TABLE other_mod.private_data(id int);
		CREATE ROLE %s NOLOGIN;
		GRANT USAGE ON SCHEMA search TO %s;
		GRANT SELECT, DELETE ON search.inbox, search.activities TO %s;
	`, roleName, roleName, roleName)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Exec("RESET ROLE")
		db.Exec(fmt.Sprintf("REVOKE ALL ON search.inbox, search.activities FROM %s; REVOKE ALL ON SCHEMA search FROM %s; DROP ROLE IF EXISTS %s", roleName, roleName, roleName))
	})

	db.SetMaxOpenConns(1)
	if _, err := db.Exec("SET ROLE " + roleName); err != nil {
		t.Fatal(err)
	}
	if err := maint.CheckPermissions(ctx); err != nil {
		t.Fatalf("expected valid maintenance role, got %v", err)
	}
	if _, err := db.Exec("SELECT * FROM search.documents"); err == nil {
		t.Fatal("maintenance role should not read documents")
	}
	if _, err := db.Exec("SELECT * FROM other_mod.private_data"); err == nil {
		t.Fatal("maintenance role should not read cross-module schema")
	}

	// Reject unsafe cutoffs (within 24h inbox replay window or within 366d max heat window).
	if _, err := maint.Cleanup(ctx, s.CleanupOptions{InboxBefore: now.Add(-12 * time.Hour)}); !errors.Is(err, s.ErrInput) {
		t.Fatalf("expected ErrInput for unsafe inbox cutoff, got %v", err)
	}
	if _, err := maint.Cleanup(ctx, s.CleanupOptions{ActivityBeforeDay: "2026-06-01"}); !errors.Is(err, s.ErrInput) {
		t.Fatalf("expected ErrInput for unsafe activity cutoff, got %v", err)
	}

	// Bounded batch cleanup (Limit=2): deletes oldest 2 expired rows per batch and preserves boundary rows.
	res1, err := maint.Cleanup(ctx, s.CleanupOptions{InboxBefore: inboxCutoff, ActivityBeforeDay: activityCutoff, Limit: 2})
	if err != nil || res1.DeletedInbox != 2 || res1.DeletedActivities != 2 {
		t.Fatalf("unexpected batch 1 cleanup result: %+v %v", res1, err)
	}
	res2, err := maint.Cleanup(ctx, s.CleanupOptions{InboxBefore: inboxCutoff, ActivityBeforeDay: activityCutoff, Limit: 2})
	if err != nil || res2.DeletedInbox != 2 || res2.DeletedActivities != 1 {
		t.Fatalf("unexpected batch 2 cleanup result: %+v %v", res2, err)
	}
	res3, err := maint.Cleanup(ctx, s.CleanupOptions{InboxBefore: inboxCutoff, ActivityBeforeDay: activityCutoff, Limit: 2})
	if err != nil || res3.DeletedInbox != 0 || res3.DeletedActivities != 0 {
		t.Fatalf("expected zero remaining expired rows, got %+v %v", res3, err)
	}

	if _, err := db.Exec("RESET ROLE"); err != nil {
		t.Fatal(err)
	}
	var boundaryInbox, boundaryAct int
	if err := db.QueryRow(`SELECT count(*) FROM search.inbox WHERE id='boundary'`).Scan(&boundaryInbox); err != nil || boundaryInbox != 1 {
		t.Fatalf("boundary inbox row was deleted: %d %v", boundaryInbox, err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM search.activities WHERE day=$1::date`, activityCutoff).Scan(&boundaryAct); err != nil || boundaryAct != 1 {
		t.Fatalf("boundary activity row was deleted: %d %v", boundaryAct, err)
	}
}
