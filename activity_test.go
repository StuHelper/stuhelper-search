package search_test

import (
	"context"
	"errors"
	"fmt"
	s "github.com/StuHelper/stuhelper-search"
	"sort"
	"sync"
	"testing"
	"time"
)

func (f *source) CurrentActivity(ctx context.Context, user, src, id string) (s.ActivitySnapshot, bool, error) {
	if f.disabled[src] {
		return s.ActivitySnapshot{}, false, s.ErrSourceDisabled
	}
	if f.activityFail == src {
		return s.ActivitySnapshot{}, false, s.ErrUnavailable
	}
	d, ok := f.activities[src+"/"+id]
	return d, ok && !f.denied[user+"/"+id], nil
}
func (f *source) ActivitySnapshot(ctx context.Context, src string, from, to time.Time, cursor string, limit int) ([]s.ActivitySnapshot, string, error) {
	if f.disabled[src] {
		return nil, "", s.ErrSourceDisabled
	}
	if f.activityFail == src {
		return nil, "", s.ErrUnavailable
	}
	docs := []s.ActivitySnapshot{}
	for _, d := range f.activities {
		if d.Source == src && d.Day >= from.Format(time.DateOnly) && d.Day < to.Format(time.DateOnly) && d.ID > cursor {
			docs = append(docs, d)
		}
	}
	sort.Slice(docs, func(i, j int) bool { return docs[i].ID < docs[j].ID })
	if len(docs) > limit {
		return docs[:limit], docs[limit-1].ID, nil
	}
	return docs, "", nil
}
func activity(src, kind, id, day string, metrics map[string]int64) s.ActivitySnapshot {
	if kind == "teacher" {
		id = "teacher-" + id
	}
	target := s.Target{Source: "catalog", Kind: kind, ID: id}
	return s.ActivitySnapshot{Source: src, ID: s.ActivityID(target, day), Target: target, Day: day, Revision: 1, Visible: true, Metrics: metrics}
}
func activityEvent(d s.ActivitySnapshot, id string) s.ActivityEvent {
	return s.ActivityEvent{SpecVersion: "1.0", ID: id, Source: d.Source, Type: "discovery.activity.v1", Subject: d.ID, Time: now, Data: d}
}
func addActivity(t *testing.T, svc *s.Service, f *source, d s.ActivitySnapshot) {
	t.Helper()
	f.activities[d.Source+"/"+d.ID] = d
	if e := svc.ConsumeActivity(context.Background(), activityEvent(d, fmt.Sprintf("%s-%d", d.ID, d.Revision))); e != nil {
		t.Fatal(e)
	}
}
func expectPopular(t *testing.T, svc *s.Service, kind string, want ...string) {
	t.Helper()
	p, e := svc.Popular(context.Background(), "u", kind, "", 100)
	if e != nil || len(p.Items) != len(want) {
		t.Fatalf("%+v %v want %v", p, e, want)
	}
	for i, id := range want {
		if kind == "teacher" {
			id = "teacher-" + id
		}
		if p.Items[i].Target.ID != id {
			t.Fatalf("%+v want %v", p, want)
		}
	}
}

// DS-05: Providers aggregate per object; weights, UTC windows and minimum samples apply after aggregation.
func TestHeatWeightsWindowAndTeachers(t *testing.T) {
	svc, repo, f, _ := setup(t)
	for _, kind := range []string{"course", "teacher"} {
		for _, id := range []string{"a", "b"} {
			targetID := id
			if kind == "teacher" {
				targetID = "teacher-" + id
			}
			add(t, svc, f, doc("catalog", kind, targetID))
		}
		addActivity(t, svc, f, activity("catalog", kind, "a", "2026-11-09", map[string]int64{"views": 5}))
		addActivity(t, svc, f, activity("reviews", kind, "b", "2026-11-09", map[string]int64{"reviews": 1, "interactions": 1}))
		addActivity(t, svc, f, activity("favorites", kind, "b", "2026-11-09", map[string]int64{"favorites": 1}))
		// Lower boundary inclusive, previous day excluded; tomorrow excluded.
		addActivity(t, svc, f, activity("catalog", kind, "b", "2026-11-02", map[string]int64{"views": 1}))
		for _, day := range []string{"2026-11-01", "2026-11-10"} {
			addActivity(t, svc, f, activity("catalog", kind, "a", day, map[string]int64{"views": 1000}))
		}
		expectPopular(t, svc, kind, "b", "a")
	}
	cfg := s.Config{QueryTimeout: time.Second, HeatWindow: 7 * 24 * time.Hour, HeatWeights: [4]float64{1, 0, 0, 0}, MinimumHeatSamples: 1, Now: func() time.Time { return now }}
	weighted, e := s.New(repo, f, cfg)
	if e != nil {
		t.Fatal(e)
	}
	expectPopular(t, weighted, "course", "a", "b")
	cfg.HeatWeights = [4]float64{1, 2, 3, 4}
	cfg.MinimumHeatSamples = 5
	threshold, e := s.New(repo, f, cfg)
	if e != nil {
		t.Fatal(e)
	}
	expectPopular(t, threshold, "course", "a")
	f.disabled["favorites"] = true
	expectPopular(t, svc, "course", "b", "a") // b: 1+2+4=7, absent favorites contributes zero.
	f.disabled["reviews"] = true
	expectPopular(t, svc, "course", "a", "b")
	f.denied["u/teacher-a"] = true
	expectPopular(t, svc, "teacher", "b")
	f.disabled["reviews"] = false
	f.activityFail = "reviews"
	if _, e = svc.Popular(context.Background(), "u", "course", "", 20); !errors.Is(e, s.ErrUnavailable) {
		t.Fatal(e)
	}
}

// DS-05: Wrong-provider fields (including zero), unsupported sources and malformed day/ID never reach storage.
func TestActivityOwnership(t *testing.T) {
	svc, _, _, db := setup(t)
	ctx := context.Background()
	for _, src := range []string{"catalog", "reviews", "favorites"} {
		for _, name := range []string{"views", "reviews", "favorites", "interactions", "unknown"} {
			metrics := map[string]int64{}
			switch src {
			case "catalog":
				metrics["views"] = 1
			case "reviews":
				metrics["reviews"] = 1
				metrics["interactions"] = 1
			case "favorites":
				metrics["favorites"] = 1
			}
			if _, ok := metrics[name]; ok {
				continue
			}
			metrics[name] = 0
			d := activity(src, "course", "a", "2026-11-09", metrics)
			if e := svc.ConsumeActivity(ctx, activityEvent(d, src+name)); !errors.Is(e, s.ErrInput) {
				t.Fatal(src, name, e)
			}
		}
	}
	for _, d := range []s.ActivitySnapshot{activity("resources", "course", "a", "2026-11-09", map[string]int64{"views": 1}), activity("catalog", "course", "a", "2026-02-30", map[string]int64{"views": 1}), activity("favorites", "course", "a", "2026-11-09", map[string]int64{"favorites": -1})} {
		if e := svc.ConsumeActivity(ctx, activityEvent(d, "bad")); !errors.Is(e, s.ErrInput) {
			t.Fatal(e)
		}
	}
	var n int
	if e := db.QueryRow("SELECT (SELECT count(*) FROM search.activities)+(SELECT count(*) FROM search.inbox)").Scan(&n); e != nil || n != 0 {
		t.Fatal(n, e)
	}
}

// DS-06, DS-07: Provider revisions, inbox replay, failed writes and rebuilds are atomic.
func TestActivityReplayAndRebuild(t *testing.T) {
	svc, _, f, db := setup(t)
	ctx := context.Background()
	add(t, svc, f, doc("catalog", "course", "a"))
	d := activity("catalog", "course", "a", "2026-11-09", map[string]int64{"views": 3})
	addActivity(t, svc, f, d)
	d.Revision = 3
	d.Metrics = map[string]int64{"views": 7}
	addActivity(t, svc, f, d)
	if e := svc.ConsumeActivity(ctx, activityEvent(d, fmt.Sprintf("%s-%d", d.ID, d.Revision))); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := svc.ConsumeActivity(ctx, activityEvent(d, "concurrent-activity")); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	var receipts int
	if e := db.QueryRow("SELECT count(*) FROM search.inbox WHERE id='concurrent-activity'").Scan(&receipts); e != nil || receipts != 1 {
		t.Fatal(receipts, e)
	}
	conflict := d
	conflict.Metrics = map[string]int64{"views": 8}
	if e := svc.ConsumeActivity(ctx, activityEvent(conflict, "concurrent-activity")); !errors.Is(e, s.ErrConflict) {
		t.Fatal(e)
	}
	old := d
	old.Revision = 2
	old.Metrics = map[string]int64{"views": 100}
	if e := svc.ConsumeActivity(ctx, activityEvent(old, "late")); e != nil {
		t.Fatal(e)
	}
	// Same activity ID in a different provider has its own revision sequence.
	other := activity("favorites", "course", "a", d.Day, map[string]int64{"favorites": 2})
	addActivity(t, svc, f, other)
	var revision, count int
	var views int
	if e := db.QueryRow("SELECT revision,(data->'metrics'->>'views')::int FROM search.activities WHERE source='catalog'").Scan(&revision, &views); e != nil || revision != 3 || views != 7 {
		t.Fatal(revision, views, e)
	}
	if e := db.QueryRow("SELECT count(*) FROM search.activities").Scan(&count); e != nil || count != 2 {
		t.Fatal(count, e)
	}
	if e := svc.Rebuild(ctx); e != nil {
		t.Fatal(e)
	}
	expectPopular(t, svc, "course", "a")
	// catalog snapshot is already written when a later provider fails; everything must roll back.
	d.Revision = 4
	d.Metrics = map[string]int64{"views": 9}
	f.activities[d.Source+"/"+d.ID] = d
	f.activityFail = "favorites"
	if e := svc.Rebuild(ctx); e == nil {
		t.Fatal("expected rebuild failure")
	}
	if e := db.QueryRow("SELECT revision FROM search.activities WHERE source='catalog'").Scan(&revision); e != nil || revision != 3 {
		t.Fatal(revision, e)
	}
	f.activityFail = ""
	if e := svc.Rebuild(ctx); e != nil {
		t.Fatal(e)
	}
	delete(f.activities, "catalog/"+d.ID)
	f.disabled["favorites"] = true
	if e := svc.Rebuild(ctx); e != nil {
		t.Fatal(e)
	}
	expectPopular(t, svc, "course")
	if e := svc.ConsumeActivity(ctx, activityEvent(old, "late-after-rebuild")); e != nil {
		t.Fatal(e)
	}
	expectPopular(t, svc, "course")
	if _, e := db.Exec(`ALTER TABLE search.inbox ADD CONSTRAINT reject_activity CHECK(id<>'fail-activity')`); e != nil {
		t.Fatal(e)
	}
	fresh := activity("catalog", "course", "new", d.Day, map[string]int64{"views": 1})
	if e := svc.ConsumeActivity(ctx, activityEvent(fresh, "fail-activity")); e == nil {
		t.Fatal("expected transaction failure")
	}
	if e := db.QueryRow("SELECT count(*) FROM search.activities WHERE object_id='new'").Scan(&count); e != nil || count != 0 {
		t.Fatal(count, e)
	}
}

// DS-07: Activity reconstruction traverses multiple bounded pages.
func TestActivitySnapshotPagination(t *testing.T) {
	svc, _, f, db := setup(t)
	ctx := context.Background()
	for i := 0; i < 105; i++ {
		d := activity("catalog", "course", fmt.Sprintf("object-%03d", i), "2026-11-09", map[string]int64{"views": 1})
		f.activities[d.Source+"/"+d.ID] = d
	}
	if e := svc.Rebuild(ctx); e != nil {
		t.Fatal(e)
	}
	var count int
	if e := db.QueryRow("SELECT count(*) FROM search.activities WHERE (data->>'visible')::boolean").Scan(&count); e != nil || count != 105 {
		t.Fatal(count, e)
	}
}
