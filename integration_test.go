package search_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	s "github.com/StuHelper/stuhelper-search"
	"github.com/StuHelper/stuhelper-search/httpapi"
	"github.com/StuHelper/stuhelper-search/postgres"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

var now = time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)

type source struct {
	docs   map[s.Target]s.Document
	denied map[string]bool
	fail   bool
	slow   bool
	votes  map[string][]s.Vote
}

func (f *source) Current(ctx context.Context, user string, t s.Target) (s.Document, bool, error) {
	if f.slow {
		<-ctx.Done()
		return s.Document{}, false, ctx.Err()
	}
	if f.fail {
		return s.Document{}, false, errors.New("source failure")
	}
	d, ok := f.docs[t]
	return d, ok && !f.denied[user+"/"+t.ID], nil
}
func (f *source) Snapshot(ctx context.Context, src, cursor string, limit int) ([]s.Document, string, error) {
	if f.fail {
		return nil, "", errors.New("snapshot unavailable")
	}
	out := []s.Document{}
	for _, d := range f.docs {
		if d.Target.Source == src {
			out = append(out, d)
		}
	}
	return out, "", nil
}
func (f *source) MyVotes(ctx context.Context, user, cursor string, limit int) (s.Votes, error) {
	return s.Votes{Items: f.votes[user]}, nil
}
func doc(src, kind, id string) s.Document {
	return s.Document{Target: s.Target{Source: src, ID: id, Kind: kind}, Revision: 1, Title: id, SchoolID: "school-a", Visible: true, At: now.Add(-time.Hour)}
}
func event(d s.Document, id string) s.Event {
	return s.Event{SpecVersion: "1.0", ID: id, Source: d.Target.Source, Subject: d.Target.ID, Type: d.Target.Source + ".discovery.v1", Time: now, Data: d}
}
func setup(t *testing.T) (*s.Service, *postgres.Repository, *source, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("STUHELPER_SEARCH_TEST_DSN")
	if dsn == "" {
		t.Fatal("isolated STUHELPER_SEARCH_TEST_DSN required")
	}
	db, e := sql.Open("pgx", dsn)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { db.Close() })
	// Test cluster is disposable. Separate database per test allows concurrent suite execution.
	name := fmt.Sprintf("search_%d", time.Now().UnixNano())
	if _, e = db.Exec("CREATE DATABASE " + name); e != nil {
		t.Fatal(e)
	}
	testDB, e := sql.Open("pgx", dsn+" dbname="+name)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { testDB.Close(); db.Exec("DROP DATABASE " + name + " WITH (FORCE)") })
	if e = postgres.Migrate(context.Background(), testDB); e != nil {
		t.Fatal(e)
	}
	if e = postgres.Migrate(context.Background(), testDB); e != nil {
		t.Fatal(e)
	}
	f := &source{docs: map[s.Target]s.Document{}, denied: map[string]bool{}, votes: map[string][]s.Vote{}}
	repo := &postgres.Repository{DB: testDB}
	svc, e := s.New(repo, f, s.Config{MinimumSamples: 2, QueryTimeout: time.Second, HeatWindow: 7 * 24 * time.Hour, HeatWeights: [4]float64{1, 2, 3, 4}, MinimumHeatSamples: 1, Now: func() time.Time { return now }})
	if e != nil {
		t.Fatal(e)
	}
	if e = svc.Rebuild(context.Background()); e != nil {
		t.Fatal(e)
	}
	return svc, repo, f, testDB
}
func add(t *testing.T, svc *s.Service, f *source, d s.Document) {
	t.Helper()
	f.docs[d.Target] = d
	if e := svc.Consume(context.Background(), event(d, fmt.Sprintf("%s-%s-%d", d.Target.Source, d.Target.ID, d.Revision))); e != nil {
		t.Fatal(e)
	}
}
func TestProjectionReplayRebuildAndIsolation(t *testing.T) {
	svc, repo, f, db := setup(t)
	ctx := context.Background()
	d := doc("catalog", "course", "course")
	add(t, svc, f, d)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := svc.Consume(ctx, event(d, "concurrent")); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	var count int
	if e := db.QueryRow("SELECT count(*) FROM search.inbox WHERE id='concurrent'").Scan(&count); e != nil || count != 1 {
		t.Fatalf("receipt %d %v", count, e)
	}
	bad := d
	bad.Title = "different"
	if e := svc.Consume(ctx, event(bad, "concurrent")); !errors.Is(e, s.ErrConflict) {
		t.Fatal(e)
	}
	d.Revision = 3
	d.Title = "latest"
	add(t, svc, f, d)
	old := d
	old.Revision = 2
	old.Title = "stale"
	if e := svc.Consume(ctx, event(old, "old")); e != nil {
		t.Fatal(e)
	}
	p, e := svc.Search(ctx, "u", "", "", "", 20)
	if e != nil || len(p.Items) != 1 || p.Items[0].Title != "latest" {
		t.Fatalf("%+v %v", p, e)
	}
	f.fail = true
	if e = svc.Rebuild(ctx); e == nil {
		t.Fatal("expected rollback")
	}
	f.fail = false
	p, e = svc.Search(ctx, "u", "", "", "", 20)
	if e != nil || len(p.Items) != 1 {
		t.Fatal(p, e)
	}
	delete(f.docs, d.Target)
	if e = svc.Rebuild(ctx); e != nil {
		t.Fatal(e)
	}
	f.docs[d.Target] = d
	if e = svc.Consume(ctx, event(old, "late-after-rebuild")); e != nil {
		t.Fatal(e)
	}
	p, e = svc.Search(ctx, "u", "", "", "", 20)
	if e != nil || len(p.Items) != 0 {
		t.Fatal("resurrected", p, e)
	}
	if e = svc.Rebuild(ctx); e != nil {
		t.Fatal(e)
	}
	p, e = svc.Search(ctx, "u", "", "", "", 20)
	if e != nil || len(p.Items) != 1 {
		t.Fatal(p, e)
	}
	if e = repo.CheckRuntime(ctx); e == nil {
		t.Fatal("migration owner accepted")
	}
	_, e = db.Exec(`CREATE ROLE search_runtime NOLOGIN; GRANT USAGE ON SCHEMA search TO search_runtime; GRANT SELECT,INSERT,UPDATE ON search.documents,search.state TO search_runtime; GRANT SELECT,INSERT ON search.inbox TO search_runtime; CREATE SCHEMA source_private; CREATE TABLE source_private.secret(id int)`)
	if e != nil {
		t.Fatal(e)
	}
	// Dedicated single connection uses runtime role throughout checks and writes.
	db.SetMaxOpenConns(1)
	if _, e = db.Exec("SET ROLE search_runtime"); e != nil {
		t.Fatal(e)
	}
	if e = repo.CheckRuntime(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = db.Exec("SELECT * FROM source_private.secret"); e == nil {
		t.Fatal("cross-module access permitted")
	}
	if _, e = db.Exec("DELETE FROM search.inbox"); e == nil {
		t.Fatal("inbox deletion permitted")
	}
	d.Revision++
	if e = svc.Consume(ctx, event(d, "runtime-write")); e != nil {
		t.Fatal(e)
	}
	if e = svc.Rebuild(ctx); !errors.Is(e, s.ErrConflict) {
		t.Fatal("stale snapshot should abort", e)
	}
	if _, e = db.Exec("RESET ROLE"); e != nil {
		t.Fatal(e)
	}
}
func TestVisibilityPaginationAndHotTargets(t *testing.T) {
	svc, _, f, _ := setup(t)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		add(t, svc, f, doc("catalog", "course", id))
	}
	f.denied["u/a"] = true
	p, e := svc.Search(ctx, "u", "", "course", "", 1)
	if e != nil || len(p.Items) != 1 || p.Items[0].Target.ID != "b" {
		t.Fatal(p, e)
	}
	p, e = svc.Search(ctx, "u", "", "course", p.NextCursor, 1)
	if e != nil || p.Items[0].Target.ID != "c" {
		t.Fatal(p, e)
	}
	d := f.docs[s.Target{Source: "catalog", ID: "b", Kind: "course"}]
	d.Visible = false
	f.docs[d.Target] = d
	p, e = svc.Search(ctx, "u", "b", "", "", 20)
	if e != nil || len(p.Items) != 0 {
		t.Fatal("stale index leaked", p, e)
	}
	activity := doc("catalog", "activity", "c/day")
	activity.CourseID = "c"
	activity.Activity = &[4]int64{3, 0, 0, 0}
	add(t, svc, f, activity)
	p, e = svc.Popular(ctx, "u", "course", "", 20)
	if e != nil || len(p.Items) != 1 || p.Items[0].Target.ID != "c" {
		t.Fatal(p, e)
	}
	f.denied["u/c"] = true
	p, e = svc.Popular(ctx, "u", "course", "", 20)
	if e != nil || len(p.Items) != 0 {
		t.Fatal(p, e)
	}
	if _, e = svc.Search(ctx, "", "", "", "", 20); !errors.Is(e, s.ErrUnauthenticated) {
		t.Fatal(e)
	}
	if _, e = svc.Search(ctx, "u", "", "", "", 101); !errors.Is(e, s.ErrInput) {
		t.Fatal(e)
	}
	f.fail = true
	if _, e = svc.Search(ctx, "u", "", "", "", 20); !errors.Is(e, s.ErrUnavailable) {
		t.Fatal(e)
	}
}
func TestStatisticsRangesThresholdsTeacherWeightingAndComparison(t *testing.T) {
	svc, _, f, _ := setup(t)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "empty"} {
		add(t, svc, f, doc("catalog", "course", id))
	}
	add(t, svc, f, doc("catalog", "teacher", "teacher"))
	for i := 0; i < 4; i++ {
		d := doc("reviews", "review", fmt.Sprint(i))
		d.CourseID = "a"
		d.TeacherIDs = []string{"teacher"}
		d.Ratings = [5]int{1, 1, 0, 1, 1}
		if i == 3 {
			d.CourseID = "b"
			d.Ratings = [5]int{5, 5, 0, 5, 5}
		}
		add(t, svc, f, d)
	}
	from, to := now.Add(-24*time.Hour), now
	v, e := svc.Statistics(ctx, "u", "teacher", "teacher", from, to)
	if e != nil || v.Samples != 4 || v.Courses != 2 || v.Dimensions[0].Level != "level_3" || v.Dimensions[2].State != "no_reviews" {
		t.Fatal(v, e)
	}
	v, e = svc.Statistics(ctx, "u", "course", "empty", from, to)
	if e != nil || v.Samples != 0 || v.Dimensions[0].State != "no_reviews" {
		t.Fatal(v, e)
	}
	v, e = svc.Statistics(ctx, "u", "course", "b", from, to)
	if e != nil || v.Dimensions[0].State != "insufficient" || v.Dimensions[0].Level != "" {
		t.Fatal(v, e)
	}
	v, e = svc.Statistics(ctx, "u", "teacher", "teacher", now.Add(-30*time.Minute), now)
	if e != nil || v.Samples != 0 {
		t.Fatal(v, e)
	}
	f.denied["u/0"] = true
	v, e = svc.Statistics(ctx, "u", "course", "a", from, to)
	if e != nil || v.Samples != 2 {
		t.Fatal(v, e)
	}
	comparison, e := svc.Compare(ctx, "u", []string{"b", "a"}, from, to)
	if e != nil || comparison.Items[0].Target.ID != "b" {
		t.Fatal(comparison, e)
	}
	f.denied["u/a"] = true
	if _, e = svc.Compare(ctx, "u", []string{"a", "b"}, from, to); !errors.Is(e, s.ErrNotFound) {
		t.Fatal(e)
	}
	v, e = svc.Statistics(ctx, "u", "teacher", "teacher", from, to)
	if e != nil || v.Samples != 1 {
		t.Fatal("hidden course counted", v, e)
	}
}
func TestMyVotesHTTPAndTimeout(t *testing.T) {
	svc, _, f, _ := setup(t)
	d := doc("reviews", "review", "review")
	d.CourseID = "course"
	add(t, svc, f, d)
	f.votes["u"] = []s.Vote{{Target: d.Target, Choice: "up"}}
	f.votes["other"] = []s.Vote{{Target: d.Target, Choice: "down"}}
	v, e := svc.MyVotes(context.Background(), "u", "", 20)
	if e != nil || len(v.Items) != 1 || v.Items[0].Choice != "up" {
		t.Fatal(v, e)
	}
	f.denied["u/review"] = true
	v, e = svc.MyVotes(context.Background(), "u", "", 20)
	if e != nil || len(v.Items) != 0 {
		t.Fatal(v, e)
	}
	h := httpapi.Handler{Service: svc, Authenticate: func(r *http.Request) (string, error) { return r.Header.Get("X-Test-Principal"), nil }}
	for _, tc := range []struct {
		path, user string
		code       int
	}{{"/api/v1/search", "", 401}, {"/api/v1/search?limit=101", "u", 400}, {"/api/v1/search?limit=1&limit=2", "u", 400}, {"/api/v1/search", "u", 200}, {"/api/v1/search/statistics?kind=course&id=x&from=bad", "u", 400}} {
		req := httptest.NewRequest("GET", tc.path, nil)
		req.Header.Set("X-Test-Principal", tc.user)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != tc.code {
			t.Fatal(tc, w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "ratings") {
			t.Fatal("raw scores exposed")
		}
	}
	f.slow = true
	start := time.Now()
	_, e = svc.Search(context.Background(), "u", "", "", "", 20)
	if !errors.Is(e, context.DeadlineExceeded) || time.Since(start) > 2*time.Second {
		t.Fatal("query timeout", e)
	}
}

func TestRatingBoundariesAndProjectionFailureRollback(t *testing.T) {
	svc, _, f, db := setup(t)
	ctx := context.Background()
	for level := 1; level <= 4; level++ {
		course := fmt.Sprint("boundary", level)
		add(t, svc, f, doc("catalog", "course", course))
		for j := 0; j < 2; j++ {
			d := doc("reviews", "review", fmt.Sprintf("%s-%d", course, j))
			d.CourseID = course
			d.Ratings = [5]int{level + j, 0, 0, 0, 0}
			add(t, svc, f, d)
		}
		v, e := svc.Statistics(ctx, "u", "course", course, now.Add(-24*time.Hour), now)
		if e != nil || v.Dimensions[0].Level != fmt.Sprint("level_", level+1) {
			t.Fatal(v, e)
		}
	}
	// Failure after the projection upsert but before inbox insertion rolls back both writes.
	if _, e := db.Exec(`ALTER TABLE search.inbox ADD CONSTRAINT reject_failure CHECK(id <> 'injected-failure')`); e != nil {
		t.Fatal(e)
	}
	d := doc("catalog", "course", "rolled-back")
	if e := svc.Consume(ctx, event(d, "injected-failure")); e == nil {
		t.Fatal("expected transaction failure")
	}
	var n int
	if e := db.QueryRow("SELECT count(*) FROM search.documents WHERE id='rolled-back'").Scan(&n); e != nil || n != 0 {
		t.Fatal("partial projection", n, e)
	}
	d.Ratings[0] = 6
	if e := svc.Consume(ctx, event(d, "invalid")); !errors.Is(e, s.ErrInput) {
		t.Fatal(e)
	}
}
