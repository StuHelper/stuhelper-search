package search_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	s "github.com/StuHelper/stuhelper-search"
	"math"
	"sort"
	"testing"
	"time"
)

type countingSource struct {
	*source
	currentCalls       int
	activityCallsBySrc map[string]int
}

func (c *countingSource) DiscoverySnapshot(ctx context.Context, src, cursor string, limit int) ([]s.Document, string, error) {
	if c.fail {
		return nil, "", errors.New("snapshot unavailable")
	}
	docs := []s.Document{}
	for _, d := range c.docs {
		if d.Target.Source == src && d.Target.ID > cursor {
			docs = append(docs, d)
		}
	}
	sort.Slice(docs, func(i, j int) bool { return docs[i].Target.ID < docs[j].Target.ID })
	if len(docs) > limit {
		return docs[:limit], docs[limit-1].Target.ID, nil
	}
	return docs, "", nil
}

func (c *countingSource) Current(ctx context.Context, user string, t s.Target) (s.Document, bool, error) {
	c.currentCalls++
	return c.source.Current(ctx, user, t)
}

func (c *countingSource) CurrentActivity(ctx context.Context, user, src, id string) (s.ActivitySnapshot, bool, error) {
	c.activityCallsBySrc[src]++
	return c.source.CurrentActivity(ctx, user, src, id)
}

type batchCountingSource struct {
	*countingSource
	batchCurrentCalls  int
	batchCurrentItems  int
	batchActivityCalls map[string]int
	maxBatchSizeSeen   int
	injectBadTarget    bool
}

func (b *batchCountingSource) CurrentBatch(ctx context.Context, user, src string, targets []s.Target) ([]s.Document, error) {
	b.batchCurrentCalls++
	b.batchCurrentItems += len(targets)
	if len(targets) > b.maxBatchSizeSeen {
		b.maxBatchSizeSeen = len(targets)
	}
	if b.injectBadTarget {
		return []s.Document{doc("catalog", "course", "unrequested")}, nil
	}
	out := make([]s.Document, 0, len(targets))
	for _, t := range targets {
		d, ok, err := b.source.Current(ctx, user, t)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, d)
		}
	}
	return out, nil
}

func (b *batchCountingSource) CurrentActivities(ctx context.Context, user, src string, ids []string) ([]s.ActivitySnapshot, error) {
	b.batchActivityCalls[src]++
	if len(ids) > b.maxBatchSizeSeen {
		b.maxBatchSizeSeen = len(ids)
	}
	if b.disabled[src] {
		return nil, s.ErrSourceDisabled
	}
	if b.activityFail == src {
		return nil, s.ErrUnavailable
	}
	out := make([]s.ActivitySnapshot, 0, len(ids))
	for _, id := range ids {
		d, ok := b.activities[src+"/"+id]
		if ok && !b.denied[user+"/"+id] {
			out = append(out, d)
		}
	}
	return out, nil
}

// DS-01, DS-05: Popular keyset cursor survives anchor score updates and removals while rejecting invalid cursors.
func TestPopularKeysetCursorSurvivesScoreChangeAndRemoval(t *testing.T) {
	svc, repo, f, _ := setup(t)
	ctx := context.Background()

	// Empty leaderboard still rejects syntactically invalid cursors.
	for _, raw := range []string{"not-a-cursor", "NaN:catalog/a", "+Inf:catalog/a", "-1:catalog/a", "10:reviews/a", "10:catalog/"} {
		bad := base64.RawURLEncoding.EncodeToString([]byte(raw))
		if _, err := svc.Popular(ctx, "u", "course", bad, 20); !errors.Is(err, s.ErrInput) {
			t.Fatalf("cursor %q expected ErrInput on empty leaderboard, got %v", raw, err)
		}
	}

	// Huge weight must be rejected at construction time so scores stay finite.
	if _, err := s.New(repo, f, s.Config{
		QueryTimeout:       time.Second,
		HeatWindow:         7 * 24 * time.Hour,
		HeatWeights:        [4]float64{math.MaxFloat64, 1, 1, 1},
		MinimumHeatSamples: 1,
		Now:                func() time.Time { return now },
	}); !errors.Is(err, s.ErrInput) {
		t.Fatalf("expected ErrInput for excessive HeatWeights, got %v", err)
	}

	scores := map[string]int64{"course-a": 30, "course-b": 20, "course-c": 10, "course-d": 10}
	for id, views := range scores {
		add(t, svc, f, doc("catalog", "course", id))
		addActivity(t, svc, f, activity("catalog", "course", id, "2026-11-09", map[string]int64{"views": views}))
	}

	page1, err := svc.Popular(ctx, "u", "course", "", 2)
	if err != nil || len(page1.Items) != 2 || page1.Items[0].Target.ID != "course-a" || page1.Items[1].Target.ID != "course-b" || page1.NextCursor == "" {
		t.Fatalf("unexpected page1: %+v err=%v", page1, err)
	}

	// Anchor course-b receives new views before the user loads page 2.
	updated := activity("catalog", "course", "course-b", "2026-11-09", map[string]int64{"views": 25})
	updated.Revision = 2
	addActivity(t, svc, f, updated)

	page2, err := svc.Popular(ctx, "u", "course", page1.NextCursor, 2)
	if err != nil {
		t.Fatalf("page2 failed after anchor score update: %v", err)
	}
	if len(page2.Items) != 2 || page2.Items[0].Target.ID != "course-c" || page2.Items[1].Target.ID != "course-d" {
		t.Fatalf("unexpected page2 after score update: %+v", page2)
	}

	// Anchor course-b is later hidden/revoked; page1 cursor must still continue cleanly.
	f.denied["u/course-b"] = true
	page2AfterHide, err := svc.Popular(ctx, "u", "course", page1.NextCursor, 1)
	if err != nil || len(page2AfterHide.Items) != 1 || page2AfterHide.Items[0].Target.ID != "course-c" || page2AfterHide.NextCursor == "" {
		t.Fatalf("unexpected page2 after anchor revocation: %+v err=%v", page2AfterHide, err)
	}
	page3, err := svc.Popular(ctx, "u", "course", page2AfterHide.NextCursor, 1)
	if err != nil || len(page3.Items) != 1 || page3.Items[0].Target.ID != "course-d" {
		t.Fatalf("unexpected tie-broken page3: %+v err=%v", page3, err)
	}
	endPage, err := svc.Popular(ctx, "u", "course", page3.NextCursor, 1)
	if err != nil || len(endPage.Items) != 0 || endPage.NextCursor != "" {
		t.Fatalf("expected empty terminal page, got %+v err=%v", endPage, err)
	}
}

// DS-05: Popular supports >10,000 daily snapshots across campus-scale courses, skips disabled sources, and batches source checks.
func TestPopularCampusScaleDisabledSourceAndLazyCurrent(t *testing.T) {
	_, repo, f, db := setup(t)
	ctx := context.Background()

	cs := &countingSource{
		source:             f,
		activityCallsBySrc: map[string]int{},
	}
	svc, err := s.New(repo, cs, s.Config{
		QueryTimeout:       5 * time.Second,
		HeatWindow:         7 * 24 * time.Hour,
		HeatWeights:        [4]float64{1, 2, 3, 4},
		MinimumHeatSamples: 1,
		Now:                func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}

	const courses = 480
	days := []string{"2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06", "2026-11-07", "2026-11-08", "2026-11-09"}
	for i := 0; i < courses; i++ {
		id := fmt.Sprintf("course-%03d", i)
		f.docs[s.Target{Source: "catalog", Kind: "course", ID: id}] = doc("catalog", "course", id)
		for _, day := range days {
			cSnap := activity("catalog", "course", id, day, map[string]int64{"views": int64(500 - i)})
			rSnap := activity("reviews", "course", id, day, map[string]int64{"reviews": 1, "interactions": 1})
			favSnap := activity("favorites", "course", id, day, map[string]int64{"favorites": 1})
			f.activities[cSnap.Source+"/"+cSnap.ID] = cSnap
			f.activities[rSnap.Source+"/"+rSnap.ID] = rSnap
			f.activities[favSnap.Source+"/"+favSnap.ID] = favSnap
		}
	}
	if err := svc.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	var totalRows int
	if err := db.QueryRow("SELECT count(*) FROM search.activities WHERE (data->>'visible')::boolean").Scan(&totalRows); err != nil || totalRows != courses*len(days)*3 {
		t.Fatalf("expected %d activity snapshots, got %d err=%v", courses*len(days)*3, totalRows, err)
	}

	// 1. Legacy single-item Sources fallback: disabled source skipped after 1 call, Current called only 20 times.
	f.disabled["favorites"] = true
	cs.currentCalls = 0
	cs.activityCallsBySrc = map[string]int{}

	page, err := svc.Popular(ctx, "u", "course", "", 20)
	if err != nil {
		t.Fatalf("Popular failed on %d snapshots (%d courses): %v", totalRows, courses, err)
	}
	if len(page.Items) != 20 || page.Items[0].Target.ID != "course-000" || page.Items[19].Target.ID != "course-019" {
		t.Fatalf("unexpected top 20 items: count=%d", len(page.Items))
	}
	if cs.activityCallsBySrc["favorites"] != 1 {
		t.Fatalf("disabled source queried %d times, expected 1", cs.activityCallsBySrc["favorites"])
	}
	if cs.currentCalls != 20 {
		t.Fatalf("Current called %d times for limit=20; expected 20", cs.currentCalls)
	}

	// 2. Batch extension interfaces: <=100 per batch, 1 CurrentBatch call for top 20, unsolicited targets rejected.
	bs := &batchCountingSource{
		countingSource:     cs,
		batchActivityCalls: map[string]int{},
	}
	batchSvc, err := s.New(repo, bs, s.Config{
		QueryTimeout:       5 * time.Second,
		HeatWindow:         7 * 24 * time.Hour,
		HeatWeights:        [4]float64{1, 2, 3, 4},
		MinimumHeatSamples: 1,
		Now:                func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err = batchSvc.Popular(ctx, "u", "course", "", 20)
	if err != nil || len(page.Items) != 20 || page.Items[0].Target.ID != "course-000" {
		t.Fatalf("batch Popular failed: %+v err=%v", page, err)
	}
	if bs.batchActivityCalls["favorites"] != 1 {
		t.Fatalf("disabled batch source called %d times, expected 1", bs.batchActivityCalls["favorites"])
	}
	if bs.batchCurrentCalls != 1 || bs.batchCurrentItems != 20 {
		t.Fatalf("expected 1 CurrentBatch call of 20 items, got calls=%d items=%d", bs.batchCurrentCalls, bs.batchCurrentItems)
	}
	if bs.maxBatchSizeSeen > 100 {
		t.Fatalf("batch size %d exceeded 100-item contract", bs.maxBatchSizeSeen)
	}

	bs.injectBadTarget = true
	if _, err := batchSvc.Popular(ctx, "u", "course", "", 20); !errors.Is(err, s.ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable for unrequested batch target, got %v", err)
	}
}

func TestPopularFixedSnapshotRenewalExpiryAndRevocation(t *testing.T) {
	svc, repo, f, _ := setup(t)
	clock := now
	cfg := s.Config{QueryTimeout: time.Second, HeatWindow: 7 * 24 * time.Hour, HeatWeights: [4]float64{1, 1, 1, 1}, MinimumHeatSamples: 1, Now: func() time.Time { return clock }}
	svc, err := s.New(repo, f, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for idx, id := range []string{"a", "b", "c", "d"} {
		add(t, svc, f, doc("catalog", "course", id))
		addActivity(t, svc, f, activity("catalog", "course", id, "2026-11-09", map[string]int64{"views": int64(40 - 10*idx)}))
	}
	first, err := svc.Popular(ctx, "u", "course", "", 1)
	if err != nil || first.Items[0].Target.ID != "a" {
		t.Fatalf("first: %+v %v", first, err)
	}
	for id, views := range map[string]int64{"a": 1, "c": 60} {
		v := activity("catalog", "course", id, "2026-11-09", map[string]int64{"views": views})
		v.Revision++
		addActivity(t, svc, f, v)
	}
	fresh, err := svc.Popular(ctx, "u", "course", "", 1)
	if err != nil || fresh.Items[0].Target.ID != "c" {
		t.Fatalf("refresh: %+v %v", fresh, err)
	}
	f.denied["u/b"] = true
	clock = clock.Add(23 * time.Hour)
	cfg.HeatWindow = 30 * 24 * time.Hour
	svc, err = s.New(repo, f, cfg)
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.Popular(ctx, "u", "course", first.NextCursor, 1)
	if err != nil || len(second.Items) != 1 || second.Items[0].Target.ID != "c" || second.WindowDays != 7 {
		t.Fatalf("fixed order with revoked b: %+v %v", second, err)
	}
	if _, err = svc.Popular(ctx, "other", "course", second.NextCursor, 1); !errors.Is(err, s.ErrInput) {
		t.Fatalf("cursor principal: %v", err)
	}
	clock = clock.Add(23 * time.Hour)
	third, err := svc.Popular(ctx, "u", "course", second.NextCursor, 5)
	if err != nil || len(third.Items) != 1 || third.Items[0].Target.ID != "d" {
		t.Fatalf("renewed snapshot: %+v %v", third, err)
	}
	clock = clock.Add(24 * time.Hour)
	if _, err = svc.Popular(ctx, "u", "course", second.NextCursor, 1); !errors.Is(err, s.ErrSnapshotExpired) {
		t.Fatalf("expired snapshot: %v", err)
	}
}
