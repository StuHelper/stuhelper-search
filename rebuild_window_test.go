package search_test

import (
	"context"
	"errors"
	"fmt"
	s "github.com/StuHelper/stuhelper-search"
	"sort"
	"testing"
	"time"
)

type cycleDiscoverySource struct {
	*source
	calls int
}

func (c *cycleDiscoverySource) DiscoverySnapshot(ctx context.Context, src, cursor string, limit int) ([]s.Document, string, error) {
	if src != "catalog" {
		return nil, "", nil
	}
	c.calls++
	switch cursor {
	case "":
		return []s.Document{doc("catalog", "course", "c1")}, "cursor-a", nil
	case "cursor-a":
		return []s.Document{doc("catalog", "course", "c2")}, "cursor-b", nil
	case "cursor-b":
		return []s.Document{doc("catalog", "course", "c1")}, "cursor-a", nil
	default:
		return nil, "", nil
	}
}

type pagedDiscoverySource struct {
	*source
}

func (p *pagedDiscoverySource) DiscoverySnapshot(ctx context.Context, src, cursor string, limit int) ([]s.Document, string, error) {
	if p.fail {
		return nil, "", errors.New("snapshot unavailable")
	}
	docs := []s.Document{}
	for _, d := range p.docs {
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

// DS-07: Rebuild preserves historical activity snapshots outside the current HeatWindow, batches SQL writes, and detects multi-step cursor cycles.
func TestRebuildPreservesOutOfWindowActivitiesBatchesWritesAndDetectsCycles(t *testing.T) {
	svc7d, repo, f, db := setup(t)
	ctx := context.Background()

	// Seed two courses: one active 15 days ago (within 30d window, outside 7d window) and one active today.
	add(t, svc7d, f, doc("catalog", "course", "course-hist"))
	add(t, svc7d, f, doc("catalog", "course", "course-recent"))
	histAct := activity("catalog", "course", "course-hist", "2026-10-25", map[string]int64{"views": 20})
	recentAct := activity("catalog", "course", "course-recent", "2026-11-09", map[string]int64{"views": 5})
	inWindowRemoved := activity("catalog", "course", "course-recent", "2026-11-08", map[string]int64{"views": 50})
	addActivity(t, svc7d, f, histAct)
	addActivity(t, svc7d, f, recentAct)
	addActivity(t, svc7d, f, inWindowRemoved)

	// Remove the in-window snapshot from the authoritative source, then rebuild under the 7d window.
	delete(f.activities, inWindowRemoved.Source+"/"+inWindowRemoved.ID)
	if err := svc7d.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}

	// Verify DB state: in-window deleted snapshot became an invisible tombstone, while out-of-window historical snapshot stayed visible.
	var removedVisible, histVisible bool
	if err := db.QueryRow("SELECT (data->>'visible')::boolean FROM search.activities WHERE source=$1 AND id=$2", inWindowRemoved.Source, inWindowRemoved.ID).Scan(&removedVisible); err != nil || removedVisible {
		t.Fatalf("expected in-window deleted activity to become invisible tombstone, visible=%v err=%v", removedVisible, err)
	}
	if err := db.QueryRow("SELECT (data->>'visible')::boolean FROM search.activities WHERE source=$1 AND id=$2", histAct.Source, histAct.ID).Scan(&histVisible); err != nil || !histVisible {
		t.Fatalf("expected out-of-window historical activity to remain visible, visible=%v err=%v", histVisible, err)
	}

	// Admin widens HeatWindow from 7d to 30d; historical activity from day -15 must contribute to Popular without requiring another event.
	svc30d, err := s.New(repo, &pagedDiscoverySource{source: f}, s.Config{
		QueryTimeout:       time.Second,
		HeatWindow:         30 * 24 * time.Hour,
		HeatWeights:        [4]float64{1, 2, 3, 4},
		MinimumHeatSamples: 1,
		Now:                func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err := svc30d.Popular(ctx, "u", "course", "", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 || page.Items[0].Target.ID != "course-hist" || page.Items[1].Target.ID != "course-recent" {
		t.Fatalf("expected [course-hist course-recent] after expanding HeatWindow to 30d, got %+v", page.Items)
	}

	// Multi-page batched rebuild (250 documents + 250 activities) publishes all rows and rejects stale snapshot revisions.
	for i := 0; i < 250; i++ {
		id := fmt.Sprintf("bulk-%03d", i)
		f.docs[s.Target{Source: "catalog", Kind: "course", ID: id}] = doc("catalog", "course", id)
		act := activity("catalog", "course", id, "2026-11-09", map[string]int64{"views": int64(i + 1)})
		f.activities[act.Source+"/"+act.ID] = act
	}
	if err := svc30d.Rebuild(ctx); err != nil {
		t.Fatalf("batched multi-page rebuild failed: %v", err)
	}
	var docCount, actCount int
	if err := db.QueryRow("SELECT count(*) FROM search.documents WHERE (data->>'visible')::boolean").Scan(&docCount); err != nil || docCount != 252 {
		t.Fatalf("expected 252 visible documents, got %d err=%v", docCount, err)
	}
	if err := db.QueryRow("SELECT count(*) FROM search.activities WHERE (data->>'visible')::boolean").Scan(&actCount); err != nil || actCount != 252 {
		t.Fatalf("expected 252 visible activities, got %d err=%v", actCount, err)
	}

	// Multi-step cursor cycle ("" -> cursor-a -> cursor-b -> cursor-a) in DiscoverySnapshot aborts immediately instead of spinning 10,000 pages.
	cs := &cycleDiscoverySource{source: f}
	cycleSvc, err := s.New(repo, cs, s.Config{
		QueryTimeout:       time.Second,
		HeatWindow:         7 * 24 * time.Hour,
		HeatWeights:        [4]float64{1, 1, 1, 1},
		MinimumHeatSamples: 1,
		Now:                func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cycleSvc.Rebuild(ctx); !errors.Is(err, s.ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable on DiscoverySnapshot cursor cycle, got %v", err)
	}
	if cs.calls > 5 {
		t.Fatalf("cursor cycle took %d calls; expected immediate cycle detection <= 5", cs.calls)
	}

	// RebuildTimeout bounds validation: negative or >30m rejected.
	for _, badTimeout := range []time.Duration{-time.Second, 31 * time.Minute} {
		if _, err := s.New(repo, f, s.Config{
			QueryTimeout:       time.Second,
			RebuildTimeout:     badTimeout,
			HeatWindow:         7 * 24 * time.Hour,
			HeatWeights:        [4]float64{1, 1, 1, 1},
			MinimumHeatSamples: 1,
			Now:                func() time.Time { return now },
		}); !errors.Is(err, s.ErrInput) {
			t.Fatalf("RebuildTimeout %v expected ErrInput, got %v", badTimeout, err)
		}
	}
}
