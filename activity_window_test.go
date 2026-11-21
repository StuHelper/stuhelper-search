package search_test

import (
	"context"
	"errors"
	"testing"
	"time"

	s "github.com/StuHelper/stuhelper-search"
)

type recordingActivitySource struct {
	*source
	from, to time.Time
}

func (r *recordingActivitySource) ActivitySnapshot(ctx context.Context, source string, from, to time.Time, cursor string, limit int) ([]s.ActivitySnapshot, string, error) {
	r.from, r.to = from, to
	return r.source.ActivitySnapshot(ctx, source, from, to, cursor, limit)
}

func TestPopularBeijingCalendarWindow(t *testing.T) {
	for _, tc := range []struct {
		name, now, first, today, afterLast string
		days                               int
	}{
		{"just before midnight", "2026-03-31T15:59:59Z", "2026-03-31", "2026-03-31", "2026-04-01", 1},
		{"midnight enters next month", "2026-03-31T16:00:00Z", "2026-04-01", "2026-04-01", "2026-04-02", 1},
		{"30 dates cross a month", "2026-03-31T16:00:00Z", "2026-03-03", "2026-04-01", "2026-04-02", 30},
		{"366 dates cross leap day", "2024-03-01T00:00:00+08:00", "2023-03-02", "2024-03-01", "2024-03-02", 366},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, repo, f, _ := setup(t)
			ctx := context.Background()
			clock, err := time.Parse(time.RFC3339, tc.now)
			if err != nil {
				t.Fatal(err)
			}
			start, err := time.Parse(time.RFC3339, tc.first+"T00:00:00+08:00")
			if err != nil {
				t.Fatal(err)
			}
			stop, err := time.Parse(time.RFC3339, tc.afterLast+"T00:00:00+08:00")
			if err != nil {
				t.Fatal(err)
			}
			sources := &recordingActivitySource{source: f}
			svc, err := s.New(repo, sources, s.Config{
				QueryTimeout:       time.Second,
				HeatWindow:         time.Duration(tc.days) * 24 * time.Hour,
				HeatWeights:        [4]float64{1},
				MinimumHeatSamples: 1,
				Now:                func() time.Time { return clock },
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range []struct {
				id, day string
				views   int64
			}{
				{"expired", start.AddDate(0, 0, -1).Format(time.DateOnly), 100},
				{"first", tc.first, 2},
				{"today", tc.today, 3},
				{"future", tc.afterLast, 100},
			} {
				add(t, svc, f, doc("catalog", "course", row.id))
				addActivity(t, svc, f, activity("catalog", "course", row.id, row.day, map[string]int64{"views": row.views}))
			}

			if err := svc.Rebuild(ctx); err != nil {
				t.Fatal(err)
			}
			if !sources.from.Equal(start) || !sources.to.Equal(stop) {
				t.Fatalf("source window [%s, %s), want [%s, %s)", sources.from, sources.to, start, stop)
			}
			status, err := svc.Status(ctx)
			if err != nil || status.ActivityWindowFrom != tc.first || status.ActivityWindowTo != tc.afterLast {
				t.Fatalf("stored rebuild window: %+v %v", status, err)
			}
			page, err := svc.Popular(ctx, "u", "course", "", 10)
			if err != nil || page.WindowDays != tc.days || len(page.Items) != 2 || page.Items[0].Target.ID != "today" || page.Items[1].Target.ID != "first" {
				t.Fatalf("popular window: %+v %v", page, err)
			}
		})
	}
}

func TestLegacyActivityReplayCannotOverwriteBeijingCounters(t *testing.T) {
	svc, _, f, db := setup(t)
	ctx := context.Background()
	snapshot := activity("catalog", "course", "a", "2026-11-09", map[string]int64{"views": 7})
	addActivity(t, svc, f, snapshot)
	legacy := activityEvent(snapshot, "legacy-pending")
	legacy.Type = s.LegacyActivityEventType
	legacy.Data.Revision = 50
	legacy.Data.Metrics = map[string]int64{"views": 100}
	for range 2 {
		if err := svc.ConsumeActivity(ctx, legacy); err != nil {
			t.Fatalf("legacy delivery: %v", err)
		}
	}
	legacy.Data.Metrics["views"]++
	if err := svc.ConsumeActivity(ctx, legacy); !errors.Is(err, s.ErrConflict) {
		t.Fatalf("conflicting legacy delivery: %v", err)
	}
	var revision, views, receipts int
	if err := db.QueryRow(`SELECT revision, (data->'metrics'->>'views')::int,
		(SELECT count(*) FROM search.inbox WHERE id='legacy-pending' AND event_type=$1)
		FROM search.activities WHERE source='catalog' AND id=$2`, s.LegacyActivityEventType, snapshot.ID).Scan(&revision, &views, &receipts); err != nil || revision != 1 || views != 7 || receipts != 1 {
		t.Fatalf("legacy delivery changed projection or receipt: revision=%d views=%d receipts=%d err=%v", revision, views, receipts, err)
	}
}
