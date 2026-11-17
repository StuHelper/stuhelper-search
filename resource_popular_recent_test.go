package search_test

import (
	"context"
	"errors"
	s "github.com/StuHelper/stuhelper-search"
	"github.com/StuHelper/stuhelper-search/httpapi"
	"github.com/StuHelper/stuhelper-search/postgres"
	"github.com/StuHelper/stuhelper-search/sdk"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func resourceActivity(id, day string, favs int64) s.ActivitySnapshot {
	target := s.Target{Source: "resources", Kind: "resource", ID: id}
	return s.ActivitySnapshot{
		Source:   "favorites",
		ID:       s.ActivityID(target, day),
		Target:   target,
		Day:      day,
		Revision: 1,
		Visible:  true,
		Metrics:  map[string]int64{"favorites": favs},
	}
}

func TestPopularResourcesAndRecentDiscovery(t *testing.T) {
	svc, _, f, db := setup(t)
	ctx := context.Background()

	// 1. Resource activity ownership & schema migration preservation.
	courseAct := activity("catalog", "course", "existing-course", "2026-11-09", map[string]int64{"views": 2})
	add(t, svc, f, doc("catalog", "course", "existing-course"))
	addActivity(t, svc, f, courseAct)

	// Simulate pre-B07 database constraint and verify Migrate expands it while keeping existing rows.
	if _, err := db.Exec(`ALTER TABLE search.activities DROP CONSTRAINT activities_target_kind; ALTER TABLE search.activities ADD CONSTRAINT activities_kind_check CHECK (kind IN ('course','teacher'))`); err != nil {
		t.Fatal(err)
	}
	if err := postgres.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}

	// catalog and reviews cannot emit resource activity snapshots.
	for _, badSrc := range []string{"catalog", "reviews"} {
		bad := resourceActivity("res-1", "2026-11-09", 1)
		bad.Source = badSrc
		if badSrc == "catalog" {
			bad.Metrics = map[string]int64{"views": 1}
		} else {
			bad.Metrics = map[string]int64{"reviews": 1, "interactions": 0}
		}
		if err := svc.ConsumeActivity(ctx, activityEvent(bad, "bad-"+badSrc)); !errors.Is(err, s.ErrInput) {
			t.Fatalf("expected ErrInput for %s resource activity, got %v", badSrc, err)
		}
	}

	res1 := doc("resources", "resource", "res-1")
	res1.Title = "操作系统往年期末题"
	res1.CourseID = "existing-course"
	res1.At = time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	add(t, svc, f, res1)

	res2 := doc("resources", "resource", "res-2")
	res2.Title = "计算机网络复习笔记"
	res2.CourseID = "existing-course"
	// Same instant as 11:30 UTC represented in +08:00 timezone to verify true chronological ordering.
	res2.At = time.Date(2026, 11, 9, 19, 30, 0, 0, time.FixedZone("CST", 8*3600))
	add(t, svc, f, res2)

	tieReview := doc("reviews", "review", "rev-tie")
	tieReview.Title = "新开课程首条评价"
	tieReview.CourseID = "existing-course"
	tieReview.At = time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC) // Ties with res-1 on timestamp!
	add(t, svc, f, tieReview)

	addActivity(t, svc, f, resourceActivity("res-1", "2026-11-09", 8))
	addActivity(t, svc, f, resourceActivity("res-2", "2026-11-09", 3))

	if err := svc.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(httpapi.Handler{
		Service: svc,
		Authenticate: func(r *http.Request) (string, error) {
			return r.Header.Get("X-Test-Principal"), nil
		},
	})
	defer srv.Close()

	client, err := sdk.NewClientWithResponses(srv.URL, sdk.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
		req.Header.Set("X-Test-Principal", "u")
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	// 2. Popular kind=resource returns ranked visible resources and immediately respects access revocation.
	popRes, err := client.PopularWithResponse(ctx, &sdk.PopularParams{Kind: sdk.PopularParamsKindResource})
	if err != nil || popRes.StatusCode() != http.StatusOK || popRes.JSON200 == nil || len(popRes.JSON200.Items) != 2 {
		t.Fatalf("unexpected popular resource response: err=%v status=%d body=%s", err, popRes.StatusCode(), string(popRes.Body))
	}
	if popRes.JSON200.Items[0].Target.Id != "res-1" || popRes.JSON200.Items[1].Target.Id != "res-2" {
		t.Fatalf("unexpected popular resource order: %+v", popRes.JSON200.Items)
	}

	f.denied["u/res-1"] = true
	popAfterRevoke, err := client.PopularWithResponse(ctx, &sdk.PopularParams{Kind: sdk.PopularParamsKindResource})
	if err != nil || popAfterRevoke.StatusCode() != http.StatusOK || len(popAfterRevoke.JSON200.Items) != 1 || popAfterRevoke.JSON200.Items[0].Target.Id != "res-2" {
		t.Fatalf("revoked resource leaked in popular: %v %+v", err, popAfterRevoke.JSON200)
	}
	delete(f.denied, "u/res-1")

	// 3. Search sort=recent orders chronologically DESC with deterministic tie-breaking and cursor mode isolation.
	// Timestamps: res-2 (11:30Z) > existing-course (11:00Z) > res-1 (10:00Z, resources/res-1) > rev-tie (10:00Z, reviews/rev-tie).
	sortRecent := sdk.Recent
	limitOne := 1
	var gotRecent []string
	var cur *string
	for i := 0; i < 6; i++ {
		page, err := client.SearchWithResponse(ctx, &sdk.SearchParams{
			Sort:   &sortRecent,
			Cursor: cur,
			Limit:  &limitOne,
		})
		if err != nil || page.StatusCode() != http.StatusOK || page.JSON200 == nil {
			t.Fatalf("recent page %d failed: err=%v status=%d body=%s", i, err, page.StatusCode(), string(page.Body))
		}
		for _, item := range page.JSON200.Items {
			gotRecent = append(gotRecent, item.Target.Id)
		}
		if page.JSON200.NextCursor == "" {
			break
		}
		if cur == nil {
			// Cross-mode cursor check: passing a recent cursor to relevance mode must fail with 400.
			sortRel := sdk.Relevance
			recentCur := page.JSON200.NextCursor
			mismatch, err := client.SearchWithResponse(ctx, &sdk.SearchParams{Sort: &sortRel, Cursor: &recentCur})
			if err != nil || mismatch.StatusCode() != http.StatusBadRequest {
				t.Fatalf("expected 400 when using recent cursor in relevance mode, got %v %d", err, mismatch.StatusCode())
			}
		}
		next := page.JSON200.NextCursor
		cur = &next
	}
	wantRecent := []string{"res-2", "existing-course", "res-1", "rev-tie"}
	if len(gotRecent) != len(wantRecent) {
		t.Fatalf("recent pagination length mismatch: got %v, want %v", gotRecent, wantRecent)
	}
	for i := range wantRecent {
		if gotRecent[i] != wantRecent[i] {
			t.Fatalf("recent rank %d: got %s, want %s (all=%v)", i, gotRecent[i], wantRecent[i], gotRecent)
		}
	}

	// Passing a relevance cursor into sort=recent or passing an unknown sort parameter returns 400.
	relPage, err := svc.Search(ctx, "u", "", "", "", 1)
	if err != nil || relPage.NextCursor == "" {
		t.Fatalf("expected relevance cursor: %+v %v", relPage, err)
	}
	if _, err := svc.SearchSort(ctx, "u", "", "", "recent", relPage.NextCursor, 1); !errors.Is(err, s.ErrInput) {
		t.Fatalf("expected ErrInput for relevance cursor in recent sort, got %v", err)
	}
	if _, err := svc.SearchSort(ctx, "u", "", "", "invalid", "", 20); !errors.Is(err, s.ErrInput) {
		t.Fatalf("expected ErrInput for invalid sort mode, got %v", err)
	}
}
