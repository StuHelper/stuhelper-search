package search_test

import (
	"context"
	"fmt"
	s "github.com/StuHelper/stuhelper-search"
	"github.com/StuHelper/stuhelper-search/httpapi"
	"github.com/StuHelper/stuhelper-search/sdk"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestSDKEndToEndContractAndPopularSnapshotPagination(t *testing.T) {
	svc, _, f, _ := setup(t)
	ctx := context.Background()

	var mu sync.RWMutex
	for i, views := range []int64{30, 20, 10} {
		id := fmt.Sprintf("course-%d", i+1)
		d := doc("catalog", "course", id)
		d.Title = fmt.Sprintf("课程 %d", i+1)
		d.Code = fmt.Sprintf("C00%d", i+1)
		add(t, svc, f, d)
		addActivity(t, svc, f, activity("catalog", "course", id, "2026-11-09", map[string]int64{"views": views}))
	}

	review := doc("reviews", "review", "rev-sdk")
	review.CourseID = "course-1"
	add(t, svc, f, review)
	f.votes["u"] = []s.Vote{{Target: review.Target, Choice: "up"}}

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

	// 1. Search via generated Go SDK.
	q := "C001"
	kind := sdk.SearchParamsKindCourse
	searchRes, err := client.SearchWithResponse(ctx, &sdk.SearchParams{Q: &q, Kind: &kind})
	if err != nil || searchRes.StatusCode() != http.StatusOK || searchRes.JSON200 == nil || len(searchRes.JSON200.Items) != 1 || searchRes.JSON200.Items[0].Target.Id != "course-1" {
		t.Fatalf("unexpected SDK search response: err=%v status=%d body=%s", err, searchRes.StatusCode(), string(searchRes.Body))
	}

	// 2. MyVotes via generated Go SDK.
	votesRes, err := client.MyVotesWithResponse(ctx, nil)
	if err != nil || votesRes.StatusCode() != http.StatusOK || votesRes.JSON200 == nil || len(votesRes.JSON200.Items) != 1 || votesRes.JSON200.Items[0].Choice != sdk.Up {
		t.Fatalf("unexpected SDK my-votes response: err=%v status=%d", err, votesRes.StatusCode())
	}

	// 3. Popular dynamic pagination: fetch page 1 (limit=1 -> course-1 with score 30), then mutate course-1's score to 5 before fetching page 2!
	limit := 1
	popPage1, err := client.PopularWithResponse(ctx, &sdk.PopularParams{
		Kind:  sdk.PopularParamsKindCourse,
		Limit: &limit,
	})
	if err != nil || popPage1.StatusCode() != http.StatusOK || popPage1.JSON200 == nil || len(popPage1.JSON200.Items) != 1 || popPage1.JSON200.Items[0].Target.Id != "course-1" || popPage1.JSON200.NextCursor == "" {
		t.Fatalf("unexpected popular page 1: err=%v %+v", err, popPage1.JSON200)
	}

	// Simulate new views arriving on course-1 between page 1 and page 2 so its score changes from 30 to 5.
	mu.Lock()
	updatedAct := activity("catalog", "course", "course-1", "2026-11-09", map[string]int64{"views": 5})
	updatedAct.Revision = 2
	addActivity(t, svc, f, updatedAct)
	mu.Unlock()

	cursor1 := popPage1.JSON200.NextCursor
	popPage2, err := client.PopularWithResponse(ctx, &sdk.PopularParams{
		Kind:   sdk.PopularParamsKindCourse,
		Cursor: &cursor1,
		Limit:  &limit,
	})
	if err != nil || popPage2.StatusCode() != http.StatusOK || popPage2.JSON200 == nil || len(popPage2.JSON200.Items) != 1 || popPage2.JSON200.Items[0].Target.Id != "course-2" {
		t.Fatalf("snapshot popular page 2 failed: err=%v status=%d body=%s", err, popPage2.StatusCode(), string(popPage2.Body))
	}

	cursor2 := popPage2.JSON200.NextCursor
	popPage3, err := client.PopularWithResponse(ctx, &sdk.PopularParams{
		Kind:   sdk.PopularParamsKindCourse,
		Cursor: &cursor2,
		Limit:  &limit,
	})
	if err != nil || popPage3.StatusCode() != http.StatusOK || popPage3.JSON200 == nil || len(popPage3.JSON200.Items) != 1 || popPage3.JSON200.Items[0].Target.Id != "course-3" {
		t.Fatalf("snapshot popular page 3 failed: err=%v status=%d body=%s", err, popPage3.StatusCode(), string(popPage3.Body))
	}

	// Malformed cursor still returns 400 invalid_input.
	badCursor := "aW52YWxpZA"
	badPop, err := client.PopularWithResponse(ctx, &sdk.PopularParams{
		Kind:   sdk.PopularParamsKindCourse,
		Cursor: &badCursor,
	})
	if err != nil || badPop.StatusCode() != http.StatusBadRequest {
		t.Fatalf("expected 400 for malformed popular cursor, got err=%v status=%d", err, badPop.StatusCode())
	}
}

func TestSharedAdvisoryLockAllowsConcurrentMultiSourceWrites(t *testing.T) {
	svc, _, f, db := setup(t)
	ctx := context.Background()

	// Hold a shared advisory lock in a separate transaction to simulate an in-flight event write from another source.
	holdTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer holdTx.Rollback()
	if _, err := holdTx.ExecContext(ctx, "SELECT pg_advisory_xact_lock_shared(726391840130)"); err != nil {
		t.Fatal(err)
	}

	// Concurrent Consume and ConsumeActivity across all 4 sources must complete without blocking on holdTx.
	course := doc("catalog", "course", "concurrent-course")
	review := doc("reviews", "review", "concurrent-review")
	review.CourseID = "concurrent-course"
	resource := doc("resources", "resource", "concurrent-resource")
	f.docs[course.Target] = course
	f.docs[review.Target] = review
	f.docs[resource.Target] = resource

	favAct := activity("favorites", "course", "concurrent-course", "2026-11-09", map[string]int64{"favorites": 2})
	f.activities[favAct.Source+"/"+favAct.ID] = favAct

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for _, fn := range []func() error{
		func() error { return svc.Consume(ctx, event(course, "ev-cat")) },
		func() error { return svc.Consume(ctx, event(review, "ev-rev")) },
		func() error { return svc.Consume(ctx, event(resource, "ev-res")) },
		func() error { return svc.ConsumeActivity(ctx, activityEvent(favAct, "ev-fav")) },
	} {
		wg.Add(1)
		go func(work func() error) {
			defer wg.Done()
			errs <- work()
		}(fn)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent multi-source write blocked or failed: %v", err)
		}
	}

	// Exclusive Rebuild must wait until holdTx releases the shared lock; releasing shortly lets Rebuild succeed atomically.
	go func() {
		time.Sleep(40 * time.Millisecond)
		_ = holdTx.Rollback()
	}()
	if err := svc.Rebuild(ctx); err != nil {
		t.Fatalf("rebuild after shared lock release failed: %v", err)
	}
}
