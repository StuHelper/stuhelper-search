package search_test

import (
	"context"
	"errors"
	"fmt"
	s "github.com/StuHelper/stuhelper-search"
	"github.com/StuHelper/stuhelper-search/httpapi"
	"github.com/StuHelper/stuhelper-search/sdk"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSummaryFieldsAndMultiDimensionalFilters(t *testing.T) {
	svc, _, f, _ := setup(t)
	ctx := context.Background()

	schoolA := doc("catalog", "school", "4111010006")
	schoolA.SchoolID = schoolA.Target.ID
	schoolA.Title = "北京航空航天大学"
	schoolA.Code = "10006"
	add(t, svc, f, schoolA)

	courseA := doc("catalog", "course", "course-a")
	courseA.SchoolID = "4111010006"
	courseA.Title = "工科数学分析"
	courseA.Code = "B3I06110"
	courseA.TeacherIDs = []string{"teacher-1", "teacher-2"}
	add(t, svc, f, courseA)

	courseB := doc("catalog", "course", "course-b")
	courseB.SchoolID = "4111010007"
	courseB.Title = "工科数学分析"
	courseB.Code = "M1001"
	courseB.TeacherIDs = []string{"teacher-3"}
	add(t, svc, f, courseB)

	reviewA := doc("reviews", "review", "review-a")
	reviewA.SchoolID = "4111010006"
	reviewA.Title = "讲解清晰，板书工整"
	reviewA.CourseID = "course-a"
	reviewA.TeacherIDs = []string{"teacher-1"}
	add(t, svc, f, reviewA)

	resourceA := doc("resources", "resource", "resource-a")
	resourceA.SchoolID = "4111010006"
	resourceA.Title = "工科数学分析历年期末题"
	resourceA.CourseID = "course-a"
	resourceA.TeacherIDs = []string{"teacher-2"}
	add(t, svc, f, resourceA)

	resourceOther := doc("resources", "resource", "resource-b")
	resourceOther.SchoolID = "4111010007"
	resourceOther.Title = "外校工科数学分析讲义"
	resourceOther.CourseID = "course-b"
	add(t, svc, f, resourceOther)

	addActivity(t, svc, f, activity("catalog", "course", "course-a", "2026-11-09", map[string]int64{"views": 5}))
	addActivity(t, svc, f, activity("catalog", "course", "course-b", "2026-11-09", map[string]int64{"views": 10}))

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

	// Filter search by school_id and teacher_id, verifying summary fields.
	schoolID := "4111010006"
	teacherID := "teacher-1"
	res, err := client.SearchWithResponse(ctx, &sdk.SearchParams{
		SchoolId:  &schoolID,
		TeacherId: &teacherID,
	})
	if err != nil || res.StatusCode() != http.StatusOK || res.JSON200 == nil {
		t.Fatalf("unexpected response: %v %d", err, res.StatusCode())
	}
	if len(res.JSON200.Items) != 2 {
		t.Fatalf("expected course-a and review-a, got %+v", res.JSON200.Items)
	}
	byID := map[string]sdk.Item{}
	for _, it := range res.JSON200.Items {
		if it.At == nil || it.At.IsZero() {
			t.Fatalf("missing item timestamp: %+v", it)
		}
		byID[it.Target.Id] = it
	}
	if got := byID["course-a"]; got.Code == nil || *got.Code != "B3I06110" || got.TeacherIds == nil || len(*got.TeacherIds) != 2 {
		t.Fatalf("unexpected course summary: %+v", got)
	}
	if got := byID["review-a"]; got.CourseId == nil || *got.CourseId != "course-a" || got.TeacherIds == nil || (*got.TeacherIds)[0] != "teacher-1" {
		t.Fatalf("unexpected review summary: %+v", got)
	}

	// Filter within a course detail page for reviews and resources.
	courseID := "course-a"
	courseScoped, err := client.SearchWithResponse(ctx, &sdk.SearchParams{CourseId: &courseID})
	if err != nil || courseScoped.StatusCode() != http.StatusOK || len(courseScoped.JSON200.Items) != 2 {
		t.Fatalf("unexpected course-scoped items: %v %+v", err, courseScoped.JSON200)
	}

	// Revoking access to resource-a immediately excludes it from filtered results.
	f.denied["u/resource-a"] = true
	courseScopedAfterRevoke, err := client.SearchWithResponse(ctx, &sdk.SearchParams{CourseId: &courseID})
	if err != nil || courseScopedAfterRevoke.StatusCode() != http.StatusOK || len(courseScopedAfterRevoke.JSON200.Items) != 1 || courseScopedAfterRevoke.JSON200.Items[0].Target.Id != "review-a" {
		t.Fatalf("revoked resource leaked: %v %+v", err, courseScopedAfterRevoke.JSON200)
	}

	// Filter popular courses by school_id.
	popRes, err := client.PopularWithResponse(ctx, &sdk.PopularParams{
		Kind:     sdk.PopularParamsKindCourse,
		SchoolId: &schoolID,
	})
	if err != nil || popRes.StatusCode() != http.StatusOK || len(popRes.JSON200.Items) != 1 || popRes.JSON200.Items[0].Target.Id != "course-a" {
		t.Fatalf("unexpected school popular response: %v %+v", err, popRes.JSON200)
	}

	// Reject malformed filter values.
	for _, bad := range []string{" ", " padded", strings.Repeat("x", 201), "bad\x00id"} {
		if _, err := svc.SearchWithParams(ctx, "u", s.SearchParams{SchoolID: bad, Limit: 20}); !errors.Is(err, s.ErrInput) {
			t.Fatalf("expected ErrInput for SchoolID %q, got %v", bad, err)
		}
		if _, err := svc.SearchWithParams(ctx, "u", s.SearchParams{CourseID: bad, Limit: 20}); !errors.Is(err, s.ErrInput) {
			t.Fatalf("expected ErrInput for CourseID %q, got %v", bad, err)
		}
		if _, err := svc.SearchWithParams(ctx, "u", s.SearchParams{TeacherID: bad, Limit: 20}); !errors.Is(err, s.ErrInput) {
			t.Fatalf("expected ErrInput for TeacherID %q, got %v", bad, err)
		}
		if _, err := svc.PopularWithParams(ctx, "u", s.PopularParams{Kind: "course", SchoolID: bad, Limit: 20}); !errors.Is(err, s.ErrInput) {
			t.Fatalf("expected ErrInput for Popular SchoolID %q, got %v", bad, err)
		}
	}
}

func TestMyVotesSummaryFieldsAndBoundedPageFill(t *testing.T) {
	svc, _, f, _ := setup(t)
	ctx := context.Background()

	// Create 6 reviews: 3 hidden/revoked and 3 visible, interleaved across upstream pages.
	visibleIDs := map[int]bool{1: true, 3: true, 5: true}
	var allVotes []s.Vote
	for i := 0; i < 6; i++ {
		id := fmt.Sprintf("rev-%d", i)
		d := doc("reviews", "review", id)
		d.Title = fmt.Sprintf("评课标题 %d", i)
		d.CourseID = fmt.Sprintf("course-%d", i)
		d.SchoolID = "4111010006"
		d.Visible = visibleIDs[i]
		add(t, svc, f, d)
		allVotes = append(allVotes, s.Vote{Target: d.Target, Choice: "up"})
	}
	f.votes["u"] = allVotes

	// Request limit=2: MyVotes should automatically scan past hidden entries to fill 2 visible votes.
	page1, err := svc.MyVotes(ctx, "u", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.Items) != 2 || page1.Items[0].Target.ID != "rev-1" || page1.Items[1].Target.ID != "rev-3" {
		t.Fatalf("unexpected page1: %+v", page1)
	}
	if page1.Items[0].Title != "评课标题 1" || page1.Items[0].CourseID != "course-1" || page1.Items[0].SchoolID != "4111010006" {
		t.Fatalf("missing vote summary metadata: %+v", page1.Items[0])
	}
	if page1.NextCursor == "" {
		t.Fatal("expected non-empty NextCursor after page1")
	}

	page2, err := svc.MyVotes(ctx, "u", page1.NextCursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Items) != 1 || page2.Items[0].Target.ID != "rev-5" || page2.NextCursor != "" {
		t.Fatalf("unexpected page2: %+v", page2)
	}

	// Verify 500-vote scan bound prevents unbounded upstream loops when hundreds of votes are hidden.
	var manyHidden []s.Vote
	for i := 0; i < 520; i++ {
		id := fmt.Sprintf("hidden-%03d", i)
		d := doc("reviews", "review", id)
		d.CourseID = "course-hidden"
		d.Visible = i >= 510
		f.docs[d.Target] = d
		manyHidden = append(manyHidden, s.Vote{Target: d.Target, Choice: "down"})
	}
	f.votes["u"] = manyHidden

	firstBatch, err := svc.MyVotes(ctx, "u", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstBatch.Items) != 0 || firstBatch.NextCursor == "" {
		t.Fatalf("expected bounded empty page with continuation cursor, got %+v", firstBatch)
	}
	secondBatch, err := svc.MyVotes(ctx, "u", firstBatch.NextCursor, 10)
	if err != nil || len(secondBatch.Items) != 10 || secondBatch.NextCursor != "" {
		t.Fatalf("expected remaining 10 visible votes on continuation, got %+v %v", secondBatch, err)
	}
}
