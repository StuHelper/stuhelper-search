package search_test

import (
	"context"
	"errors"
	s "github.com/StuHelper/stuhelper-search"
	"testing"
)

func TestMultiTermNFKCTeacherAliasesAndRelevanceRanking(t *testing.T) {
	svc, _, f, _ := setup(t)
	ctx := context.Background()

	school := doc("catalog", "school", "4111010006")
	school.SchoolID = school.Target.ID
	school.Title = "北京航空航天大学"
	school.Code = "10006"
	school.Aliases = []string{"北航", "Beihang"}
	add(t, svc, f, school)

	teacher := doc("catalog", "teacher", "teacher-wang")
	teacher.Title = "王小明"
	teacher.Aliases = []string{"wxm", "王老师"}
	add(t, svc, f, teacher)

	// Insert documents whose raw IDs are intentionally reversed relative to relevance buckets:
	// 0-alias (bucket 3) < 1-review-contains (bucket 2, review) < 2-resource-contains (bucket 2, resource) < 3-prefix (bucket 1) < 4-exact (bucket 0).
	aliasCourse := doc("catalog", "course", "0-alias")
	aliasCourse.Title = "工科数学分析"
	aliasCourse.Code = "B3I06110"
	aliasCourse.Aliases = []string{"工数", "gksxfx"}
	add(t, svc, f, aliasCourse)

	containsReview := doc("reviews", "review", "1-review-contains")
	containsReview.Title = "高等工数期末复习体会"
	containsReview.CourseID = "0-alias"
	add(t, svc, f, containsReview)

	containsResource := doc("resources", "resource", "2-resource-contains")
	containsResource.Title = "高等工数 2025 期末试卷"
	add(t, svc, f, containsResource)

	prefixCourse := doc("catalog", "course", "3-prefix")
	prefixCourse.Title = "工数导论"
	add(t, svc, f, prefixCourse)

	exactCourse := doc("catalog", "course", "4-exact")
	exactCourse.Title = "工数"
	add(t, svc, f, exactCourse)

	frenchCourse := doc("catalog", "course", "5-french")
	frenchCourse.Title = "École Centrale C语言程序设计"
	frenchCourse.EnglishName = "Introduction to Computer Systems and Networks"
	add(t, svc, f, frenchCourse)

	// 1. Teacher pinyin/alias search and validation boundary.
	for _, q := range []string{"wxm", "WXM", "王老师", "wxm 王小明"} {
		page, err := svc.Search(ctx, "u", q, "teacher", "", 20)
		if err != nil || len(page.Items) != 1 || page.Items[0].Target != teacher.Target {
			t.Fatalf("teacher alias query %q failed: %+v %v", q, page, err)
		}
	}
	badTeacher := teacher
	badTeacher.Code = "EMP001"
	if err := s.Validate(badTeacher); !errors.Is(err, s.ErrInput) {
		t.Fatalf("expected teacher code to be rejected, got %v", err)
	}

	// 2. Fullwidth NFKC normalization, multilingual case-folding, and >5-word query handling.
	for _, tc := range []struct {
		query string
		kind  string
		want  string
	}{
		{"　１０００６　", "school", "4111010006"},
		{"北航 １０００６", "school", "4111010006"},
		{"Ｂ３Ｉ０６１１０", "course", "0-alias"},
		{"gksxfx 工科", "course", "0-alias"},
		{"école Ｃ语言", "course", "5-french"},
		{"Introduction to Computer Systems and Networks", "course", "5-french"},
		{"工数 期末", "", "2-resource-contains"},
	} {
		page, err := svc.Search(ctx, "u", tc.query, tc.kind, "", 20)
		if err != nil || len(page.Items) == 0 || page.Items[0].Target.ID != tc.want {
			t.Fatalf("query %q (%s): got %+v err=%v, want first=%s", tc.query, tc.kind, page, err, tc.want)
		}
	}

	// 3. Relevance bucket ordering and single-item keyset pagination across all buckets & kind priorities.
	wantOrder := []string{"4-exact", "3-prefix", "2-resource-contains", "1-review-contains", "0-alias"}
	var gotOrder []string
	cursor := ""
	for step := 0; step < 10; step++ {
		page, err := svc.Search(ctx, "u", "工数", "", cursor, 1)
		if err != nil {
			t.Fatalf("step %d failed: %v", step, err)
		}
		for _, item := range page.Items {
			gotOrder = append(gotOrder, item.Target.ID)
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(gotOrder) != len(wantOrder) {
		t.Fatalf("paginated order mismatch: got %v, want %v", gotOrder, wantOrder)
	}
	for i := range wantOrder {
		if gotOrder[i] != wantOrder[i] {
			t.Fatalf("rank position %d: got %s, want %s (full=%v)", i, gotOrder[i], wantOrder[i], gotOrder)
		}
	}

	// 4. Reject forged or mismatched cursors.
	if _, err := svc.Search(ctx, "u", "工数", "", "aW52YWxpZA", 20); !errors.Is(err, s.ErrInput) {
		t.Fatalf("expected ErrInput for legacy/malformed cursor, got %v", err)
	}
}
