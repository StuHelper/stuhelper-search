package search_test

import (
	"context"
	s "github.com/StuHelper/stuhelper-search"
	"github.com/StuHelper/stuhelper-search/postgres"
	"testing"
)

func TestCourseSearchIncludesCurrentFormerAndEnglishNames(t *testing.T) {
	svc, _, sources, db := setup(t)
	ctx := context.Background()
	d := doc("catalog", "course", "canonical-course")
	d.Title = "课程现名"
	d.Code = "NEW001"
	d.EnglishName = "Discrete Mathematics"
	d.FormerCodes = []string{"OLD001"}
	d.FormerNames = []string{"课程曾用名", "literal%_"}
	add(t, svc, sources, d)
	other := doc("catalog", "course", "different-course")
	other.Title = "Another course"
	other.Code = "NEW002"
	add(t, svc, sources, other)
	for _, query := range []string{"NEW001", "old001", "课程现名", "discrete mathematics", "课程曾用名", "literal%_"} {
		for _, kind := range []string{"course", ""} {
			page, err := svc.Search(ctx, "u", query, kind, "", 20)
			if err != nil || len(page.Items) != 1 || page.Items[0].Target != d.Target || page.Items[0].EnglishName != d.EnglishName || page.Items[0].Code != d.Code || len(page.Items[0].FormerCodes) != 1 {
				t.Fatal(query, page, err)
			}
		}
	}
	if err := postgres.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := svc.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	sources.denied["u/"+d.Target.ID] = true
	page, err := svc.Search(ctx, "u", "OLD001", "course", "", 20)
	if err != nil || len(page.Items) != 0 {
		t.Fatal("permission revocation ignored", page, err)
	}
	delete(sources.denied, "u/"+d.Target.ID)
	d.Visible = false
	d.Revision++
	add(t, svc, sources, d)
	page, err = svc.Search(ctx, "u", "OLD001", "course", "", 20)
	if err != nil || len(page.Items) != 0 {
		t.Fatal("hidden course resurrected", page, err)
	}
}

func TestCourseSearchFieldsValidateAtEventBoundary(t *testing.T) {
	d := doc("catalog", "course", "c")
	d.EnglishName = "English"
	d.FormerCodes = []string{"old"}
	d.FormerNames = []string{"Former"}
	if err := s.Validate(d); err != nil {
		t.Fatal(err)
	}
	d.Target.Kind = "teacher"
	if err := s.Validate(d); err == nil {
		t.Fatal("course fields accepted on teacher")
	}
	d.Target.Kind = "course"
	d.FormerCodes = make([]string, 51)
	if err := s.Validate(d); err == nil {
		t.Fatal("unbounded former codes accepted")
	}
}
