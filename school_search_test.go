package search_test

import (
	"context"
	"errors"
	s "github.com/StuHelper/stuhelper-search"
	"testing"
)

func TestSchoolSearchCodesNamesAliasesCurrentRightsAndRebuild(t *testing.T) {
	svc, repo, f, _ := setup(t)
	ctx := context.Background()
	d := doc("catalog", "school", "4111010006")
	d.SchoolID = d.Target.ID
	d.Title = "北京航空航天大学"
	d.Code = "10006"
	d.Aliases = []string{"北航", "Beihang", "literal%_"}
	add(t, svc, f, d)
	other := doc("catalog", "school", "4111010007")
	other.SchoolID = other.Target.ID
	other.Title = "北京理工大学"
	add(t, svc, f, other)
	for _, q := range []string{"4111010006", "10006", " 北京航空航天大学 ", "北航", "BEIHANG", "literal%_"} {
		for _, kind := range []string{"school", ""} {
			page, e := svc.Search(ctx, "u", q, kind, "", 20)
			if e != nil || len(page.Items) != 1 || page.Items[0].Target != d.Target {
				t.Fatalf("%q %s: %+v %v", q, kind, page, e)
			}
		}
	}
	for _, q := range []string{"99999", "1000", "_unknown%"} {
		page, e := svc.Search(ctx, "u", q, "school", "", 20)
		if e != nil || len(page.Items) != 0 {
			t.Fatal(q, page, e)
		}
	}
	page, e := svc.Search(ctx, "u", "大学", "school", "", 1)
	if e != nil || len(page.Items) != 1 || page.NextCursor == "" {
		t.Fatal(page, e)
	}
	next, e := svc.Search(ctx, "u", "大学", "school", page.NextCursor, 1)
	if e != nil || len(next.Items) != 1 || next.Items[0].Target == page.Items[0].Target {
		t.Fatal(next, e)
	}
	f.denied["u/"+d.Target.ID] = true
	page, e = svc.Search(ctx, "u", "10006", "school", "", 20)
	if e != nil || len(page.Items) != 0 {
		t.Fatal("revocation ignored", page, e)
	}
	delete(f.denied, "u/"+d.Target.ID)
	d.Aliases = []string{"新别名"}
	d.Revision++
	f.docs[d.Target] = d
	page, e = svc.Search(ctx, "u", "北航", "school", "", 20)
	if e != nil || len(page.Items) != 0 {
		t.Fatal("stale revision returned", page, e)
	}
	if e = svc.Rebuild(ctx); e != nil {
		t.Fatal(e)
	}
	page, e = svc.Search(ctx, "u", "新别名", "school", "", 20)
	if e != nil || len(page.Items) != 1 {
		t.Fatal(page, e)
	}
	rows, e := repo.Candidates(ctx, s.Filter{Query: "10006", Kind: "school", Limit: 100})
	if e != nil || len(rows) != 1 {
		t.Fatal(rows, e)
	}
	f.fail = true
	if _, e = svc.Search(ctx, "u", "10006", "school", "", 20); !errors.Is(e, s.ErrUnavailable) {
		t.Fatal("dependency failure", e)
	}
}
