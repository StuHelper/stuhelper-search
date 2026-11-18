package search_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	s "github.com/StuHelper/stuhelper-search"
	"sort"
	"strings"
	"testing"
	"time"
)

type delayedDeniedSource struct {
	*source
	perCallDelay time.Duration
}

func (d *delayedDeniedSource) DiscoverySnapshot(ctx context.Context, src, cursor string, limit int) ([]s.Document, string, error) {
	docs := []s.Document{}
	for _, doc := range d.docs {
		if doc.Target.Source == src && doc.Target.ID > cursor {
			docs = append(docs, doc)
		}
	}
	sort.Slice(docs, func(i, j int) bool { return docs[i].Target.ID < docs[j].Target.ID })
	if len(docs) > limit {
		return docs[:limit], docs[limit-1].Target.ID, nil
	}
	return docs, "", nil
}

func (d *delayedDeniedSource) Current(ctx context.Context, user string, t s.Target) (s.Document, bool, error) {
	if d.perCallDelay > 0 {
		select {
		case <-ctx.Done():
			return s.Document{}, false, ctx.Err()
		case <-time.After(d.perCallDelay):
		}
	}
	return d.source.Current(ctx, user, t)
}

// DS-01, DS-SCHOOL-01: Multilingual case-insensitive search, school numeric alias and exact code rules, encrypted user-bound cursors, and index-ordered pagination without timeout stall.
func TestIndexOrderUnicodeCollationOpaqueCursorAndSchoolQueryRules(t *testing.T) {
	svc, repo, f, db := setup(t)
	ctx := context.Background()

	// A-12: French, Russian, and Spanish accented/Cyrillic titles fold case-insensitively even in C-locale clusters.
	fr := doc("catalog", "course", "course-fr")
	fr.Title = "École Centrale Ingénierie"
	ru := doc("catalog", "course", "course-ru")
	ru.Title = "МОСКВА АВИАЦИЯ"
	es := doc("catalog", "course", "course-es")
	es.Title = "ESPAÑA DISEÑO"
	for _, d := range []s.Document{fr, ru, es} {
		add(t, svc, f, d)
	}
	for _, tc := range []struct{ q, wantID string }{
		{"école", "course-fr"},
		{"москва", "course-ru"},
		{"españa", "course-es"},
		{"diseño", "course-es"},
	} {
		p, err := svc.Search(ctx, "u", tc.q, "course", "", 20)
		if err != nil || len(p.Items) != 1 || p.Items[0].Target.ID != tc.wantID {
			t.Fatalf("multilingual search %q failed: page=%+v err=%v", tc.q, p, err)
		}
	}

	// A-13.2: School pure-numeric query still matches registered aliases/titles, while non-numeric school query does not substring-match code.
	sch := doc("catalog", "school", "4111010006")
	sch.SchoolID = sch.Target.ID
	sch.Title = "北京航空航天大学"
	sch.Code = "SCH-10006"
	sch.Aliases = []string{"985", "北航"}
	add(t, svc, f, sch)

	for _, okQuery := range []string{"985", "SCH-10006", "sch-10006", "4111010006", "10006"} {
		p, err := svc.Search(ctx, "u", okQuery, "school", "", 20)
		if err != nil || len(p.Items) != 1 || p.Items[0].Target.ID != sch.Target.ID {
			t.Fatalf("school query %q expected match, got %+v err=%v", okQuery, p, err)
		}
	}
	for _, noMatchQuery := range []string{"SCH-1000", "1000"} {
		p, err := svc.Search(ctx, "u", noMatchQuery, "school", "", 20)
		if err != nil || len(p.Items) != 0 {
			t.Fatalf("school query %q expected 0 matches, got %+v err=%v", noMatchQuery, p, err)
		}
	}

	// A-09.1: Verify PostgreSQL query plans for Candidates and Activities do not emit a Sort node when ordered index scan is selected.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		t.Fatal(err)
	}
	for _, explainSQL := range []string{
		`EXPLAIN SELECT data FROM search.documents WHERE (data->>'visible')::boolean AND (source COLLATE "C", id COLLATE "C") > ('catalog' COLLATE "C", '' COLLATE "C") AND data->'target'->>'kind' = 'school' ORDER BY source COLLATE "C", id COLLATE "C" LIMIT 100`,
		`EXPLAIN SELECT data FROM search.activities WHERE kind = 'course' AND day >= '2026-11-02' AND day < '2026-11-10' AND (source COLLATE "C", id COLLATE "C") > ('' COLLATE "C", '' COLLATE "C") AND (data->>'visible')::boolean ORDER BY source COLLATE "C", id COLLATE "C" LIMIT 100`,
	} {
		rows, err := tx.QueryContext(ctx, explainSQL)
		if err != nil {
			t.Fatal(err)
		}
		var planLines []string
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			planLines = append(planLines, line)
		}
		rows.Close()
		plan := strings.Join(planLines, "\n")
		if strings.Contains(plan, "Sort Key:") || !strings.Contains(plan, "Index Scan") {
			t.Fatalf("expected Index Scan without Sort node, got plan:\n%s", plan)
		}
	}
	_ = tx.Rollback()

	// A-09.2 & A-13.1: Large block of denied candidates advances cursor before QueryTimeout without exposing unauthorized source/id or allowing cross-user replay.
	ds := &delayedDeniedSource{source: f, perCallDelay: time.Millisecond}
	slowSvc, err := s.New(repo, ds, s.Config{
		QueryTimeout:       200 * time.Millisecond,
		HeatWindow:         7 * 24 * time.Hour,
		HeatWeights:        [4]float64{1, 1, 1, 1},
		MinimumHeatSamples: 1,
		Now:                func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	// Reset docs to 250 denied courses + 1 visible course at the end (250 * 1ms = 250ms > 200ms QueryTimeout).
	f.docs = map[s.Target]s.Document{}
	f.denied = map[string]bool{}
	for i := 0; i < 250; i++ {
		id := fmt.Sprintf("restricted-secret-%04d", i)
		d := doc("catalog", "course", id)
		f.docs[d.Target] = d
		f.denied["alice/"+id] = true
	}
	visibleEnd := doc("catalog", "course", "zzz-visible-course")
	f.docs[visibleEnd.Target] = visibleEnd
	if err := slowSvc.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}

	firstPage, err := slowSvc.Search(ctx, "alice", "", "course", "", 20)
	if err != nil {
		t.Fatalf("expected pre-timeout continuation cursor instead of DeadlineExceeded, got %v", err)
	}
	if firstPage.NextCursor == "" {
		t.Fatalf("expected non-empty NextCursor after partial scan, got %+v", firstPage)
	}

	// Verify cursor does not leak unauthorized object IDs in plaintext or raw base64.
	rawBytes, decErr := base64.RawURLEncoding.DecodeString(firstPage.NextCursor)
	if decErr != nil {
		t.Fatalf("cursor is not base64url: %v", decErr)
	}
	if bytes.Contains(rawBytes, []byte("restricted-secret")) || bytes.Contains(rawBytes, []byte("catalog")) {
		t.Fatalf("cursor leaked unauthorized source/id in decoded bytes: %q", string(rawBytes))
	}

	// Another user or a different endpoint cannot replay alice's cursor.
	if _, err := slowSvc.Search(ctx, "bob", "", "course", firstPage.NextCursor, 20); !errors.Is(err, s.ErrInput) {
		t.Fatalf("cross-user cursor replay expected ErrInput, got %v", err)
	}
	if _, err := slowSvc.Popular(ctx, "alice", "course", firstPage.NextCursor, 20); !errors.Is(err, s.ErrInput) {
		t.Fatalf("cross-endpoint cursor replay expected ErrInput, got %v", err)
	}

	// A second Service replica connected to the same DB seamlessly continues pagination with alice's cursor until reaching zzz-visible-course.
	replicaSvc, err := s.New(repo, ds, s.Config{
		QueryTimeout:       200 * time.Millisecond,
		HeatWindow:         7 * 24 * time.Hour,
		HeatWeights:        [4]float64{1, 1, 1, 1},
		MinimumHeatSamples: 1,
		Now:                func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	cursor := firstPage.NextCursor
	var collected []string
	for steps := 0; steps < 10 && cursor != ""; steps++ {
		nextPage, err := replicaSvc.Search(ctx, "alice", "", "course", cursor, 20)
		if err != nil {
			t.Fatalf("step %d continuation failed: %v", steps, err)
		}
		for _, item := range nextPage.Items {
			collected = append(collected, item.Target.ID)
		}
		cursor = nextPage.NextCursor
	}
	if len(collected) != 1 || collected[0] != "zzz-visible-course" {
		t.Fatalf("expected [zzz-visible-course] across continuation pages, got %v", collected)
	}
}
