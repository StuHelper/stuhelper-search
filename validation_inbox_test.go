package search_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	s "github.com/StuHelper/stuhelper-search"
	"github.com/StuHelper/stuhelper-search/postgres"
	"strings"
	"testing"
	"time"
)

// DS-01, DS-06, DS-07: UTF-8 and NUL-byte validation, Unicode code point limits, canonical timezone-independent inbox digests, and event-type isolation.
func TestUTF8NullValidationUnicodeLengthsAndInboxCanonicalDedup(t *testing.T) {
	svc, _, f, db := setup(t)
	ctx := context.Background()

	// A-06.1: Search query and decoded cursor with NUL byte or invalid UTF-8 must return ErrInput (400), never PostgreSQL 22021 ErrUnavailable (503).
	for _, badQ := range []string{"abc\x00def", string([]byte{0xff, 0xfe})} {
		if _, err := svc.Search(ctx, "u", badQ, "", "", 20); !errors.Is(err, s.ErrInput) {
			t.Fatalf("Search bad query %x expected ErrInput, got %v", badQ, err)
		}
	}
	for _, badRaw := range [][]byte{[]byte("catalog/a\x00b"), {0xff, 0xfe}} {
		badCur := base64.RawURLEncoding.EncodeToString(badRaw)
		if _, err := svc.Search(ctx, "u", "", "", badCur, 20); !errors.Is(err, s.ErrInput) {
			t.Fatalf("Search bad cursor %x expected ErrInput, got %v", badRaw, err)
		}
		if _, err := svc.Popular(ctx, "u", "course", badCur, 20); !errors.Is(err, s.ErrInput) {
			t.Fatalf("Popular bad cursor %x expected ErrInput, got %v", badRaw, err)
		}
	}
	if _, err := svc.MyVotes(ctx, "u", "cur\x00sor", 20); !errors.Is(err, s.ErrInput) {
		t.Fatalf("MyVotes NUL cursor expected ErrInput, got %v", err)
	}

	// A-06.2: Event and Document fields containing NUL bytes or invalid UTF-8 must fail validation with ErrInput before hitting PostgreSQL jsonb/text.
	baseDoc := doc("catalog", "course", "valid-id")
	invalidDocs := []func(*s.Document){
		func(d *s.Document) { d.Title = "操作系统\x00期末" },
		func(d *s.Document) { d.Target.ID = string([]byte{'c', 0xff}) },
		func(d *s.Document) { d.SchoolID = "school\x00a" },
		func(d *s.Document) { d.SchoolID = string([]byte{0xff}) },
		func(d *s.Document) { d.CourseID = "course\x00a" },
		func(d *s.Document) { d.CourseID = string([]byte{0xff}) },
		func(d *s.Document) { d.TeacherIDs = []string{"teacher\x001"} },
		func(d *s.Document) { d.TeacherIDs = []string{string([]byte{0xff})} },
	}
	for i, mutate := range invalidDocs {
		bad := baseDoc
		mutate(&bad)
		if err := svc.Consume(ctx, event(bad, "bad-doc")); !errors.Is(err, s.ErrInput) {
			t.Fatalf("invalid doc case %d expected ErrInput, got %v", i, err)
		}
	}
	for _, badEventID := range []string{"evt\x001", string([]byte{0xff, 0x01})} {
		if err := svc.Consume(ctx, event(baseDoc, badEventID)); !errors.Is(err, s.ErrInput) {
			t.Fatalf("Consume bad event ID %x expected ErrInput, got %v", badEventID, err)
		}
		act := activity("catalog", "course", "valid-id", "2026-11-09", map[string]int64{"views": 1})
		if err := svc.ConsumeActivity(ctx, activityEvent(act, badEventID)); !errors.Is(err, s.ErrInput) {
			t.Fatalf("ConsumeActivity bad event ID %x expected ErrInput, got %v", badEventID, err)
		}
	}

	// A-11.1: 334 Chinese characters (1002 UTF-8 bytes, <= 1000 Unicode code points) must pass Validate, Consume, and Search.
	longChineseDoc := doc("catalog", "course", strings.Repeat("课", 120))
	longChineseDoc.Title = strings.Repeat("高等工程数学导论", 60) // 480 runes, 1440 UTF-8 bytes (> 1000 bytes, <= 1000 runes)
	add(t, svc, f, longChineseDoc)
	page, err := svc.Search(ctx, "u", "高等工程数学导论", "course", "", 20)
	if err != nil || len(page.Items) != 1 || page.Items[0].Title != longChineseDoc.Title {
		t.Fatalf("expected 480-rune Chinese title to succeed, got page=%+v err=%v", page, err)
	}
	tooLongDoc := doc("catalog", "course", "too-long")
	tooLongDoc.Title = strings.Repeat("字", 1001)
	if err := s.Validate(tooLongDoc); !errors.Is(err, s.ErrInput) {
		t.Fatalf("expected 1001-rune title to fail with ErrInput, got %v", err)
	}

	// A-10.1: Replaying the exact same instant in CST (+08:00) vs UTC (Z) and empty slices vs nil must succeed idempotently without ErrConflict.
	cst := time.FixedZone("CST", 8*3600)
	tzDocUTC := doc("catalog", "course", "tz-course")
	tzDocUTC.At = now.UTC()
	tzDocUTC.Aliases = nil
	tzDocUTC.TeacherIDs = nil
	evUTC := event(tzDocUTC, "same-instant-id")
	evUTC.Time = now.UTC()
	if err := svc.Consume(ctx, evUTC); err != nil {
		t.Fatalf("first delivery in UTC failed: %v", err)
	}

	tzDocCST := tzDocUTC
	tzDocCST.At = now.In(cst)
	tzDocCST.Aliases = []string{}
	tzDocCST.TeacherIDs = []string{}
	evCST := event(tzDocCST, "same-instant-id")
	evCST.Time = now.In(cst)
	if err := svc.Consume(ctx, evCST); err != nil {
		t.Fatalf("idempotent replay in +08:00 timezone failed with %v (expected nil)", err)
	}

	actSnap := activity("catalog", "course", "tz-course", "2026-11-09", map[string]int64{"views": 2})
	actUTC := activityEvent(actSnap, "same-instant-act")
	actUTC.Time = now.UTC()
	if err := svc.ConsumeActivity(ctx, actUTC); err != nil {
		t.Fatal(err)
	}
	actCST := actUTC
	actCST.Time = now.In(cst)
	if err := svc.ConsumeActivity(ctx, actCST); err != nil {
		t.Fatalf("activity replay in +08:00 timezone failed with %v (expected nil)", err)
	}

	// A-10.2: Content event and activity event from the same source sharing local outbox ID "shared-seq-1" must not collide, and inbox records received_at.
	sharedDoc := doc("catalog", "course", "shared-seq-course")
	if err := svc.Consume(ctx, event(sharedDoc, "shared-seq-1")); err != nil {
		t.Fatal(err)
	}
	sharedAct := activity("catalog", "course", "shared-seq-course", "2026-11-09", map[string]int64{"views": 3})
	if err := svc.ConsumeActivity(ctx, activityEvent(sharedAct, "shared-seq-1")); err != nil {
		t.Fatalf("activity event with same local ID as content event failed: %v", err)
	}
	var inboxCount int
	var hasReceivedAt bool
	if err := db.QueryRow(`SELECT count(*), bool_and(received_at IS NOT NULL AND event_type <> '') FROM search.inbox WHERE source='catalog' AND id='shared-seq-1'`).Scan(&inboxCount, &hasReceivedAt); err != nil || inboxCount != 2 || !hasReceivedAt {
		t.Fatalf("expected 2 typed/timestamped inbox rows for shared-seq-1, got count=%d hasReceivedAt=%v err=%v", inboxCount, hasReceivedAt, err)
	}

	// Upgrading from legacy 2-column inbox primary key preserves existing receipts and expands primary key to (source, event_type, id).
	if _, err := db.Exec(`DROP TABLE search.inbox; CREATE TABLE search.inbox(source text NOT NULL, id text NOT NULL, digest text NOT NULL, PRIMARY KEY(source, id)); INSERT INTO search.inbox(source, id, digest) VALUES ('catalog', 'legacy-1', 'digest-1')`); err != nil {
		t.Fatal(err)
	}
	if err := postgres.Migrate(ctx, db); err != nil {
		t.Fatalf("legacy inbox migration failed: %v", err)
	}
	var legacyDigest string
	if err := db.QueryRow(`SELECT digest FROM search.inbox WHERE source='catalog' AND id='legacy-1'`).Scan(&legacyDigest); err != nil || legacyDigest != "digest-1" {
		t.Fatalf("legacy receipt lost after migration: digest=%q err=%v", legacyDigest, err)
	}
}

func TestUnicodeTargetRoundTripsThroughSearchAndActivity(t *testing.T) {
	svc, _, f, _ := setup(t)
	ctx := context.Background()
	for _, suffix := range []string{"a", "b"} {
		d := doc("catalog", "course", strings.Repeat("🌍", 199)+suffix)
		d.Title = "Unicode target"
		f.docs[d.Target] = d
		if err := svc.Consume(ctx, event(d, suffix)); err != nil {
			t.Fatal(err)
		}
		a := activity("catalog", "course", d.Target.ID, "2026-11-09", map[string]int64{"views": 1})
		if err := svc.ConsumeActivity(ctx, activityEvent(a, "activity-"+suffix)); err != nil {
			t.Errorf("activity: %v", err)
		}
	}
	p, err := svc.Search(ctx, "u", "Unicode target", "course", "", 1)
	if err != nil || len(p.Items) != 1 || p.NextCursor == "" {
		t.Fatalf("first page: %+v %v", p, err)
	}
	next, err := svc.Search(ctx, "u", "Unicode target", "course", p.NextCursor, 1)
	if err != nil || len(next.Items) != 1 || next.Items[0].Target == p.Items[0].Target {
		t.Fatalf("Unicode continuation failed: %+v %v", next, err)
	}
}

func TestLegacyInboxReceiptsProtectBothEventTypes(t *testing.T) {
	svc, _, _, db := setup(t)
	ctx := context.Background()
	d := doc("catalog", "course", "legacy-doc")
	d.At = d.At.In(time.FixedZone("CST", 8*3600))
	docEvent := event(d, "legacy-doc-id")
	docEvent.Time = d.At
	a := activity("catalog", "course", "legacy-doc", "2026-11-09", map[string]int64{"views": 1})
	actEvent := activityEvent(a, "legacy-act-id")
	actEvent.Time = d.At
	if _, err := db.Exec(`DROP TABLE search.inbox; CREATE TABLE search.inbox(source text NOT NULL,id text NOT NULL,digest text NOT NULL,PRIMARY KEY(source,id))`); err != nil {
		t.Fatal(err)
	}
	for id, ev := range map[string]any{docEvent.ID: docEvent, actEvent.ID: actEvent} {
		raw, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		if _, err = db.Exec(`INSERT INTO search.inbox VALUES ('catalog',$1,$2)`, id, hex.EncodeToString(sum[:])); err != nil {
			t.Fatal(err)
		}
	}
	if err := postgres.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := svc.Consume(ctx, docEvent); err != nil {
		t.Errorf("legacy content replay: %v", err)
	}
	if err := svc.ConsumeActivity(ctx, actEvent); err != nil {
		t.Errorf("legacy activity replay: %v", err)
	}
	docEvent.Data.Title = "different"
	actEvent.Data.Metrics["views"] = 2
	if err := svc.Consume(ctx, docEvent); !errors.Is(err, s.ErrConflict) {
		t.Errorf("legacy content conflict: %v", err)
	}
	if err := svc.ConsumeActivity(ctx, actEvent); !errors.Is(err, s.ErrConflict) {
		t.Errorf("legacy activity conflict: %v", err)
	}
	var rows int
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM search.documents)+(SELECT count(*) FROM search.activities)+(SELECT count(*) FROM search.inbox WHERE event_type <> '')`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("legacy replay wrote %d rows: %v", rows, err)
	}
}
