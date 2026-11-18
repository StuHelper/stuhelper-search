package search_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	s "github.com/StuHelper/stuhelper-search"
	"github.com/StuHelper/stuhelper-search/httpapi"
	"github.com/StuHelper/stuhelper-search/postgres"
	"github.com/StuHelper/stuhelper-search/sdk"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestErrorCodeEnumRetryAfterAndPopularWindowDays(t *testing.T) {
	dsn := os.Getenv("STUHELPER_SEARCH_TEST_DSN")
	if dsn == "" {
		t.Fatal("isolated STUHELPER_SEARCH_TEST_DSN required")
	}
	adminDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { adminDB.Close() })

	name := fmt.Sprintf("search_b05_%d", time.Now().UnixNano())
	if _, err := adminDB.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", dsn+" dbname="+name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(); adminDB.Exec("DROP DATABASE " + name + " WITH (FORCE)") })

	ctx := context.Background()
	if err := postgres.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}

	f := &source{
		activities: map[string]s.ActivitySnapshot{},
		disabled:   map[string]bool{},
		docs:       map[s.Target]s.Document{},
		denied:     map[string]bool{},
		votes:      map[string][]s.Vote{},
	}
	repo := &postgres.Repository{DB: db}
	svc, err := s.New(repo, f, s.Config{
		QueryTimeout:       time.Second,
		HeatWindow:         30 * 24 * time.Hour,
		HeatWeights:        [4]float64{1, 2, 3, 4},
		MinimumHeatSamples: 5,
		Now:                func() time.Time { return now },
	})
	if err != nil {
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
		if u := req.Header.Get("X-Override-Principal"); u != "" {
			if u != "<empty>" {
				req.Header.Set("X-Test-Principal", u)
			}
			req.Header.Del("X-Override-Principal")
		} else {
			req.Header.Set("X-Test-Principal", "u")
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	// 1. Before initial Rebuild, state.ready is false: returns 503 index_building + Retry-After, while Go errors.Is matches both ErrIndexBuilding and ErrUnavailable.
	_, goErr := svc.Search(ctx, "u", "", "", "", 20)
	if !errors.Is(goErr, s.ErrIndexBuilding) || !errors.Is(goErr, s.ErrUnavailable) {
		t.Fatalf("expected ErrIndexBuilding wrapping ErrUnavailable, got %v", goErr)
	}
	notReadyRes, err := client.SearchWithResponse(ctx, nil)
	if err != nil || notReadyRes.StatusCode() != http.StatusServiceUnavailable || notReadyRes.JSON503 == nil || notReadyRes.JSON503.Code != sdk.IndexBuilding {
		t.Fatalf("expected 503 index_building, got err=%v status=%d body=%s", err, notReadyRes.StatusCode(), string(notReadyRes.Body))
	}
	if notReadyRes.HTTPResponse.Header.Get("Retry-After") != "10" {
		t.Fatalf("expected Retry-After=10 for index_building, got %q", notReadyRes.HTTPResponse.Header.Get("Retry-After"))
	}

	notReadyPop, err := client.PopularWithResponse(ctx, &sdk.PopularParams{Kind: sdk.PopularParamsKindCourse})
	if err != nil || notReadyPop.StatusCode() != http.StatusServiceUnavailable || notReadyPop.JSON503 == nil || notReadyPop.JSON503.Code != sdk.IndexBuilding {
		t.Fatalf("expected popular 503 index_building, got err=%v status=%d", err, notReadyPop.StatusCode())
	}

	// 2. Complete Rebuild: empty popular list still carries window_days=30 for frontend empty-state explanation, while search omits window_days.
	if err := svc.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	emptyPop, err := client.PopularWithResponse(ctx, &sdk.PopularParams{Kind: sdk.PopularParamsKindCourse})
	if err != nil || emptyPop.StatusCode() != http.StatusOK || emptyPop.JSON200 == nil {
		t.Fatalf("unexpected popular response: %v %d", err, emptyPop.StatusCode())
	}
	if len(emptyPop.JSON200.Items) != 0 || emptyPop.JSON200.WindowDays == nil || *emptyPop.JSON200.WindowDays != 30 {
		t.Fatalf("expected empty items with window_days=30, got %+v", emptyPop.JSON200)
	}

	searchOK, err := client.SearchWithResponse(ctx, nil)
	if err != nil || searchOK.StatusCode() != http.StatusOK || searchOK.JSON200 == nil || searchOK.JSON200.WindowDays != nil {
		t.Fatalf("search response should omit window_days, got %+v %v", searchOK.JSON200, err)
	}

	// 3. Upstream transient failure returns 503 unavailable + Retry-After=5; invalid input returns 400 invalid_input without Retry-After.
	add(t, svc, f, doc("catalog", "course", "c1"))
	f.fail = true
	unavailRes, err := client.SearchWithResponse(ctx, nil)
	if err != nil || unavailRes.StatusCode() != http.StatusServiceUnavailable || unavailRes.JSON503 == nil || unavailRes.JSON503.Code != sdk.Unavailable {
		t.Fatalf("expected 503 unavailable, got %v %d", err, unavailRes.StatusCode())
	}
	if unavailRes.HTTPResponse.Header.Get("Retry-After") != "5" {
		t.Fatalf("expected Retry-After=5 for unavailable, got %q", unavailRes.HTTPResponse.Header.Get("Retry-After"))
	}
	f.fail = false

	badLimit := 101
	badRes, err := client.SearchWithResponse(ctx, &sdk.SearchParams{Limit: &badLimit})
	if err != nil || badRes.StatusCode() != http.StatusBadRequest || badRes.JSON400 == nil || badRes.JSON400.Code != sdk.InvalidInput {
		t.Fatalf("expected 400 invalid_input, got %v %d", err, badRes.StatusCode())
	}
	if badRes.HTTPResponse.Header.Get("Retry-After") != "" {
		t.Fatalf("unexpected Retry-After on 400: %q", badRes.HTTPResponse.Header.Get("Retry-After"))
	}
}
