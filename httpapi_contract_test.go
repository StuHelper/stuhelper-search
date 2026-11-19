package search_test

import (
	"context"
	"encoding/json"
	"errors"
	s "github.com/StuHelper/stuhelper-search"
	"github.com/StuHelper/stuhelper-search/httpapi"
	"github.com/StuHelper/stuhelper-search/sdk"
	"net/http"
	"net/http/httptest"
	"testing"
)

// DS-01, DS-04: HTTP handler rejects malformed or unknown query parameters, preserves upstream auth outages vs client aborts, and returns RFC-compliant 405 JSON.
func TestHTTPQueryParsingAuthFaultsCancellationAndMethodNotAllowed(t *testing.T) {
	svc, _, f, _ := setup(t)
	add(t, svc, f, doc("catalog", "course", "course-1"))

	var authErr error
	h := httpapi.Handler{
		Service: svc,
		Authenticate: func(r *http.Request) (string, error) {
			if authErr != nil {
				return "", authErr
			}
			return r.Header.Get("X-User"), nil
		},
	}

	call := func(method, target, user string, ctx context.Context) (*httptest.ResponseRecorder, map[string]string) {
		t.Helper()
		req := httptest.NewRequest(method, target, nil)
		if ctx != nil {
			req = req.WithContext(ctx)
		}
		if user != "" {
			req.Header.Set("X-User", user)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		var body map[string]string
		if w.Code != http.StatusOK {
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("%s %s status=%d returned invalid JSON body %q: %v", method, target, w.Code, w.Body.String(), err)
			}
		}
		return w, body
	}

	// A-04: Malformed percent-encoding, semicolons, unknown query keys, or empty keys must return 400 invalid_input (never unfiltered 200).
	for _, badURL := range []string{
		"/api/v1/search?q=100%",
		"/api/v1/search?q=C++;Java",
		"/api/v1/search?kind=school%",
		"/api/v1/search?cursor=bad;token",
		"/api/v1/search?faculty_id=4111010006",
		"/api/v1/search?school_id=a&school_id=b",
		"/api/v1/search?=1",
		"/api/v1/search/popular?kind=course&q=unexpected",
		"/api/v1/search/my-votes?kind=review",
	} {
		w, body := call(http.MethodGet, badURL, "u", nil)
		if w.Code != http.StatusBadRequest || body["code"] != "invalid_input" {
			t.Fatalf("URL %s expected 400 invalid_input, got %d %v (%s)", badURL, w.Code, body, w.Body.String())
		}
	}

	// A-11.2: Non-GET method on known endpoint must return 405 with Allow: GET header and valid JSON body.
	w, body := call(http.MethodPost, "/api/v1/search", "u", nil)
	if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != http.MethodGet || body["code"] != "method_not_allowed" {
		t.Fatalf("POST /api/v1/search expected 405 Allow=GET method_not_allowed, got %d Allow=%q body=%v", w.Code, w.Header().Get("Allow"), body)
	}
	w, body = call(http.MethodPost, "/api/v1/search/unknown", "u", nil)
	if w.Code != http.StatusNotFound || body["code"] != "not_found" {
		t.Fatalf("POST /api/v1/search/unknown expected 404 not_found, got %d body=%v", w.Code, body)
	}

	// A-05.1: Auth dependency outages and timeouts must not be misreported as 401 unauthenticated.
	authErr = s.ErrUnavailable
	w, body = call(http.MethodGet, "/api/v1/search", "u", nil)
	if w.Code != http.StatusServiceUnavailable || body["code"] != "unavailable" {
		t.Fatalf("auth ErrUnavailable expected 503 unavailable, got %d %v", w.Code, body)
	}
	authErr = errors.New("authorization unavailable")
	w, body = call(http.MethodGet, "/api/v1/search", "u", nil)
	if w.Code != http.StatusServiceUnavailable || body["code"] != "unavailable" {
		t.Fatalf("platform authorization unavailable expected 503 unavailable, got %d %v", w.Code, body)
	}
	authErr = context.DeadlineExceeded
	w, body = call(http.MethodGet, "/api/v1/search", "u", nil)
	if w.Code != http.StatusGatewayTimeout || body["code"] != "timeout" {
		t.Fatalf("auth DeadlineExceeded expected 504 timeout, got %d %v", w.Code, body)
	}
	authErr = errors.New("invalid bearer token")
	w, body = call(http.MethodGet, "/api/v1/search", "u", nil)
	if w.Code != http.StatusUnauthorized || body["code"] != "unauthenticated" {
		t.Fatalf("invalid token expected 401 unauthenticated, got %d %v", w.Code, body)
	}
	authErr = nil

	// A-05.2: Client-aborted request (AbortController -> r.Context().Err() == context.Canceled) must not return 5xx (504).
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	w, body = call(http.MethodGet, "/api/v1/search?q=course", "u", canceledCtx)
	if w.Code >= 500 || w.Code != 499 || body["code"] != "cancelled" {
		t.Fatalf("client-canceled request expected non-5xx 499 cancelled, got %d %v", w.Code, body)
	}

	// Internal dependency context cancellation while client request context is still active maps to 503 unavailable.
	authErr = context.Canceled
	w, body = call(http.MethodGet, "/api/v1/search?q=course", "u", nil)
	if w.Code != http.StatusServiceUnavailable || body["code"] != "unavailable" {
		t.Fatalf("internal context.Canceled with live client expected 503 unavailable, got %d %v", w.Code, body)
	}
	authErr = nil

	// Generated Go SDK round-trip verification against httpapi.Handler.
	srv := httptest.NewServer(h)
	defer srv.Close()
	client, err := sdk.NewClientWithResponses(srv.URL, sdk.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
		req.Header.Set("X-User", "u")
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	q := "course-1"
	resp, err := client.SearchWithResponse(context.Background(), &sdk.SearchParams{Q: &q})
	if err != nil || resp.StatusCode() != http.StatusOK || resp.JSON200 == nil || len(resp.JSON200.Items) != 1 || resp.JSON200.Items[0].Target.Id != "course-1" {
		t.Fatalf("SDK search failed: status=%v body=%v err=%v", resp.StatusCode(), resp.JSON200, err)
	}
}
