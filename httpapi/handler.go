package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	s "github.com/StuHelper/stuhelper-search"
	"net/http"
	neturl "net/url"
	"strconv"
	"strings"
)

type Handler struct {
	Service      *s.Service
	Authenticate func(*http.Request) (string, error)
}

var routeParams = map[string]map[string]bool{
	"/api/v1/search":          {"sort": true, "q": true, "school_id": true, "course_id": true, "teacher_id": true, "kind": true, "cursor": true, "limit": true},
	"/api/v1/search/popular":  {"school_id": true, "kind": true, "cursor": true, "limit": true},
	"/api/v1/search/my-votes": {"cursor": true, "limit": true},
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if h.Service == nil || h.Authenticate == nil {
		fail(w, r, s.ErrUnavailable)
		return
	}
	user, e := h.Authenticate(r)
	if e != nil {
		if isAuthDependencyFailure(e) {
			fail(w, r, e)
			return
		}
		fail(w, r, s.ErrUnauthenticated)
		return
	}
	if user == "" {
		fail(w, r, s.ErrUnauthenticated)
		return
	}
	allowed, knownRoute := routeParams[r.URL.Path]
	if !knownRoute {
		fail(w, r, s.ErrNotFound)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"code": "method_not_allowed"})
		return
	}
	q, e := neturl.ParseQuery(r.URL.RawQuery)
	if e != nil {
		fail(w, r, s.ErrInput)
		return
	}
	for k, v := range q {
		if !allowed[k] || len(v) != 1 {
			fail(w, r, s.ErrInput)
			return
		}
	}
	limit := 20
	if _, ok := q["limit"]; ok {
		limit, e = strconv.Atoi(q.Get("limit"))
		if e != nil {
			fail(w, r, s.ErrInput)
			return
		}
	}
	var out any
	switch r.URL.Path {
	case "/api/v1/search":
		out, e = h.Service.SearchWithParams(r.Context(), user, s.SearchParams{
			Sort:      q.Get("sort"),
			Query:     q.Get("q"),
			Kind:      q.Get("kind"),
			SchoolID:  q.Get("school_id"),
			CourseID:  q.Get("course_id"),
			TeacherID: q.Get("teacher_id"),
			Cursor:    q.Get("cursor"),
			Limit:     limit,
		})
	case "/api/v1/search/popular":
		out, e = h.Service.PopularWithParams(r.Context(), user, s.PopularParams{
			Kind:     q.Get("kind"),
			SchoolID: q.Get("school_id"),
			Cursor:   q.Get("cursor"),
			Limit:    limit,
		})
	case "/api/v1/search/my-votes":
		out, e = h.Service.MyVotes(r.Context(), user, q.Get("cursor"), limit)
	}
	if e != nil {
		fail(w, r, e)
		return
	}
	_ = json.NewEncoder(w).Encode(out)
}

func isAuthDependencyFailure(e error) bool {
	if errors.Is(e, s.ErrUnavailable) || errors.Is(e, context.DeadlineExceeded) || errors.Is(e, context.Canceled) {
		return true
	}
	// Preserve platform transport.ErrUnavailable ("authorization unavailable") without coupling packages.
	return strings.Contains(strings.ToLower(e.Error()), "unavailable")
}

func fail(w http.ResponseWriter, r *http.Request, e error) {
	code := http.StatusServiceUnavailable
	name := "unavailable"
	switch {
	case errors.Is(e, s.ErrSnapshotExpired):
		code = http.StatusGone
		name = "snapshot_expired"
	case errors.Is(e, s.ErrInput):
		code = http.StatusBadRequest
		name = "invalid_input"
	case errors.Is(e, s.ErrUnauthenticated):
		code = http.StatusUnauthorized
		name = "unauthenticated"
	case errors.Is(e, s.ErrNotFound):
		code = http.StatusNotFound
		name = "not_found"
	case errors.Is(e, s.ErrIndexBuilding):
		code = 503
		name = "index_building"
		w.Header().Set("Retry-After", "10")
	case errors.Is(e, context.Canceled) && r != nil && errors.Is(r.Context().Err(), context.Canceled):
		// Client aborted the in-flight request (e.g. AbortController); avoid polluting 5xx metrics.
		code = 499
		name = "cancelled"
	case errors.Is(e, context.DeadlineExceeded) && (r == nil || !errors.Is(r.Context().Err(), context.Canceled)):
		code = http.StatusGatewayTimeout
		name = "timeout"
	default:
		w.Header().Set("Retry-After", "5")
	}
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": name})
}
