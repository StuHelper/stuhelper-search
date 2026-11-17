package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	s "github.com/StuHelper/stuhelper-search"
	"net/http"
	"strconv"
)

type Handler struct {
	Service      *s.Service
	Authenticate func(*http.Request) (string, error)
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if h.Service == nil || h.Authenticate == nil {
		fail(w, s.ErrUnavailable)
		return
	}
	user, e := h.Authenticate(r)
	if e != nil || user == "" {
		fail(w, s.ErrUnauthenticated)
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	for _, v := range q {
		if len(v) != 1 {
			fail(w, s.ErrInput)
			return
		}
	}
	limit := 20
	if q.Has("limit") {
		limit, e = strconv.Atoi(q.Get("limit"))
		if e != nil {
			fail(w, s.ErrInput)
			return
		}
	}
	var out any
	switch r.URL.Path {
	case "/api/v1/search":
		out, e = h.Service.SearchWithParams(r.Context(), user, s.SearchParams{
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
	default:
		e = s.ErrNotFound
	}
	if e != nil {
		fail(w, e)
		return
	}
	if e = json.NewEncoder(w).Encode(out); e != nil {
		return
	}
}
func fail(w http.ResponseWriter, e error) {
	code := 503
	name := "unavailable"
	switch {
	case errors.Is(e, s.ErrInput):
		code = 400
		name = "invalid_input"
	case errors.Is(e, s.ErrUnauthenticated):
		code = 401
		name = "unauthenticated"
	case errors.Is(e, s.ErrNotFound):
		code = 404
		name = "not_found"
	case errors.Is(e, context.DeadlineExceeded), errors.Is(e, context.Canceled):
		code = 504
		name = "timeout"
	}
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": name})
}
