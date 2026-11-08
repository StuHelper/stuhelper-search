package search

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

type Service struct {
	repo    Repository
	sources Sources
	config  Config
}

func New(repo Repository, sources Sources, c Config) (*Service, error) {
	if repo == nil || sources == nil || c.MinimumSamples < 1 || c.QueryTimeout <= 0 || c.QueryTimeout > 30*time.Second || c.HeatWindow <= 0 || c.HeatWindow > 366*24*time.Hour || c.MinimumHeatSamples < 1 {
		return nil, ErrInput
	}
	positive := false
	for _, w := range c.HeatWeights {
		if math.IsNaN(w) || math.IsInf(w, 0) || w < 0 {
			return nil, ErrInput
		}
		positive = positive || w > 0
	}
	if !positive {
		return nil, ErrInput
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return &Service{repo, sources, c}, nil
}
func validTarget(t Target) bool {
	return len(t.ID) > 0 && len(t.ID) <= 200 && !strings.ContainsRune(t.ID, 0) && ((t.Kind == "activity" && (t.Source == "catalog" || t.Source == "reviews" || t.Source == "resources")) || (t.Source == "catalog" && (t.Kind == "course" || t.Kind == "teacher")) || (t.Source == "reviews" && t.Kind == "review") || (t.Source == "resources" && t.Kind == "resource"))
}
func Validate(d Document) error {
	if !validTarget(d.Target) || d.Revision < 1 || len(d.Title) > 1000 || !utf8.ValidString(d.Title) || len(d.SchoolID) > 200 || len(d.TeacherIDs) > 100 || d.At.IsZero() {
		return ErrInput
	}
	if d.Visible && (d.Title == "" || d.SchoolID == "") {
		return ErrInput
	}
	seen := map[string]bool{}
	for _, id := range d.TeacherIDs {
		if id == "" || len(id) > 200 || seen[id] {
			return ErrInput
		}
		seen[id] = true
	}
	if len(d.CourseID) > 200 {
		return ErrInput
	}
	for _, r := range d.Ratings {
		if r < 0 || r > 5 {
			return ErrInput
		}
	}
	if d.Visible && d.Target.Kind == "review" && d.CourseID == "" {
		return ErrInput
	}
	if (d.Activity != nil) != (d.Target.Kind == "activity") {
		return ErrInput
	}
	if d.Activity != nil {
		for _, n := range d.Activity {
			if n < 0 || n > 1000000000 {
				return ErrInput
			}
		}
	}
	return nil
}
func (s *Service) Consume(ctx context.Context, e Event) error {
	if e.SpecVersion != "1.0" || e.ID == "" || len(e.ID) > 200 || e.Source != e.Data.Target.Source || e.Subject != e.Data.Target.ID || e.Type != e.Source+".discovery.v1" || e.Time.IsZero() {
		return ErrInput
	}
	if err := Validate(e.Data); err != nil {
		return err
	}
	b, err := json.Marshal(e)
	if err != nil {
		return ErrInput
	}
	h := sha256.Sum256(b)
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	return s.repo.Apply(ctx, e, hex.EncodeToString(h[:]))
}
func (s *Service) Rebuild(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	return s.repo.Rebuild(ctx, func(put func([]Document) error) error {
		for _, source := range []string{"catalog", "reviews", "resources"} {
			cursor := ""
			for pages := 0; ; pages++ {
				if pages >= 10000 {
					return ErrUnavailable
				}
				docs, next, e := s.sources.Snapshot(ctx, source, cursor, 100)
				if e != nil {
					return e
				}
				if len(docs) > 100 || next != "" && next == cursor {
					return ErrUnavailable
				}
				for _, d := range docs {
					if d.Target.Source != source || Validate(d) != nil {
						return ErrUnavailable
					}
				}
				if e = put(docs); e != nil {
					return e
				}
				if next == "" {
					break
				}
				cursor = next
			}
		}
		return nil
	})
}
func key(d Document) string  { return d.Target.Source + "/" + d.Target.ID }
func encode(v string) string { return base64.RawURLEncoding.EncodeToString([]byte(v)) }
func decode(v string) (string, error) {
	if len(v) > 1024 {
		return "", ErrInput
	}
	b, e := base64.RawURLEncoding.DecodeString(v)
	if e != nil {
		return "", ErrInput
	}
	return string(b), nil
}
func (s *Service) current(ctx context.Context, user string, d Document) (Document, bool, error) {
	live, ok, e := s.sources.Current(ctx, user, d.Target)
	if e != nil {
		return Document{}, false, upstreamError(ctx)
	}
	if !ok {
		return Document{}, false, nil
	}
	if Validate(live) != nil || live.Target != d.Target {
		return Document{}, false, upstreamError(ctx)
	}
	return live, live.Visible && live.Revision == d.Revision, nil
}
func auth(user string) error {
	if user == "" {
		return ErrUnauthenticated
	}
	return nil
}
func (s *Service) Search(ctx context.Context, user, q, kind, cursor string, limit int) (Page, error) {
	out := Page{Items: []Item{}}
	if e := auth(user); e != nil {
		return out, e
	}
	if utf8.RuneCountInString(q) > 200 || limit < 1 || limit > 100 || kind != "" && kind != "course" && kind != "teacher" && kind != "review" && kind != "resource" {
		return out, ErrInput
	}
	after, e := decode(cursor)
	if e != nil {
		return out, e
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	// The cursor tracks examined candidates, so invisible entries cannot stall pagination.
	for scanned := 0; scanned < 10000; {
		docs, e := s.repo.Candidates(ctx, Filter{Query: q, Kind: kind, After: after, Limit: 100})
		if e != nil {
			return out, e
		}
		for _, d := range docs {
			scanned++
			after = key(d)
			live, ok, e := s.current(ctx, user, d)
			if e != nil {
				return Page{}, e
			}
			if ok {
				out.Items = append(out.Items, Item{live.Target, live.Title, live.SchoolID})
			}
			if len(out.Items) == limit {
				out.NextCursor = encode(after)
				return out, nil
			}
		}
		if len(docs) < 100 {
			return out, nil
		}
	}
	out.NextCursor = encode(after)
	return out, nil
}
func period(from, to time.Time) bool {
	return !from.IsZero() && to.After(from) && to.Sub(from) <= 366*24*time.Hour
}
func (s *Service) Statistics(ctx context.Context, user, kind, id string, from, to time.Time) (Statistics, error) {
	if e := auth(user); e != nil {
		return Statistics{}, e
	}
	if (kind != "course" && kind != "teacher") || !validTarget(Target{"catalog", id, kind}) || !period(from, to) {
		return Statistics{}, ErrInput
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	return s.statistics(ctx, user, kind, id, from, to)
}
func (s *Service) statistics(ctx context.Context, user, kind, id string, from, to time.Time) (Statistics, error) {
	out := Statistics{Target: Target{"catalog", id, kind}, From: from, To: to}
	target, ok, e := s.sources.Current(ctx, user, out.Target)
	if e != nil {
		return out, upstreamError(ctx)
	}
	if !ok || !target.Visible {
		return out, ErrNotFound
	}
	if target.Target != out.Target || Validate(target) != nil {
		return out, upstreamError(ctx)
	}
	type aggregate struct {
		sum   [5]float64
		count [5]int
	}
	courses := map[string]*aggregate{}
	f := Filter{Kind: "review", From: from, To: to, Limit: 100}
	if kind == "course" {
		f.CourseID = id
	} else {
		f.TeacherID = id
	}
	for scanned := 0; ; {
		docs, e := s.repo.Candidates(ctx, f)
		if e != nil {
			return out, e
		}
		for _, d := range docs {
			scanned++
			if scanned > 10000 {
				return out, upstreamError(ctx)
			}
			f.After = key(d)
			live, ok, e := s.current(ctx, user, d)
			if e != nil {
				return out, e
			}
			if !ok {
				continue
			}
			// Revalidate relation and range too: a current source response cannot substitute unrelated facts.
			if (kind == "teacher" && !slices.Contains(live.TeacherIDs, id)) || live.CourseID != d.CourseID || live.At.Before(from) || !live.At.Before(to) {
				continue
			}
			course, readable, e := s.sources.Current(ctx, user, Target{"catalog", live.CourseID, "course"})
			if e != nil {
				return out, upstreamError(ctx)
			}
			if readable && (course.Target != (Target{"catalog", live.CourseID, "course"}) || Validate(course) != nil) {
				return out, ErrUnavailable
			}
			if !readable || !course.Visible {
				continue
			}
			a := courses[live.CourseID]
			if a == nil {
				a = &aggregate{}
				courses[live.CourseID] = a
			}
			out.Samples++
			for i, r := range live.Ratings {
				if r > 0 {
					a.sum[i] += float64(r)
					a.count[i]++
				}
			}
		}
		if len(docs) < 100 {
			break
		}
	}
	out.Courses = len(courses)
	courseIDs := make([]string, 0, len(courses))
	for id := range courses {
		courseIDs = append(courseIDs, id)
	}
	sort.Strings(courseIDs)
	for i := 0; i < 5; i++ {
		var sum float64
		divisor := 0
		count := 0
		for _, courseID := range courseIDs {
			a := courses[courseID]
			count += a.count[i]
			if a.count[i] > 0 {
				if kind == "teacher" {
					sum += a.sum[i] / float64(a.count[i])
					divisor++
				} else {
					sum += a.sum[i]
					divisor += a.count[i]
				}
			}
		}
		d := Dimension{Samples: count, State: "no_reviews"}
		if count > 0 {
			d.State = "insufficient"
		}
		if count >= s.config.MinimumSamples {
			d.State = "available"
			d.Level = fmt.Sprintf("level_%d", int(math.Floor(sum/float64(divisor)+0.5)))
		}
		out.Dimensions[i] = d
	}
	return out, nil
}
func (s *Service) Compare(ctx context.Context, user string, ids []string, from, to time.Time) (Comparison, error) {
	out := Comparison{Items: []Statistics{}}
	if e := auth(user); e != nil {
		return out, e
	}
	if len(ids) < 2 || len(ids) > 5 || !period(from, to) {
		return out, ErrInput
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] || !validTarget(Target{"catalog", id, "course"}) {
			return out, ErrInput
		}
		seen[id] = true
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	for _, id := range ids {
		v, e := s.statistics(ctx, user, "course", id, from, to)
		if e != nil {
			return Comparison{}, e
		}
		out.Items = append(out.Items, v)
	}
	return out, nil
}
func (s *Service) MyVotes(ctx context.Context, user, cursor string, limit int) (Votes, error) {
	if e := auth(user); e != nil {
		return Votes{}, e
	}
	if limit < 1 || limit > 100 || len(cursor) > 1024 {
		return Votes{}, ErrInput
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	v, e := s.sources.MyVotes(ctx, user, cursor, limit)
	if e != nil {
		return Votes{}, upstreamError(ctx)
	}
	if len(v.Items) > limit || len(v.NextCursor) > 1024 {
		return Votes{}, upstreamError(ctx)
	}
	out := Votes{Items: []Vote{}, NextCursor: v.NextCursor}
	for _, vote := range v.Items {
		if vote.Target.Source != "reviews" || !validTarget(vote.Target) || (vote.Choice != "up" && vote.Choice != "down") {
			return Votes{}, upstreamError(ctx)
		}
		d, ok, e := s.sources.Current(ctx, user, vote.Target)
		if e != nil {
			return Votes{}, upstreamError(ctx)
		}
		if ok && (d.Target != vote.Target || Validate(d) != nil) {
			return Votes{}, ErrUnavailable
		}
		if ok && d.Visible {
			out.Items = append(out.Items, vote)
		}
	}
	return out, nil
}
func (s *Service) Popular(ctx context.Context, user, kind, cursor string, limit int) (Page, error) {
	out := Page{Items: []Item{}}
	if e := auth(user); e != nil {
		return out, e
	}
	if limit < 1 || limit > 100 || (kind != "course" && kind != "teacher") {
		return out, ErrInput
	}
	after, e := decode(cursor)
	if e != nil {
		return out, e
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	type ranked struct {
		d       Document
		score   float64
		samples int64
	}
	scores := map[Target]*ranked{}
	now := s.config.Now()
	f := Filter{Activity: true, From: now.Add(-s.config.HeatWindow), To: now, Limit: 100}
	for scanned := 0; ; {
		docs, e := s.repo.Candidates(ctx, f)
		if e != nil {
			return out, e
		}
		for _, d := range docs {
			scanned++
			if scanned > 10000 {
				return out, upstreamError(ctx)
			}
			f.After = key(d)
			live, ok, e := s.current(ctx, user, d)
			if e != nil {
				return out, e
			}
			if !ok || live.Activity == nil {
				continue
			}
			ids := []string{live.CourseID}
			if kind == "teacher" {
				ids = live.TeacherIDs
			}
			for _, id := range ids {
				if id == "" {
					continue
				}
				t := Target{"catalog", id, kind}
				v := scores[t]
				if v == nil {
					v = &ranked{}
					v.d.Target = t
					scores[t] = v
				}
				for i, n := range *live.Activity {
					v.score += float64(n) * s.config.HeatWeights[i]
					v.samples += n
				}
			}
		}
		if len(docs) < 100 {
			break
		}
	}
	rows := []ranked{}
	for t, v := range scores {
		if v.samples < int64(s.config.MinimumHeatSamples) {
			continue
		}
		d, ok, e := s.sources.Current(ctx, user, t)
		if e != nil {
			return out, upstreamError(ctx)
		}
		if ok && (d.Target != t || Validate(d) != nil) {
			return out, ErrUnavailable
		}
		if ok && d.Visible {
			v.d = d
			rows = append(rows, *v)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].score == rows[j].score {
			return key(rows[i].d) < key(rows[j].d)
		}
		return rows[i].score > rows[j].score
	})
	passed := after == ""
	for _, r := range rows {
		token := fmt.Sprintf("%.17g:%s", r.score, key(r.d))
		if !passed {
			passed = token == after
			continue
		}
		out.Items = append(out.Items, Item{r.d.Target, r.d.Title, r.d.SchoolID})
		if len(out.Items) == limit {
			out.NextCursor = encode(token)
			break
		}
	}
	if !passed {
		return Page{}, ErrInput
	}
	return out, nil
}

func upstreamError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return ErrUnavailable
}
