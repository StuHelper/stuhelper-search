package search

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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
	if repo == nil || sources == nil || c.QueryTimeout <= 0 || c.QueryTimeout > 30*time.Second || c.RebuildTimeout < 0 || c.RebuildTimeout > 30*time.Minute || c.HeatWindow <= 0 || c.HeatWindow > 366*24*time.Hour || c.MinimumHeatSamples < 1 {
		return nil, ErrInput
	}
	if c.RebuildTimeout == 0 {
		c.RebuildTimeout = 2 * time.Minute
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
	return len(t.ID) > 0 && len(t.ID) <= 200 && !strings.ContainsRune(t.ID, 0) && ((t.Source == "catalog" && (t.Kind == "school" || t.Kind == "course" || t.Kind == "teacher")) || (t.Source == "reviews" && t.Kind == "review") || (t.Source == "resources" && t.Kind == "resource"))
}
func Validate(d Document) error {
	if d.Target.Kind == "school" || d.Target.Kind == "course" {
		if d.Target.Kind == "school" && d.Target.ID != d.SchoolID {
			return ErrInput
		}
		if !searchText(d.Code, 128) || !searchNames(d.Aliases, 256) {
			return ErrInput
		}
	} else if d.Code != "" || len(d.Aliases) != 0 {
		return ErrInput
	}
	if d.Target.Kind == "course" {
		if !searchText(d.EnglishName, 256) || !searchNames(d.FormerCodes, 128) || !searchNames(d.FormerNames, 256) {
			return ErrInput
		}
	} else if d.EnglishName != "" || len(d.FormerCodes) > 0 || len(d.FormerNames) > 0 {
		return ErrInput
	}

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
	if d.Visible && d.Target.Kind == "review" && d.CourseID == "" {
		return ErrInput
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

type windowRebuilder interface {
	RebuildWindow(context.Context, time.Time, time.Time, func(func([]Document) error, func([]ActivitySnapshot) error) error) error
}

func (s *Service) Rebuild(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.RebuildTimeout)
	defer cancel()
	from, to := s.activityWindow()
	load := func(put func([]Document) error, putActivity func([]ActivitySnapshot) error) error {
		for _, source := range []string{"catalog", "reviews", "resources"} {
			cursor := ""
			seen := map[string]bool{}
			for pages := 0; ; pages++ {
				if pages >= 10000 {
					return ErrUnavailable
				}
				docs, next, e := s.sources.DiscoverySnapshot(ctx, source, cursor, 100)
				if pages == 0 && errors.Is(e, ErrSourceDisabled) {
					break
				}
				if e != nil {
					return e
				}
				if len(docs) > 100 || next != "" && (next == cursor || seen[next]) {
					return ErrUnavailable
				}
				seen[next] = true
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
		return s.rebuildActivitiesWindow(ctx, from, to, putActivity)
	}
	if wr, ok := s.repo.(windowRebuilder); ok {
		return wr.RebuildWindow(ctx, from, to, load)
	}
	return s.repo.Rebuild(ctx, load)
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
	q = strings.TrimSpace(q)
	out := Page{Items: []Item{}}
	if e := auth(user); e != nil {
		return out, e
	}
	if utf8.RuneCountInString(q) > 200 || limit < 1 || limit > 100 || kind != "" && kind != "school" && kind != "course" && kind != "teacher" && kind != "review" && kind != "resource" {
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
				out.Items = append(out.Items, documentItem(live))
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
	from, to := s.activityWindow()
	f := ActivityFilter{Kind: kind, From: from, To: to, Limit: 100}
	for scanned := 0; ; {
		docs, e := s.repo.Activities(ctx, f)
		if e != nil {
			return out, e
		}
		for _, d := range docs {
			scanned++
			if scanned > 10000 {
				return out, ErrUnavailable
			}
			f.After = d.Source + "/" + d.ID
			live, ok, e := s.sources.CurrentActivity(ctx, user, d.Source, d.ID)
			if errors.Is(e, ErrSourceDisabled) {
				continue
			}
			if e != nil {
				return out, upstreamError(ctx)
			}
			if !ok {
				continue
			}
			if ValidateActivity(live) != nil || live.Source != d.Source || live.ID != d.ID || live.Target != d.Target || live.Day != d.Day {
				return out, ErrUnavailable
			}
			if !live.Visible || live.Revision != d.Revision {
				continue
			}
			v := scores[d.Target]
			if v == nil {
				v = &ranked{}
				v.d.Target = d.Target
				scores[d.Target] = v
			}
			for i, name := range []string{"views", "reviews", "favorites", "interactions"} {
				n := live.Metrics[name]
				v.score += float64(n) * s.config.HeatWeights[i]
				v.samples += n
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
		out.Items = append(out.Items, documentItem(r.d))
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

func searchText(v string, max int) bool {
	return utf8.ValidString(v) && utf8.RuneCountInString(v) <= max && !strings.ContainsRune(v, 0)
}
func searchNames(values []string, max int) bool {
	if len(values) > 50 {
		return false
	}
	for _, value := range values {
		if !searchText(value, max) {
			return false
		}
	}
	return true
}
func documentItem(d Document) Item {
	return Item{Target: d.Target, Title: d.Title, SchoolID: d.SchoolID, Code: d.Code, EnglishName: d.EnglishName, FormerCodes: append([]string{}, d.FormerCodes...), FormerNames: append([]string{}, d.FormerNames...)}
}
