package search

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxHeatWeight          = 1e9
	maxPopularCandidates   = 10000
	maxPopularActivityRows = 500000
)

type Service struct {
	repo    Repository
	sources Sources
	config  Config
}

func New(repo Repository, sources Sources, c Config) (*Service, error) {
	if repo == nil || sources == nil || c.QueryTimeout <= 0 || c.QueryTimeout > 30*time.Second || c.HeatWindow <= 0 || c.HeatWindow > 366*24*time.Hour || c.MinimumHeatSamples < 1 {
		return nil, ErrInput
	}
	positive := false
	for _, w := range c.HeatWeights {
		if math.IsNaN(w) || math.IsInf(w, 0) || w < 0 || w > maxHeatWeight {
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
func (s *Service) Rebuild(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	return s.repo.Rebuild(ctx, func(put func([]Document) error, putActivity func([]ActivitySnapshot) error) error {
		for _, source := range []string{"catalog", "reviews", "resources"} {
			cursor := ""
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
		return s.rebuildActivities(ctx, putActivity)
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

type rankedCandidate struct {
	target  Target
	score   float64
	samples int64
}

func targetKey(t Target) string { return t.Source + "/" + t.ID }

func formatPopularCursor(score float64, t Target) string {
	return strconv.FormatFloat(score, 'g', -1, 64) + ":" + targetKey(t)
}

func parsePopularCursor(after, kind string) (float64, string, error) {
	if after == "" {
		return 0, "", nil
	}
	scoreText, afterKey, ok := strings.Cut(after, ":")
	if !ok {
		return 0, "", ErrInput
	}
	score, err := strconv.ParseFloat(scoreText, 64)
	if err != nil || math.IsNaN(score) || math.IsInf(score, 0) || score < 0 {
		return 0, "", ErrInput
	}
	src, id, ok := strings.Cut(afterKey, "/")
	if !ok || src != "catalog" || !validTarget(Target{Source: src, Kind: kind, ID: id}) {
		return 0, "", ErrInput
	}
	return score, afterKey, nil
}

func (s *Service) accumulateActivity(scores map[Target]*rankedCandidate, d, live ActivitySnapshot) error {
	if ValidateActivity(live) != nil || live.Source != d.Source || live.ID != d.ID || live.Target != d.Target || live.Day != d.Day {
		return ErrUnavailable
	}
	if !live.Visible || live.Revision != d.Revision {
		return nil
	}
	v := scores[d.Target]
	if v == nil {
		if len(scores) >= maxPopularCandidates {
			return ErrUnavailable
		}
		v = &rankedCandidate{target: d.Target}
		scores[d.Target] = v
	}
	for i, name := range []string{"views", "reviews", "favorites", "interactions"} {
		n := live.Metrics[name]
		v.score += float64(n) * s.config.HeatWeights[i]
		v.samples += n
	}
	return nil
}

func (s *Service) scanPopularActivities(ctx context.Context, user, kind string) (map[Target]*rankedCandidate, error) {
	scores := map[Target]*rankedCandidate{}
	disabled := map[string]bool{}
	from, to := s.activityWindow()
	f := ActivityFilter{Kind: kind, From: from, To: to, Limit: 500}
	batchSrc, hasBatch := s.sources.(BatchActivitySource)
	scanned := 0
	for {
		docs, e := s.repo.Activities(ctx, f)
		if e != nil {
			return nil, e
		}
		if len(docs) > 0 {
			f.After = docs[len(docs)-1].Source + "/" + docs[len(docs)-1].ID
		}
		if hasBatch {
			groups := map[string][]ActivitySnapshot{}
			order := []string{}
			for _, d := range docs {
				if disabled[d.Source] {
					continue
				}
				if _, ok := groups[d.Source]; !ok {
					order = append(order, d.Source)
				}
				groups[d.Source] = append(groups[d.Source], d)
			}
			for _, src := range order {
				group := groups[src]
				for start := 0; start < len(group); start += 100 {
					if disabled[src] {
						break
					}
					end := min(start+100, len(group))
					chunk := group[start:end]
					ids := make([]string, len(chunk))
					wanted := make(map[string]bool, len(chunk))
					for i, d := range chunk {
						ids[i] = d.ID
						wanted[d.ID] = true
					}
					liveRows, e := batchSrc.CurrentActivities(ctx, user, src, ids)
					if errors.Is(e, ErrSourceDisabled) {
						disabled[src] = true
						f.ExcludeSources = append(f.ExcludeSources, src)
						break
					}
					if e != nil {
						return nil, upstreamError(ctx)
					}
					if len(liveRows) > len(chunk) {
						return nil, ErrUnavailable
					}
					byID := make(map[string]ActivitySnapshot, len(liveRows))
					for _, live := range liveRows {
						if !wanted[live.ID] {
							return nil, ErrUnavailable
						}
						if _, dup := byID[live.ID]; dup {
							return nil, ErrUnavailable
						}
						byID[live.ID] = live
					}
					for _, d := range chunk {
						scanned++
						if scanned > maxPopularActivityRows {
							return nil, ErrUnavailable
						}
						live, ok := byID[d.ID]
						if !ok {
							continue
						}
						if e := s.accumulateActivity(scores, d, live); e != nil {
							return nil, e
						}
					}
				}
			}
		} else {
			for _, d := range docs {
				if disabled[d.Source] {
					continue
				}
				live, ok, e := s.sources.CurrentActivity(ctx, user, d.Source, d.ID)
				if errors.Is(e, ErrSourceDisabled) {
					disabled[d.Source] = true
					f.ExcludeSources = append(f.ExcludeSources, d.Source)
					continue
				}
				scanned++
				if scanned > maxPopularActivityRows {
					return nil, ErrUnavailable
				}
				if e != nil {
					return nil, upstreamError(ctx)
				}
				if !ok {
					continue
				}
				if e := s.accumulateActivity(scores, d, live); e != nil {
					return nil, e
				}
			}
		}
		if len(docs) < f.Limit {
			break
		}
	}
	return scores, nil
}

func (s *Service) resolvePopularPage(ctx context.Context, user string, candidates []rankedCandidate, limit int) (Page, error) {
	out := Page{Items: []Item{}}
	batchSrc, hasBatch := s.sources.(BatchCurrentSource)
	if !hasBatch {
		for _, c := range candidates {
			d, ok, e := s.sources.Current(ctx, user, c.target)
			if e != nil {
				return Page{}, upstreamError(ctx)
			}
			if ok && (d.Target != c.target || Validate(d) != nil) {
				return Page{}, ErrUnavailable
			}
			if ok && d.Visible {
				out.Items = append(out.Items, documentItem(d))
				if len(out.Items) == limit {
					out.NextCursor = encode(formatPopularCursor(c.score, c.target))
					break
				}
			}
		}
		return out, nil
	}
	firstBatch := true
	for idx := 0; idx < len(candidates) && len(out.Items) < limit; {
		need := limit - len(out.Items)
		batchSize := need
		if !firstBatch && batchSize < 32 {
			batchSize = 32
		}
		firstBatch = false
		batchSize = min(batchSize, 100, len(candidates)-idx)
		chunk := candidates[idx : idx+batchSize]
		idx += batchSize
		targets := make([]Target, len(chunk))
		wanted := make(map[Target]bool, len(chunk))
		for i, c := range chunk {
			targets[i] = c.target
			wanted[c.target] = true
		}
		// All popular targets belong to catalog; check them in a single bounded batch.
		liveDocs, e := batchSrc.CurrentBatch(ctx, user, "catalog", targets)
		if e != nil {
			return Page{}, upstreamError(ctx)
		}
		if len(liveDocs) > len(chunk) {
			return Page{}, ErrUnavailable
		}
		byTarget := make(map[Target]Document, len(liveDocs))
		for _, d := range liveDocs {
			if !wanted[d.Target] || Validate(d) != nil {
				return Page{}, ErrUnavailable
			}
			if _, dup := byTarget[d.Target]; dup {
				return Page{}, ErrUnavailable
			}
			byTarget[d.Target] = d
		}
		for _, c := range chunk {
			d, ok := byTarget[c.target]
			if ok && d.Visible {
				out.Items = append(out.Items, documentItem(d))
				if len(out.Items) == limit {
					out.NextCursor = encode(formatPopularCursor(c.score, c.target))
					break
				}
			}
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
	afterScore, afterKey, e := parsePopularCursor(after, kind)
	if e != nil {
		return out, e
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	scores, e := s.scanPopularActivities(ctx, user, kind)
	if e != nil {
		return out, e
	}
	candidates := make([]rankedCandidate, 0, len(scores))
	for _, v := range scores {
		if v.samples >= int64(s.config.MinimumHeatSamples) {
			candidates = append(candidates, *v)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score == candidates[j].score {
			return targetKey(candidates[i].target) < targetKey(candidates[j].target)
		}
		return candidates[i].score > candidates[j].score
	})
	if after != "" {
		start := len(candidates)
		for i, c := range candidates {
			// Keyset continuation tolerates score updates and removed anchors between page loads.
			if c.score < afterScore || (c.score == afterScore && targetKey(c.target) > afterKey) {
				start = i
				break
			}
		}
		candidates = candidates[start:]
	}
	return s.resolvePopularPage(ctx, user, candidates, limit)
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
