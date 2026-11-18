package search

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type cursorKeyProvider interface {
	CursorKey(context.Context) ([]byte, error)
}

type Service struct {
	repo        Repository
	sources     Sources
	config      Config
	fallbackKey []byte
}

func New(repo Repository, sources Sources, c Config) (*Service, error) {
	if repo == nil || sources == nil || c.QueryTimeout <= 0 || c.QueryTimeout > 30*time.Second || c.RebuildTimeout < 0 || c.RebuildTimeout > 30*time.Minute || c.HeatWindow <= 0 || c.HeatWindow > 366*24*time.Hour || c.MinimumHeatSamples < 1 || (len(c.CursorKey) != 0 && len(c.CursorKey) != 32) {
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
	fallback := make([]byte, 32)
	if len(c.CursorKey) == 32 {
		c.CursorKey = append([]byte(nil), c.CursorKey...)
		copy(fallback, c.CursorKey)
	} else if _, err := rand.Read(fallback); err != nil {
		return nil, ErrUnavailable
	}
	return &Service{repo: repo, sources: sources, config: c, fallbackKey: fallback}, nil
}
func validTarget(t Target) bool {
	return t.ID != "" && searchText(t.ID, 200) && ((t.Source == "catalog" && (t.Kind == "school" || t.Kind == "course" || t.Kind == "teacher")) || (t.Source == "reviews" && t.Kind == "review") || (t.Source == "resources" && t.Kind == "resource"))
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
		if !searchText(d.EnglishName, 256) || !nonEmptyUniqueNames(d.FormerCodes, 128) || !nonEmptyUniqueNames(d.FormerNames, 256) {
			return ErrInput
		}
	} else if d.EnglishName != "" || len(d.FormerCodes) > 0 || len(d.FormerNames) > 0 {
		return ErrInput
	}

	if !validTarget(d.Target) || d.Revision < 1 || !searchText(d.Title, 1000) || !searchText(d.SchoolID, 200) || !searchText(d.CourseID, 200) || len(d.TeacherIDs) > 100 || d.At.IsZero() {
		return ErrInput
	}
	if d.Visible && (d.Title == "" || d.SchoolID == "") {
		return ErrInput
	}
	seen := map[string]bool{}
	for _, id := range d.TeacherIDs {
		if id == "" || !searchText(id, 200) || seen[id] {
			return ErrInput
		}
		seen[id] = true
	}
	if d.Visible && d.Target.Kind == "review" && d.CourseID == "" {
		return ErrInput
	}

	return nil
}

func canonicalDocument(d Document) Document {
	d.At = d.At.UTC()
	if len(d.Aliases) == 0 {
		d.Aliases = nil
	}
	if len(d.TeacherIDs) == 0 {
		d.TeacherIDs = nil
	}
	if len(d.FormerCodes) == 0 {
		d.FormerCodes = nil
	}
	if len(d.FormerNames) == 0 {
		d.FormerNames = nil
	}
	return d
}

func (s *Service) Consume(ctx context.Context, e Event) error {
	if e.SpecVersion != "1.0" || e.ID == "" || !searchText(e.ID, 200) || e.Source != e.Data.Target.Source || e.Subject != e.Data.Target.ID || e.Type != e.Source+".discovery.v1" || e.Time.IsZero() {
		return ErrInput
	}
	if err := Validate(e.Data); err != nil {
		return err
	}
	canonical := e
	canonical.Time = e.Time.UTC()
	canonical.Data = canonicalDocument(e.Data)
	b, err := json.Marshal(canonical)
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

func (s *Service) resolveCursorKey(ctx context.Context) ([]byte, error) {
	if len(s.config.CursorKey) == 32 {
		return s.config.CursorKey, nil
	}
	if p, ok := s.repo.(cursorKeyProvider); ok {
		return p.CursorKey(ctx)
	}
	return s.fallbackKey, nil
}

// sealCursor encrypts pagination state with AES-256-GCM bound to (scope, user) so restricted IDs never leak.
func (s *Service) sealCursor(ctx context.Context, scope, user, plain string) (string, error) {
	rawKey, err := s.resolveCursorKey(ctx)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(rawKey)
	if err != nil {
		return "", ErrUnavailable
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", ErrUnavailable
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", ErrUnavailable
	}
	aad := []byte(scope + "\x00" + user)
	sealed := gcm.Seal(nonce, nonce, []byte(plain), aad)
	return base64.RawURLEncoding.EncodeToString(sealed), nil
}

func (s *Service) openCursor(ctx context.Context, scope, user, token string) (string, error) {
	if token == "" {
		return "", nil
	}
	if len(token) > 2048 {
		return "", ErrInput
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) < 12+16 {
		return "", ErrInput
	}
	rawKey, err := s.resolveCursorKey(ctx)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(rawKey)
	if err != nil {
		return "", ErrUnavailable
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil || len(raw) < gcm.NonceSize()+gcm.Overhead() {
		return "", ErrInput
	}
	aad := []byte(scope + "\x00" + user)
	plain, err := gcm.Open(nil, raw[:gcm.NonceSize()], raw[gcm.NonceSize():], aad)
	if err != nil {
		return "", ErrInput
	}
	out := string(plain)
	if !utf8.ValidString(out) || strings.ContainsRune(out, 0) {
		return "", ErrInput
	}
	return out, nil
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
	if !searchText(q, 200) || limit < 1 || limit > 100 || kind != "" && kind != "school" && kind != "course" && kind != "teacher" && kind != "review" && kind != "resource" {
		return out, ErrInput
	}
	started := time.Now()
	softBudget := s.config.QueryTimeout * 3 / 4
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	after, e := s.openCursor(ctx, "search", user, cursor)
	if e != nil {
		return out, e
	}
	// The cursor tracks examined candidates and stops before QueryTimeout so restricted blocks cannot stall pagination.
	for scanned := 0; scanned < 10000; {
		if scanned > 0 && time.Since(started) >= softBudget {
			next, err := s.sealCursor(ctx, "search", user, after)
			return Page{Items: out.Items, NextCursor: next}, err
		}
		docs, e := s.repo.Candidates(ctx, Filter{Query: q, Kind: kind, After: after, Limit: 100})
		if e != nil {
			return out, e
		}
		for i, d := range docs {
			if scanned > 0 && time.Since(started) >= softBudget {
				next, err := s.sealCursor(ctx, "search", user, after)
				return Page{Items: out.Items, NextCursor: next}, err
			}
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
				next, err := s.sealCursor(ctx, "search", user, after)
				if err != nil {
					return Page{}, err
				}
				out.NextCursor = next
				return out, nil
			}
			if scanned == 10000 && (i < len(docs)-1 || len(docs) == 100) {
				next, err := s.sealCursor(ctx, "search", user, after)
				if err != nil {
					return Page{}, err
				}
				out.NextCursor = next
				return out, nil
			}
		}
		if len(docs) < 100 {
			return out, nil
		}
	}
	next, e := s.sealCursor(ctx, "search", user, after)
	if e != nil {
		return Page{}, e
	}
	out.NextCursor = next
	return out, nil
}
func (s *Service) MyVotes(ctx context.Context, user, cursor string, limit int) (Votes, error) {
	if e := auth(user); e != nil {
		return Votes{}, e
	}
	if limit < 1 || limit > 100 || len(cursor) > 1024 || !searchText(cursor, 1024) {
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
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	scope := "popular:" + kind
	after, e := s.openCursor(ctx, scope, user, cursor)
	if e != nil {
		return out, e
	}
	var afterScore float64
	var afterKey string
	if after != "" {
		scorePart, keyPart, ok := strings.Cut(after, ":")
		if !ok {
			return out, ErrInput
		}
		parsedScore, err := strconv.ParseFloat(scorePart, 64)
		if err != nil || math.IsNaN(parsedScore) || math.IsInf(parsedScore, 0) || parsedScore < 0 {
			return out, ErrInput
		}
		src, id, ok := strings.Cut(keyPart, "/")
		if !ok || src != "catalog" || !searchText(id, 200) || id == "" {
			return out, ErrInput
		}
		afterScore = parsedScore
		afterKey = keyPart
	}
	type ranked struct {
		d       Document
		score   float64
		samples int64
	}
	scores := map[Target]*ranked{}
	from, to := s.activityWindow()
	f := ActivityFilter{Kind: kind, From: from, To: to, Limit: 100, AfterDay: from.UTC().Format(time.DateOnly)}
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
			f.AfterDay = d.Day
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
			if !live.Visible || live.Revision < d.Revision {
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
	for _, r := range rows {
		k := key(r.d)
		if after != "" && !(r.score < afterScore || (r.score == afterScore && k > afterKey)) {
			continue
		}
		token := fmt.Sprintf("%.17g:%s", r.score, k)
		out.Items = append(out.Items, documentItem(r.d))
		if len(out.Items) == limit {
			next, err := s.sealCursor(ctx, scope, user, token)
			if err != nil {
				return Page{}, err
			}
			out.NextCursor = next
			break
		}
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
func nonEmptyUniqueNames(values []string, max int) bool {
	if len(values) > 50 {
		return false
	}
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		if value == "" || !searchText(value, max) || seen[value] {
			return false
		}
		seen[value] = true
	}
	return true
}
func documentItem(d Document) Item {
	return Item{Target: d.Target, Title: d.Title, SchoolID: d.SchoolID, Code: d.Code, EnglishName: d.EnglishName, FormerCodes: append([]string{}, d.FormerCodes...), FormerNames: append([]string{}, d.FormerNames...)}
}
