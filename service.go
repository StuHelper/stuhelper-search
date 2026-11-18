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
	"golang.org/x/text/unicode/norm"
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
	switch d.Target.Kind {
	case "school":
		if d.Target.ID != d.SchoolID || !searchText(d.Code, 128) || !searchNames(d.Aliases, 256) {
			return ErrInput
		}
	case "course":
		if !searchText(d.Code, 128) || !searchNames(d.Aliases, 256) {
			return ErrInput
		}
	case "teacher":
		if d.Code != "" || !searchNames(d.Aliases, 256) {
			return ErrInput
		}
	default:
		if d.Code != "" || len(d.Aliases) != 0 {
			return ErrInput
		}
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

func (s *Service) observe(op, src, code string, start time.Time, scanned, skips int) {
	if s.config.Observer != nil {
		switch src {
		case "catalog", "reviews", "resources", "favorites":
		default:
			src = ""
		}
		s.config.Observer.ObserveSearch(Observation{
			Operation:     op,
			Source:        src,
			ErrorCode:     code,
			Duration:      time.Since(start),
			Scanned:       scanned,
			RevisionSkips: skips,
		})
	}
}

func errorCode(err error, defaultCode string) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrInput):
		return "invalid_input"
	case errors.Is(err, ErrUnauthenticated):
		return "unauthenticated"
	case errors.Is(err, ErrConflict):
		return "event_conflict"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return defaultCode
	}
}

type statusWindowRebuilder interface {
	RebuildWindowAt(context.Context, time.Time, time.Time, time.Time, func(func([]Document) error, func([]ActivitySnapshot) error) error) error
}

func (s *Service) Status(ctx context.Context) (Status, error) {
	start := time.Now()
	sr, ok := s.repo.(StatusReader)
	if !ok {
		s.observe("status", "", "storage_unavailable", start, 0, 0)
		return Status{}, ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	st, err := sr.Status(ctx)
	s.observe("status", "", errorCode(err, "storage_unavailable"), start, 0, 0)
	return st, err
}

func (s *Service) Consume(ctx context.Context, e Event) error {
	start := time.Now()
	if e.SpecVersion != "1.0" || e.ID == "" || !searchText(e.ID, 200) || e.Source != e.Data.Target.Source || e.Subject != e.Data.Target.ID || e.Type != e.Source+".discovery.v1" || e.Time.IsZero() {
		s.observe("consume", "", "invalid_input", start, 0, 0)
		return ErrInput
	}
	if err := Validate(e.Data); err != nil {
		s.observe("consume", e.Source, errorCode(err, "invalid_input"), start, 0, 0)
		return err
	}
	canonical := e
	canonical.Time = e.Time.UTC()
	canonical.Data = canonicalDocument(e.Data)
	b, err := json.Marshal(canonical)
	if err != nil {
		s.observe("consume", e.Source, "invalid_input", start, 0, 0)
		return ErrInput
	}
	h := sha256.Sum256(b)
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	err = s.repo.Apply(ctx, e, hex.EncodeToString(h[:]))
	s.observe("consume", e.Source, errorCode(err, "storage_unavailable"), start, 1, 0)
	return err
}

type windowRebuilder interface {
	RebuildWindow(context.Context, time.Time, time.Time, func(func([]Document) error, func([]ActivitySnapshot) error) error) error
}

func (s *Service) Rebuild(ctx context.Context) error {
	start := time.Now()
	rebuiltAt := s.config.Now().UTC()
	from, to := s.activityWindowAt(rebuiltAt)
	ctx, cancel := context.WithTimeout(ctx, s.config.RebuildTimeout)
	defer cancel()
	var activeSource string
	var errCode = "storage_unavailable"
	var scanned int
	loader := func(put func([]Document) error, putActivity func([]ActivitySnapshot) error) error {
		for _, source := range []string{"catalog", "reviews", "resources"} {
			cursor := ""
			seen := map[string]bool{}
			for pages := 0; ; pages++ {
				activeSource = source
				if pages >= 10000 {
					errCode = "invalid_source_data"
					return ErrUnavailable
				}
				docs, next, e := s.sources.DiscoverySnapshot(ctx, source, cursor, 100)
				if pages == 0 && errors.Is(e, ErrSourceDisabled) {
					break
				}
				if e != nil {
					errCode = "upstream_unavailable"
					return e
				}
				if len(docs) > 100 || next != "" && (next == cursor || seen[next]) {
					errCode = "invalid_source_data"
					return ErrUnavailable
				}
				seen[next] = true
				for _, d := range docs {
					scanned++
					if d.Target.Source != source || Validate(d) != nil {
						errCode = "invalid_source_data"
						return ErrUnavailable
					}
				}
				activeSource = ""
				if e = put(docs); e != nil {
					errCode = "storage_unavailable"
					return e
				}
				if next == "" {
					break
				}
				cursor = next
			}
		}
		src, code, actScanned, e := s.rebuildActivitiesWindow(ctx, from, to, putActivity)
		scanned += actScanned
		if e != nil {
			activeSource = src
			errCode = code
			return e
		}
		activeSource = ""
		return nil
	}
	var err error
	if wr, ok := s.repo.(statusWindowRebuilder); ok {
		err = wr.RebuildWindowAt(ctx, rebuiltAt, from, to, loader)
	} else if wr, ok := s.repo.(windowRebuilder); ok {
		err = wr.RebuildWindow(ctx, from, to, loader)
	} else {
		err = s.repo.Rebuild(ctx, loader)
	}
	s.observe("rebuild", activeSource, errorCode(err, errCode), start, scanned, 0)
	return err
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

func (s *Service) current(ctx context.Context, user string, d Document) (Document, bool, bool, string, error) {
	live, ok, e := s.sources.Current(ctx, user, d.Target)
	if e != nil {
		return Document{}, false, false, "upstream_unavailable", upstreamError(ctx)
	}
	if !ok {
		return Document{}, false, false, "", nil
	}
	if Validate(live) != nil || live.Target != d.Target {
		return Document{}, false, false, "invalid_source_data", upstreamError(ctx)
	}
	if !live.Visible {
		return live, false, false, "", nil
	}
	if live.Revision != d.Revision {
		return live, false, true, "", nil
	}
	return live, true, false, "", nil
}
func auth(user string) error {
	if user == "" {
		return ErrUnauthenticated
	}
	return nil
}
func (s *Service) Search(ctx context.Context, user, q, kind, cursor string, limit int) (Page, error) {
	return s.SearchWithParams(ctx, user, SearchParams{Query: q, Kind: kind, Cursor: cursor, Limit: limit})
}
func (s *Service) SearchWithParams(ctx context.Context, user string, p SearchParams) (Page, error) {
	q := strings.TrimSpace(p.Query)
	start := time.Now()
	kind, limit, cursor := p.Kind, p.Limit, p.Cursor
	out := Page{Items: []Item{}}
	if e := auth(user); e != nil {
		s.observe("search", "", "unauthenticated", start, 0, 0)
		return out, e
	}
	if !validFilterID(p.SchoolID) || !validFilterID(p.CourseID) || !validFilterID(p.TeacherID) || !searchText(q, 200) || limit < 1 || limit > 100 || kind != "" && kind != "school" && kind != "course" && kind != "teacher" && kind != "review" && kind != "resource" {
		s.observe("search", "", "invalid_input", start, 0, 0)
		return out, ErrInput
	}
	normQ, terms := NormalizeQuery(q)
	softBudget := s.config.QueryTimeout * 3 / 4
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	after, e := s.openCursor(ctx, "search", user, cursor)
	if e != nil {
		s.observe("search", "", "invalid_input", start, 0, 0)
		return out, e
	}
	if after != "" {
		if normQ != "" && !strings.Contains(strings.SplitN(after, "/", 2)[0], ":") {
			return out, ErrInput
		}
		if _, _, _, _, _, err := ParseSearchAfter(after, normQ != ""); err != nil {
			return out, err
		}
	}
	scanned, skips := 0, 0
	continuation := func() (Page, error) {
		next, err := s.sealCursor(ctx, "search", user, after)
		s.observe("search", "", errorCode(err, "storage_unavailable"), start, scanned, skips)
		if err != nil {
			return Page{}, err
		}
		out.NextCursor = next
		return out, nil
	}
	// The cursor tracks examined candidates and stops before QueryTimeout so restricted blocks cannot stall pagination.
	for scanned < 10000 {
		if scanned > 0 && time.Since(start) >= softBudget {
			return continuation()
		}
		docs, e := s.repo.Candidates(ctx, Filter{Query: normQ, Kind: kind, SchoolID: p.SchoolID, CourseID: p.CourseID, TeacherID: p.TeacherID, After: after, Limit: 100})
		if e != nil {
			s.observe("search", "", errorCode(e, "storage_unavailable"), start, scanned, skips)
			return out, e
		}
		for i, d := range docs {
			if scanned > 0 && time.Since(start) >= softBudget {
				return continuation()
			}
			scanned++
			after = SearchAfterToken(d, normQ, terms)
			live, ok, skippedRev, errCategory, e := s.current(ctx, user, d)
			if e != nil {
				s.observe("search", d.Target.Source, errorCode(e, errCategory), start, scanned, skips)
				return Page{}, e
			}
			if skippedRev {
				skips++
			}
			if ok && matchesSearchFilter(live, p) {
				out.Items = append(out.Items, documentItem(live))
			}
			if len(out.Items) == limit {
				return continuation()
			}
			if scanned == 10000 && (i < len(docs)-1 || len(docs) == 100) {
				return continuation()
			}
		}
		if len(docs) < 100 {
			s.observe("search", "", "ok", start, scanned, skips)
			return out, nil
		}
	}
	return continuation()
}
func (s *Service) MyVotes(ctx context.Context, user, cursor string, limit int) (result Votes, err error) {
	start := time.Now()
	scanned := 0
	source, category := "", "invalid_input"
	defer func() { s.observe("my_votes", source, errorCode(err, category), start, scanned, 0) }()

	if e := auth(user); e != nil {
		return Votes{}, e
	}
	if limit < 1 || limit > 100 || len(cursor) > 1024 || !searchText(cursor, 1024) {
		return Votes{}, ErrInput
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	const maxMyVotesScan = 500
	out := Votes{Items: []Vote{}}
	cur := cursor
	seen := map[string]bool{}
	if cur != "" {
		seen[cur] = true
	}
	for len(out.Items) < limit && scanned < maxMyVotesScan {
		need := limit - len(out.Items)
		if rem := maxMyVotesScan - scanned; need > rem {
			need = rem
		}
		source, category = "reviews", "upstream_unavailable"
		v, e := s.sources.MyVotes(ctx, user, cur, need)
		if e != nil {
			return Votes{}, upstreamError(ctx)
		}
		category = "invalid_source_data"
		if len(v.Items) > need || len(v.NextCursor) > 1024 {
			return Votes{}, upstreamError(ctx)
		}
		if v.NextCursor != "" && (len(v.Items) == 0 || seen[v.NextCursor]) {
			return Votes{}, ErrUnavailable
		}
		for _, vote := range v.Items {
			scanned++
			if vote.Target.Source != "reviews" || vote.Target.Kind != "review" || !validTarget(vote.Target) || (vote.Choice != "up" && vote.Choice != "down") {
				return Votes{}, upstreamError(ctx)
			}
			d, ok, e := s.sources.Current(ctx, user, vote.Target)
			if e != nil {
				category = "upstream_unavailable"
				return Votes{}, upstreamError(ctx)
			}
			if ok && (d.Target != vote.Target || Validate(d) != nil) {
				return Votes{}, ErrUnavailable
			}
			if ok && d.Visible {
				out.Items = append(out.Items, Vote{
					Target:   vote.Target,
					Choice:   vote.Choice,
					Title:    d.Title,
					CourseID: d.CourseID,
					SchoolID: d.SchoolID,
				})
			}
		}
		cur = v.NextCursor
		if cur == "" {
			out.NextCursor = ""
			break
		}
		seen[cur] = true
		out.NextCursor = cur
	}
	source = ""
	return out, nil
}
func (s *Service) Popular(ctx context.Context, user, kind, cursor string, limit int) (Page, error) {
	return s.PopularWithParams(ctx, user, PopularParams{Kind: kind, Cursor: cursor, Limit: limit})
}
func (s *Service) PopularWithParams(ctx context.Context, user string, p PopularParams) (Page, error) {
	kind, limit, cursor := p.Kind, p.Limit, p.Cursor
	start := time.Now()
	windowDays := int(math.Ceil(s.config.HeatWindow.Hours() / 24))
	if windowDays < 1 {
		windowDays = 1
	}
	out := Page{Items: []Item{}, WindowDays: windowDays}
	if e := auth(user); e != nil {
		s.observe("popular", "", "unauthenticated", start, 0, 0)
		return out, e
	}
	if !validFilterID(p.SchoolID) || limit < 1 || limit > 100 || (kind != "course" && kind != "teacher") {
		s.observe("popular", "", "invalid_input", start, 0, 0)
		return out, ErrInput
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	scope := "popular:" + kind
	after, e := s.openCursor(ctx, scope, user, cursor)
	if e != nil {
		s.observe("popular", "", "invalid_input", start, 0, 0)
		return out, e
	}
	type ranked struct {
		d       Document
		score   float64
		samples int64
	}
	scores := map[Target]*ranked{}
	from, to := s.activityWindow()
	f := ActivityFilter{Kind: kind, From: from, To: to, Limit: 100, AfterDay: from.UTC().Format(time.DateOnly)}
	scanned := 0
	skips := 0
	for {
		docs, e := s.repo.Activities(ctx, f)
		if e != nil {
			s.observe("popular", "", errorCode(e, "storage_unavailable"), start, scanned, skips)
			return out, e
		}
		for _, d := range docs {
			scanned++
			if scanned > 10000 {
				s.observe("popular", "", "scan_limit_exceeded", start, scanned, skips)
				return out, ErrUnavailable
			}
			f.After = d.Source + "/" + d.ID
			f.AfterDay = d.Day
			live, ok, e := s.sources.CurrentActivity(ctx, user, d.Source, d.ID)
			if errors.Is(e, ErrSourceDisabled) {
				continue
			}
			if e != nil {
				err := upstreamError(ctx)
				s.observe("popular", d.Source, errorCode(err, "upstream_unavailable"), start, scanned, skips)
				return out, err
			}
			if !ok {
				continue
			}
			if ValidateActivity(live) != nil || live.Source != d.Source || live.ID != d.ID || live.Target != d.Target || live.Day != d.Day {
				s.observe("popular", d.Source, "invalid_source_data", start, scanned, skips)
				return out, ErrUnavailable
			}
			if !live.Visible {
				continue
			}
			if live.Revision < d.Revision {
				skips++
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
			err := upstreamError(ctx)
			s.observe("popular", t.Source, errorCode(err, "upstream_unavailable"), start, scanned, skips)
			return out, err
		}
		if ok && (d.Target != t || Validate(d) != nil) {
			s.observe("popular", t.Source, "invalid_source_data", start, scanned, skips)
			return out, ErrUnavailable
		}
		if ok && d.Visible && (p.SchoolID == "" || d.SchoolID == p.SchoolID) {
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
			next, err := s.sealCursor(ctx, scope, user, token)
			if err != nil {
				return Page{}, err
			}
			out.NextCursor = next
			break
		}
	}
	if !passed {
		s.observe("popular", "", "invalid_input", start, scanned, skips)
		return Page{}, ErrInput
	}
	s.observe("popular", "", "ok", start, scanned, skips)
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
func validFilterID(v string) bool {
	return v == "" || (searchText(v, 200) && strings.TrimSpace(v) == v && v != "")
}
func matchesSearchFilter(d Document, p SearchParams) bool {
	if p.Kind != "" && d.Target.Kind != p.Kind {
		return false
	}
	if p.SchoolID != "" && d.SchoolID != p.SchoolID {
		return false
	}
	if p.CourseID != "" && d.CourseID != p.CourseID {
		return false
	}
	if p.TeacherID != "" {
		found := false
		for _, id := range d.TeacherIDs {
			if id == p.TeacherID {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
func documentItem(d Document) Item {
	var at *time.Time
	if !d.At.IsZero() {
		utc := d.At.UTC()
		at = &utc
	}
	return Item{
		Target:      d.Target,
		Title:       d.Title,
		SchoolID:    d.SchoolID,
		Code:        d.Code,
		EnglishName: d.EnglishName,
		FormerCodes: append([]string{}, d.FormerCodes...),
		FormerNames: append([]string{}, d.FormerNames...),
		CourseID:    d.CourseID,
		TeacherIDs:  append([]string(nil), d.TeacherIDs...),
		At:          at,
	}
}

// NormalizeQuery applies Unicode NFKC normalization and splits into at most 5 non-empty tokens.
func NormalizeQuery(q string) (string, []string) {
	normQ := strings.TrimSpace(norm.NFKC.String(q))
	if normQ == "" {
		return "", nil
	}
	fields := strings.Fields(normQ)
	full := strings.Join(fields, " ")
	if len(fields) <= 5 {
		return full, fields
	}
	terms := append([]string(nil), fields[:4]...)
	terms = append(terms, strings.Join(fields[4:], " "))
	return full, terms
}

func foldSearch(s string) string {
	return strings.ToLower(norm.NFKC.String(s))
}

func isASCIIAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func kindPriority(kind string, hasQuery bool) int {
	if !hasQuery {
		return 0
	}
	switch kind {
	case "school":
		return 1
	case "course":
		return 2
	case "teacher":
		return 3
	case "resource":
		return 4
	case "review":
		return 5
	default:
		return 6
	}
}

func rankBucket(d Document, normQ string, terms []string) int {
	if normQ == "" {
		return 0
	}
	lq := foldSearch(normQ)
	title := foldSearch(d.Title)
	code := foldSearch(d.Code)
	eng := foldSearch(d.EnglishName)

	if d.Target.Kind == "school" && isASCIIAllDigits(normQ) {
		normCode := norm.NFKC.String(d.Code)
		if d.Target.ID == normQ || normCode == normQ || (len(normQ) == 5 && ((len(d.Target.ID) == 10 && isASCIIAllDigits(d.Target.ID) && d.Target.ID[5:] == normQ) || (len(normCode) == 10 && isASCIIAllDigits(normCode) && normCode[5:] == normQ))) {
			return 0
		}
	}
	if title == lq || ((d.Target.Kind == "school" || d.Target.Kind == "course") && code != "" && code == lq) {
		return 0
	}
	if d.Target.Kind == "course" {
		if eng != "" && eng == lq {
			return 0
		}
		for _, fc := range d.FormerCodes {
			if foldSearch(fc) == lq {
				return 0
			}
		}
	}

	if strings.HasPrefix(title, lq) || ((d.Target.Kind == "school" || d.Target.Kind == "course") && code != "" && strings.HasPrefix(code, lq)) || (d.Target.Kind == "course" && eng != "" && strings.HasPrefix(eng, lq)) {
		return 1
	}

	allTitle := true
	for _, t := range terms {
		if !strings.Contains(title, foldSearch(t)) {
			allTitle = false
			break
		}
	}
	if allTitle {
		return 2
	}
	if d.Target.Kind == "course" && eng != "" {
		allEng := true
		for _, t := range terms {
			if !strings.Contains(eng, foldSearch(t)) {
				allEng = false
				break
			}
		}
		if allEng {
			return 2
		}
	}
	return 3
}

// SearchAfterToken encodes (rank_bucket, kind_priority, source, id) for keyset pagination.
func SearchAfterToken(d Document, normQ string, terms []string) string {
	return fmt.Sprintf("%d:%d:%s/%s", rankBucket(d, normQ, terms), kindPriority(d.Target.Kind, normQ != ""), d.Target.Source, d.Target.ID)
}

// ParseSearchAfter parses either "<bucket>:<kind_priority>:<source>/<id>" or direct "<source>/<id>".
func ParseSearchAfter(after string, hasQuery bool) (int, int, string, string, bool, error) {
	if after == "" {
		return 0, 0, "", "", false, nil
	}
	if !utf8.ValidString(after) || strings.ContainsRune(after, 0) {
		return 0, 0, "", "", false, ErrInput
	}
	if !strings.Contains(strings.SplitN(after, "/", 2)[0], ":") {
		src, id, ok := strings.Cut(after, "/")
		if !ok || src == "" || id == "" || !searchText(id, 200) || (src != "catalog" && src != "reviews" && src != "resources") {
			return 0, 0, "", "", false, ErrInput
		}
		return 0, 0, src, id, true, nil
	}
	parts := strings.SplitN(after, ":", 3)
	if len(parts) != 3 {
		return 0, 0, "", "", false, ErrInput
	}
	bucket, err := strconv.Atoi(parts[0])
	if err != nil || bucket < 0 || bucket > 3 || (!hasQuery && bucket != 0) {
		return 0, 0, "", "", false, ErrInput
	}
	kr, err := strconv.Atoi(parts[1])
	if err != nil || (!hasQuery && kr != 0) || (hasQuery && (kr < 1 || kr > 5)) {
		return 0, 0, "", "", false, ErrInput
	}
	src, id, ok := strings.Cut(parts[2], "/")
	if !ok || src == "" || id == "" || !searchText(id, 200) || (src != "catalog" && src != "reviews" && src != "resources") {
		return 0, 0, "", "", false, ErrInput
	}
	return bucket, kr, src, id, true, nil
}
