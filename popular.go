package search

import (
	"context"
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

const maxPopularCandidates = 10000
const maxPopularActivityRows = 500000
const rankingTTL = 24 * time.Hour

type rankedCandidate struct {
	target  Target
	score   float64
	samples int64
}

func targetKey(t Target) string { return t.Source + "/" + t.ID }

func (s *Service) accumulateActivity(scores map[Target]*rankedCandidate, d, live ActivitySnapshot) error {
	if ValidateActivity(live) != nil || live.Source != d.Source || live.ID != d.ID || live.Target != d.Target || live.Day != d.Day {
		return ErrUnavailable
	}
	if !live.Visible || live.Revision < d.Revision {
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
	startTime := time.Now()
	scores := map[Target]*rankedCandidate{}
	disabled := map[string]bool{}
	from, to := s.activityWindow()
	f := ActivityFilter{Kind: kind, From: from, To: to, Limit: 500, AfterDay: from.UTC().Format(time.DateOnly)}
	batchSrc, hasBatch := s.sources.(BatchActivitySource)
	scanned := 0
	for {
		docs, e := s.repo.Activities(ctx, f)
		if e != nil {
			return nil, e
		}
		scanned += len(docs)
		if scanned > maxPopularActivityRows {
			return nil, ErrUnavailable
		}
		if len(docs) > 0 {
			f.AfterDay = docs[len(docs)-1].Day
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
						s.observe("popular", src, "upstream_unavailable", startTime, scanned, 0)
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
				if e != nil {
					s.observe("popular", d.Source, "upstream_unavailable", startTime, scanned, 0)
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

// First-page scores fix the order. Subsequent pages only resolve current readable metadata.
func (s *Service) rankedTargets(ctx context.Context, user, kind string) ([]Target, error) {
	scores, err := s.scanPopularActivities(ctx, user, kind)
	if err != nil {
		return nil, err
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
	targets := make([]Target, len(candidates))
	for i, c := range candidates {
		targets[i] = c.target
	}
	return targets, nil
}

func (s *Service) currentRanked(ctx context.Context, user string, targets []Target) (map[Target]Document, error) {
	out := make(map[Target]Document, len(targets))
	if batch, ok := s.sources.(BatchCurrentSource); ok {
		docs, e := batch.CurrentBatch(ctx, user, targets[0].Source, targets)
		if errors.Is(e, ErrSourceDisabled) {
			return out, nil
		}
		if e != nil {
			return nil, upstreamError(ctx)
		}
		wanted := make(map[Target]bool, len(targets))
		for _, t := range targets {
			wanted[t] = true
		}
		for _, d := range docs {
			if !wanted[d.Target] || Validate(d) != nil {
				return nil, ErrUnavailable
			}
			delete(wanted, d.Target)
			out[d.Target] = d
		}
		return out, nil
	}
	for _, t := range targets {
		d, ok, e := s.sources.Current(ctx, user, t)
		if errors.Is(e, ErrSourceDisabled) {
			continue
		}
		if e != nil {
			return nil, upstreamError(ctx)
		}
		if ok {
			if d.Target != t || Validate(d) != nil {
				return nil, ErrUnavailable
			}
			out[t] = d
		}
	}
	return out, nil
}

func (s *Service) PopularWithParams(ctx context.Context, user string, p PopularParams) (Page, error) {

	start := time.Now()
	out := Page{Items: []Item{}, WindowDays: int(math.Ceil(s.config.HeatWindow.Hours() / 24))}
	if e := auth(user); e != nil {
		return out, e
	}
	if !validFilterID(p.SchoolID) || p.Limit < 1 || p.Limit > 100 || (p.Kind != "course" && p.Kind != "teacher" && p.Kind != "resource") {
		return out, ErrInput
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	scope := "popular:" + p.Kind + ":" + p.SchoolID
	after, err := s.openCursor(ctx, scope, user, p.Cursor)
	if err != nil {
		return out, err
	}
	var targets []Target
	var snapshot string
	offset := 0
	if after == "" {
		targets, err = s.rankedTargets(ctx, user, p.Kind)
	} else {
		var index string
		var ok bool
		snapshot, index, ok = strings.Cut(after, ":")
		if !ok || len(snapshot) != 32 {
			return out, ErrInput
		}
		offset, err = strconv.Atoi(index)
		if err != nil || offset < 1 {
			return out, ErrInput
		}
		targets, err = s.repo.ResumeRanking(ctx, snapshot, s.config.Now(), rankingTTL)
	}
	if err != nil {
		s.observe("popular", "", errorCode(err, "storage_unavailable"), start, 0, 0)
		return out, err
	}
	if offset > len(targets) {
		return out, ErrInput
	}
	for offset < len(targets) && len(out.Items) < p.Limit {
		stop := min(len(targets), offset+p.Limit-len(out.Items))
		chunk := targets[offset:stop]
		docs, e := s.currentRanked(ctx, user, chunk)
		if e != nil {
			return out, e
		}
		for _, target := range chunk {
			d, ok := docs[target]
			if ok && d.Visible && (p.SchoolID == "" || d.SchoolID == p.SchoolID) {
				out.Items = append(out.Items, documentItem(d))
			}
		}
		offset = stop
	}
	if len(out.Items) == p.Limit {
		if snapshot == "" {
			snapshot, err = s.repo.SaveRanking(ctx, targets, s.config.Now(), rankingTTL)
			if err != nil {
				return Page{}, err
			}
		}
		out.NextCursor, err = s.sealCursor(ctx, scope, user, snapshot+":"+strconv.Itoa(offset))
	}
	s.observe("popular", "", "ok", start, offset, 0)
	return out, err
}
