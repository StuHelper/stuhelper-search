package search

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

// ActivitySnapshot is one provider's complete daily counters for one canonical object.
// Missing providers contribute zero. Metrics are never transferred between providers.
type ActivitySnapshot struct {
	Source   string           `json:"source"`
	ID       string           `json:"id"`
	Target   Target           `json:"target"`
	Day      string           `json:"day"`
	Revision int64            `json:"revision"`
	Visible  bool             `json:"visible"`
	Metrics  map[string]int64 `json:"metrics"`
}
type ActivityEvent struct {
	SpecVersion string           `json:"specversion"`
	ID          string           `json:"id"`
	Source      string           `json:"source"`
	Type        string           `json:"type"`
	Subject     string           `json:"subject"`
	Time        time.Time        `json:"time"`
	Data        ActivitySnapshot `json:"data"`
}
type ActivityFilter struct {
	Kind, After string
	From, To    time.Time
	Limit       int
}

// ActivityID is independent of provider; the database key includes Source.
func ActivityID(target Target, day string) string {
	return target.Kind + "/" + encode(target.ID) + "/" + day
}
func ValidateActivity(d ActivitySnapshot) error {
	if d.Target.Source != "catalog" || !validTarget(d.Target) || d.Revision < 1 || d.ID != ActivityID(d.Target, d.Day) {
		return ErrInput
	}
	day, e := time.Parse(time.DateOnly, d.Day)
	if e != nil || day.Format(time.DateOnly) != d.Day {
		return ErrInput
	}
	var fields []string
	switch d.Source {
	case "catalog":
		fields = []string{"views"}
	case "reviews":
		fields = []string{"reviews", "interactions"}
	case "favorites":
		fields = []string{"favorites"}
	default:
		return ErrInput
	}
	if len(d.Metrics) != len(fields) {
		return ErrInput
	}
	for _, name := range fields {
		n, ok := d.Metrics[name]
		if !ok || n < 0 || n > 1000000000 {
			return ErrInput
		}
	}
	return nil
}
func (s *Service) ConsumeActivity(ctx context.Context, e ActivityEvent) error {
	if e.SpecVersion != "1.0" || e.ID == "" || len(e.ID) > 200 || e.Type != "discovery.activity.v1" || e.Source != e.Data.Source || e.Subject != e.Data.ID || e.Time.IsZero() {
		return ErrInput
	}
	if err := ValidateActivity(e.Data); err != nil {
		return err
	}
	b, err := json.Marshal(e)
	if err != nil {
		return ErrInput
	}
	h := sha256.Sum256(b)
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	return s.repo.ApplyActivity(ctx, e, hex.EncodeToString(h[:]))
}

// Daily windows include the UTC day containing now-HeatWindow and the current day.
// From is inclusive; To is the next UTC midnight, exclusive.
func (s *Service) activityWindow() (time.Time, time.Time) {
	now := s.config.Now().UTC()
	return now.Add(-s.config.HeatWindow).Truncate(24 * time.Hour), now.Truncate(24 * time.Hour).Add(24 * time.Hour)
}
func (s *Service) rebuildActivities(ctx context.Context, put func([]ActivitySnapshot) error) error {
	from, to := s.activityWindow()
	for _, source := range []string{"catalog", "reviews", "favorites"} {
		cursor := ""
		seen := map[string]bool{}
		for pages := 0; ; pages++ {
			if pages >= 10000 {
				return ErrUnavailable
			}
			docs, next, e := s.sources.ActivitySnapshot(ctx, source, from, to, cursor, 100)
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
				if ValidateActivity(d) != nil || d.Source != source || d.Day < from.Format(time.DateOnly) || d.Day >= to.Format(time.DateOnly) {
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
}
