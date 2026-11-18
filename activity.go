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
	// AfterDay selects window-ordered traversal; omitted by callers using the source/ID order.
	AfterDay    string
	Kind, After string
	From, To    time.Time
	Limit       int
}

// ActivityID is independent of provider; the database key includes Source.
func ActivityID(target Target, day string) string {
	return target.Kind + "/" + encode(target.ID) + "/" + day
}
func ValidateActivity(d ActivitySnapshot) error {
	if !validTarget(d.Target) || d.Revision < 1 || !searchText(d.ID, 1087) || d.ID != ActivityID(d.Target, d.Day) {
		return ErrInput
	}
	day, e := time.Parse(time.DateOnly, d.Day)
	if e != nil || day.Format(time.DateOnly) != d.Day {
		return ErrInput
	}
	var fields []string
	switch d.Source {
	case "catalog":
		if d.Target.Source != "catalog" || (d.Target.Kind != "course" && d.Target.Kind != "teacher") {
			return ErrInput
		}
		fields = []string{"views"}
	case "reviews":
		if d.Target.Source != "catalog" || (d.Target.Kind != "course" && d.Target.Kind != "teacher") {
			return ErrInput
		}
		fields = []string{"reviews", "interactions"}
	case "favorites":
		if !((d.Target.Source == "catalog" && (d.Target.Kind == "course" || d.Target.Kind == "teacher")) || (d.Target.Source == "resources" && d.Target.Kind == "resource")) {
			return ErrInput
		}
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
	start := time.Now()
	if e.SpecVersion != "1.0" || e.ID == "" || !searchText(e.ID, 200) || e.Type != "discovery.activity.v1" || e.Source != e.Data.Source || e.Subject != e.Data.ID || e.Time.IsZero() {
		s.observe("consume_activity", "", "invalid_input", start, 0, 0)
		return ErrInput
	}
	if err := ValidateActivity(e.Data); err != nil {
		s.observe("consume_activity", e.Source, errorCode(err, "invalid_input"), start, 0, 0)
		return err
	}
	canonical := e
	canonical.Time = e.Time.UTC()
	b, err := json.Marshal(canonical)
	if err != nil {
		s.observe("consume_activity", e.Source, "invalid_input", start, 0, 0)
		return ErrInput
	}
	h := sha256.Sum256(b)
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	err = s.repo.ApplyActivity(ctx, e, hex.EncodeToString(h[:]))
	s.observe("consume_activity", e.Source, errorCode(err, "storage_unavailable"), start, 1, 0)
	return err
}

// Daily windows include the UTC day containing now-HeatWindow and the current day.
// From is inclusive; To is the next UTC midnight, exclusive.
func (s *Service) activityWindow() (time.Time, time.Time) {
	return s.activityWindowAt(s.config.Now().UTC())
}
func (s *Service) activityWindowAt(now time.Time) (time.Time, time.Time) {
	now = now.UTC()
	return now.Add(-s.config.HeatWindow).Truncate(24 * time.Hour), now.Truncate(24 * time.Hour).Add(24 * time.Hour)
}
func (s *Service) rebuildActivitiesWindow(ctx context.Context, from, to time.Time, put func([]ActivitySnapshot) error) (string, string, int, error) {
	scanned := 0
	for _, source := range []string{"catalog", "reviews", "favorites"} {
		cursor := ""
		seen := map[string]bool{}
		for pages := 0; ; pages++ {
			if pages >= 10000 {
				return source, "invalid_source_data", scanned, ErrUnavailable
			}
			docs, next, e := s.sources.ActivitySnapshot(ctx, source, from, to, cursor, 100)
			if pages == 0 && errors.Is(e, ErrSourceDisabled) {
				break
			}
			if e != nil {
				return source, "upstream_unavailable", scanned, e
			}
			if len(docs) > 100 || next != "" && (next == cursor || seen[next]) {
				return source, "invalid_source_data", scanned, ErrUnavailable
			}
			seen[next] = true
			for _, d := range docs {
				scanned++
				if ValidateActivity(d) != nil || d.Source != source || d.Day < from.Format(time.DateOnly) || d.Day >= to.Format(time.DateOnly) {
					return source, "invalid_source_data", scanned, ErrUnavailable
				}
			}
			if e = put(docs); e != nil {
				return "", "storage_unavailable", scanned, e
			}
			if next == "" {
				break
			}
			cursor = next
		}
	}
	return "", "", scanned, nil
}
