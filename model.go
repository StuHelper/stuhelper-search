package search

import (
	"context"
	"errors"
	"time"
)

var (
	ErrInput           = errors.New("invalid_input")
	ErrUnauthenticated = errors.New("unauthenticated")
	ErrNotFound        = errors.New("not_found")
	ErrUnavailable     = errors.New("unavailable")
	ErrConflict        = errors.New("event_conflict")
)

type Target struct {
	Source string `json:"source"`
	ID     string `json:"id"`
	Kind   string `json:"kind"`
}
type Item struct {
	Target   Target `json:"target"`
	Title    string `json:"title"`
	SchoolID string `json:"school_id"`
}

// Document is a source-owned public projection snapshot, never review body or identity data.
// Activity counters cover At's UTC day; separate activity documents avoid overwriting content revisions.
type Document struct {
	Target     Target    `json:"target"`
	Revision   int64     `json:"revision"`
	Title      string    `json:"title"`
	SchoolID   string    `json:"school_id"`
	Visible    bool      `json:"visible"`
	CourseID   string    `json:"course_id,omitempty"`
	TeacherIDs []string  `json:"teacher_ids,omitempty"`
	At         time.Time `json:"at"`
	Activity   *[4]int64 `json:"activity,omitempty"`
}
type Event struct {
	SpecVersion string    `json:"specversion"`
	ID          string    `json:"id"`
	Source      string    `json:"source"`
	Type        string    `json:"type"`
	Subject     string    `json:"subject"`
	Time        time.Time `json:"time"`
	Data        Document  `json:"data"`
}
type Page struct {
	Items      []Item `json:"items"`
	NextCursor string `json:"next_cursor"`
}
type Vote struct {
	Target Target `json:"target"`
	Choice string `json:"choice"`
}
type Votes struct {
	Items      []Vote `json:"items"`
	NextCursor string `json:"next_cursor"`
}
type Filter struct {
	Query, Kind, After string
	Limit              int
	From, To           time.Time
	Activity           bool
}
type Repository interface {
	Apply(context.Context, Event, string) error
	Candidates(context.Context, Filter) ([]Document, error)
	// Rebuild holds the projection write lock and atomically publishes all source snapshots.
	Rebuild(context.Context, func(func([]Document) error) error) error
}

// Sources must use module public APIs and the authenticated principal. Never read source tables.
// Current returns current readable metadata; false includes deletion, withdrawal and revoked access.
type Sources interface {
	Current(context.Context, string, Target) (Document, bool, error)
	// Snapshot returns at most limit records, including tombstones, using a consistent source snapshot.
	// A nonempty cursor must advance. Empty next marks the end. Missing source is an error.
	Snapshot(context.Context, string, string, int) ([]Document, string, error)
	MyVotes(context.Context, string, string, int) (Votes, error)
}
type Config struct {
	QueryTimeout       time.Duration
	HeatWindow         time.Duration
	HeatWeights        [4]float64
	MinimumHeatSamples int
	Now                func() time.Time
}
