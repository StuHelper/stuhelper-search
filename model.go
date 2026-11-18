package search

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	ErrInput           = errors.New("invalid_input")
	ErrUnauthenticated = errors.New("unauthenticated")
	ErrNotFound        = errors.New("not_found")
	ErrUnavailable     = errors.New("unavailable")
	ErrIndexBuilding   = fmt.Errorf("%w: index_building", ErrUnavailable)
	ErrSourceDisabled  = errors.New("source_disabled")
	ErrConflict        = errors.New("event_conflict")
)

type Target struct {
	Source string `json:"source"`
	ID     string `json:"id"`
	Kind   string `json:"kind"`
}
type Item struct {
	Code        string     `json:"code,omitempty"`
	EnglishName string     `json:"english_name,omitempty"`
	FormerCodes []string   `json:"former_codes,omitempty"`
	FormerNames []string   `json:"former_names,omitempty"`
	CourseID    string     `json:"course_id,omitempty"`
	TeacherIDs  []string   `json:"teacher_ids,omitempty"`
	At          *time.Time `json:"at,omitempty"`

	Target   Target `json:"target"`
	Title    string `json:"title"`
	SchoolID string `json:"school_id"`
}

// Document is a source-owned public projection snapshot, never review body or identity data.
type Document struct {
	EnglishName string   `json:"english_name,omitempty"`
	FormerCodes []string `json:"former_codes,omitempty"`
	FormerNames []string `json:"former_names,omitempty"`

	Code       string    `json:"code,omitempty"`
	Aliases    []string  `json:"aliases,omitempty"`
	Target     Target    `json:"target"`
	Revision   int64     `json:"revision"`
	Title      string    `json:"title"`
	SchoolID   string    `json:"school_id"`
	Visible    bool      `json:"visible"`
	CourseID   string    `json:"course_id,omitempty"`
	TeacherIDs []string  `json:"teacher_ids,omitempty"`
	At         time.Time `json:"at"`
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
	WindowDays int    `json:"window_days,omitempty"`
}
type Vote struct {
	Target   Target `json:"target"`
	Choice   string `json:"choice"`
	Title    string `json:"title,omitempty"`
	CourseID string `json:"course_id,omitempty"`
	SchoolID string `json:"school_id,omitempty"`
}
type Votes struct {
	Items      []Vote `json:"items"`
	NextCursor string `json:"next_cursor"`
}
type SearchParams struct {
	Sort      string
	Query     string
	Kind      string
	SchoolID  string
	CourseID  string
	TeacherID string
	Cursor    string
	Limit     int
}
type PopularParams struct {
	Kind     string
	SchoolID string
	Cursor   string
	Limit    int
}
type Filter struct {
	Query, Kind, Sort, SchoolID, CourseID, TeacherID, After string
	Limit                                                   int
}
type Repository interface {
	Apply(context.Context, Event, string) error
	Candidates(context.Context, Filter) ([]Document, error)
	// Rebuild holds the projection write lock and atomically publishes all source snapshots.
	Rebuild(context.Context, func(func([]Document) error, func([]ActivitySnapshot) error) error) error
	ApplyActivity(context.Context, ActivityEvent, string) error
	Activities(context.Context, ActivityFilter) ([]ActivitySnapshot, error)
}

// Sources must use module public APIs and the authenticated principal. Never read source tables.
// Current returns current readable metadata; false includes deletion, withdrawal and revoked access.
type Sources interface {
	Current(context.Context, string, Target) (Document, bool, error)
	// DiscoverySnapshot returns at most limit records, including tombstones, using a consistent source snapshot.
	// A nonempty cursor must advance. Empty next marks the end. Disabled sources return ErrSourceDisabled.
	DiscoverySnapshot(context.Context, string, string, int) ([]Document, string, error)
	ActivitySnapshot(context.Context, string, time.Time, time.Time, string, int) ([]ActivitySnapshot, string, error)
	CurrentActivity(context.Context, string, string, string) (ActivitySnapshot, bool, error)
	MyVotes(context.Context, string, string, int) (Votes, error)
}
type Config struct {
	QueryTimeout       time.Duration
	RebuildTimeout     time.Duration
	HeatWindow         time.Duration
	HeatWeights        [4]float64
	MinimumHeatSamples int
	CursorKey          []byte
	Now                func() time.Time
	Observer           Observer
}

// Observation records sanitized runtime diagnostics, never raw query text or user identifiers.
type Observation struct {
	Operation     string
	Source        string
	ErrorCode     string
	Duration      time.Duration
	Scanned       int
	RevisionSkips int
}

type Observer interface {
	ObserveSearch(Observation)
}

type ObserverFunc func(Observation)

func (f ObserverFunc) ObserveSearch(o Observation) {
	if f != nil {
		f(o)
	}
}

type SourceCounts struct {
	Total   int64 `json:"total"`
	Visible int64 `json:"visible"`
}

type Status struct {
	Ready              bool                    `json:"ready"`
	RebuiltAt          *time.Time              `json:"rebuilt_at,omitempty"`
	ActivityWindowFrom string                  `json:"activity_window_from,omitempty"`
	ActivityWindowTo   string                  `json:"activity_window_to,omitempty"`
	Documents          map[string]SourceCounts `json:"documents"`
	Activities         map[string]SourceCounts `json:"activities"`
	InboxCount         int64                   `json:"inbox_count"`
}

type StatusReader interface {
	Status(context.Context) (Status, error)
}

type CleanupOptions struct {
	InboxBefore       time.Time
	ActivityBeforeDay string
	Limit             int
	Timeout           time.Duration
}

type CleanupResult struct {
	DeletedInbox      int64 `json:"deleted_inbox"`
	DeletedActivities int64 `json:"deleted_activities"`
}
