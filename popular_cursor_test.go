package search

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPopularLegacyUTCCursorExpires(t *testing.T) {
	ctx := context.Background()
	svc := &Service{
		config:      Config{QueryTimeout: time.Second},
		fallbackKey: make([]byte, 32),
		windowDays:  30,
	}
	cursor, err := svc.sealCursor(ctx, "popular:course:", "u", strings.Repeat("a", 32)+":1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Popular(ctx, "u", "course", cursor, 10); !errors.Is(err, ErrSnapshotExpired) {
		t.Fatalf("expected old ranking window to expire, got %v", err)
	}
}
