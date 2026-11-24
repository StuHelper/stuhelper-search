package postgres

import (
	"context"
	s "github.com/StuHelper/stuhelper-search"
)

// ConfirmHiddenRevision proves that this subscriber applied the source's terminal
// revision. A missing projection does not establish delivery.
func (r *Repository) ConfirmHiddenRevision(ctx context.Context, source, id string, rev int64) (bool, error) {
	var ok bool
	err := r.DB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM search.documents d
 WHERE d.source=$1 AND d.id=$2 AND (d.revision>$3 OR (d.revision=$3 AND d.data->>'visible'='false'))
 AND NOT EXISTS(SELECT 1 FROM search.document_holds h WHERE h.source=d.source AND h.id=d.id AND h.until_at>statement_timestamp()))`, source, id, rev).Scan(&ok)
	return ok, safe(err)
}

// RetireConfirmedRevision accepts the source's durable, already-sealed decision.
// Once enabled for a source, first inserts must match its current revision.
func (r *Repository) RetireConfirmedRevision(ctx context.Context, source, id string, rev int64) (bool, error) {
	if r.CheckFirstRevision == nil {
		return false, s.ErrUnavailable
	}
	var done bool
	err := r.DB.QueryRowContext(ctx, "SELECT search.retire_confirmed_revision($1,$2,$3)", source, id, rev).Scan(&done)
	return done, safe(err)
}
