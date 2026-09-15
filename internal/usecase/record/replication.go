package record

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/impl/interop"
	"github.com/concrnt/concrnt/internal/domain"
)

// Replicate pages the commit log by server receipt time for external
// followers (CIP-16). Cursors are derived before read-access filtering, as in
// Query; record and association commits are filtered per requester and every
// other kind passes through. The system service account bypasses the check.
func (uc *Usecase) Replicate(
	ctx context.Context,
	owner string,
	since, until *time.Time,
	limit int,
	order string,
) (concrnt.QueryResult, error) {
	ctx, span := tracer.Start(ctx, "Usecase.Record.Replicate")
	defer span.End()

	rows, err := uc.repo.QueryCommitLogs(ctx, owner, since, until, limit+1, order)
	if err != nil {
		span.RecordError(err)
		return concrnt.QueryResult{}, err
	}

	items, prev, next := paginateWindow(rows, limit)

	serviceAccountType, _ := ctx.Value(interop.ServiceAccountTypeCtxKey).(string)
	if serviceAccountType == "system" {
		return concrnt.QueryResult{Items: items, Prev: prev, Next: next}, nil
	}

	filtered := make([]concrnt.SignedDocument, 0, len(items))
	for _, sd := range items {
		var doc concrnt.Document[any]
		err := json.Unmarshal([]byte(sd.Document), &doc)
		if err != nil {
			span.RecordError(err)
			return concrnt.QueryResult{}, err
		}

		var uri string
		switch doc.Kind {
		case "record":
			uri = doc.Key
		case "association":
			uri = *doc.Associate
		default:
			filtered = append(filtered, sd)
			continue
		}

		err = uc.checkReadAccess(ctx, uri, sd)
		if err != nil {
			if errors.Is(err, domain.ErrPermissionDenied) {
				continue
			}
			span.RecordError(err)
			return concrnt.QueryResult{}, err
		}

		filtered = append(filtered, sd)
	}

	return concrnt.QueryResult{Items: filtered, Prev: prev, Next: next}, nil
}
