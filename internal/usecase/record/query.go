package record

import (
	"context"
	"errors"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/utils"
)

// paginateWindow derives pagination cursors from rows fetched with limit+1:
// the peeked row past the window becomes next, the window head becomes prev.
// Cursors are computed before any read-access filtering so that clients can
// page past rows that get filtered out. The cursor is the row's sort key:
// its cckv for orderBy "key", otherwise its CreatedAt as RFC3339Nano (the
// same text encoding/json gave the former *time.Time cursors).
func paginateWindow(rows []QueryRow, limit int, orderBy string) ([]concrnt.SignedDocument, *string, *string) {
	cursor := func(row QueryRow) *string {
		var value string
		if orderBy == "key" {
			if row.Row.CCKV != nil {
				value = *row.Row.CCKV
			}
		} else {
			value = row.CreatedAt.Format(time.RFC3339Nano)
		}
		return &value
	}
	var prev, next *string
	if len(rows) > limit {
		next = cursor(rows[limit])
		rows = rows[:limit]
	}
	if len(rows) > 0 {
		prev = cursor(rows[0])
	}
	items := make([]concrnt.SignedDocument, 0, len(rows))
	for _, row := range rows {
		items = append(items, row.Row)
	}
	return items, prev, next
}

func (uc *Usecase) GetAcknowledgeRecords(ctx context.Context, from, to, schema string, since, until *time.Time, limit int, order string) (concrnt.QueryResult, error) {
	rows, err := uc.repo.GetAcknowledgeRecords(ctx, from, to, schema, since, until, limit+1, order)
	if err != nil {
		return concrnt.QueryResult{}, err
	}
	items, prev, next := paginateWindow(rows, limit, "createdAt")
	return concrnt.QueryResult{Items: items, Prev: prev, Next: next}, nil
}

func (uc *Usecase) GetAcknowledgeRecordCounts(ctx context.Context, from, to, schema string) (map[string]int64, error) {
	return uc.repo.GetAcknowledgeRecordCounts(ctx, from, to, schema)
}

func (uc *Usecase) GetAssociatedRecords(ctx context.Context, targetURI, schema, variant, author string, since, until *time.Time, limit int, order string) (concrnt.QueryResult, error) {
	rows, err := uc.repo.GetAssociatedRecords(ctx, targetURI, schema, variant, author, since, until, limit+1, order)
	if err != nil {
		return concrnt.QueryResult{}, err
	}
	items, prev, next := paginateWindow(rows, limit, "createdAt")
	return concrnt.QueryResult{Items: items, Prev: prev, Next: next}, nil
}

func (uc *Usecase) GetAssociatedRecordCountsBySchema(ctx context.Context, targetURI string) (map[string]int64, error) {
	return uc.repo.GetAssociatedRecordCountsBySchema(ctx, targetURI)
}

func (uc *Usecase) GetAssociatedRecordCountsByVariant(ctx context.Context, targetURI, schema string) (*utils.OrderedKVMap[int64], error) {
	return uc.repo.GetAssociatedRecordCountsByVariant(ctx, targetURI, schema)
}

// Query lists records by prefix or parent (CIP-5). orderby=key is a
// parent-only listing in cckv order whose rows may be key-only intermediate
// keys (§3.2.1); those go through the same read-access check, evaluated
// against the ancestor policy stack.
func (uc *Usecase) Query(ctx context.Context, p QueryParams) (concrnt.QueryResult, error) {
	var (
		rows []QueryRow
		err  error
	)

	if p.Prefix != "" && p.Parent != "" {
		return concrnt.QueryResult{}, errors.New("prefix and parent cannot be specified at the same time")
	}

	orderBy := p.OrderBy
	if orderBy == "" {
		orderBy = "createdAt"
	}

	switch {
	case orderBy == "key" && p.Parent != "":
		rows, err = uc.repo.QueryByParentOrderByKey(ctx, p.Parent, p.Schema, p.Author, p.SinceKey, p.UntilKey, p.Limit+1, p.Order)
	case orderBy == "key":
		return concrnt.QueryResult{}, errors.New("orderby=key requires parent")
	case orderBy != "createdAt":
		return concrnt.QueryResult{}, errors.New("invalid orderby parameter")
	case p.Prefix != "":
		rows, err = uc.repo.QueryByPrefix(ctx, p.Prefix, p.Schema, p.Author, p.Since, p.Until, p.Limit+1, p.Order)
	case p.Parent != "":
		rows, err = uc.repo.QueryByParent(ctx, p.Parent, p.Schema, p.Author, p.Since, p.Until, p.Limit+1, p.Order)
	default:
		return concrnt.QueryResult{}, errors.New("either prefix or parent must be specified")
	}

	if err != nil {
		return concrnt.QueryResult{}, err
	}

	items, prev, next := paginateWindow(rows, p.Limit, orderBy)

	filtered := make([]concrnt.SignedDocument, 0, len(items))
	for _, sd := range items {
		if sd.CCKV == nil {
			return concrnt.QueryResult{}, errors.New("queried record has no cckv")
		}

		err := uc.checkReadAccess(ctx, *sd.CCKV, sd)
		if err != nil {
			if errors.Is(err, domain.ErrPermissionDenied) {
				continue
			}
			return concrnt.QueryResult{}, err
		}

		filtered = append(filtered, sd)
	}

	return concrnt.QueryResult{Items: filtered, Prev: prev, Next: next}, nil
}
