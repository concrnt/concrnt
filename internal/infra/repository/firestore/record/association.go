package record

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"

	"github.com/concrnt/concrnt/internal/domain"
	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// ApplyAssociation reads the commit, the target key and the uniqueness
// marker in one batch get. The association takes the gen the target key
// carries at that moment; the marker counts as taken only when it carries
// the same gen (one left by an ended incarnation is overwritten).
func (r *Repository) ApplyAssociation(ctx context.Context, commit ucrecord.AssociationCommit) (ucrecord.ApplyStatus, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ApplyAssociation")
	defer span.End()

	entry, err := newCommit(commit.Commit, fs.ColAssociations, "")
	if err != nil {
		span.RecordError(err)
		return ucrecord.Applied, err
	}

	commitRef := r.col(fs.ColCommits).Doc(commit.ID)
	targetRef := r.keyRef(commit.Target)
	uniqueRef := r.uniqueRef(commit.Target, commit.Unique)

	status, err := r.apply(ctx, func(tx *firestore.Transaction) (ucrecord.ApplyStatus, error) {
		reads := newReadSet(commitRef, targetRef, uniqueRef)
		if err := reads.loadTx(tx); err != nil {
			return ucrecord.Applied, err
		}
		if _, exists := reads.data(commitRef); exists {
			return ucrecord.Duplicate, nil
		}
		target, exists := reads.data(targetRef)
		if !exists {
			return ucrecord.Applied, domain.NotFoundError{Resource: "record key"}
		}
		gen := str(target, "gen")
		// the same logical association under another document is Stale (C6)
		if marker, exists := reads.data(uniqueRef); exists && str(marker, "targetGen") == gen {
			return ucrecord.Stale, nil
		}

		if err := tx.Create(commitRef, entry.toMap()); err != nil {
			return ucrecord.Applied, err
		}
		err := tx.Set(r.col(fs.ColAssociations).Doc(commit.ID), associationDoc{
			TargetURI: commit.Target,
			TargetGen: gen,
			Owner:     commit.Owner,
			Author:    commit.Author,
			Schema:    commit.Schema,
			Variant:   fs.Deref(commit.Variant),
			Unique:    commit.Unique,
			CreatedAt: commit.CreatedAt,
			Document:  entry.Document,
			Proof:     entry.Proof,
		}.toMap())
		if err != nil {
			return ucrecord.Applied, err
		}
		return ucrecord.Applied, tx.Set(uniqueRef, uniqueMarker(commit.Target, gen, commit.ID))
	})
	if err != nil {
		span.RecordError(err)
	}
	return status, err
}

// GetAssociatedRecords is one query on the target's associations, run in
// parallel with the read of the target key, whose gen tells which of them
// are live. Normally all of them are and the page is the answer. Rows of
// an ended incarnation only exist between the removal of a key and the end
// of its Cleanup; when a page holds some, they are dropped and the listing
// reads on until the page is full again.
func (r *Repository) GetAssociatedRecords(
	ctx context.Context,
	targetURI, schema, variant, author string,
	since, until *time.Time,
	limit int,
	order string,
) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAssociatedRecords")
	defer span.End()

	q := r.col(fs.ColAssociations).Where("targetURI", "==", targetURI)
	if schema != "" {
		q = q.Where("schema", "==", schema)
	}
	if variant != "" {
		q = q.Where("variant", "==", variant)
	}
	if author != "" {
		q = q.Where("author", "==", author)
	}
	// the association's document id is its Firestore document id
	q = applyWindow(q, "createdAt", firestore.DocumentID, since, until, limit, order)

	var snaps []*firestore.DocumentSnapshot
	target, err := r.withKey(ctx, targetURI, func(ctx context.Context) error {
		var err error
		snaps, err = q.Documents(ctx).GetAll()
		return fs.QueryErr(err)
	})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if target == nil {
		return []ucrecord.QueryRow{}, nil
	}

	rows := make([]ucrecord.QueryRow, 0, len(snaps))
	for {
		for _, snap := range snaps {
			a := associationFromMap(snap.Data())
			if a.TargetGen != target.Gen {
				continue
			}
			row, err := commitRow(a.Owner, snap.Ref.ID, a.Document, a.Proof, a.CreatedAt)
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
			rows = append(rows, row)
		}
		// an unbounded query has returned everything; a short page was the
		// last one
		if limit <= 0 || len(rows) >= limit || len(snaps) < limit {
			break
		}
		snaps, err = q.StartAfter(snaps[len(snaps)-1]).Documents(ctx).GetAll()
		if err != nil {
			span.RecordError(err)
			return nil, fs.QueryErr(err)
		}
	}
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

// ComputeAssociationSummary scans the target's associations once,
// projected to the fields it aggregates (Firestore has count() but no GROUP
// BY), in parallel with the read of the target key whose gen tells the
// live ones. The cost is one document read per association, which is why
// only the aggregate layer calls it (C2).
func (r *Repository) ComputeAssociationSummary(ctx context.Context, targetURI string, maxAuthors int) (ucrecord.AssociationSummary, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ComputeAssociationSummary")
	defer span.End()

	type scanned struct {
		gen, schema, variant, author string
		createdAt                    time.Time
	}
	var all []scanned
	target, err := r.withKey(ctx, targetURI, func(ctx context.Context) error {
		it := r.col(fs.ColAssociations).Where("targetURI", "==", targetURI).
			Select("targetGen", "schema", "variant", "author", "createdAt").Documents(ctx)
		defer it.Stop()
		for {
			snap, err := it.Next()
			if errors.Is(err, iterator.Done) {
				return nil
			}
			if err != nil {
				return fs.QueryErr(err)
			}
			m := snap.Data()
			createdAt, _ := timeAt(m, "createdAt")
			all = append(all, scanned{str(m, "targetGen"), str(m, "schema"), str(m, "variant"), str(m, "author"), createdAt.UTC()})
		}
	})
	if err != nil {
		span.RecordError(err)
		return ucrecord.AssociationSummary{}, err
	}
	if target == nil {
		return ucrecord.AssociationSummary{}, nil
	}

	type variantKey struct{ schema, variant string }
	variants := map[variantKey]*ucrecord.AssociationVariant{}
	authors := map[string]bool{}
	var summary ucrecord.AssociationSummary
	for _, a := range all {
		if a.gen != target.Gen {
			continue
		}
		k := variantKey{a.schema, a.variant}
		v, ok := variants[k]
		if !ok {
			v = &ucrecord.AssociationVariant{Schema: a.schema, Variant: a.variant, Earliest: a.createdAt}
			variants[k] = v
		}
		v.Count++
		if a.createdAt.Before(v.Earliest) {
			v.Earliest = a.createdAt
		}
		// one author past the bound is all an overflow needs to know
		if !authors[a.author] && len(authors) <= maxAuthors {
			authors[a.author] = true
		}
	}
	for _, v := range variants {
		summary.Variants = append(summary.Variants, *v)
	}
	if len(authors) > maxAuthors {
		summary.AuthorsOverflow = true
		return summary, nil
	}
	for author := range authors {
		summary.Authors = append(summary.Authors, author)
	}
	return summary, nil
}
