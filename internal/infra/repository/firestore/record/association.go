//go:build contractv1

package record

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/concrnt/concrnt/internal/utils"
)

func (r *Repository) CreateAssociation(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, targetURI string, owner string, author string, schema string, variant *string, unique string, createdAt time.Time) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateAssociation")
	defer span.End()

	t, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	targetRef := r.col(fs.ColRecordKeys).Doc(keyID(targetURI))
	assocRef := r.col(fs.ColAssociations).Doc(documentID)
	uniqueRef := r.col(fs.ColAssocUniques).Doc(uniqueID(unique))
	states, err := t.readAll([]*firestore.DocumentRef{targetRef, assocRef, uniqueRef})
	if err != nil {
		span.RecordError(err)
		return false, err
	}
	if !states[0].exists {
		err := domain.NotFoundError{Resource: "record key"}
		span.RecordError(err)
		return false, err
	}
	// duplicates on the document id (same document re-delivered) and on the
	// logical unique key (same association under a new document id) are a
	// silent success, like ON CONFLICT DO NOTHING (C6)
	if states[1].exists || states[2].exists {
		return false, nil
	}

	t.set(assocRef, associationDoc{
		TargetURI: targetURI,
		Owner:     owner,
		Author:    author,
		Schema:    schema,
		Variant:   fs.Deref(variant),
		Unique:    unique,
		CreatedAt: createdAt,
	}.toMap())
	t.set(uniqueRef, map[string]any{"associationID": documentID, "unique": unique})
	return true, nil
}

func (r *Repository) DeleteAssociation(ctx context.Context, tx ucrecord.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteAssociation")
	defer span.End()

	t, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	ref := r.col(fs.ColAssociations).Doc(documentID)
	m, exists, err := t.read(ref)
	if err != nil {
		span.RecordError(err)
		return err
	}
	if !exists {
		return nil
	}

	t.del(ref)
	if unique := str(m, "unique"); unique != "" {
		t.del(r.col(fs.ColAssocUniques).Doc(uniqueID(unique)))
	}
	return nil
}

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

	snaps, err := q.Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return nil, fs.QueryErr(err)
	}

	assocs := make([]associationDoc, 0, len(snaps))
	ids := make([]string, 0, len(snaps))
	for _, snap := range snaps {
		assocs = append(assocs, associationFromMap(snap.Data()))
		ids = append(ids, snap.Ref.ID)
	}
	commits, err := r.getCommits(ctx, ids)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	rows := make([]ucrecord.QueryRow, 0, len(assocs))
	for i, a := range assocs {
		commit, ok := commits[ids[i]]
		if !ok {
			continue
		}
		proof, err := commit.proof()
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		// CCFS is composed from the association's owner
		ccfs := concrnt.ComposeCCFSURI(a.Owner, concrnt.CCFSTypeConcrnt, ids[i])
		rows = append(rows, ucrecord.QueryRow{
			Row:       concrnt.SignedDocument{CCFS: &ccfs, Document: commit.Document, Proof: proof},
			CreatedAt: a.CreatedAt,
		})
	}
	return rows, nil
}

// Firestore has count() but no GROUP BY, so the grouped counts scan the
// matching documents (projected to the grouping fields) and aggregate in
// memory; cost is one document read per matching row, which is why only
// the aggregate layer calls them (C2).

func (r *Repository) ComputeAssociationCounts(ctx context.Context, targetURI string) (map[string]utils.OrderedKVMap[int64], error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ComputeAssociationCounts")
	defer span.End()

	// the earliest association of each variant decides the variant order
	result := make(map[string]utils.OrderedKVMap[int64])
	it := r.col(fs.ColAssociations).Where("targetURI", "==", targetURI).Select("schema", "variant", "createdAt").Documents(ctx)
	defer it.Stop()
	for {
		snap, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			span.RecordError(err)
			return nil, fs.QueryErr(err)
		}
		m := snap.Data()
		schema := str(m, "schema")
		variants, ok := result[schema]
		if !ok {
			variants = make(utils.OrderedKVMap[int64])
			result[schema] = variants
		}
		variant := str(m, "variant")
		createdAt, _ := timeAt(m, "createdAt")
		order := createdAt.UnixNano()
		c, ok := variants[variant]
		if !ok || order < c.Order {
			c.Order = order
		}
		c.Value++
		variants[variant] = c
	}
	return result, nil
}

// ListAssociationAuthors has no DISTINCT to lean on: the author field of
// the target's associations is read, bounded at a multiple of limit, and
// deduplicated here. The caller only asks for small limits on targets with
// few associations (the count is known first), so the scan stays short.
func (r *Repository) ListAssociationAuthors(ctx context.Context, targetURI string, limit int) ([]string, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ListAssociationAuthors")
	defer span.End()

	seen := map[string]bool{}
	authors := []string{}
	it := r.col(fs.ColAssociations).Where("targetURI", "==", targetURI).Select("author").Limit(limit * 4).Documents(ctx)
	defer it.Stop()
	for len(authors) < limit {
		snap, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			span.RecordError(err)
			return nil, fs.QueryErr(err)
		}
		author := str(snap.Data(), "author")
		if !seen[author] {
			seen[author] = true
			authors = append(authors, author)
		}
	}
	return authors, nil
}
