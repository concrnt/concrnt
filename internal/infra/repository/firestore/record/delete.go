package record

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"cloud.google.com/go/firestore"

	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// ApplyDelete reads, in one batch get, every document its arguments name:
// the delete commit, the commits to flag, and per removal the key at its
// URI plus the retained-version marker or the association of its document
// id. Removals are then decided in order against that snapshot, minus what
// an earlier removal of the same call took.
//
// A removed key loses its document and nothing else: without it no
// association of that incarnation is live any more (reads and uniqueness
// compare against the key's gen). The gens of the removed keys are handed
// back as cleanup tokens.
func (r *Repository) ApplyDelete(ctx context.Context, commit ucrecord.DeleteCommit) (ucrecord.DeleteOutcome, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ApplyDelete")
	defer span.End()

	keyRef := func(uri string) *firestore.DocumentRef { return r.col(fs.ColRecordKeys).Doc(keyID(uri)) }
	commitRef := func(id string) *firestore.DocumentRef { return r.col(fs.ColCommits).Doc(id) }
	retainedRef := func(id string) *firestore.DocumentRef { return r.col(fs.ColRetainedVersions).Doc(id) }
	associationRef := func(id string) *firestore.DocumentRef { return r.col(fs.ColAssociations).Doc(id) }

	var refs []*firestore.DocumentRef
	var entry commitDoc
	gcIDs := slices.Compact(slices.Sorted(slices.Values(commit.GcCommitIDs)))
	if commit.Commit != nil {
		var err error
		if entry, err = newCommit(*commit.Commit, "", ""); err != nil {
			span.RecordError(err)
			return ucrecord.DeleteOutcome{}, err
		}
		// the delete commit of this very call can be among the flagged
		entry.GcCandidate = slices.Contains(gcIDs, commit.Commit.ID)
		refs = append(refs, commitRef(commit.Commit.ID))
	}
	for _, id := range gcIDs {
		refs = append(refs, commitRef(id))
	}
	for _, removal := range commit.Removals {
		if removal.URI != "" {
			refs = append(refs, keyRef(removal.URI))
		}
		switch removal.Kind {
		case ucrecord.RemoveKey:
		case ucrecord.RemoveVersion:
			refs = append(refs, retainedRef(removal.DocumentID))
		case ucrecord.RemoveAssociation:
			refs = append(refs, associationRef(removal.DocumentID))
		default:
			err := fmt.Errorf("unknown removal kind %d", removal.Kind)
			span.RecordError(err)
			return ucrecord.DeleteOutcome{}, err
		}
	}

	var removed []bool
	var gens []string
	status, err := r.apply(ctx, func(tx *firestore.Transaction) (ucrecord.ApplyStatus, error) {
		removed = make([]bool, len(commit.Removals))
		gens = nil

		reads := newReadSet(refs...)
		if err := reads.loadTx(tx); err != nil {
			return ucrecord.Applied, err
		}
		if commit.Commit != nil {
			if _, exists := reads.data(commitRef(commit.Commit.ID)); exists {
				return ucrecord.Duplicate, nil
			}
		}

		// gone holds what this call has deleted so far: a later removal sees
		// the state the earlier ones left
		gone := map[string]bool{}
		live := func(ref *firestore.DocumentRef) (map[string]any, bool) {
			if gone[ref.Path] {
				return nil, false
			}
			return reads.data(ref)
		}
		remove := func(ref *firestore.DocumentRef) error {
			gone[ref.Path] = true
			return tx.Delete(ref)
		}

		for i, removal := range commit.Removals {
			var key recordKeyDoc
			keyExists := false
			if removal.URI != "" {
				if m, exists := live(keyRef(removal.URI)); exists {
					key, keyExists = recordKeyFromMap(m), true
				}
			}

			switch removal.Kind {
			case ucrecord.RemoveKey, ucrecord.RemoveVersion:
				// the version a key points at goes with the key; a
				// placeholder holds no record and stays
				if keyExists && !key.isPlaceholder() && (removal.Kind == ucrecord.RemoveKey || key.RecordID == removal.DocumentID) {
					if err := remove(keyRef(removal.URI)); err != nil {
						return ucrecord.Applied, err
					}
					gens = append(gens, key.Gen)
					removed[i] = true
					continue
				}
				if removal.Kind == ucrecord.RemoveVersion {
					if _, exists := live(retainedRef(removal.DocumentID)); exists {
						if err := remove(retainedRef(removal.DocumentID)); err != nil {
							return ucrecord.Applied, err
						}
						removed[i] = true
					}
				}

			case ucrecord.RemoveAssociation:
				m, exists := live(associationRef(removal.DocumentID))
				if !exists || !keyExists {
					continue
				}
				// an association of an ended incarnation is not there to
				// remove (Cleanup takes it, and the marker of its unique may
				// by now belong to the current incarnation)
				association := associationFromMap(m)
				if association.TargetURI != removal.URI || association.TargetGen != key.Gen {
					continue
				}
				if err := remove(associationRef(removal.DocumentID)); err != nil {
					return ucrecord.Applied, err
				}
				// within one incarnation the marker of a unique is that of
				// the one association holding it
				if err := tx.Delete(r.col(fs.ColAssocUniques).Doc(uniqueID(association.TargetURI, association.Unique))); err != nil {
					return ucrecord.Applied, err
				}
				removed[i] = true
			}
		}

		if commit.Commit != nil {
			if err := tx.Create(commitRef(commit.Commit.ID), entry.toMap()); err != nil {
				return ucrecord.Applied, err
			}
		}
		for _, id := range gcIDs {
			// unknown ids are ignored; the commit of this call was created
			// flagged
			if _, exists := reads.data(commitRef(id)); !exists {
				continue
			}
			if err := tx.Update(commitRef(id), flagGc); err != nil {
				return ucrecord.Applied, err
			}
		}
		return ucrecord.Applied, nil
	})
	if err != nil {
		span.RecordError(err)
		return ucrecord.DeleteOutcome{}, err
	}
	if status != ucrecord.Applied {
		return ucrecord.DeleteOutcome{Status: status}, nil
	}
	return ucrecord.DeleteOutcome{Status: status, Removed: removed, Cleanups: cleanupTokens(gens)}, nil
}

const (
	// cleanupTokenPrefix versions the token format: "gen1:" followed by the
	// gens of ended incarnations, comma separated.
	cleanupTokenPrefix = "gen1:"
	// cleanupTokenGens is how many incarnations one token names: Firestore's
	// cap on the values of an "in" filter, so a step is one query.
	cleanupTokenGens = 30
	// cleanupPage bounds the associations one Cleanup step removes.
	cleanupPage = 100
)

// cleanupTokens packs the gens of ended incarnations into tokens. A token
// names incarnations, not keys: a gen is never reused, so the work a token
// stands for cannot touch a later incarnation of the same key, whenever it
// runs.
func cleanupTokens(gens []string) []string {
	gens = slices.DeleteFunc(gens, func(gen string) bool { return gen == "" })
	var tokens []string
	for chunk := range slices.Chunk(gens, cleanupTokenGens) {
		tokens = append(tokens, cleanupTokenPrefix+strings.Join(chunk, ","))
	}
	return tokens
}

// Cleanup removes up to cleanupPage associations of the incarnations the
// token names, with their uniqueness markers, outside any transaction:
// none of it is visible to a read any more. The token stays the same until
// a step finds less than a page.
//
// A marker is shared by every incarnation of its target (its id does not
// depend on the gen), so the marker of a removed association may already
// belong to a later incarnation: it is deleted only while it still carries
// a gen of the token, and only if nothing rewrote it since it was read.
// Markers go first, so a step that fails halfway leaves associations for
// the next run to find, never a marker without one.
func (r *Repository) Cleanup(ctx context.Context, token string) (string, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.Cleanup")
	defer span.End()

	list, ok := strings.CutPrefix(token, cleanupTokenPrefix)
	if !ok || list == "" {
		err := fmt.Errorf("not a firestore cleanup token: %q", token)
		span.RecordError(err)
		return "", err
	}
	gens := strings.Split(list, ",")
	if len(gens) > cleanupTokenGens {
		err := fmt.Errorf("cleanup token names %d incarnations, at most %d fit one step", len(gens), cleanupTokenGens)
		span.RecordError(err)
		return "", err
	}

	snaps, err := r.col(fs.ColAssociations).
		Where("targetGen", "in", gens).
		Select("targetURI", "unique").
		Limit(cleanupPage).
		Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return "", fs.QueryErr(err)
	}
	if len(snaps) == 0 {
		return "", nil
	}

	markers := newReadSet()
	for _, snap := range snaps {
		m := snap.Data()
		markers.add(r.col(fs.ColAssocUniques).Doc(uniqueID(str(m, "targetURI"), str(m, "unique"))))
	}
	if err := markers.load(ctx, r.client); err != nil {
		span.RecordError(err)
		return "", err
	}
	b := fs.NewBulk(ctx, r.client)
	for _, ref := range markers.refs {
		if snap := markers.snap(ref); snap != nil && slices.Contains(gens, str(snap.Data(), "targetGen")) {
			b.DeleteIfUnchanged(snap)
		}
	}
	if err := b.End(); err != nil {
		span.RecordError(err)
		return "", err
	}

	b = fs.NewBulk(ctx, r.client)
	for _, snap := range snaps {
		b.Delete(snap.Ref)
	}
	if err := b.End(); err != nil {
		span.RecordError(err)
		return "", err
	}

	if len(snaps) < cleanupPage {
		return "", nil
	}
	return token, nil
}

// runCleanup runs the cleanup of gens to its end (offline tooling).
func (r *Repository) runCleanup(ctx context.Context, gens []string) error {
	for _, token := range cleanupTokens(gens) {
		for token != "" {
			next, err := r.Cleanup(ctx, token)
			if err != nil {
				return err
			}
			token = next
		}
	}
	return nil
}
