package record

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"cloud.google.com/go/spanner"

	spannerdb "github.com/concrnt/concrnt/internal/infra/repository/spanner"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// incarnation names one life of a key: the gen its key row carried from
// the moment the row was created until it was removed. A gen is never
// given to another row, so an incarnation that has ended stays ended.
type incarnation struct {
	URI string `spanner:"uri"`
	Gen string `spanner:"gen"`
}

// cleanupPrefix starts every token this backend issues; the rest is the
// incarnation whose associations are left to remove, gen first (it holds no
// colon; it is empty for a key row older than the gen column).
const cleanupPrefix = "associations:"

func (inc incarnation) token() string {
	return cleanupPrefix + inc.Gen + ":" + inc.URI
}

func parseCleanupToken(token string) (incarnation, error) {
	rest, ok := strings.CutPrefix(token, cleanupPrefix)
	gen, uri, found := strings.Cut(rest, ":")
	if !ok || !found {
		return incarnation{}, fmt.Errorf("not a cleanup token of the spanner backend: %q", token)
	}
	return incarnation{URI: uri, Gen: gen}, nil
}

// deleteRead is everything ApplyDelete decides on, read in one statement.
type deleteRead struct {
	Duplicate bool `spanner:"duplicate"`
	// Keys are the key rows RemoveKey names.
	Keys []*struct {
		URI             string  `spanner:"uri"`
		RecordID        *string `spanner:"record_id"`
		Gen             string  `spanner:"gen"`
		HasAssociations bool    `spanner:"has_associations"`
	} `spanner:"keys"`
	// Versions are the records RemoveVersion names, each with the key row
	// pointing at it, if one still does.
	Versions []*struct {
		ID              string  `spanner:"id"`
		URI             *string `spanner:"uri"`
		Gen             *string `spanner:"gen"`
		HasAssociations bool    `spanner:"has_associations"`
	} `spanner:"versions"`
	// Associations are the rows RemoveAssociation names, each with the gen
	// of the key row its target has now (nil without one).
	Associations []*struct {
		ID        string  `spanner:"id"`
		TargetURI string  `spanner:"target_uri"`
		TargetGen string  `spanner:"target_gen"`
		KeyGen    *string `spanner:"key_gen"`
	} `spanner:"associations"`
	// Gc are the GcCommitIDs the log holds.
	Gc []string `spanner:"gc"`
}

const hasAssociations = "EXISTS(SELECT 1 FROM associations a WHERE a.target_uri = rk.uri AND a.target_gen = rk.gen) AS has_associations"

const deleteReadSQL = "SELECT " + duplicateCommit + `,
	ARRAY(SELECT AS STRUCT rk.uri, rk.record_id, rk.gen, ` + hasAssociations + `
		FROM record_keys rk WHERE rk.uri IN UNNEST(@keys)) AS keys,
	ARRAY(SELECT AS STRUCT r.id, rk.uri, rk.gen, ` + hasAssociations + `
		FROM records r LEFT JOIN record_keys rk ON rk.record_id = r.id WHERE r.id IN UNNEST(@versions)) AS versions,
	ARRAY(SELECT AS STRUCT a.id, a.target_uri, a.target_gen, rk.gen AS key_gen
		FROM associations a LEFT JOIN record_keys rk ON rk.uri = a.target_uri WHERE a.id IN UNNEST(@associations)) AS associations,
	ARRAY(SELECT id FROM commit_logs WHERE id IN UNNEST(@gc)) AS gc`

// ApplyDelete reads every row the removals and gc flags name in one
// statement, plays the removals in order against that picture (a later
// removal sees what an earlier one of the same call removed), and buffers
// the deletes. Deleting a record takes the key row pointing at it along
// (the foreign key of record_keys.record_id); that ends the key's
// incarnation and nothing more is deleted for it here: the associations
// become unreadable with the key row and are handed back as a cleanup
// token. A removal costs the record, the key row, the gc flag and their
// index entries (a dozen mutations), whatever number of associations the
// key has.
func (r *Repository) ApplyDelete(ctx context.Context, commit ucrecord.DeleteCommit) (ucrecord.DeleteOutcome, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ApplyDelete")
	defer span.End()

	// an array parameter must not be nil
	keys, versions, associations := []string{}, []string{}, []string{}
	for _, removal := range commit.Removals {
		switch removal.Kind {
		case ucrecord.RemoveKey:
			keys = append(keys, removal.URI)
		case ucrecord.RemoveVersion:
			versions = append(versions, removal.DocumentID)
		case ucrecord.RemoveAssociation:
			associations = append(associations, removal.DocumentID)
		default:
			err := fmt.Errorf("unknown removal kind %d", removal.Kind)
			span.RecordError(err)
			return ucrecord.DeleteOutcome{}, err
		}
	}
	commitID := ""
	if commit.Commit != nil {
		commitID = commit.Commit.ID
	}
	params := map[string]any{
		"id": commitID, "keys": keys, "versions": versions, "associations": associations,
		"gc": append([]string{}, commit.GcCommitIDs...),
	}

	var outcome ucrecord.DeleteOutcome
	status, err := r.apply(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) (ucrecord.ApplyStatus, error) {
		outcome = ucrecord.DeleteOutcome{Removed: make([]bool, len(commit.Removals))}

		read, err := readSet[deleteRead](ctx, tx, spanner.Statement{SQL: deleteReadSQL, Params: params})
		if err != nil {
			return ucrecord.Applied, err
		}
		if read.Duplicate {
			return ucrecord.Duplicate, nil
		}

		var mutations []*spanner.Mutation
		inLog := make(map[string]bool, len(read.Gc))
		for _, id := range read.Gc {
			inLog[id] = true
		}
		if commit.Commit != nil {
			// the delete commit may ask to be flagged itself: it is not in
			// the log to update yet, so it is inserted flagged
			insert, err := insertCommit(*commit.Commit, slices.Contains(commit.GcCommitIDs, commit.Commit.ID))
			if err != nil {
				return ucrecord.Applied, err
			}
			mutations = append(mutations, insert)
		}

		// what the removals so far have taken away
		goneRecords, goneKeys := map[string]bool{}, map[string]bool{}
		// removeRecord deletes the record id and, with it, the key row
		// pointing at it, whose incarnation ends
		removeRecord := func(id string, key *incarnation, hasAssociations bool) {
			goneRecords[id] = true
			mutations = append(mutations, spanner.Delete("records", spanner.Key{id}))
			if key == nil {
				return
			}
			goneKeys[key.URI] = true
			if hasAssociations {
				outcome.Cleanups = append(outcome.Cleanups, key.token())
			}
		}
		keys := make(map[string]int, len(read.Keys))
		for i, key := range read.Keys {
			keys[key.URI] = i
		}
		versions := make(map[string]int, len(read.Versions))
		for i, version := range read.Versions {
			versions[version.ID] = i
		}
		associations := make(map[string]int, len(read.Associations))
		for i, association := range read.Associations {
			associations[association.ID] = i
		}

		for i, removal := range commit.Removals {
			switch removal.Kind {
			case ucrecord.RemoveKey:
				at, ok := keys[removal.URI]
				if !ok || read.Keys[at].RecordID == nil || goneKeys[removal.URI] {
					// no such key, a placeholder, or removed above
					continue
				}
				key := read.Keys[at]
				removeRecord(*key.RecordID, &incarnation{URI: key.URI, Gen: key.Gen}, key.HasAssociations)
			case ucrecord.RemoveVersion:
				at, ok := versions[removal.DocumentID]
				if !ok || goneRecords[removal.DocumentID] {
					continue
				}
				version := read.Versions[at]
				var key *incarnation
				if version.URI != nil {
					key = &incarnation{URI: *version.URI, Gen: *version.Gen}
				}
				removeRecord(version.ID, key, version.HasAssociations)
			case ucrecord.RemoveAssociation:
				at, ok := associations[removal.DocumentID]
				if !ok {
					continue
				}
				association := read.Associations[at]
				if association.KeyGen == nil || *association.KeyGen != association.TargetGen || goneKeys[association.TargetURI] {
					// of an incarnation that has ended: no read returns it,
					// and its row is Cleanup's to remove
					continue
				}
				// a second removal of the same association in this call
				// must find nothing
				delete(associations, removal.DocumentID)
				mutations = append(mutations, spanner.Delete("associations", spanner.Key{association.ID}))
			}
			outcome.Removed[i] = true
		}

		for _, id := range commit.GcCommitIDs {
			if inLog[id] {
				// flagged once, however often the id is listed
				delete(inLog, id)
				mutations = append(mutations, spanner.Update("commit_logs", []string{"id", "gc_candidate"}, []any{id, true}))
			}
		}
		return ucrecord.Applied, tx.BufferWrite(mutations)
	})
	if err != nil {
		span.RecordError(err)
		return ucrecord.DeleteOutcome{}, err
	}
	if status != ucrecord.Applied {
		return ucrecord.DeleteOutcome{Status: status}, nil
	}
	outcome.Status = status
	return outcome, nil
}

// cleanupPage is the number of associations one Cleanup step deletes: with
// the four index entries of each, 10,000 of the 80,000 mutations a commit
// may hold.
const cleanupPage = 2000

// Cleanup deletes one page of the associations of the ended incarnation the
// token names. The step does not move a cursor (it deletes the first rows
// it finds), so the token of the next step is the token itself, until a
// step finds less than a page.
func (r *Repository) Cleanup(ctx context.Context, token string) (string, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.Cleanup")
	defer span.End()

	inc, err := parseCleanupToken(token)
	if err != nil {
		span.RecordError(err)
		return "", err
	}
	more, err := r.cleanupStep(ctx, inc)
	if err != nil {
		span.RecordError(err)
		return "", err
	}
	if !more {
		return "", nil
	}
	return token, nil
}

// cleanupStep deletes up to cleanupPage associations of inc and reports
// whether there may be more. An incarnation whose key row is still there is
// left alone: tokens are only issued for ended ones, and the check keeps a
// token that is not one of ours from deleting what reads still return.
func (r *Repository) cleanupStep(ctx context.Context, inc incarnation) (bool, error) {
	var found int
	err := r.write(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		page, err := spannerdb.QueryAll[struct {
			ID string `spanner:"id"`
		}](ctx, tx, spanner.Statement{
			SQL: `SELECT a.id FROM associations a
				WHERE a.target_uri = @uri AND a.target_gen = @gen
					AND NOT EXISTS(SELECT 1 FROM record_keys rk WHERE rk.uri = @uri AND rk.gen = @gen)
				LIMIT @page`,
			Params: map[string]any{"uri": inc.URI, "gen": inc.Gen, "page": int64(cleanupPage)},
		})
		if err != nil {
			return err
		}
		found = len(page)
		if found == 0 {
			return nil
		}
		ids := make([]spanner.KeySet, len(page))
		for i, row := range page {
			ids[i] = spanner.Key{row.ID}
		}
		return tx.BufferWrite([]*spanner.Mutation{spanner.Delete("associations", spanner.KeySets(ids...))})
	})
	return found == cleanupPage, err
}
