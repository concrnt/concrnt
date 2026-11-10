package record

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	spannerdb "github.com/concrnt/concrnt/internal/infra/repository/spanner"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// keyState is what ApplyRecord reads of a key row before deciding.
type keyState struct {
	URI             string     `spanner:"uri"`
	RecordID        *string    `spanner:"record_id"`
	RecordCreatedAt *time.Time `spanner:"record_created_at"`
	CleanOnUpdate   bool       `spanner:"clean_on_update"`
}

// distributionsOf is the value of the NOT NULL distributions column.
func distributionsOf(commit ucrecord.RecordCommit) []string {
	if commit.Distributions == nil {
		return []string{}
	}
	return commit.Distributions
}

// replaceForgetting are the statements of an ApplyRecord that replaces the
// version oldID and forgets it, in the order they must run: the commit, the
// record interleaved in it, the key row moving on to that record, and only
// then the old record and its gc flag.
func replaceForgetting(commit ucrecord.RecordCommit, parent *string, cleanOnUpdate bool, oldID string) ([]spanner.Statement, error) {
	proof, err := json.Marshal(commit.Proof)
	if err != nil {
		return nil, err
	}
	return []spanner.Statement{{
		SQL: `INSERT INTO commit_logs (id, ip, document, proof, owner, gc_candidate, c_date)
			VALUES (@id, @ip, @document, @proof, @owner, false, PENDING_COMMIT_TIMESTAMP())`,
		Params: map[string]any{"id": commit.ID, "ip": commit.IP, "document": commit.Document, "proof": string(proof), "owner": commit.Owner},
	}, {
		SQL: `INSERT INTO records (id, owner, author, redirect, schema, policies, distributions, created_at, c_date)
			VALUES (@id, @owner, @author, @redirect, @schema, @policies, @distributions, @created_at, @c_date)`,
		Params: map[string]any{
			"id": commit.ID, "owner": commit.Owner, "author": commit.Author, "redirect": commit.Redirect, "schema": commit.Schema,
			"policies": commit.Policies, "distributions": distributionsOf(commit), "created_at": commit.CreatedAt, "c_date": time.Now().UTC(),
		},
	}, {
		SQL: `UPDATE record_keys SET parent_uri = @parent_uri, record_id = @record_id, record_created_at = @record_created_at,
			redirect = @redirect, clean_on_update = @clean_on_update WHERE uri = @uri`,
		Params: map[string]any{
			"uri": commit.Key, "parent_uri": parent, "record_id": commit.ID, "record_created_at": commit.CreatedAt,
			"redirect": commit.Redirect, "clean_on_update": cleanOnUpdate,
		},
	}, {
		SQL:    "UPDATE commit_logs SET gc_candidate = true WHERE id = @id",
		Params: map[string]any{"id": oldID},
	}, {
		SQL:    "DELETE FROM records WHERE id = @id",
		Params: map[string]any{"id": oldID},
	}}, nil
}

// ApplyRecord reads the key row and those of all its ancestors in one
// statement. The read locks them (a missing row included), so of two
// commits to the same key, fresh or not, one is aborted and re-run on what
// the other stored: CIP-3 §3.4 accept-if-newer is decided on a row no one
// else can change before this transaction commits.
func (r *Repository) ApplyRecord(ctx context.Context, commit ucrecord.RecordCommit) (ucrecord.RecordOutcome, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ApplyRecord")
	defer span.End()

	// nearest ancestor first
	var ancestors []string
	for uri := commit.Key; ; {
		parent, ok, err := repoutil.ParentURI(uri)
		if err != nil {
			span.RecordError(err)
			return ucrecord.RecordOutcome{}, err
		}
		if !ok {
			break
		}
		ancestors = append(ancestors, parent)
		uri = parent
	}

	var outcome ucrecord.RecordOutcome
	status, err := r.apply(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) (ucrecord.ApplyStatus, error) {
		outcome = ucrecord.RecordOutcome{}

		read, err := readSet[struct {
			Duplicate bool        `spanner:"duplicate"`
			Keys      []*keyState `spanner:"keys"`
		}](ctx, tx, spanner.Statement{
			SQL: "SELECT " + duplicateCommit + `, ARRAY(SELECT AS STRUCT uri, record_id, record_created_at, clean_on_update
				FROM record_keys WHERE uri IN UNNEST(@uris)) AS keys`,
			Params: map[string]any{"id": commit.ID, "uris": append([]string{commit.Key}, ancestors...)},
		})
		if err != nil {
			return ucrecord.Applied, err
		}
		if read.Duplicate {
			return ucrecord.Duplicate, nil
		}
		keys := make(map[string]*keyState, len(read.Keys))
		for _, key := range read.Keys {
			keys[key.URI] = key
		}

		// only a strictly newer document takes the key; a placeholder
		// (RecordID nil) is taken by any
		old := keys[commit.Key]
		if old != nil && old.RecordID != nil && old.RecordCreatedAt != nil && !commit.CreatedAt.After(*old.RecordCreatedAt) {
			return ucrecord.Stale, nil
		}

		outcome.FreshKey = old == nil || old.RecordID == nil
		if !outcome.FreshKey {
			outcome.PreviousID = *old.RecordID
		}

		// a placeholder for every ancestor up to the nearest one that
		// exists, each the start of that key's incarnation
		var mutations []*spanner.Mutation
		for i, uri := range ancestors {
			if keys[uri] != nil {
				break
			}
			var parent *string
			if i+1 < len(ancestors) {
				parent = &ancestors[i+1]
			}
			mutations = append(mutations, spanner.Insert("record_keys",
				[]string{"uri", "parent_uri", "clean_on_update", "gen"},
				[]any{uri, parent, false, uuid.NewString()}))
		}

		var parent *string
		if len(ancestors) > 0 {
			parent = &ancestors[0]
		}
		cleanOnUpdate := commit.OnUpdate == nil || *commit.OnUpdate != "retain"

		if !outcome.FreshKey && old.CleanOnUpdate {
			// 古いRecordKeyが指していたCommitのGCフラグを立て、Recordは消す
			// Deleting a record takes the key row pointing at it along, and
			// for buffered mutations Spanner decides that on the rows as
			// they were before the transaction, where the key still points
			// at the old record. So this case is written as DML, which
			// runs in order: the key row moves on first, then the old
			// record goes. The rows the key row and the record refer to
			// must then be DML too (the foreign key and the interleaving
			// are checked per statement).
			statements, err := replaceForgetting(commit, parent, cleanOnUpdate, *old.RecordID)
			if err != nil {
				return ucrecord.Applied, err
			}
			if _, err := tx.BatchUpdate(ctx, statements); err != nil {
				return ucrecord.Applied, err
			}
			return ucrecord.Applied, tx.BufferWrite(mutations)
		}

		insert, err := insertCommit(commit.Commit, false)
		if err != nil {
			return ucrecord.Applied, err
		}
		keyColumns := []string{"uri", "parent_uri", "record_id", "record_created_at", "redirect", "clean_on_update"}
		keyValues := []any{commit.Key, parent, commit.ID, commit.CreatedAt, commit.Redirect, cleanOnUpdate}
		// an update leaves the row its gen: a record arriving on a
		// placeholder, and one replacing a retained version, continue the
		// incarnation
		key := spanner.Update("record_keys", keyColumns, keyValues)
		if old == nil {
			key = spanner.Insert("record_keys", append(keyColumns, "gen"), append(keyValues, uuid.NewString()))
		}
		mutations = append(mutations, insert,
			spanner.Insert("records",
				[]string{"id", "owner", "author", "redirect", "schema", "policies", "distributions", "created_at", "c_date"},
				[]any{commit.ID, commit.Owner, commit.Author, commit.Redirect, commit.Schema, commit.Policies, distributionsOf(commit), commit.CreatedAt, time.Now().UTC()}),
			key)
		return ucrecord.Applied, tx.BufferWrite(mutations)
	})
	if err != nil {
		span.RecordError(err)
		return ucrecord.RecordOutcome{}, err
	}
	outcome.Status = status
	return outcome, nil
}

func (r *Repository) GetHierarchicalRecordPolicies(ctx context.Context, uri string) ([]concrnt.Policy, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetHierarchicalRecordPolicies")
	defer span.End()

	parsed, err := concrnt.ParseCCURI(uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	cckv := uri
	if parsed.Scheme == "ccfs" {
		if parsed.Type != concrnt.CCFSTypeConcrnt {
			return nil, domain.NotFoundError{Resource: uri}
		}
		row, found, err := spannerdb.QueryOne[struct {
			URI string `spanner:"uri"`
		}](ctx, r.client.Single(), spanner.Statement{
			SQL:    "SELECT uri FROM record_keys WHERE record_id = @id",
			Params: map[string]any{"id": parsed.CDID},
		})
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		if !found {
			return nil, domain.NotFoundError{Resource: uri}
		}
		cckv = row.URI
	}

	stacks, err := r.policiesFor(ctx, []string{cckv})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return stacks[cckv], nil
}

// GetHierarchicalRecordPoliciesMulti is GetHierarchicalRecordPolicies for
// several cckv URIs in one statement: the union of their ancestor chains is
// fetched at once and each stack assembled from it.
func (r *Repository) GetHierarchicalRecordPoliciesMulti(ctx context.Context, uris []string) (map[string][]concrnt.Policy, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetHierarchicalRecordPoliciesMulti")
	defer span.End()

	if err := repoutil.RequireCCKV(uris); err != nil {
		span.RecordError(err)
		return nil, err
	}
	stacks, err := r.policiesFor(ctx, uris)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return stacks, nil
}

// policiesFor builds the policy stack of every cckv URI with one query
// over the union of their ancestor chains.
func (r *Repository) policiesFor(ctx context.Context, cckvs []string) (map[string][]concrnt.Policy, error) {
	return repoutil.PolicyStacks(cckvs, repoutil.HierarchyOf, func(uris []string) (map[string]repoutil.PolicyLayer, error) {
		entries, err := spannerdb.QueryAll[struct {
			URI           string   `spanner:"uri"`
			Policy        *string  `spanner:"policy"`
			Distributions []string `spanner:"distributions"`
		}](ctx, r.client.Single(), spanner.Statement{
			SQL: `SELECT rk.uri AS uri, r.policies AS policy, r.distributions AS distributions
				FROM record_keys rk JOIN records r ON r.id = rk.record_id WHERE rk.uri IN UNNEST(@uris)`,
			Params: map[string]any{"uris": uris},
		})
		if err != nil {
			return nil, err
		}
		layers := make(map[string]repoutil.PolicyLayer, len(entries))
		for _, e := range entries {
			layers[e.URI] = repoutil.PolicyLayer{Policy: e.Policy, Distributions: e.Distributions}
		}
		return layers, nil
	})
}

func (r *Repository) GetSignedDocument(ctx context.Context, uri string) (*concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetSignedDocument")
	defer span.End()

	sds, err := r.GetSignedDocuments(ctx, []string{uri})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	sd, ok := sds[uri]
	if !ok {
		return nil, domain.NotFoundError{Resource: uri}
	}
	return sd, nil
}

// GetSignedDocuments fetches several documents in one statement per URI
// scheme; URIs that resolve to nothing are left out of the map.
func (r *Repository) GetSignedDocuments(ctx context.Context, uris []string) (map[string]*concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetSignedDocuments")
	defer span.End()

	keys, ids, idOf, err := repoutil.PartitionDocumentURIs(uris)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	out := make(map[string]*concrnt.SignedDocument, len(uris))
	if len(keys) > 0 {
		rows, err := scanRecordRows(ctx, r.client.Single(), joinRecords(false)+"WHERE rk.uri IN UNNEST(@uris)", map[string]any{"uris": keys})
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		for _, row := range rows {
			sd, err := row.signedDocument()
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
			out[row.URI] = &sd
		}
	}
	if len(ids) > 0 {
		sds, err := r.signedDocumentsByID(ctx, ids)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		for uri, id := range idOf {
			if sd, ok := sds[id]; ok {
				uri := uri
				sd.CCFS = &uri
				out[uri] = &sd
			}
		}
	}
	return out, nil
}

// signedDocumentsByID reads commit logs by id, each with the key currently
// pointing at it (if any), in one statement.
func (r *Repository) signedDocumentsByID(ctx context.Context, ids []string) (map[string]concrnt.SignedDocument, error) {
	rows, err := spannerdb.QueryAll[struct {
		ID       string  `spanner:"id"`
		Document string  `spanner:"document"`
		Proof    string  `spanner:"proof"`
		URI      *string `spanner:"uri"`
	}](ctx, r.client.Single(), spanner.Statement{
		SQL: `SELECT c.id AS id, c.document AS document, c.proof AS proof, rk.uri AS uri
			FROM commit_logs c LEFT JOIN record_keys rk ON rk.record_id = c.id WHERE c.id IN UNNEST(@ids)`,
		Params: map[string]any{"ids": ids},
	})
	if err != nil {
		return nil, err
	}
	out := make(map[string]concrnt.SignedDocument, len(rows))
	for _, row := range rows {
		var proof concrnt.Proof
		if err := json.Unmarshal([]byte(row.Proof), &proof); err != nil {
			return nil, errors.Join(errors.New("failed to unmarshal proof from commitlog"), err)
		}
		out[row.ID] = concrnt.SignedDocument{CCKV: row.URI, Document: row.Document, Proof: proof}
	}
	return out, nil
}

func (r *Repository) GetTimelineRemoval(ctx context.Context, keyURI string) (string, string, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetTimelineRemoval")
	defer span.End()

	row, err := r.client.Single().ReadRow(ctx, "record_keys", spanner.Key{keyURI}, []string{"parent_uri", "record_created_at", "redirect"})
	if isNotFound(err) {
		return "", "", domain.NotFoundError{Resource: keyURI}
	}
	if err != nil {
		span.RecordError(err)
		return "", "", err
	}
	var rk struct {
		ParentURI       *string    `spanner:"parent_uri"`
		RecordCreatedAt *time.Time `spanner:"record_created_at"`
		Redirect        *string    `spanner:"redirect"`
	}
	if err := row.ToStruct(&rk); err != nil {
		span.RecordError(err)
		return "", "", err
	}

	// only rows with a parent and a record_created_at appear in chunkline bodies
	if rk.ParentURI == nil || rk.RecordCreatedAt == nil {
		return "", "", nil
	}

	// derive the item ID exactly the way LoadLocalBody derives BodyItem.Href
	// (which is what BodyItem.ID() returns): the redirect target for
	// reference records, the record key URI otherwise
	itemID := keyURI
	if rk.Redirect != nil {
		itemID = *rk.Redirect
	}

	return *rk.ParentURI, itemID, nil
}
