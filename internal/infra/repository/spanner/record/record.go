package record

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	spannerdb "github.com/concrnt/concrnt/internal/infra/repository/spanner"

	"cloud.google.com/go/spanner"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

type recordKeyRow struct {
	URI             string     `spanner:"uri"`
	ParentURI       *string    `spanner:"parent_uri"`
	RecordID        *string    `spanner:"record_id"`
	RecordCreatedAt *time.Time `spanner:"record_created_at"`
	Redirect        *string    `spanner:"redirect"`
	CleanOnUpdate   bool       `spanner:"clean_on_update"`
}

var recordKeyColumns = []string{"uri", "parent_uri", "record_id", "record_created_at", "redirect", "clean_on_update"}

// readRecordKey reads the key row of uri; nil when there is none.
func readRecordKey(ctx context.Context, q spannerdb.Reader, uri string) (*recordKeyRow, error) {
	row, err := q.ReadRow(ctx, "record_keys", spanner.Key{uri}, recordKeyColumns)
	if isNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rk recordKeyRow
	if err := row.ToStruct(&rk); err != nil {
		return nil, err
	}
	return &rk, nil
}

func (r *Repository) CreateRecord(
	ctx context.Context,
	tx ucrecord.RepositoryTx,
	documentID string,
	key string,
	owner string,
	author string,
	schema string,
	onUpdate *string,
	policies *string,
	distributions []string,
	redirect *string,
	createdAt time.Time,
) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateRecord")
	defer span.End()

	stx, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	// CIP-3 §3.4 accept-if-newer on the document's createdAt: only a
	// strictly newer document takes the key, an equal or older one is a
	// no-op. The read takes a shared lock on the key row; a concurrent
	// writer of the same key aborts one of the two transactions, which the
	// client re-runs from the top.
	oldRecordKey, err := readRecordKey(ctx, stx, key)
	if err != nil {
		span.RecordError(err)
		return false, err
	}
	if oldRecordKey != nil && oldRecordKey.RecordID != nil && oldRecordKey.RecordCreatedAt != nil && !createdAt.After(*oldRecordKey.RecordCreatedAt) {
		return false, nil
	}

	if distributions == nil {
		distributions = []string{}
	}
	_, err = stx.Update(ctx, spanner.Statement{
		SQL: `INSERT OR IGNORE INTO records (id, owner, author, redirect, schema, policies, distributions, created_at, c_date)
			VALUES (@id, @owner, @author, @redirect, @schema, @policies, @distributions, @created_at, CURRENT_TIMESTAMP())`,
		Params: map[string]any{
			"id": documentID, "owner": owner, "author": author, "redirect": redirect, "schema": schema,
			"policies": policies, "distributions": distributions, "created_at": createdAt,
		},
	})
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	parentURI, err := ensureParentRecordKeys(ctx, stx, key)
	if err != nil {
		span.RecordError(err)
		return false, err
	}
	cleanOnUpdate := onUpdate == nil || *onUpdate != "retain"

	// The conditional write closes the window two concurrent commits to a
	// fresh key have (same as Postgres): only a strictly newer document may
	// take the key. IS NULL keeps normal writes to parent placeholder rows
	// (RecordID nil) working.
	params := map[string]any{
		"uri": key, "parent_uri": parentURI, "record_id": documentID, "record_created_at": createdAt,
		"redirect": redirect, "clean_on_update": cleanOnUpdate,
	}
	var affected int64
	if oldRecordKey == nil {
		affected, err = stx.Update(ctx, spanner.Statement{
			SQL: `INSERT OR IGNORE INTO record_keys (uri, parent_uri, record_id, record_created_at, redirect, clean_on_update)
				VALUES (@uri, @parent_uri, @record_id, @record_created_at, @redirect, @clean_on_update)`,
			Params: params,
		})
	} else {
		affected, err = stx.Update(ctx, spanner.Statement{
			SQL: `UPDATE record_keys SET parent_uri = @parent_uri, record_id = @record_id, record_created_at = @record_created_at, redirect = @redirect, clean_on_update = @clean_on_update
				WHERE uri = @uri AND (record_id IS NULL OR record_created_at IS NULL OR record_created_at < @record_created_at)`,
			Params: params,
		})
	}
	if err != nil {
		span.RecordError(err)
		return false, err
	}
	if affected == 0 {
		return false, nil
	}

	// 古いRecordKeyが指していたCommitのGCフラグを立て、Recordは消す
	if oldRecordKey != nil && oldRecordKey.CleanOnUpdate && oldRecordKey.RecordID != nil && *oldRecordKey.RecordID != documentID {
		if _, err := stx.Update(ctx, spanner.Statement{
			SQL:    "UPDATE commit_logs SET gc_candidate = true WHERE id = @id",
			Params: map[string]any{"id": *oldRecordKey.RecordID},
		}); err != nil {
			span.RecordError(err)
			return false, err
		}
		if _, err := stx.Update(ctx, spanner.Statement{
			SQL:    "DELETE FROM records WHERE id = @id",
			Params: map[string]any{"id": *oldRecordKey.RecordID},
		}); err != nil {
			span.RecordError(err)
			return false, err
		}
	}

	return true, nil
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

// DeleteRecordByKey deletes the record the key points at; the key row and
// the associations targeting it cascade through the foreign keys, which
// counts every one of them against the commit's mutation limit.
func (r *Repository) DeleteRecordByKey(ctx context.Context, tx ucrecord.RepositoryTx, targetURI string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteRecordByKey")
	defer span.End()

	stx, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	_, err = stx.Update(ctx, spanner.Statement{
		SQL:    "DELETE FROM records WHERE id IN (SELECT record_id FROM record_keys WHERE uri = @uri AND record_id IS NOT NULL)",
		Params: map[string]any{"uri": targetURI},
	})
	return err
}

func (r *Repository) DeleteRecordByDocumentID(ctx context.Context, tx ucrecord.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteRecordByDocumentID")
	defer span.End()

	stx, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	_, err = stx.Update(ctx, spanner.Statement{
		SQL:    "DELETE FROM records WHERE id = @id",
		Params: map[string]any{"id": documentID},
	})
	return err
}

func (r *Repository) GetTimelineRemoval(ctx context.Context, keyURI string) (string, string, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetTimelineRemoval")
	defer span.End()

	rk, err := readRecordKey(ctx, r.client.Single(), keyURI)
	if err != nil {
		span.RecordError(err)
		return "", "", err
	}
	if rk == nil {
		return "", "", domain.NotFoundError{Resource: keyURI}
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

// ensureParentRecordKeys creates the placeholder keys of uri's ancestors
// that do not exist yet (root first, so a parent always precedes its
// child) and returns uri's parent key, nil for a top-level key.
func ensureParentRecordKeys(ctx context.Context, stx *spanner.ReadWriteTransaction, uri string) (*string, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ensureParentRecordKeys")
	defer span.End()

	parentURI, ok, err := repoutil.ParentURI(uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	existing, err := readRecordKey(ctx, stx, parentURI)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if existing != nil {
		return &parentURI, nil
	}

	grandParentURI, err := ensureParentRecordKeys(ctx, stx, parentURI)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if _, err := stx.Update(ctx, spanner.Statement{
		SQL:    "INSERT OR IGNORE INTO record_keys (uri, parent_uri, clean_on_update) VALUES (@uri, @parent_uri, false)",
		Params: map[string]any{"uri": parentURI, "parent_uri": grandParentURI},
	}); err != nil {
		span.RecordError(err)
		return nil, err
	}
	return &parentURI, nil
}
