package record

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// recordKey is the subset of a record_keys row the write paths look at.
type recordKey struct {
	ID              int64
	RecordID        sql.NullString
	RecordCreatedAt sql.NullInt64
	CleanOnUpdate   bool
}

func getRecordKey(ctx context.Context, q querier, uri string) (*recordKey, error) {
	var rk recordKey
	err := q.QueryRowContext(ctx, "SELECT id, record_id, record_created_at, clean_on_update FROM record_keys WHERE uri = ?", uri).
		Scan(&rk.ID, &rk.RecordID, &rk.RecordCreatedAt, &rk.CleanOnUpdate)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.NotFoundError{Resource: "record key"}
	}
	if err != nil {
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

	db, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	// CIP-3 §3.4 accept-if-newer on the document's createdAt: only a
	// strictly newer document takes the key. The IMMEDIATE transaction holds
	// the single writer lock, so this read is already serialised against
	// every other writer (Postgres locks the row instead).
	oldRecordKey, err := getRecordKey(ctx, db, key)
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		span.RecordError(err)
		return false, err
	}
	if oldRecordKey != nil && oldRecordKey.RecordID.Valid && oldRecordKey.RecordCreatedAt.Valid && !createdAt.After(sqlitedb.Time(oldRecordKey.RecordCreatedAt.Int64)) {
		return false, nil
	}

	_, err = db.ExecContext(ctx, `INSERT INTO records (document_id, owner, author, redirect, schema, policies, distributions, created_at, c_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (document_id) DO NOTHING`,
		documentID, owner, author, sqlitedb.NullString(redirect), schema, sqlitedb.NullString(policies), sqlitedb.JSONStrings(distributions), sqlitedb.Micros(createdAt), nowMicros())
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	parentID, err := getOrCreateParentRecordKey(ctx, db, key)
	if err != nil {
		span.RecordError(err)
		return false, err
	}
	cleanOnUpdate := onUpdate == nil || *onUpdate != "retain"

	// the conditional upsert closes the window two concurrent commits to a
	// fresh key have (same as Postgres): only a strictly newer document may
	// take the key; IS NULL keeps writes to placeholder rows working
	res, err := db.ExecContext(ctx, `INSERT INTO record_keys (uri, parent_id, record_id, record_created_at, redirect, clean_on_update) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (uri) DO UPDATE SET record_id = excluded.record_id, parent_id = excluded.parent_id, record_created_at = excluded.record_created_at, redirect = excluded.redirect, clean_on_update = excluded.clean_on_update
		WHERE record_keys.record_id IS NULL OR record_keys.record_created_at IS NULL OR record_keys.record_created_at < excluded.record_created_at`,
		key, parentID, documentID, sqlitedb.Micros(createdAt), sqlitedb.NullString(redirect), cleanOnUpdate)
	if err != nil {
		span.RecordError(err)
		return false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected == 0 {
		return false, nil
	}

	// the previous generation: flag its commit, drop the record
	if oldRecordKey != nil && oldRecordKey.CleanOnUpdate && oldRecordKey.RecordID.Valid && oldRecordKey.RecordID.String != documentID {
		if _, err := db.ExecContext(ctx, "UPDATE commit_logs SET gc_candidate = 1 WHERE id = ?", oldRecordKey.RecordID.String); err != nil {
			span.RecordError(err)
			return false, err
		}
		if _, err := db.ExecContext(ctx, "DELETE FROM records WHERE document_id = ?", oldRecordKey.RecordID.String); err != nil {
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
		err := r.db.QueryRowContext(ctx, "SELECT uri FROM record_keys WHERE record_id = ?", parsed.CDID).Scan(&cckv)
		if err != nil {
			span.RecordError(err)
			return nil, errors.Join(domain.NotFoundError{Resource: uri}, err)
		}
	}

	stacks, err := r.policiesFor(ctx, []string{cckv})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return stacks[cckv], nil
}

func (r *Repository) GetHierarchicalRecordPoliciesMulti(ctx context.Context, uris []string) (map[string][]concrnt.Policy, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetHierarchicalRecordPoliciesMulti")
	defer span.End()

	for _, uri := range uris {
		parsed, err := concrnt.ParseCCURI(uri)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		if parsed.Scheme != "cckv" {
			err := fmt.Errorf("policy stacks are keyed by cckv, got %s", uri)
			span.RecordError(err)
			return nil, err
		}
	}
	stacks, err := r.policiesFor(ctx, uris)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return stacks, nil
}

// hierarchyOf lists the ancestor chain of a cckv URI, root first, the URI
// itself last.
func hierarchyOf(cckv string) ([]string, error) {
	hierarchy := []string{}
	currentURI := cckv
	for {
		hierarchy = append([]string{currentURI}, hierarchy...)
		parentURI, err := url.JoinPath(currentURI, "..")
		if err != nil {
			return nil, err
		}
		if parentURI == currentURI {
			break
		}
		currentURI = parentURI
	}
	return hierarchy, nil
}

// policiesFor builds the CIP-12 §5.3 policy stack (root first, the resource
// itself last) of every cckv URI with one query over the union of their
// ancestor chains.
func (r *Repository) policiesFor(ctx context.Context, cckvs []string) (map[string][]concrnt.Policy, error) {
	hierarchies := make(map[string][]string, len(cckvs))
	wanted := map[string]bool{}
	for _, cckv := range cckvs {
		if _, ok := hierarchies[cckv]; ok {
			continue
		}
		hierarchy, err := hierarchyOf(cckv)
		if err != nil {
			return nil, err
		}
		hierarchies[cckv] = hierarchy
		for _, uri := range hierarchy {
			wanted[uri] = true
		}
	}
	uris := make([]string, 0, len(wanted))
	for uri := range wanted {
		uris = append(uris, uri)
	}

	type tuple struct {
		Policy        *string
		Distributions []string
	}
	tupleMap := map[string]tuple{}
	if len(uris) > 0 {
		rows, err := r.db.QueryContext(ctx, "SELECT rk.uri, r.policies, r.distributions FROM records r JOIN record_keys rk ON rk.record_id = r.document_id WHERE rk.uri IN ("+sqlitedb.Placeholders(len(uris))+")", sqlitedb.Args(uris)...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var uri string
			var policy sql.NullString
			var distributionsJSON string
			if err := rows.Scan(&uri, &policy, &distributionsJSON); err != nil {
				return nil, err
			}
			distributions, err := sqlitedb.Strings(distributionsJSON)
			if err != nil {
				return nil, err
			}
			// a level with no policy of its own still matters when it
			// distributes: its virtual parents must reach the stack
			if policy.Valid || len(distributions) > 0 {
				tupleMap[uri] = tuple{Policy: sqlitedb.StringPtr(policy), Distributions: distributions}
			}
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	stacks := make(map[string][]concrnt.Policy, len(hierarchies))
	for cckv, hierarchy := range hierarchies {
		policies := []concrnt.Policy{}
		for _, uri := range hierarchy {
			t, ok := tupleMap[uri]
			if !ok {
				continue
			}
			var policyDoc concrnt.Policy
			if t.Policy != nil {
				if err := json.Unmarshal([]byte(*t.Policy), &policyDoc); err != nil {
					return nil, err
				}
			}
			policyDoc.Source = uri
			virtualParents := append([]string{}, t.Distributions...)
			policyDoc.VirtualParents = &virtualParents
			policies = append(policies, policyDoc)
		}
		stacks[cckv] = policies
	}
	return stacks, nil
}

func (r *Repository) GetSignedDocument(ctx context.Context, uri string) (*concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetSignedDocument")
	defer span.End()

	parsed, err := concrnt.ParseCCURI(uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	switch parsed.Scheme {
	case "cckv":
		rows, err := scanRecordRows(ctx, r.db, joinRecords(false)+"WHERE record_keys.uri = ?", uri)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		if len(rows) == 0 {
			return nil, domain.NotFoundError{Resource: uri}
		}
		sd, err := rows[0].signedDocument()
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		return &sd, nil

	case "ccfs":
		if parsed.Type != concrnt.CCFSTypeConcrnt {
			return nil, domain.NotFoundError{Resource: uri}
		}
		sds, err := r.signedDocumentsByID(ctx, []string{parsed.CDID})
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		sd, ok := sds[parsed.CDID]
		if !ok {
			return nil, domain.NotFoundError{Resource: uri}
		}
		sd.CCFS = &uri
		return &sd, nil
	default:
		err := fmt.Errorf("unsupported uri scheme: %s", parsed.Scheme)
		span.RecordError(err)
		return nil, err
	}
}

func (r *Repository) GetSignedDocuments(ctx context.Context, uris []string) (map[string]*concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetSignedDocuments")
	defer span.End()

	keys := []string{}
	ids := []string{}
	idOf := map[string]string{}
	for _, uri := range uris {
		parsed, err := concrnt.ParseCCURI(uri)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		switch parsed.Scheme {
		case "cckv":
			keys = append(keys, uri)
		case "ccfs":
			if parsed.Type == concrnt.CCFSTypeConcrnt {
				ids = append(ids, parsed.CDID)
				idOf[uri] = parsed.CDID
			}
		default:
			err := fmt.Errorf("unsupported uri scheme: %s", parsed.Scheme)
			span.RecordError(err)
			return nil, err
		}
	}

	out := make(map[string]*concrnt.SignedDocument, len(uris))
	if len(keys) > 0 {
		rows, err := scanRecordRows(ctx, r.db, joinRecords(false)+"WHERE record_keys.uri IN ("+sqlitedb.Placeholders(len(keys))+")", sqlitedb.Args(keys)...)
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

func (r *Repository) signedDocumentsByID(ctx context.Context, ids []string) (map[string]concrnt.SignedDocument, error) {
	rows, err := r.db.QueryContext(ctx, "SELECT c.id, c.document, c.proof, rk.uri FROM commit_logs c LEFT JOIN record_keys rk ON rk.record_id = c.id WHERE c.id IN ("+sqlitedb.Placeholders(len(ids))+")", sqlitedb.Args(ids)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]concrnt.SignedDocument{}
	for rows.Next() {
		var id, document, proofJSON string
		var uri sql.NullString
		if err := rows.Scan(&id, &document, &proofJSON, &uri); err != nil {
			return nil, err
		}
		var proof concrnt.Proof
		if err := json.Unmarshal([]byte(proofJSON), &proof); err != nil {
			return nil, errors.Join(errors.New("failed to unmarshal proof from commitlog"), err)
		}
		out[id] = concrnt.SignedDocument{CCKV: sqlitedb.StringPtr(uri), Document: document, Proof: proof}
	}
	return out, rows.Err()
}

func (r *Repository) DeleteRecordByKey(ctx context.Context, tx ucrecord.RepositoryTx, targetURI string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteRecordByKey")
	defer span.End()

	db, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}
	_, err = db.ExecContext(ctx, "DELETE FROM records WHERE document_id = (SELECT record_id FROM record_keys WHERE uri = ?)", targetURI)
	return err
}

func (r *Repository) DeleteRecordByDocumentID(ctx context.Context, tx ucrecord.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteRecordByDocumentID")
	defer span.End()

	db, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}
	_, err = db.ExecContext(ctx, "DELETE FROM records WHERE document_id = ?", documentID)
	return err
}

func (r *Repository) GetTimelineRemoval(ctx context.Context, keyURI string) (string, string, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetTimelineRemoval")
	defer span.End()

	var redirect, parentURI sql.NullString
	var recordCreatedAt sql.NullInt64
	err := r.db.QueryRowContext(ctx, "SELECT rk.redirect, rk.record_created_at, p.uri FROM record_keys rk LEFT JOIN record_keys p ON p.id = rk.parent_id WHERE rk.uri = ?", keyURI).
		Scan(&redirect, &recordCreatedAt, &parentURI)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", domain.NotFoundError{Resource: keyURI}
	}
	if err != nil {
		span.RecordError(err)
		return "", "", err
	}

	// only rows with a parent and a record_created_at appear in chunkline bodies
	if !parentURI.Valid || !recordCreatedAt.Valid {
		return "", "", nil
	}

	// the item ID exactly as LoadLocalBody derives BodyItem.Href
	itemID := keyURI
	if redirect.Valid {
		itemID = redirect.String
	}
	return parentURI.String, itemID, nil
}

// getOrCreateParentRecordKey returns the id of key's parent placeholder
// chain, creating the missing levels; nil for a top-level key.
func getOrCreateParentRecordKey(ctx context.Context, db querier, uri string) (*int64, error) {
	parentURI, err := url.JoinPath(uri, "..")
	if err != nil {
		return nil, err
	}
	parsed, err := url.Parse(parentURI)
	if err != nil {
		return nil, err
	}
	if parsed.Path == "" || parsed.Path == "/" {
		return nil, nil
	}

	parent, err := getRecordKey(ctx, db, parentURI)
	if err == nil {
		return &parent.ID, nil
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return nil, err
	}

	grandparentID, err := getOrCreateParentRecordKey(ctx, db, parentURI)
	if err != nil {
		return nil, err
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO record_keys (uri, parent_id) VALUES (?, ?) ON CONFLICT (uri) DO NOTHING", parentURI, grandparentID); err != nil {
		return nil, err
	}
	parent, err = getRecordKey(ctx, db, parentURI)
	if err != nil {
		return nil, err
	}
	return &parent.ID, nil
}
