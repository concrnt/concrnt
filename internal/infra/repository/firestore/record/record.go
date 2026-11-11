package record

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	"golang.org/x/sync/errgroup"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// ApplyRecord reads the commit, the key and its ancestor chain in one batch
// get, which is also what the transaction locks: the serializable
// validation at commit closes the fresh-key race the SQL backends close
// with a row lock and a conditional upsert.
func (r *Repository) ApplyRecord(ctx context.Context, commit ucrecord.RecordCommit) (ucrecord.RecordOutcome, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ApplyRecord")
	defer span.End()

	entry, err := newCommit(commit.Commit, fs.ColRecordKeys, commit.Key)
	if err != nil {
		span.RecordError(err)
		return ucrecord.RecordOutcome{}, err
	}

	// chain is top-down with the key itself last
	chain := keyChainOf(commit.Key)
	if len(chain) == 0 {
		err := fmt.Errorf("not a record key: %s", commit.Key)
		span.RecordError(err)
		return ucrecord.RecordOutcome{}, err
	}
	commitRef := r.col(fs.ColCommits).Doc(commit.ID)
	chainRefs := make([]*firestore.DocumentRef, len(chain))
	for i, uri := range chain {
		chainRefs[i] = r.keyRef(uri)
	}
	keyRef := chainRefs[len(chainRefs)-1]

	var outcome ucrecord.RecordOutcome
	status, err := r.apply(ctx, func(tx *firestore.Transaction) (ucrecord.ApplyStatus, error) {
		outcome = ucrecord.RecordOutcome{}

		reads := newReadSet(commitRef)
		for _, ref := range chainRefs {
			reads.add(ref)
		}
		if err := reads.loadTx(tx); err != nil {
			return ucrecord.Applied, err
		}
		if _, exists := reads.data(commitRef); exists {
			return ucrecord.Duplicate, nil
		}

		var old recordKeyDoc
		if m, exists := reads.data(keyRef); exists {
			old = recordKeyFromMap(m)
		}
		// CIP-3 §3.4 accept-if-newer: only a strictly newer document takes
		// the key; an equal or older one is Stale.
		if !old.isPlaceholder() && !commit.CreatedAt.After(*old.RecordCreatedAt) {
			return ucrecord.Stale, nil
		}

		if err := tx.Create(commitRef, entry.toMap()); err != nil {
			return ucrecord.Applied, err
		}

		// the missing ancestors become placeholders (existing ones are only
		// read, so a busy timeline's key is never rewritten per child)
		for i, ref := range chainRefs[:len(chainRefs)-1] {
			if _, exists := reads.data(ref); exists {
				continue
			}
			placeholder := recordKeyDoc{URI: chain[i], ParentURI: parentOf(chain[i]), Ancestors: ancestorsOf(chain[i]), Gen: newGen()}
			if err := tx.Create(ref, placeholder.toMap()); err != nil {
				return ucrecord.Applied, err
			}
		}

		// the incarnation continues over an overwrite and over a
		// placeholder; only a key that did not exist starts a new one
		gen := old.Gen
		if gen == "" {
			gen = newGen()
		}
		createdAt := commit.CreatedAt
		err := tx.Set(keyRef, recordKeyDoc{
			URI:             commit.Key,
			ParentURI:       parentOf(commit.Key),
			Ancestors:       ancestorsOf(commit.Key),
			Gen:             gen,
			RecordID:        commit.ID,
			RecordCreatedAt: &createdAt,
			CleanOnUpdate:   commit.OnUpdate == nil || *commit.OnUpdate != "retain",
			Owner:           commit.Owner,
			Author:          commit.Author,
			Schema:          commit.Schema,
			Redirect:        fs.Deref(commit.Redirect),
			Policies:        fs.Deref(commit.Policies),
			Distributions:   commit.Distributions,
			Document:        entry.Document,
			Proof:           entry.Proof,
		}.toMap())
		if err != nil {
			return ucrecord.Applied, err
		}

		if !old.isPlaceholder() {
			outcome.PreviousID = old.RecordID
			if old.CleanOnUpdate {
				// the replaced version is forgotten. Its commit is there: the
				// gc removes what is anchored to a commit before the commit.
				err = tx.Update(r.col(fs.ColCommits).Doc(old.RecordID), flagGc)
			} else {
				// a retained version is served from its commit; the marker
				// is what RemoveVersion finds
				err = tx.Set(r.col(fs.ColRetainedVersions).Doc(old.RecordID), map[string]any{"uri": commit.Key})
			}
			if err != nil {
				return ucrecord.Applied, err
			}
		}
		outcome.FreshKey = old.isPlaceholder()
		return ucrecord.Applied, nil
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

	cckv, recordID := uri, ""
	if parsed.Scheme == "ccfs" {
		if parsed.Type != concrnt.CCFSTypeConcrnt {
			return nil, domain.NotFoundError{Resource: uri}
		}
		// the commit names the key it was written to; whether that key
		// still points at it shows in the chain read below
		snap, err := r.col(fs.ColCommits).Doc(parsed.CDID).Get(ctx)
		if err != nil && !fs.IsNotFound(err) {
			span.RecordError(err)
			return nil, err
		}
		if err != nil || !snap.Exists() {
			return nil, domain.NotFoundError{Resource: uri}
		}
		commit := commitFromMap(snap.Data())
		if commit.Anchor != fs.ColRecordKeys {
			return nil, domain.NotFoundError{Resource: uri}
		}
		cckv, recordID = commit.AnchorID, parsed.CDID
	}

	stacks, keys, err := r.policiesFor(ctx, []string{cckv})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if recordID != "" && keys[cckv].RecordID != recordID {
		return nil, domain.NotFoundError{Resource: uri}
	}
	return stacks[cckv], nil
}

// GetHierarchicalRecordPoliciesMulti is GetHierarchicalRecordPolicies for
// several cckv URIs in one batch read: the union of their ancestor chains is
// fetched at once and each stack assembled from it.
func (r *Repository) GetHierarchicalRecordPoliciesMulti(ctx context.Context, uris []string) (map[string][]concrnt.Policy, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetHierarchicalRecordPoliciesMulti")
	defer span.End()

	if err := repoutil.RequireCCKV(uris); err != nil {
		span.RecordError(err)
		return nil, err
	}
	stacks, _, err := r.policiesFor(ctx, uris)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return stacks, nil
}

// policiesFor builds the policy stack of every cckv URI with one batch
// read over the union of their key chains, and hands back the keys read
// that hold a record.
func (r *Repository) policiesFor(ctx context.Context, cckvs []string) (map[string][]concrnt.Policy, map[string]recordKeyDoc, error) {
	keys := map[string]recordKeyDoc{}
	hierarchyOf := func(cckv string) ([]string, error) { return keyChainOf(cckv), nil }
	stacks, err := repoutil.PolicyStacks(cckvs, hierarchyOf, func(uris []string) (map[string]repoutil.PolicyLayer, error) {
		reads := newReadSet()
		for _, uri := range uris {
			reads.add(r.keyRef(uri))
		}
		if err := reads.load(ctx, r.client); err != nil {
			return nil, err
		}
		layers := make(map[string]repoutil.PolicyLayer, len(uris))
		for _, uri := range uris {
			m, exists := reads.data(r.keyRef(uri))
			if !exists {
				continue
			}
			key := recordKeyFromMap(m)
			if key.isPlaceholder() {
				continue
			}
			keys[uri] = key
			layer := repoutil.PolicyLayer{Distributions: key.Distributions}
			if key.Policies != "" {
				layer.Policy = &key.Policies
			}
			layers[uri] = layer
		}
		return layers, nil
	})
	return stacks, keys, err
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

// GetSignedDocuments reads the keys of the cckv URIs and the commits of the
// ccfs URIs in one batch get: a key carries its document, so a cckv URI is
// done. A commit that stored a record names the key it was written to;
// those keys are read in a second batch get to tell whether they still
// point at it. URIs that resolve to nothing are left out of the map.
func (r *Repository) GetSignedDocuments(ctx context.Context, uris []string) (map[string]*concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetSignedDocuments")
	defer span.End()

	cckvs, ids, idOf, err := repoutil.PartitionDocumentURIs(uris)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	reads := newReadSet()
	for _, uri := range cckvs {
		reads.add(r.keyRef(uri))
	}
	for _, id := range ids {
		reads.add(r.col(fs.ColCommits).Doc(id))
	}
	if err := reads.load(ctx, r.client); err != nil {
		span.RecordError(err)
		return nil, err
	}

	out := make(map[string]*concrnt.SignedDocument, len(uris))
	for _, uri := range cckvs {
		m, exists := reads.data(r.keyRef(uri))
		if !exists {
			continue
		}
		key := recordKeyFromMap(m)
		if key.isPlaceholder() {
			continue
		}
		sd, err := key.signedDocument()
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		out[uri] = &sd
	}

	commits := make(map[string]commitDoc, len(ids))
	anchors := newReadSet()
	for _, id := range ids {
		m, exists := reads.data(r.col(fs.ColCommits).Doc(id))
		if !exists {
			continue
		}
		commit := commitFromMap(m)
		commits[id] = commit
		if commit.Anchor == fs.ColRecordKeys {
			anchors.add(r.keyRef(commit.AnchorID))
		}
	}
	if err := anchors.load(ctx, r.client); err != nil {
		span.RecordError(err)
		return nil, err
	}
	for uri, id := range idOf {
		commit, ok := commits[id]
		if !ok {
			continue
		}
		proof, err := repoutil.ParseProof(commit.Proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		ccfs := uri
		sd := &concrnt.SignedDocument{CCFS: &ccfs, Document: commit.Document, Proof: proof}
		if commit.Anchor == fs.ColRecordKeys {
			if m, exists := anchors.data(r.keyRef(commit.AnchorID)); exists && str(m, "recordID") == id {
				cckv := commit.AnchorID
				sd.CCKV = &cckv
			}
		}
		out[uri] = sd
	}
	return out, nil
}

func (r *Repository) GetTimelineRemoval(ctx context.Context, keyURI string) (string, string, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetTimelineRemoval")
	defer span.End()

	key, err := r.getKey(ctx, keyURI)
	if err != nil {
		span.RecordError(err)
		return "", "", err
	}
	if key == nil {
		return "", "", domain.NotFoundError{Resource: keyURI}
	}

	// only keys under a timeline key (not the owner root) and holding a
	// record appear in chunkline bodies
	if key.ParentURI == "" || len(ancestorsOf(key.ParentURI)) == 0 || key.RecordCreatedAt == nil {
		return "", "", nil
	}

	return key.ParentURI, repoutil.ItemID(keyURI, &key.Redirect), nil
}

// withKey runs fn in parallel with the read of the key at uri, so a read
// that needs both the key and a query stays one round trip. The key is nil
// when it does not exist.
func (r *Repository) withKey(ctx context.Context, uri string, fn func(ctx context.Context) error) (*recordKeyDoc, error) {
	var key *recordKeyDoc
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		key, err = r.getKey(gctx, uri)
		return err
	})
	g.Go(func() error { return fn(gctx) })
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return key, nil
}
