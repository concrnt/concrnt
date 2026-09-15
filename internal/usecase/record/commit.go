package record

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/impl/interop"
	"github.com/concrnt/concrnt/internal/domain"
)

func (uc *Usecase) Commit(ctx context.Context, ip string, sd concrnt.SignedDocument, mode domain.CommitMode) (*concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Usecase.Record.Commit")
	defer span.End()

	if len(sd.Document) > domain.MaxDocumentSize {
		err := domain.ValidationError{Field: "document", Message: fmt.Sprintf("document exceeds the maximum size of %d bytes", domain.MaxDocumentSize)}
		span.RecordError(err)
		return nil, err
	}

	doc, err := sd.ParsedDocument()
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := rejectAliasOwners(doc); err != nil {
		span.RecordError(err)
		return nil, err
	}

	// acked/unacked are server-derived holdings of the associate owner
	// (CIP-10 §5.2) and follow their own verification and exemption rules.
	isAckedKind := doc.Kind == "acked" || doc.Kind == "unacked"

	documentID, err := sd.CDID()
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	alreadyCommitted, err := uc.repo.HasCommitLog(ctx, documentID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if alreadyCommitted {
		return &sd, nil
	}

	serviceAccountType, _ := ctx.Value(interop.ServiceAccountTypeCtxKey).(string)
	isServiceAccount := serviceAccountType == "system"

	if doc.CreatedAt.After(time.Now().Add(domain.MaxFutureSkew)) {
		err := domain.ValidationError{Field: "createdAt", Message: "createdAt is too far in the future"}
		span.RecordError(err)
		return nil, err
	}

	if !isServiceAccount {

		// Entity documents must be master-key signed (CIP-0 §8.2); acked and
		// unacked documents are server derivations of an embedded ack and are
		// only legitimate under a document-direct proof (CIP-10 §5.2) — an
		// author-signed "acked" would let anyone write the target-side state.
		var allowedProofs []string
		if doc.Kind == "entity" {
			allowedProofs = []string{concrnt.ProofTypeEcrecover}
		}
		if isAckedKind {
			allowedProofs = []string{concrnt.ProofTypeDocumentDirect}
		}
		if err := sd.VerifyWithProofTypes(ctx, uc.resolver(), allowedProofs); err != nil {
			span.RecordError(err)
			if errors.Is(err, concrnt.ErrNoneProofNotAllowed) {
				slog.Error("Unauthorized commit with none proof", "error", err.Error())
				return nil, errors.Join(domain.ValidationError{Field: "proof.type", Message: "none proof type is only allowed for system service accounts"}, err)
			}
			if doc.Kind == "entity" && errors.Is(err, concrnt.ErrProofTypeNotAllowed) {
				return nil, errors.Join(domain.ValidationError{Field: "proof.type", Message: "entity documents must be signed with " + concrnt.ProofTypeEcrecover}, err)
			}
			if isAckedKind && errors.Is(err, concrnt.ErrProofTypeNotAllowed) {
				return nil, errors.Join(domain.ValidationError{Field: "proof.type", Message: "acked/unacked documents must carry a " + concrnt.ProofTypeDocumentDirect + " proof"}, err)
			}
			if errors.Is(err, concrnt.ErrUnsupportedProofType) {
				return nil, errors.Join(domain.ValidationError{Field: "proof.type", Message: "unsupported proof type: " + sd.Proof.Type}, err)
			}
			return nil, errors.Join(domain.ValidationError{Field: "proof", Message: "signature verification failed"}, err)
		}

		// Entity documents are exempt from the backdate window (CIP-3 §3.4);
		// so are acked/unacked documents, whose createdAt is inherited from
		// the embedded ack that already passed the window when it was
		// accepted — delivery retries and repository replays may arrive
		// later than the window (CIP-10 §5.2).
		backdateExempt := doc.Kind == "entity" || isAckedKind
		if !backdateExempt && mode == domain.CommitModeLocalOnlyExecute {
			if authenticated, ok := ctx.Value(interop.RequesterCtxKey).(domain.Entity); ok {
				backdateExempt = doc.Author == authenticated.ID
				if !backdateExempt && doc.Key != "" {
					if parsed, err := concrnt.ParseCCURI(doc.Key); err == nil && parsed.Owner == authenticated.ID {
						backdateExempt = true
					}
				}
				if !backdateExempt && doc.Associate != nil {
					if parsed, err := concrnt.ParseCCURI(*doc.Associate); err == nil && parsed.Owner == authenticated.ID {
						backdateExempt = true
					}
				}
			}
		}

		if !backdateExempt && doc.CreatedAt.Before(time.Now().Add(-domain.MaxBackdate)) {
			err := domain.ValidationError{Field: "createdAt", Message: "createdAt is older than the allowed backdate window"}
			span.RecordError(err)
			return nil, err
		}
	}

	requesterID := doc.Author

	// An entity commit is the entity document itself; every other kind may
	// inline the author's current entity document, which is applied before
	// the author is resolved so a changed alias/domain takes effect here.
	var requester *domain.Entity
	var requesterErr error
	if doc.Kind == "entity" {
		requester, requesterErr = uc.GetEntity(ctx, concrnt.CCURI{Scheme: "cckv", Owner: requesterID}.String())
	} else {
		requester, requesterErr = uc.getEntityWithReference(ctx, ip, requesterID, sd)
	}
	if requesterErr != nil {
		span.RecordError(requesterErr)
	}

	// Only "entity" commits (self-registration) may proceed without an
	// already-resolvable requester entity. So may acked/unacked documents:
	// they are the associate owner's own holding, replayable from a dump
	// after the acker's server is gone, and the embedded signature already
	// vouches for the author (CIP-10 §5.2) — an unresolvable author is simply
	// not local.
	if requester == nil {
		if doc.Kind != "entity" && !isAckedKind {
			err := errors.Join(domain.ValidationError{Field: "document.author", Message: fmt.Sprintf("requester entity not found for %s operation", doc.Kind)}, requesterErr)
			span.RecordError(err)
			return nil, err
		}
		if isAckedKind {
			requester = &domain.Entity{ID: requesterID}
		}
	}

	targetUserID := ""
	if doc.Key != "" {
		parsed, err := concrnt.ParseCCURI(doc.Key)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		targetUserID = parsed.Owner
	}
	if doc.Associate != nil {
		parsed, err := concrnt.ParseCCURI(*doc.Associate)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		targetUserID = parsed.Owner
	}
	// acked/unacked skip the block check: the embedded ack passed it when it
	// was accepted, and a later block must not make the associate owner's
	// own holding un-replayable (CIP-10 §5.2).
	if targetUserID != "" && targetUserID != requesterID && !isAckedKind {

		blockingUsers, err := uc.getBlockingUsers(ctx, requesterID)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		if slices.Contains(blockingUsers, targetUserID) {
			err := errors.New("operation blocked due to user block relationship")
			span.RecordError(err)
			return nil, err
		}
	}

	var applyCommit func(tx RepositoryTx) (*commitApplyResult, error)

	switch doc.Kind {
	case "entity":
		applyCommit = func(tx RepositoryTx) (*commitApplyResult, error) {
			return uc.saveEntity(ctx, tx, ip, sd)
		}

	case "record":
		applyCommit = func(tx RepositoryTx) (*commitApplyResult, error) {
			return uc.createRecord(ctx, tx, ip, *requester, doc, sd, mode)
		}

	case "association":
		applyCommit = func(tx RepositoryTx) (*commitApplyResult, error) {
			return uc.createAssociation(ctx, tx, ip, *requester, doc, sd, mode)
		}

	case "ack", "acked", "unack", "unacked":
		targetUser, err := uc.getEntityWithReference(ctx, ip, targetUserID, sd)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		applyCommit = func(tx RepositoryTx) (*commitApplyResult, error) {
			return uc.processAck(ctx, tx, ip, *requester, *targetUser, doc, sd, mode)
		}

	case "delete":
		applyCommit = func(tx RepositoryTx) (*commitApplyResult, error) {
			return uc.deleteRecord(ctx, tx, ip, *requester, sd, mode)
		}
	default:
		err := errors.New("unsupported document kind: " + doc.Kind)
		span.RecordError(err)
		return nil, err
	}

	//  ** start transatction **

	tx, err := uc.repo.BeginTx(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	applyResult, err := applyCommit(tx)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	if applyResult.noop {
		slog.Info("commit no-op (accept-if-newer loss)",
			"documentID", documentID, "kind", doc.Kind, "author", doc.Author)
		return applyResult.result, nil
	}

	if err := tx.Commit(ctx); err != nil {
		span.RecordError(err)
		return nil, err
	}
	committed = true

	// A same-key overwrite or delete must be visible to this server's own
	// verification paths immediately, not after the resource cache's TTL:
	// auth and commit verification resolve documents (e.g. subkey enact,
	// CIP-13 revocation) through uc.client, which caches for 10 minutes.
	if uc.client != nil {
		if doc.Key != "" {
			uc.client.InvalidateResource(doc.Key)
		}
		// an entity document has no key; the client resolves routing
		// (ccid -> domain) through this server's own copy of it
		if doc.Kind == "entity" {
			uc.client.InvalidateResource(concrnt.CCURI{Scheme: "cckv", Owner: doc.Author}.String())
		}
		if doc.Kind == "delete" {
			if target, ok := doc.Value.(string); ok && target != "" {
				uc.client.InvalidateResource(target)
			}
		}
	}

	for _, task := range applyResult.postProcesses {
		if err := task(ctx); err != nil {
			slog.Error(
				"failed to run post-commit action",
				slog.String("error", err.Error()),
			)
			span.RecordError(err)
		}
	}

	return applyResult.result, nil
}

// rejectAliasOwners rejects alias-form owners (@<FQDN>, CIP-0 §7.2) in the
// authority-bearing fields of a commit — key, associate, the delete target,
// and each distributes entry (CIP-3 §3.1). Unparseable URIs pass through:
// they are rejected downstream where the field actually matters.
func rejectAliasOwners(doc concrnt.Document[any]) error {
	check := func(field, uri string) error {
		parsed, err := concrnt.ParseCCURI(uri)
		if err == nil && strings.HasPrefix(parsed.Owner, "@") {
			return domain.ValidationError{Field: field, Message: "alias-form owners are not allowed in commits"}
		}
		return nil
	}
	if doc.Key != "" {
		if err := check("key", doc.Key); err != nil {
			return err
		}
	}
	if doc.Associate != nil {
		if err := check("associate", *doc.Associate); err != nil {
			return err
		}
	}
	if doc.Kind == "delete" {
		if target, ok := doc.Value.(string); ok {
			base := strings.TrimSuffix(strings.TrimSuffix(target, "*"), "/")
			if err := check("value", base); err != nil {
				return err
			}
		}
	}
	for _, dest := range distributionsFromPtr(doc.Distributes) {
		if err := check("distributes", dest); err != nil {
			return err
		}
	}
	return nil
}

func distributionsFromPtr(distributions *[]string) []string {
	if distributions == nil {
		return nil
	}
	return *distributions
}

func (uc *Usecase) getBlockingUsers(ctx context.Context, userID string) ([]string, error) {
	ctx, span := tracer.Start(ctx, "Usecase.Record.GetBlockingUsers")
	defer span.End()

	if cached, found := uc.cache.Get("blockingUsers:" + userID); found {
		if ids, ok := cached.([]string); ok {
			return ids, nil
		}
	}

	blockingKey := concrnt.ComposeCCURI("cckv", userID, ".concrnt/blocking")

	blockingUsers, err := uc.repo.QueryByParent(ctx, blockingKey, "", "", nil, nil, 0, "")
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	prefix := blockingKey + "/"

	ids := make([]string, len(blockingUsers))
	for i, row := range blockingUsers {
		id := strings.TrimPrefix(*row.Row.CCKV, prefix)
		ids[i] = id
	}

	uc.cache.Set("blockingUsers:"+userID, ids, cache.DefaultExpiration)

	return ids, nil
}
