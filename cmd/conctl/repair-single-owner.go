package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	"github.com/concrnt/concrnt/schemas"
)

var repairSingleOwnerDryRun bool

var repairSingleOwnerCmd = &cobra.Command{
	Use:   "repair-single-owner",
	Short: "Migrate pre-existing data to the single-owner commit / acked model",
	Long: "Every commit now belongs to exactly one owner (commit_logs.owner), and an ack\n" +
		"relationship is held as two separate commits: the ack/unack owned by its author on the\n" +
		"author's server, and a derived acked/unacked (document-direct proof embedding the\n" +
		"original) owned by the associate owner on the associate owner's server (CIP-10 §5).\n" +
		"Data written before that model needs:\n" +
		"  1. commit_logs.owner derived for rows that have none — from the legacy commit_owners\n" +
		"     table where it still exists (multi-owner ack rows resolve to the author), else\n" +
		"     from the document itself,\n" +
		"  2. entities.created_at filled from the entity document (the accept-if-newer key),\n" +
		"  3. an acked/unacked commit plus ackeds row for every acks row whose target is local\n" +
		"     (the target's own holding, which used to be implied by the raw ack),\n" +
		"  4. acks rows whose author is NOT local removed, and their raw ack commit — the old\n" +
		"     receiving-side copy — disowned and flagged gc_candidate.\n" +
		"Run once right after deploying the build; re-running is a no-op. commit_owners is left\n" +
		"in place for the operator to drop. Reads and writes Postgres directly.",
	Args: cobra.NoArgs,
	RunE: withOperationContext(func(cmd *cobra.Command, args []string, op *operationContext) error {
		stats, err := repairSingleOwner(cmd.Context(), op.DB, op.GlobalConfig.FQDN, repairSingleOwnerDryRun)
		if err != nil {
			return err
		}
		verb := "repaired"
		if repairSingleOwnerDryRun {
			verb = "would repair"
		}
		fmt.Fprintf(os.Stderr, "%s: %d commit owners, %d entity timestamps, %d acked holdings, %d receiving-side raw acks\n",
			verb, stats.Owners, stats.EntityTimestamps, stats.AckedHoldings, stats.DisownedRawAcks)
		return nil
	}),
}

func init() {
	operationCmd.AddCommand(repairSingleOwnerCmd)
	repairSingleOwnerCmd.Flags().BoolVar(&repairSingleOwnerDryRun, "dry-run", false, "Only count what would change")
}

type repairSingleOwnerStats struct {
	Owners           int
	EntityTimestamps int
	AckedHoldings    int
	DisownedRawAcks  int
}

func documentKindOf(document string) string {
	var doc struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal([]byte(document), &doc); err != nil {
		return ""
	}
	return doc.Kind
}

// commitOwnerOf derives the single owner of a commit from its document
// (CIP-3 §3.1): record = key owner, association = target owner, ack/unack =
// author, acked/unacked = associate owner, entity = author, delete = owner of
// the deleted target. Empty when the document yields none.
func commitOwnerOf(document string) string {
	var doc concrnt.Document[json.RawMessage]
	if err := json.Unmarshal([]byte(document), &doc); err != nil {
		return ""
	}
	ownerOf := func(uri string) string {
		parsed, err := concrnt.ParseCCURI(uri)
		if err != nil {
			return ""
		}
		return parsed.Owner
	}
	switch doc.Kind {
	case "record":
		return ownerOf(doc.Key)
	case "association", "acked", "unacked":
		if doc.Associate == nil {
			return ""
		}
		return ownerOf(*doc.Associate)
	case "ack", "unack", "entity":
		return doc.Author
	case "delete":
		var target schemas.Delete
		if err := json.Unmarshal(doc.Value, &target); err != nil {
			return ""
		}
		base := strings.TrimSuffix(strings.TrimSuffix(string(target), "*"), "/")
		return ownerOf(base)
	}
	return ""
}

// repairSingleOwner performs the migration; see repairSingleOwnerCmd. Each
// step is idempotent, so a partial run can simply be repeated.
func repairSingleOwner(ctx context.Context, db *gorm.DB, fqdn string, dryRun bool) (repairSingleOwnerStats, error) {
	var stats repairSingleOwnerStats

	isLocal := localEntityChecker(ctx, db, fqdn)

	// Step 1: commit_logs.owner for rows that have none. Writes are collected
	// per batch and applied in one statement: a round trip and a commit per
	// row is what makes a production-sized table take hours.
	hasLegacyOwners := db.Migrator().HasTable("commit_owners")
	var unowned []models.CommitLog
	if err := db.WithContext(ctx).Select("id", "document").Where("(owner = '' OR owner IS NULL)").FindInBatches(&unowned, 500, func(tx *gorm.DB, batch int) error {
		legacyOwners := map[string][]string{}
		if hasLegacyOwners {
			ids := make([]string, 0, len(unowned))
			for _, log := range unowned {
				ids = append(ids, log.ID)
			}
			var rows []struct{ CommitLogID, Owner string }
			if err := db.WithContext(ctx).Raw("SELECT commit_log_id, owner FROM commit_owners WHERE commit_log_id IN ?", ids).Scan(&rows).Error; err != nil {
				return err
			}
			for _, row := range rows {
				legacyOwners[row.CommitLogID] = append(legacyOwners[row.CommitLogID], row.Owner)
			}
		}
		var rows []string // "(id, owner)" placeholders
		var args []any
		for _, log := range unowned {
			owner := ""
			if owners := legacyOwners[log.ID]; len(owners) == 1 {
				owner = owners[0]
			}
			// an ack/unack belongs to its author whatever the legacy table
			// says; one whose author is not local is a receiving-side copy
			// and stays ownerless (step 4 flags it for gc)
			if kind := documentKindOf(log.Document); kind == "ack" || kind == "unack" {
				owner = commitOwnerOf(log.Document)
				authorLocal, err := isLocal(owner)
				if err != nil {
					return err
				}
				if !authorLocal {
					continue
				}
			}
			if owner == "" {
				owner = commitOwnerOf(log.Document)
			}
			if owner == "" {
				continue
			}
			stats.Owners++
			rows = append(rows, "(?, ?)")
			args = append(args, log.ID, owner)
		}
		if dryRun || len(rows) == 0 {
			return nil
		}
		return db.WithContext(ctx).Exec("UPDATE commit_logs AS c SET owner = v.owner FROM (VALUES "+strings.Join(rows, ", ")+") AS v(id, owner) WHERE c.id = v.id", args...).Error
	}).Error; err != nil {
		return stats, fmt.Errorf("commit owner backfill: %w", err)
	}

	// Step 2: entities.created_at from the entity document.
	var entities []models.Entity
	if err := db.WithContext(ctx).Preload("Document").Where("created_at = 'epoch'").FindInBatches(&entities, 500, func(tx *gorm.DB, batch int) error {
		var rows []string // "(id, createdAt)" placeholders
		var args []any
		for _, entity := range entities {
			var doc concrnt.Document[json.RawMessage]
			if err := json.Unmarshal([]byte(entity.Document.Document), &doc); err != nil {
				fmt.Fprintf(os.Stderr, "entity %s: unparseable document, skipping: %v\n", entity.ID, err)
				continue
			}
			if doc.CreatedAt.IsZero() {
				continue
			}
			stats.EntityTimestamps++
			rows = append(rows, "(?, ?::timestamptz)")
			args = append(args, entity.ID, doc.CreatedAt.UTC().Format(time.RFC3339Nano))
		}
		if dryRun || len(rows) == 0 {
			return nil
		}
		return db.WithContext(ctx).Exec("UPDATE entities AS e SET created_at = v.created_at FROM (VALUES "+strings.Join(rows, ", ")+") AS v(id, created_at) WHERE e.id = v.id", args...).Error
	}).Error; err != nil {
		return stats, fmt.Errorf("entity timestamp backfill: %w", err)
	}

	// Step 3: the target's holding for every ack whose target is local, and
	// Step 4: removal of the receiving-side raw ack copies. Both are applied
	// in one transaction per batch, holdings first.
	var acks []models.Ack
	if err := db.WithContext(ctx).Preload("Document").FindInBatches(&acks, 500, func(tx *gorm.DB, batch int) error {
		// the holdings already in place for this batch, keyed by (from, to, schema)
		var keys []string // "(from, to, schema)" placeholders
		var keyArgs []any
		for _, ack := range acks {
			keys = append(keys, "(?, ?, ?)")
			keyArgs = append(keyArgs, ack.From, ack.To, ack.Schema)
		}
		var held []models.Acked
		if err := db.WithContext(ctx).Where(`("from", "to", schema) IN (`+strings.Join(keys, ", ")+`)`, keyArgs...).Find(&held).Error; err != nil {
			return err
		}
		existing := map[[3]string]models.Acked{}
		for _, h := range held {
			existing[[3]string{h.From, h.To, h.Schema}] = h
		}

		var holdingCommits []models.CommitLog
		var holdings []models.Acked
		var disowned []string
		for _, ack := range acks {
			fromLocal, err := isLocal(ack.From)
			if err != nil {
				return err
			}
			toLocal, err := isLocal(ack.To)
			if err != nil {
				return err
			}

			if toLocal {
				var proof concrnt.Proof
				if err := json.Unmarshal([]byte(ack.Document.Proof), &proof); err != nil {
					return fmt.Errorf("ack %s: unparseable proof: %w", ack.DocumentID, err)
				}
				original := concrnt.SignedDocument{Document: ack.Document.Document, Proof: proof}
				acked, err := original.DeriveAcked()
				if err != nil {
					return fmt.Errorf("ack %s: %w", ack.DocumentID, err)
				}
				ackedID, err := acked.CDID()
				if err != nil {
					return fmt.Errorf("ack %s: %w", ack.DocumentID, err)
				}

				// skip when a holding at least as new as this ack is in place
				if held, ok := existing[[3]string{ack.From, ack.To, ack.Schema}]; !ok || held.CreatedAt.Before(ack.CreatedAt) {
					stats.AckedHoldings++
					proofBytes, err := json.Marshal(acked.Proof)
					if err != nil {
						return err
					}
					holdingCommits = append(holdingCommits, models.CommitLog{
						ID:       ackedID,
						IP:       ack.Document.IP,
						Document: acked.Document,
						Proof:    string(proofBytes),
						Owner:    ack.To,
					})
					holdings = append(holdings, models.Acked{
						From:       ack.From,
						To:         ack.To,
						Schema:     ack.Schema,
						DocumentID: ackedID,
						Valid:      ack.Valid,
						CreatedAt:  ack.CreatedAt,
					})
				}
			}

			if !fromLocal {
				// the raw ack was only ever the target's implied holding; now
				// that the acked commit carries it, the copy is history to gc
				stats.DisownedRawAcks++
				disowned = append(disowned, ack.DocumentID)
			}
		}
		if dryRun || (len(holdings) == 0 && len(disowned) == 0) {
			return nil
		}
		return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if len(holdings) > 0 {
				if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&holdingCommits).Error; err != nil {
					return fmt.Errorf("acked holdings: %w", err)
				}
				if err := tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "from"}, {Name: "to"}, {Name: "schema"}},
					DoUpdates: clause.AssignmentColumns([]string{"valid", "document_id", "created_at"}),
					Where:     clause.Where{Exprs: []clause.Expression{gorm.Expr("ackeds.created_at < excluded.created_at")}},
				}).Create(&holdings).Error; err != nil {
					return fmt.Errorf("acked holdings: %w", err)
				}
			}
			if len(disowned) > 0 {
				if err := tx.Where("document_id IN ?", disowned).Delete(&models.Ack{}).Error; err != nil {
					return fmt.Errorf("disown raw copies: %w", err)
				}
				if err := tx.Model(&models.CommitLog{}).Where("id IN ?", disowned).
					Updates(map[string]any{"owner": "", "gc_candidate": true}).Error; err != nil {
					return fmt.Errorf("disown raw copies: %w", err)
				}
			}
			return nil
		})
	}).Error; err != nil {
		return stats, fmt.Errorf("acked backfill: %w", err)
	}

	return stats, nil
}
