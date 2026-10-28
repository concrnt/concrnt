package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/database"
	"github.com/concrnt/concrnt/internal/infra/database/models"
)

// legacyProofTypeAckReference is the proof type the v1.11.0-beta builds gave
// the ackee-side mirror commit; its successor is concrnt.ProofTypeDocumentDirect.
const legacyProofTypeAckReference = "ack-reference"

var repairAnchoredAcksDryRun bool

var repairAnchoredAcksCmd = &cobra.Command{
	Use:   "repair-anchored-acks",
	Short: "Migrate a v1.11.0-beta (double-anchored acks) database to the acks / ackeds model",
	Long: "The v1.11.0-beta builds held both sides of an ack in one acks row, anchored to the\n" +
		"acker-side ack commit (ack_commit_id) and to an ackee-side mirror commit\n" +
		"(acked_commit_id, proof type ack-reference) — the row's document_id was the original\n" +
		"ack's CDID whether or not that commit existed here. The current model keeps the\n" +
		"acker's side in acks (document_id = the ack commit, a foreign key) and the associate\n" +
		"owner's side in ackeds (document_id = the acked commit, document-direct proof), and\n" +
		"the acked document is derived through the current canonical encoding, so a beta\n" +
		"mirror may not even hash to the same CDID.\n" +
		"This operation, for every acks row:\n" +
		"  - if the associate owner is local, regenerates the acked commit from the original\n" +
		"    ack (taken from the ack commit, else from the mirror's embedded copy) and writes\n" +
		"    the ackeds holding,\n" +
		"  - if the author is local, makes sure the ack commit is present (restoring it from\n" +
		"    the mirror's embedded copy when it is not),\n" +
		"  - if the author is not local, removes the row (that side is the ackee's holding now),\n" +
		"then drops the anchor columns, deletes every remaining ack-reference commit (a\n" +
		"superseded or replaced mirror, unverifiable under the current proof rules), turns\n" +
		"NULL commit owners into '' and finally brings the schema up to date.\n" +
		"Run it with the server STOPPED and before starting the new build: the new build's\n" +
		"startup migration adds the acks foreign key, which the beta rows violate. Follow up\n" +
		"with `conctl op repair-single-owner` (entity timestamps, remaining owners). Re-running\n" +
		"is a no-op. Reads and writes Postgres directly.",
	Args: cobra.NoArgs,
	RunE: withOperationContext(func(cmd *cobra.Command, args []string, op *operationContext) error {
		stats, err := repairAnchoredAcks(cmd.Context(), op.DB, op.GlobalConfig.FQDN, repairAnchoredAcksDryRun)
		if err != nil {
			return err
		}
		verb := "repaired"
		if repairAnchoredAcksDryRun {
			verb = "would repair"
		}
		fmt.Fprintf(os.Stderr, "%s: %d acked holdings, %d restored ack commits, %d receiving-side rows removed, %d orphan rows dropped, %d stale mirrors deleted, %d null owners\n",
			verb, stats.Holdings, stats.RestoredAcks, stats.RemovedRows, stats.Orphans, stats.StaleMirrors, stats.NullOwners)
		if repairAnchoredAcksDryRun {
			fmt.Fprintln(os.Stderr, "schema migration (ackeds table, entities.created_at, acks foreign key) is applied by the real run")
		}
		return nil
	}),
}

func init() {
	operationCmd.AddCommand(repairAnchoredAcksCmd)
	repairAnchoredAcksCmd.Flags().BoolVar(&repairAnchoredAcksDryRun, "dry-run", false, "Only count what would change")
}

type repairAnchoredAcksStats struct {
	Holdings     int // ackeds rows written (with their regenerated acked commit)
	RestoredAcks int // ack commits re-created from a mirror's embedded original
	RemovedRows  int // acks rows whose author is not local
	Orphans      int // acks rows with no commit to derive anything from, dropped
	StaleMirrors int // ack-reference commits deleted
	NullOwners   int // commit_logs.owner NULL rows set to ''
}

// anchoredAck is the beta-era acks row shape (models.Ack plus the two anchors).
type anchoredAck struct {
	From          string `gorm:"column:from"`
	To            string `gorm:"column:to"`
	Schema        string
	DocumentID    string `gorm:"primaryKey"`
	AckCommitID   *string
	AckedCommitID *string
	Valid         bool
	CreatedAt     time.Time
}

// repairAnchoredAcks performs the migration; see repairAnchoredAcksCmd. Each
// step is idempotent, so a partial run can simply be repeated.
func repairAnchoredAcks(ctx context.Context, db *gorm.DB, fqdn string, dryRun bool) (repairAnchoredAcksStats, error) {
	var stats repairAnchoredAcksStats
	isLocal := localEntityChecker(ctx, db, fqdn)

	loadCommit := func(id string) (*models.CommitLog, error) {
		var log models.CommitLog
		err := db.WithContext(ctx).Where("id = ?", id).Take(&log).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return &log, nil
	}
	parseSigned := func(log *models.CommitLog) (concrnt.SignedDocument, error) {
		var proof concrnt.Proof
		if err := json.Unmarshal([]byte(log.Proof), &proof); err != nil {
			return concrnt.SignedDocument{}, fmt.Errorf("commit %s: unparseable proof: %w", log.ID, err)
		}
		return concrnt.SignedDocument{Document: log.Document, Proof: proof}, nil
	}

	// Step 1: split every double-anchored row into the acks / ackeds sides.
	regenerated := map[string]bool{} // acked commit ids written (or rewritten in place)
	if db.Migrator().HasColumn("acks", "ack_commit_id") {
		hasAckeds := db.Migrator().HasTable(&models.Acked{})
		if !dryRun && !hasAckeds {
			if err := db.WithContext(ctx).AutoMigrate(&models.Acked{}); err != nil {
				return stats, fmt.Errorf("create ackeds: %w", err)
			}
			hasAckeds = true
		}

		var rows []anchoredAck
		if err := db.WithContext(ctx).Table("acks").FindInBatches(&rows, 500, func(tx *gorm.DB, batch int) error {
			for _, row := range rows {
				// the original ack: the ack commit itself, else the copy the
				// mirror embeds in its proof
				var original *concrnt.SignedDocument
				ip := ""
				ackCommit, err := loadCommit(row.DocumentID)
				if err != nil {
					return err
				}
				if ackCommit != nil {
					sd, err := parseSigned(ackCommit)
					if err != nil {
						return err
					}
					original = &sd
					ip = ackCommit.IP
				} else if row.AckedCommitID != nil {
					mirror, err := loadCommit(*row.AckedCommitID)
					if err != nil {
						return err
					}
					if mirror != nil {
						sd, err := parseSigned(mirror)
						if err != nil {
							return err
						}
						if sd.Proof.Document != nil && sd.Proof.Proof != nil {
							original = &concrnt.SignedDocument{Document: *sd.Proof.Document, Proof: *sd.Proof.Proof}
							ip = mirror.IP
						}
					}
				}
				if original == nil {
					fmt.Fprintf(os.Stderr, "ack %s (%s -> %s): neither its commit nor a mirror exists, dropping the row\n", row.DocumentID, row.From, row.To)
					stats.Orphans++
					if !dryRun {
						if err := db.WithContext(ctx).Exec("DELETE FROM acks WHERE document_id = ?", row.DocumentID).Error; err != nil {
							return err
						}
					}
					continue
				}

				fromLocal, err := isLocal(row.From)
				if err != nil {
					return err
				}
				toLocal, err := isLocal(row.To)
				if err != nil {
					return err
				}

				var acked concrnt.SignedDocument
				ackedID := ""
				writeHolding := false
				if toLocal {
					acked, err = original.DeriveAcked()
					if err != nil {
						return fmt.Errorf("ack %s: %w", row.DocumentID, err)
					}
					ackedID, err = acked.CDID()
					if err != nil {
						return fmt.Errorf("ack %s: %w", row.DocumentID, err)
					}
					writeHolding = true
					if hasAckeds {
						var existing models.Acked
						err := db.WithContext(ctx).Where(`"from" = ? AND "to" = ? AND schema = ?`, row.From, row.To, row.Schema).Take(&existing).Error
						if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
							return err
						}
						// a holding at least as new as this row is already in place
						if err == nil && !existing.CreatedAt.Before(row.CreatedAt) {
							writeHolding = false
						}
					}
					if writeHolding {
						stats.Holdings++
						regenerated[ackedID] = true
					}
				}
				restoreAck := fromLocal && ackCommit == nil
				if restoreAck {
					stats.RestoredAcks++
				}
				if !fromLocal {
					stats.RemovedRows++
				}
				if dryRun {
					continue
				}

				if err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
					if writeHolding {
						proofBytes, err := json.Marshal(acked.Proof)
						if err != nil {
							return err
						}
						// a beta mirror that happens to hash to the same CDID is
						// rewritten in place (its proof type is the legacy one)
						if err := tx.Clauses(clause.OnConflict{
							Columns:   []clause.Column{{Name: "id"}},
							DoUpdates: clause.Assignments(map[string]any{"proof": string(proofBytes), "owner": row.To}),
						}).Create(&models.CommitLog{
							ID:       ackedID,
							IP:       ip,
							Document: acked.Document,
							Proof:    string(proofBytes),
							Owner:    row.To,
						}).Error; err != nil {
							return err
						}
						if err := tx.Clauses(clause.OnConflict{
							Columns:   []clause.Column{{Name: "from"}, {Name: "to"}, {Name: "schema"}},
							DoUpdates: clause.Assignments(map[string]any{"valid": row.Valid, "document_id": ackedID, "created_at": row.CreatedAt}),
							Where:     clause.Where{Exprs: []clause.Expression{gorm.Expr("ackeds.created_at < excluded.created_at")}},
						}).Create(&models.Acked{
							From:       row.From,
							To:         row.To,
							Schema:     row.Schema,
							DocumentID: ackedID,
							Valid:      row.Valid,
							CreatedAt:  row.CreatedAt,
						}).Error; err != nil {
							return err
						}
					}
					if restoreAck {
						proofBytes, err := json.Marshal(original.Proof)
						if err != nil {
							return err
						}
						if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&models.CommitLog{
							ID:       row.DocumentID,
							IP:       ip,
							Document: original.Document,
							Proof:    string(proofBytes),
							Owner:    row.From,
						}).Error; err != nil {
							return err
						}
					}
					if !fromLocal {
						return tx.Exec("DELETE FROM acks WHERE document_id = ?", row.DocumentID).Error
					}
					return nil
				}); err != nil {
					return fmt.Errorf("ack %s: %w", row.DocumentID, err)
				}
			}
			return nil
		}).Error; err != nil {
			return stats, fmt.Errorf("ack split: %w", err)
		}

		// Step 2: the anchors are history (their foreign keys go with them).
		if !dryRun {
			if err := db.WithContext(ctx).Exec("ALTER TABLE acks DROP COLUMN IF EXISTS ack_commit_id, DROP COLUMN IF EXISTS acked_commit_id").Error; err != nil {
				return stats, fmt.Errorf("drop anchor columns: %w", err)
			}
		}
	}

	// Step 3: every ack-reference commit still around is a mirror nothing
	// holds any more (superseded, or replaced by a regenerated acked commit
	// under a different CDID). It cannot verify under the current rules, so
	// it must not survive into dumps: delete it outright. A mirror step 1
	// rewrote in place is skipped only for the dry run's count — the real
	// run has already changed its proof type.
	var stale []models.CommitLog
	if err := db.WithContext(ctx).Select("id", "proof").Where("proof LIKE ?", "%"+legacyProofTypeAckReference+"%").FindInBatches(&stale, 500, func(tx *gorm.DB, batch int) error {
		ids := []string{}
		for _, log := range stale {
			var proof concrnt.Proof
			if err := json.Unmarshal([]byte(log.Proof), &proof); err != nil || proof.Type != legacyProofTypeAckReference || regenerated[log.ID] {
				continue
			}
			ids = append(ids, log.ID)
		}
		stats.StaleMirrors += len(ids)
		if dryRun || len(ids) == 0 {
			return nil
		}
		return db.WithContext(ctx).Where("id IN ?", ids).Delete(&models.CommitLog{}).Error
	}).Error; err != nil {
		return stats, fmt.Errorf("stale mirror sweep: %w", err)
	}

	// Step 4: pass-through commits used to carry a NULL owner; '' is the
	// current spelling.
	var nullOwners int64
	if err := db.WithContext(ctx).Model(&models.CommitLog{}).Where("owner IS NULL").Count(&nullOwners).Error; err != nil {
		return stats, fmt.Errorf("null owner count: %w", err)
	}
	stats.NullOwners = int(nullOwners)
	if !dryRun && nullOwners > 0 {
		if err := db.WithContext(ctx).Exec("UPDATE commit_logs SET owner = '' WHERE owner IS NULL").Error; err != nil {
			return stats, fmt.Errorf("null owner backfill: %w", err)
		}
	}

	// Step 5: the rest of the schema (ackeds indexes, entities.created_at, the
	// acks foreign key the split just made satisfiable).
	if !dryRun {
		if err := database.MigratePostgres(db.WithContext(ctx)); err != nil {
			return stats, fmt.Errorf("schema migration: %w", err)
		}
	}

	return stats, nil
}
