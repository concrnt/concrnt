package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/cobra"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	"github.com/concrnt/concrnt/internal/infra/objectstore"
)

var (
	deleteUserDryRun      bool
	deleteUserBackup      string
	deleteUserS3PathStyle bool
	deleteUserNoBackup    bool
)

const deleteUserBatchSize = 1000

var deleteUserCmd = &cobra.Command{
	Use:   "delete-user <ccid>",
	Short: "Delete everything this server holds for a user, immediately",
	Long: "Removes a user's data from this server's database right away — the operator's\n" +
		"counterpart to the user's own DELETE /register, which only flags the data for a later\n" +
		"gc-commitlog run. Deleted: every commit log owned by the user (its records, the record\n" +
		"keys of those records, associations on the user's keys, the user's own acks, the acked\n" +
		"holdings addressed to them and their entity row all cascade with it), the user's\n" +
		"registration meta (entity_metas) and their web push subscriptions. Rows where the user\n" +
		"is only a counterparty stay: records and associations they authored on other users'\n" +
		"keys, other local users' acks to them and acked holdings from them, abuse reports\n" +
		"they filed, placeholder record keys (cckv directory nodes) and disowned commit logs\n" +
		"with no owner — the same residue gc-commitlog leaves. Redis-side notification\n" +
		"counters and caches are not touched. (util filter-commitlog --exclude-owner drops the\n" +
		"wider, author-based set from a dump.)\n" +
		"\n" +
		"With --backup s3://bucket/prefix the rows are first written under\n" +
		"<prefix>/delete-user/<ccid>/<timestamp> as .commits.jsonl (one\n" +
		"concrnt.SignedDocumentWithMeta per line: the replayable document/proof plus meta\n" +
		"{id, owner, ip, cdate}), .metas.jsonl (one domain.EntityMeta) and .subscriptions.jsonl\n" +
		"(one domain.NotificationSubscription), empty files omitted, and nothing is deleted\n" +
		"until every upload has succeeded; commits that arrive while the backup is being\n" +
		"taken are left for a re-run. The commits and metas files can be replayed as-is with\n" +
		"import-commitlog <commits> [metas]. AWS connection settings (credentials, region,\n" +
		"endpoint_url for S3-compatible stores, request_checksum_calculation=when_required for\n" +
		"stores that reject checksum trailers) come from the AWS SDK default chain —\n" +
		"~/.aws/config, ~/.aws/credentials, AWS_PROFILE, AWS_ENDPOINT_URL_S3 and friends — not\n" +
		"from the concrnt config; MinIO additionally needs --s3-path-style. The backup is\n" +
		"written to a temp file first (TMPDIR). Without --backup the command refuses to delete\n" +
		"unless --no-backup is given. The backup re-materialises exactly the data (and request\n" +
		"IPs) this command erases: treat the bucket as personal data — restrict access, set an\n" +
		"expiration lifecycle (and one aborting incomplete multipart uploads), or use\n" +
		"--no-backup when erasure must be final.\n" +
		"\n" +
		"Unlike unregister there is no waiting period: the entity row and the registration meta\n" +
		"go together, so the user's documents cannot be replayed onto this server without a\n" +
		"fresh registration. The user is not told; the command does not federate anything.\n" +
		"Reads and writes Postgres directly; safe to run against a live server. A user with\n" +
		"nothing on this server is a no-op, and so is re-running.",
	Args: cobra.ExactArgs(1),
	RunE: withOperationContext(func(cmd *cobra.Command, args []string, op *operationContext) error {
		ctx := cmd.Context()
		ccid := args[0]
		if !concrnt.IsCCID(ccid) {
			return fmt.Errorf("invalid ccid %q", ccid)
		}

		if deleteUserBackup != "" && deleteUserNoBackup {
			return fmt.Errorf("--backup and --no-backup are mutually exclusive")
		}
		var backup *deleteUserBackupTarget
		if deleteUserBackup != "" {
			bucket, prefix, err := parseBackupDestination(deleteUserBackup, "delete-user/"+ccid)
			if err != nil {
				return err
			}
			backup = &deleteUserBackupTarget{bucket: bucket, prefix: prefix, fqdn: op.GlobalConfig.FQDN}
		}
		if !deleteUserDryRun && backup == nil && !deleteUserNoBackup {
			return fmt.Errorf("no backup destination: pass --backup s3://bucket/prefix, or --no-backup to delete without a backup")
		}
		if backup != nil && !deleteUserDryRun {
			s3client, err := objectstore.NewS3(ctx, deleteUserS3PathStyle)
			if err != nil {
				return err
			}
			backup.s3 = s3client
		}

		stats, err := deleteUser(ctx, op.DB, ccid, backup, deleteUserDryRun)
		if err != nil {
			return err
		}
		verb := "deleted"
		if deleteUserDryRun {
			verb = "would delete"
		}
		fmt.Fprintf(os.Stderr, "%s: %d commit logs, %d entity metas, %d subscriptions\n",
			verb, stats.Commits, stats.Metas, stats.Subscriptions)
		return nil
	}),
}

func init() {
	operationCmd.AddCommand(deleteUserCmd)

	deleteUserCmd.Flags().BoolVar(&deleteUserDryRun, "dry-run", false, "Only report what would be deleted")
	deleteUserCmd.Flags().StringVar(&deleteUserBackup, "backup", "", "Back the deleted rows up to this S3 location (s3://bucket[/prefix]) before deleting; AWS settings come from the SDK default chain")
	deleteUserCmd.Flags().BoolVar(&deleteUserS3PathStyle, "s3-path-style", false, "Use path-style S3 addressing for --backup (required for MinIO)")
	deleteUserCmd.Flags().BoolVar(&deleteUserNoBackup, "no-backup", false, "Delete without a backup (required when --backup is not given)")
}

type deleteUserStats struct {
	Commits       int64
	Metas         int64
	Subscriptions int64
}

// deleteUserBackupTarget is where deleteUser writes its backup; nil means
// --no-backup. s3 may be nil on a dry run (the destination is only printed).
type deleteUserBackupTarget struct {
	s3     *s3.Client
	bucket string
	prefix string
	fqdn   string
}

// deleteUser performs the deletion; see deleteUserCmd. With a backup target
// the commit logs to delete are exactly the rows that were backed up (by id);
// without one they are deleted by owner in batches. The order — commits,
// subscriptions, meta last — mirrors Unregister: a failure part-way leaves
// the meta, so the user still looks registered and a re-run finishes the job.
func deleteUser(ctx context.Context, db *gorm.DB, ccid string, backup *deleteUserBackupTarget, dryRun bool) (deleteUserStats, error) {
	var stats deleteUserStats

	// What the server knows about this user, for the operator to confirm the
	// target before anything happens.
	var entity models.Entity
	switch err := db.WithContext(ctx).Where("id = ?", ccid).Take(&entity).Error; {
	case err == nil:
		alias := ""
		if entity.Alias != nil {
			alias = " alias=" + *entity.Alias
		}
		fmt.Fprintf(os.Stderr, "entity: domain=%s%s\n", entity.Domain, alias)
	case errors.Is(err, gorm.ErrRecordNotFound):
		fmt.Fprintf(os.Stderr, "entity: not found\n")
	default:
		return stats, fmt.Errorf("failed to query entity: %w", err)
	}
	var metas []models.EntityMeta
	if err := db.WithContext(ctx).Where("id = ?", ccid).Find(&metas).Error; err != nil {
		return stats, fmt.Errorf("failed to query entity meta: %w", err)
	}
	var subscriptions []models.Subscription
	if err := db.WithContext(ctx).Where("owner = ?", ccid).Find(&subscriptions).Error; err != nil {
		return stats, fmt.Errorf("failed to query subscriptions: %w", err)
	}
	var commits int64
	if err := db.WithContext(ctx).Model(&models.CommitLog{}).Where("owner = ?", ccid).Count(&commits).Error; err != nil {
		return stats, fmt.Errorf("failed to count commit logs: %w", err)
	}
	fmt.Fprintf(os.Stderr, "registered: %t, commit logs: %d, subscriptions: %d\n", len(metas) > 0, commits, len(subscriptions))

	if dryRun {
		stats = deleteUserStats{Commits: commits, Metas: int64(len(metas)), Subscriptions: int64(len(subscriptions))}
		if backup != nil {
			fmt.Fprintf(os.Stderr, "backup: s3://%s/%s\n", backup.bucket, backup.prefix)
		} else {
			fmt.Fprintf(os.Stderr, "backup: none (would refuse without --no-backup)\n")
		}
		return stats, nil
	}
	if commits == 0 && len(metas) == 0 && len(subscriptions) == 0 {
		fmt.Fprintf(os.Stderr, "nothing to delete\n")
		return stats, nil
	}

	if backup == nil {
		for {
			// Postgres has no DELETE ... LIMIT; batching via a subquery keeps
			// each statement (and its cascade) bounded.
			res := db.WithContext(ctx).
				Exec("DELETE FROM commit_logs WHERE id IN (SELECT id FROM commit_logs WHERE owner = ? LIMIT ?)", ccid, deleteUserBatchSize)
			if res.Error != nil {
				return stats, fmt.Errorf("failed to delete commit logs: %w", res.Error)
			}
			if res.RowsAffected == 0 {
				break
			}
			stats.Commits += res.RowsAffected
			fmt.Fprintf(os.Stderr, "deleted %d commit logs so far...\n", stats.Commits)
		}
	} else {
		commitsFile, err := newBackupFile("delete-user-*.commits.jsonl")
		if err != nil {
			return stats, err
		}
		defer commitsFile.close()
		metasFile, err := newBackupFile("delete-user-*.metas.jsonl")
		if err != nil {
			return stats, err
		}
		defer metasFile.close()
		subscriptionsFile, err := newBackupFile("delete-user-*.subscriptions.jsonl")
		if err != nil {
			return stats, err
		}
		defer subscriptionsFile.close()

		// Snapshot every owned commit into the backup, remembering the ids so
		// the delete below touches exactly the rows that were backed up.
		// Paging by id keeps the primary-key index in use (see dump-commitlog).
		var ids []string
		cursor := ""
		for {
			var logs []models.CommitLog
			q := db.WithContext(ctx).
				Where("owner = ?", ccid).
				Order("id ASC").Limit(deleteUserBatchSize)
			if cursor != "" {
				q = q.Where("id > ?", cursor)
			}
			if err := q.Find(&logs).Error; err != nil {
				return stats, fmt.Errorf("failed to query commit logs: %w", err)
			}
			if len(logs) == 0 {
				break
			}
			for _, cl := range logs {
				if err := commitsFile.writeCommitLog(cl); err != nil {
					return stats, err
				}
				ids = append(ids, cl.ID)
			}
			cursor = logs[len(logs)-1].ID
			fmt.Fprintf(os.Stderr, "collected %d commit logs so far...\n", len(ids))
		}
		for _, m := range metas {
			if err := metasFile.writeJSON(domain.EntityMeta{ID: m.ID, Inviter: m.Inviter, Info: m.Info}); err != nil {
				return stats, fmt.Errorf("failed to marshal entity meta %s: %w", m.ID, err)
			}
		}
		for _, s := range subscriptions {
			err := subscriptionsFile.writeJSON(domain.NotificationSubscription{
				VendorID:     s.VendorID,
				Owner:        s.Owner,
				Schemas:      []string(s.Schemas),
				Prefixes:     []string(s.Prefixes),
				Subscription: s.Subscription,
				CDate:        s.CDate,
				MDate:        s.MDate,
			})
			if err != nil {
				return stats, fmt.Errorf("failed to marshal subscription %s/%s: %w", s.VendorID, s.Owner, err)
			}
		}

		base := backup.prefix + backupTimestamp()
		for _, part := range []struct {
			file   *backupFile
			suffix string
		}{
			{commitsFile, ".commits.jsonl"},
			{metasFile, ".metas.jsonl"},
			{subscriptionsFile, ".subscriptions.jsonl"},
		} {
			if part.file.rows == 0 {
				continue
			}
			key := base + part.suffix
			err := part.file.upload(ctx, backup.s3, backup.bucket, key, map[string]string{
				"rows": strconv.Itoa(part.file.rows),
				"ccid": ccid,
				"fqdn": backup.fqdn,
			})
			if err != nil {
				return stats, err
			}
			fmt.Fprintf(os.Stderr, "backed up %d rows -> s3://%s/%s\n", part.file.rows, backup.bucket, key)
		}

		// Only now, with every backup durable, delete exactly the backed-up commits.
		for start := 0; start < len(ids); start += deleteUserBatchSize {
			batch := ids[start:min(start+deleteUserBatchSize, len(ids))]
			res := db.WithContext(ctx).Exec("DELETE FROM commit_logs WHERE id IN ?", batch)
			if res.Error != nil {
				return stats, fmt.Errorf("failed to delete commit logs: %w", res.Error)
			}
			if res.RowsAffected != int64(len(batch)) {
				fmt.Fprintf(os.Stderr, "warning: expected to delete %d commit logs but deleted %d (concurrent gc?)\n", len(batch), res.RowsAffected)
			}
			stats.Commits += res.RowsAffected
			fmt.Fprintf(os.Stderr, "deleted %d commit logs so far...\n", stats.Commits)
		}
	}

	res := db.WithContext(ctx).Where("owner = ?", ccid).Delete(&models.Subscription{})
	if res.Error != nil {
		return stats, fmt.Errorf("failed to delete subscriptions: %w", res.Error)
	}
	stats.Subscriptions = res.RowsAffected

	res = db.WithContext(ctx).Where("id = ?", ccid).Delete(&models.EntityMeta{})
	if res.Error != nil {
		return stats, fmt.Errorf("failed to delete entity meta: %w", res.Error)
	}
	stats.Metas = res.RowsAffected

	return stats, nil
}
