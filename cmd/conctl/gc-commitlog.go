package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/concrnt/concrnt/cdid"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	"github.com/concrnt/concrnt/internal/infra/objectstore"
)

var (
	gcCommitlogDryRun      bool
	gcCommitlogRetention   time.Duration
	gcCommitlogBackup      string
	gcCommitlogS3PathStyle bool
	gcCommitlogNoBackup    bool
)

const gcCommitlogBatchSize = 1000

var gcCommitlogCmd = &cobra.Command{
	Use:   "gc-commitlog",
	Short: "Delete GC-flagged commit logs older than the replay window",
	Long: "When a record key is overwritten, the superseded document's commit log is kept\n" +
		"with gc_candidate set: it acts as a replay tombstone, making a re-commit of the\n" +
		"captured old document a no-op. A delete flags the deleted document's commit and\n" +
		"its own the same way unless the document asked for history (onUpdate=retain), and\n" +
		"unregister (account deletion) flags every commit log owned by the departing user.\n" +
		"Once the document's\n" +
		"createdAt has fallen out of the backdate window a replay is rejected as too\n" +
		"old anyway, so the tombstone is redundant and the row can be deleted\n" +
		"(any remaining record/ack/acked/association/entity rows cascade\n" +
		"with it — for unregistered users this is what actually removes their data).\n" +
		"This deletes every gc_candidate commit log whose document createdAt is older\n" +
		"than now minus --retention. Retention below the backdate window would reopen\n" +
		"the replay hole, so shorter values are refused.\n" +
		"\n" +
		"With --backup s3://bucket/prefix the rows are first written to\n" +
		"<prefix>/gc-commitlog/<timestamp>.jsonl, one concrnt.SignedDocumentWithMeta per\n" +
		"line (the replayable document/proof plus meta {id, owner, ip, cdate}), and nothing\n" +
		"is deleted until the upload has succeeded; rows flagged while the backup was being\n" +
		"taken are left for the next run. The file can be replayed as-is with import-commitlog.\n" +
		"AWS connection settings (credentials, region, endpoint_url for S3-compatible stores,\n" +
		"request_checksum_calculation=when_required for stores that reject checksum trailers)\n" +
		"come from the AWS SDK default chain — ~/.aws/config, ~/.aws/credentials, AWS_PROFILE,\n" +
		"AWS_ENDPOINT_URL_S3 and friends — not from the concrnt config; MinIO additionally\n" +
		"needs --s3-path-style. The backup is written to a temp file first (TMPDIR).\n" +
		"Without --backup the command refuses to delete unless --no-backup is given.\n" +
		"Note that for unregistered users the backup re-materialises exactly the data (and\n" +
		"request IPs) the erase path removes: treat the bucket as personal data — restrict\n" +
		"access, set an expiration lifecycle (and one aborting incomplete multipart uploads),\n" +
		"or use --no-backup when erasure must be final.\n" +
		"Reads and writes Postgres directly; safe to run against a live server. Re-running is a no-op.",
	Args: cobra.NoArgs,
	RunE: withOperationContext(func(cmd *cobra.Command, args []string, op *operationContext) error {
		ctx := cmd.Context()

		if gcCommitlogRetention < domain.MaxBackdate {
			return fmt.Errorf("retention %s is shorter than the backdate window %s: a captured document could be replayed after its tombstone is gone", gcCommitlogRetention, domain.MaxBackdate)
		}

		if gcCommitlogBackup != "" && gcCommitlogNoBackup {
			return fmt.Errorf("--backup and --no-backup are mutually exclusive")
		}
		bucket, prefix := "", ""
		if gcCommitlogBackup != "" {
			var err error
			bucket, prefix, err = parseBackupDestination(gcCommitlogBackup, "gc-commitlog")
			if err != nil {
				return err
			}
		}
		if !gcCommitlogDryRun && bucket == "" && !gcCommitlogNoBackup {
			return fmt.Errorf("no backup destination: pass --backup s3://bucket/prefix, or --no-backup to delete without a backup")
		}

		// Commit log ids are time-prefixed CDIDs of the document's author-signed
		// createdAt, so a lexicographic bound on id is a bound on createdAt; the
		// zero data suffix sorts below every real id in the cutoff millisecond,
		// keeping the match strictly older-than. Hash-based ids ('x'-prefixed)
		// sort above any realistic time prefix and are never gc candidates.
		cutoff := cdid.New([10]byte{}, time.Now().Add(-gcCommitlogRetention)).String()

		if gcCommitlogDryRun {
			var count int64
			err := op.DB.WithContext(ctx).
				Raw("SELECT count(*) FROM commit_logs WHERE gc_candidate AND id < ?", cutoff).
				Scan(&count).Error
			if err != nil {
				return fmt.Errorf("failed to count commit logs: %w", err)
			}
			fmt.Fprintf(os.Stderr, "would delete %d commit logs\n", count)
			if bucket != "" {
				fmt.Fprintf(os.Stderr, "backup: s3://%s/%s\n", bucket, prefix)
			} else {
				fmt.Fprintf(os.Stderr, "backup: none (would refuse without --no-backup)\n")
			}
			return nil
		}

		var total int64
		if bucket == "" {
			for {
				// Postgres has no DELETE ... LIMIT; batching via a subquery keeps
				// each statement (and its cascade) bounded.
				res := op.DB.WithContext(ctx).
					Exec("DELETE FROM commit_logs WHERE id IN (SELECT id FROM commit_logs WHERE gc_candidate AND id < ? LIMIT ?)", cutoff, gcCommitlogBatchSize)
				if res.Error != nil {
					return fmt.Errorf("failed to delete commit logs: %w", res.Error)
				}
				if res.RowsAffected == 0 {
					break
				}
				total += res.RowsAffected
				fmt.Fprintf(os.Stderr, "deleted %d commit logs so far...\n", total)
			}
			fmt.Fprintf(os.Stderr, "deleted %d commit logs\n", total)
			return nil
		}

		s3client, err := objectstore.NewS3(ctx, gcCommitlogS3PathStyle)
		if err != nil {
			return err
		}

		backup, err := newBackupFile("gc-commitlog-*.jsonl")
		if err != nil {
			return err
		}
		defer backup.close()

		// Snapshot every candidate into the backup file, remembering the ids so
		// the delete below touches exactly the rows that were backed up. Paging
		// by id keeps the primary-key index in use (see dump-commitlog).
		var ids []string
		cursor := ""
		for {
			var logs []models.CommitLog
			q := op.DB.WithContext(ctx).
				Where("gc_candidate AND id < ?", cutoff).
				Order("id ASC").Limit(gcCommitlogBatchSize)
			if cursor != "" {
				q = q.Where("id > ?", cursor)
			}
			if err := q.Find(&logs).Error; err != nil {
				return fmt.Errorf("failed to query commit logs: %w", err)
			}
			if len(logs) == 0 {
				break
			}
			for _, cl := range logs {
				if err := backup.writeCommitLog(cl); err != nil {
					return err
				}
				ids = append(ids, cl.ID)
			}
			cursor = logs[len(logs)-1].ID
			fmt.Fprintf(os.Stderr, "collected %d commit logs so far...\n", len(ids))
		}
		if len(ids) == 0 {
			fmt.Fprintf(os.Stderr, "nothing to delete\n")
			return nil
		}

		key := prefix + backupTimestamp() + ".jsonl"
		err = backup.upload(ctx, s3client, bucket, key, map[string]string{
			"rows":   strconv.Itoa(len(ids)),
			"cutoff": cutoff,
			"fqdn":   op.GlobalConfig.FQDN,
		})
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "backed up %d commit logs -> s3://%s/%s\n", len(ids), bucket, key)

		// Only now, with the backup durable, delete exactly the backed-up rows.
		for start := 0; start < len(ids); start += gcCommitlogBatchSize {
			batch := ids[start:min(start+gcCommitlogBatchSize, len(ids))]
			res := op.DB.WithContext(ctx).Exec("DELETE FROM commit_logs WHERE id IN ?", batch)
			if res.Error != nil {
				return fmt.Errorf("failed to delete commit logs: %w", res.Error)
			}
			if res.RowsAffected != int64(len(batch)) {
				fmt.Fprintf(os.Stderr, "warning: expected to delete %d commit logs but deleted %d (concurrent gc?)\n", len(batch), res.RowsAffected)
			}
			total += res.RowsAffected
			fmt.Fprintf(os.Stderr, "deleted %d commit logs so far...\n", total)
		}
		fmt.Fprintf(os.Stderr, "deleted %d commit logs\n", total)
		return nil
	}),
}

func init() {
	operationCmd.AddCommand(gcCommitlogCmd)

	gcCommitlogCmd.Flags().BoolVar(&gcCommitlogDryRun, "dry-run", false, "Only count the commit logs that would be deleted")
	gcCommitlogCmd.Flags().DurationVar(&gcCommitlogRetention, "retention", domain.MaxBackdate, "How far back to keep GC-flagged commit logs; must be at least the backdate window")
	gcCommitlogCmd.Flags().StringVar(&gcCommitlogBackup, "backup", "", "Back the deleted commit logs up to this S3 location (s3://bucket[/prefix]) before deleting; AWS settings come from the SDK default chain")
	gcCommitlogCmd.Flags().BoolVar(&gcCommitlogS3PathStyle, "s3-path-style", false, "Use path-style S3 addressing for --backup (required for MinIO)")
	gcCommitlogCmd.Flags().BoolVar(&gcCommitlogNoBackup, "no-backup", false, "Delete without a backup (required when --backup is not given)")
}
