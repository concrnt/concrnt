package main

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/concrnt/concrnt/cdid"
	"github.com/concrnt/concrnt/internal/domain"
)

var (
	gcCommitlogDryRun    bool
	gcCommitlogRetention time.Duration
)

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
		"Reads and writes the database directly; safe to run against a live server. Re-running is a no-op.",
	Args: cobra.NoArgs,
	RunE: withOperationContext(func(cmd *cobra.Command, args []string, op *operationContext) error {
		ctx := cmd.Context()

		if gcCommitlogRetention < domain.MaxBackdate {
			return fmt.Errorf("retention %s is shorter than the backdate window %s: a captured document could be replayed after its tombstone is gone", gcCommitlogRetention, domain.MaxBackdate)
		}

		// Commit log ids are time-prefixed CDIDs of the document's author-signed
		// createdAt, so a lexicographic bound on id is a bound on createdAt; the
		// zero data suffix sorts below every real id in the cutoff millisecond,
		// keeping the match strictly older-than. Hash-based ids ('x'-prefixed)
		// sort above any realistic time prefix and are never gc candidates.
		cutoff := cdid.New([10]byte{}, time.Now().Add(-gcCommitlogRetention)).String()

		maint := op.Repos.RecordMaintenance

		if gcCommitlogDryRun {
			count, err := maint.CountGcCandidates(ctx, cutoff)
			if err != nil {
				return fmt.Errorf("failed to count commit logs: %w", err)
			}
			fmt.Fprintf(os.Stderr, "would delete %d commit logs\n", count)
			return nil
		}

		var total int64
		for {
			// batching keeps each delete (and its cascade) bounded
			ids, err := maint.ListGcCandidateIDs(ctx, cutoff, 1000)
			if err != nil {
				return fmt.Errorf("failed to list commit logs: %w", err)
			}
			if len(ids) == 0 {
				break
			}
			deleted, err := maint.DeleteCommitLogs(ctx, ids)
			if err != nil {
				return fmt.Errorf("failed to delete commit logs: %w", err)
			}
			total += int64(deleted)
			fmt.Fprintf(os.Stderr, "deleted %d commit logs so far...\n", total)
			if deleted == 0 {
				break
			}
		}
		fmt.Fprintf(os.Stderr, "deleted %d commit logs\n", total)
		return nil
	}),
}

func init() {
	operationCmd.AddCommand(gcCommitlogCmd)

	gcCommitlogCmd.Flags().BoolVar(&gcCommitlogDryRun, "dry-run", false, "Only count the commit logs that would be deleted")
	gcCommitlogCmd.Flags().DurationVar(&gcCommitlogRetention, "retention", domain.MaxBackdate, "How far back to keep GC-flagged commit logs; must be at least the backdate window")
}
