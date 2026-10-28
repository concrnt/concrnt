package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/cdid"
	pgrecord "github.com/concrnt/concrnt/internal/infra/repository/postgres/record"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

var (
	aggregateInvalidateAll    bool
	aggregateInvalidateDef    string
	aggregateInvalidateTarget string

	aggregateVerifySample int
	aggregateVerifyDef    string
	aggregateVerifyScan   bool
	aggregateVerifyFix    bool
)

func init() {
	aggregateInvalidateCmd.Flags().BoolVar(&aggregateInvalidateAll, "all", false, "drop every cached count")
	aggregateInvalidateCmd.Flags().StringVar(&aggregateInvalidateDef, "def", "", "drop the entries of one definition ("+strings.Join(record.AggregateNames(), ", ")+")")
	aggregateInvalidateCmd.Flags().StringVar(&aggregateInvalidateTarget, "target", "", "with --def: drop one target's entry only")
	operationCmd.AddCommand(aggregateInvalidateCmd)

	aggregateVerifyCmd.Flags().IntVar(&aggregateVerifySample, "sample", 100, "how many cache entries to check")
	aggregateVerifyCmd.Flags().StringVar(&aggregateVerifyDef, "def", "", "check one definition only")
	aggregateVerifyCmd.Flags().BoolVar(&aggregateVerifyScan, "scan", false, "sample uniformly over the whole commit log (reads every commit) instead of probing random points in time")
	aggregateVerifyCmd.Flags().BoolVar(&aggregateVerifyFix, "fix", true, "drop the entries that drifted so they are recomputed")
	operationCmd.AddCommand(aggregateVerifyCmd)
}

var aggregateInvalidateCmd = &cobra.Command{
	Use:   "aggregate-invalidate",
	Short: "Drop cached counts so they are recomputed from the primary data",
	Long: `The count caches (association counts, ack counts) are derived data: dropping
them is always safe, every entry is recomputed on its next read or write.
Run with --all after a release that changed how a count is computed, after
a bulk operation that bypassed the usecase (delete-user, repair-*), or when
aggregate-verify reports drift.`,
	RunE: withOperationContext(func(cmd *cobra.Command, args []string, op *operationContext) error {
		ctx := cmd.Context()
		switch {
		case aggregateInvalidateAll:
			if err := op.Aggregate.InvalidateAll(ctx, ""); err != nil {
				return err
			}
			fmt.Fprintln(os.Stderr, "dropped every cached count")
		case aggregateInvalidateDef != "" && aggregateInvalidateTarget != "":
			if err := op.Aggregate.Invalidate(ctx, aggregateInvalidateDef, aggregateInvalidateTarget); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "dropped %s for %s\n", aggregateInvalidateDef, aggregateInvalidateTarget)
		case aggregateInvalidateDef != "":
			if err := op.Aggregate.InvalidateAll(ctx, aggregateInvalidateDef+":"); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "dropped every %s entry\n", aggregateInvalidateDef)
		default:
			return fmt.Errorf("pass --all, or --def <name> [--target <uri>]")
		}
		return nil
	}),
}

var aggregateVerifyCmd = &cobra.Command{
	Use:   "aggregate-verify",
	Short: "Compare cached counts with a fresh recount of the primary data",
	Long: `Picks targets from the commit log (association and ack-kind documents),
compares each one's cached count with repo.Compute* and reports how many
were cold (not cached), equal or drifted. Drifted entries are dropped unless
--fix=false. The default sampling probes random points in time with a short
page each; --scan reads the whole commit log for an exact uniform sample.`,
	RunE: withOperationContext(func(cmd *cobra.Command, args []string, op *operationContext) error {
		ctx := cmd.Context()
		if aggregateVerifySample <= 0 {
			return fmt.Errorf("--sample must be positive")
		}
		recordRepo := pgrecord.New(op.DB, op.Client, op.GlobalConfig)
		recordUC := record.New(recordRepo, nil, nil, &op.GlobalConfig, op.Client, nil, nil, nil, nil)
		recordUC.SetAggregates(op.Aggregate)

		refs, err := sampleAggregateRefs(ctx, recordRepo, aggregateVerifySample, aggregateVerifyDef, aggregateVerifyScan)
		if err != nil {
			return err
		}

		var cold, equal, drifted int
		for _, ref := range refs {
			v, err := recordUC.VerifyAggregate(ctx, ref)
			if err != nil {
				return fmt.Errorf("%s %s: %w", ref.Name, ref.Target, err)
			}
			switch {
			case v.Cold:
				cold++
			case v.Equal:
				equal++
			default:
				drifted++
				fmt.Fprintf(os.Stderr, "drift: %s %q\n  cached:   %s\n  computed: %s\n", ref.Name, ref.Target, v.Cached, v.Computed)
				if aggregateVerifyFix {
					if err := recordUC.InvalidateAggregate(ctx, ref); err != nil {
						return err
					}
				}
			}
		}
		fmt.Fprintf(os.Stderr, "sampled %d: cold %d, equal %d, drifted %d\n", len(refs), cold, equal, drifted)
		if drifted > 0 && aggregateVerifyFix {
			fmt.Fprintf(os.Stderr, "dropped %d drifted entries; they rebuild on their next read\n", drifted)
		}
		return nil
	}),
}

// sampleAggregateRefs picks up to n distinct cache entries from the commit
// log. scan walks every page and keeps a uniform reservoir; otherwise random
// instants between the first commit and now are probed with one short page
// each, which is cheap on a large log but favours busy periods less evenly.
func sampleAggregateRefs(ctx context.Context, repo record.Repository, n int, def string, scan bool) ([]record.AggregateRef, error) {
	const pageSize = 256
	seen := map[record.AggregateRef]bool{}
	var out []record.AggregateRef
	total := 0 // candidates seen, for the reservoir
	consider := func(entries []record.CommitLogEntry) {
		for _, cl := range entries {
			var doc concrnt.Document[any]
			if err := json.Unmarshal([]byte(cl.Document), &doc); err != nil {
				continue
			}
			for _, ref := range record.AggregateTargets(doc) {
				if (def != "" && ref.Name != def) || seen[ref] {
					continue
				}
				seen[ref] = true
				total++
				if len(out) < n {
					out = append(out, ref)
				} else if j := rand.IntN(total); j < n {
					out[j] = ref
				}
			}
		}
	}

	if scan {
		after := ""
		for {
			entries, err := repo.ListCommitLogs(ctx, record.CommitLogPage{AfterID: after, Limit: pageSize})
			if err != nil {
				return nil, err
			}
			if len(entries) == 0 {
				return out, nil
			}
			consider(entries)
			after = entries[len(entries)-1].ID
		}
	}

	first, err := repo.ListCommitLogs(ctx, record.CommitLogPage{Limit: 1})
	if err != nil {
		return nil, err
	}
	if len(first) == 0 {
		return nil, nil
	}
	firstID, err := cdid.Parse(first[0].ID)
	if err != nil {
		return nil, fmt.Errorf("first commit id %q: %w", first[0].ID, err)
	}
	start, end := firstID.GetTime(), time.Now()
	probes := 0
	for len(out) < n && probes < 10*n/8+10 {
		probes++
		at := start.Add(time.Duration(rand.Int64N(int64(end.Sub(start)) + 1)))
		entries, err := repo.ListCommitLogs(ctx, record.CommitLogPage{FromID: cdid.New([10]byte{}, at).String(), Limit: pageSize})
		if err != nil {
			return nil, err
		}
		consider(entries)
	}
	return out, nil
}
