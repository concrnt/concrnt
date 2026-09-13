package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/cdid"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

var (
	dumpCommitlogSince   string
	dumpCommitlogSinceID string
	dumpCommitlogUntil   string
	dumpCommitlogUntilID string
	dumpCommitlogOwner   string
)

const dumpCommitlogPageSize = 1000

var dumpCommitlogCmd = &cobra.Command{
	Use:   "dump-commitlog [name]",
	Short: "Dump commit logs and entity metas to a pair of JSONL files",
	Long: "Streams the server's commit logs (the canonical immutable ledger) to <name>.commits.jsonl\n" +
		"as JSONL, one concrnt.SignedDocument per line, ordered chronologically by id — the same\n" +
		"format the server returns from GET /api/v2/repository, so it can be replayed by\n" +
		"import-commitlog. Local-entity registration state (entity_metas: inviter/info), which\n" +
		"lives outside the commit log, is written separately to <name>.metas.jsonl, one\n" +
		"domain.EntityMeta per line. [name] defaults to a timestamp when omitted.\n" +
		"Use --since/--since-id (and optionally --until/--until-id) to export only commits from\n" +
		"a given point in time onward. Reads directly from the database; no running server required.\n" +
		"Web push subscriptions are not part of the commit log: dump them with dump-subscriptions.",
	Args: cobra.MaximumNArgs(1),
	RunE: withOperationContext(func(cmd *cobra.Command, args []string, op *operationContext) error {
		ctx := cmd.Context()

		// Resolve the inclusive id bounds from --since*/--until*. Time-kind CDIDs
		// are lexicographically ordered by their embedded millisecond timestamp,
		// so a plain string comparison on the id column selects a time window.
		if dumpCommitlogSince != "" && dumpCommitlogSinceID != "" {
			return fmt.Errorf("--since and --since-id are mutually exclusive")
		}
		if dumpCommitlogUntil != "" && dumpCommitlogUntilID != "" {
			return fmt.Errorf("--until and --until-id are mutually exclusive")
		}
		lowerBound := dumpCommitlogSinceID
		if dumpCommitlogSinceID != "" {
			if _, err := cdid.Parse(dumpCommitlogSinceID); err != nil {
				return fmt.Errorf("invalid --since-id %q: %w", dumpCommitlogSinceID, err)
			}
		}
		if dumpCommitlogSince != "" {
			t, err := time.Parse(time.RFC3339, dumpCommitlogSince)
			if err != nil {
				return fmt.Errorf("invalid --since %q (expected RFC3339): %w", dumpCommitlogSince, err)
			}
			lowerBound = cdid.New([10]byte{}, t.UTC()).String() // smallest id at t
		}
		upperBound := dumpCommitlogUntilID
		if dumpCommitlogUntilID != "" {
			if _, err := cdid.Parse(dumpCommitlogUntilID); err != nil {
				return fmt.Errorf("invalid --until-id %q: %w", dumpCommitlogUntilID, err)
			}
		}
		if dumpCommitlogUntil != "" {
			t, err := time.Parse(time.RFC3339, dumpCommitlogUntil)
			if err != nil {
				return fmt.Errorf("invalid --until %q (expected RFC3339): %w", dumpCommitlogUntil, err)
			}
			maxData := [10]byte{}
			for i := range maxData {
				maxData[i] = 0xff
			}
			upperBound = cdid.New(maxData, t.UTC()).String() // largest id at t
		}

		baseName := ""
		if len(args) == 1 {
			baseName = args[0]
		}
		if baseName == "" {
			baseName = "commitlog-" + time.Now().UTC().Format("20060102T150405Z")
		}
		commitsPath := baseName + ".commits.jsonl"
		metasPath := baseName + ".metas.jsonl"

		metasFile, err := os.Create(metasPath)
		if err != nil {
			return fmt.Errorf("failed to create metas file: %w", err)
		}
		defer metasFile.Close()
		commitsFile, err := os.Create(commitsPath)
		if err != nil {
			return fmt.Errorf("failed to create commits file: %w", err)
		}
		defer commitsFile.Close()

		metasW := bufio.NewWriter(metasFile)
		commitsW := bufio.NewWriter(commitsFile)

		// Entity metas carry local-entity registration state (inviter/info) that
		// is not part of the commit log, and import must restore them before
		// replaying entity commits. Time filters don't apply to meta; --owner does.
		metas, err := op.Repos.Residence.ListMetas(ctx, dumpCommitlogOwner)
		if err != nil {
			return fmt.Errorf("failed to query entity meta: %w", err)
		}
		for _, m := range metas {
			line, err := json.Marshal(m)
			if err != nil {
				return fmt.Errorf("failed to marshal entity meta %s: %w", m.ID, err)
			}
			if _, err := metasW.Write(append(line, '\n')); err != nil {
				return fmt.Errorf("failed to write metas file: %w", err)
			}
		}
		if err := metasW.Flush(); err != nil {
			return fmt.Errorf("failed to flush metas file: %w", err)
		}

		// Page through commit logs by id (time-kind CDIDs order by their
		// embedded timestamp).
		cursor := lowerBound
		first := true
		total := 0
		for {
			page := record.CommitLogPage{
				UntilID: upperBound,
				Owner:   dumpCommitlogOwner,
				Limit:   dumpCommitlogPageSize,
			}
			if first {
				page.FromID = cursor
			} else {
				page.AfterID = cursor
			}
			logs, err := op.Repos.RecordMaintenance.ListCommitLogs(ctx, page)
			if err != nil {
				return fmt.Errorf("failed to query commit logs: %w", err)
			}
			if len(logs) == 0 {
				break
			}

			for _, cl := range logs {
				line, err := json.Marshal(concrnt.SignedDocument{
					Document: cl.Document,
					Proof:    cl.Proof,
				})
				if err != nil {
					return fmt.Errorf("failed to marshal commit %s: %w", cl.ID, err)
				}
				if _, err := commitsW.Write(append(line, '\n')); err != nil {
					return fmt.Errorf("failed to write commits file: %w", err)
				}
			}

			total += len(logs)
			cursor = logs[len(logs)-1].ID
			first = false
		}
		if err := commitsW.Flush(); err != nil {
			return fmt.Errorf("failed to flush commits file: %w", err)
		}

		fmt.Fprintf(os.Stderr, "dumped %d commit logs -> %s\n", total, commitsPath)
		fmt.Fprintf(os.Stderr, "dumped %d entity metas -> %s\n", len(metas), metasPath)
		return nil
	}),
}

func init() {
	operationCmd.AddCommand(dumpCommitlogCmd)

	dumpCommitlogCmd.Flags().StringVar(&dumpCommitlogSince, "since", "", "Only dump commits at/after this RFC3339 time (inclusive)")
	dumpCommitlogCmd.Flags().StringVar(&dumpCommitlogSinceID, "since-id", "", "Only dump commits at/after this commit-log CDID (inclusive)")
	dumpCommitlogCmd.Flags().StringVar(&dumpCommitlogUntil, "until", "", "Only dump commits at/before this RFC3339 time (inclusive)")
	dumpCommitlogCmd.Flags().StringVar(&dumpCommitlogUntilID, "until-id", "", "Only dump commits at/before this commit-log CDID (inclusive)")
	dumpCommitlogCmd.Flags().StringVar(&dumpCommitlogOwner, "owner", "", "Restrict dump to commits owned by this CCID")
}
