package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/concrnt/concrnt/internal/domain"
)

var importSubscriptionsDryRun bool

var importSubscriptionsCmd = &cobra.Command{
	Use:   "import-subscriptions <file>",
	Short: "Restore web push subscriptions from a dump-subscriptions file",
	Long: "Reads a JSONL file produced by dump-subscriptions (one domain.NotificationSubscription per\n" +
		"line) and upserts each subscription directly into the database, keyed by (vendorID, owner),\n" +
		"so re-running is safe. Nothing is pushed to the subscribers. Restore subscriptions AFTER\n" +
		"import-commitlog so the owners exist. Reads and writes the database directly; no running\n" +
		"server required.",
	Args: cobra.ExactArgs(1),
	RunE: withOperationContext(func(cmd *cobra.Command, args []string, op *operationContext) error {
		ctx := cmd.Context()

		f, err := os.Open(args[0])
		if err != nil {
			return fmt.Errorf("failed to open subscriptions file: %w", err)
		}
		defer f.Close()

		var ok, failed int
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 1<<20), importScanBuf)
		lineNo := 0
		for sc.Scan() {
			lineNo++
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var sub domain.NotificationSubscription
			if err := json.Unmarshal([]byte(line), &sub); err != nil {
				failed++
				fmt.Fprintf(os.Stderr, "line %d: failed to parse: %v\n", lineNo, err)
				continue
			}
			if sub.VendorID == "" || sub.Owner == "" {
				failed++
				fmt.Fprintf(os.Stderr, "line %d: vendorID and owner are required\n", lineNo)
				continue
			}
			if importSubscriptionsDryRun {
				ok++
				continue
			}
			if _, err := op.Repos.Notification.Subscribe(ctx, sub); err != nil {
				failed++
				fmt.Fprintf(os.Stderr, "line %d (%s/%s): failed to save: %v\n", lineNo, sub.VendorID, sub.Owner, err)
				continue
			}
			ok++
		}
		if err := sc.Err(); err != nil {
			return fmt.Errorf("failed to read subscriptions file: %w", err)
		}

		verb := "imported"
		if importSubscriptionsDryRun {
			verb = "validated (dry-run)"
		}
		fmt.Fprintf(os.Stderr, "%s %d subscriptions; %d failed\n", verb, ok, failed)
		if failed > 0 {
			return fmt.Errorf("%d lines failed to import", failed)
		}
		return nil
	}),
}

func init() {
	operationCmd.AddCommand(importSubscriptionsCmd)

	importSubscriptionsCmd.Flags().BoolVar(&importSubscriptionsDryRun, "dry-run", false, "Parse and count without writing")
}
