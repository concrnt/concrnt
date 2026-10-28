package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var dumpSubscriptionsOwner string

var dumpSubscriptionsCmd = &cobra.Command{
	Use:   "dump-subscriptions [file]",
	Short: "Dump web push subscriptions to a JSONL file",
	Long: "Writes every web push subscription (the notification_subscriptions state the server keeps\n" +
		"per vendor and owner) to [file] as JSONL, one domain.NotificationSubscription per line, so\n" +
		"it can be restored on another server with import-subscriptions. Subscriptions live outside\n" +
		"the commit log and are therefore not part of dump-commitlog. [file] defaults to\n" +
		"subscriptions-<timestamp>.jsonl when omitted. Use --owner to dump one user's subscriptions\n" +
		"only. Reads directly from the database; no running server required.",
	Args: cobra.MaximumNArgs(1),
	RunE: withOperationContext(func(cmd *cobra.Command, args []string, op *operationContext) error {
		ctx := cmd.Context()

		path := ""
		if len(args) == 1 {
			path = args[0]
		}
		if path == "" {
			path = "subscriptions-" + time.Now().UTC().Format("20060102T150405Z") + ".jsonl"
		}

		subscriptions, err := op.Repos.Notification.List(ctx)
		if err != nil {
			return fmt.Errorf("failed to list subscriptions: %w", err)
		}

		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("failed to create subscriptions file: %w", err)
		}
		defer f.Close()
		w := bufio.NewWriter(f)

		total := 0
		for _, s := range subscriptions {
			if dumpSubscriptionsOwner != "" && s.Owner != dumpSubscriptionsOwner {
				continue
			}
			line, err := json.Marshal(s)
			if err != nil {
				return fmt.Errorf("failed to marshal subscription %s/%s: %w", s.VendorID, s.Owner, err)
			}
			if _, err := w.Write(append(line, '\n')); err != nil {
				return fmt.Errorf("failed to write subscriptions file: %w", err)
			}
			total++
		}
		if err := w.Flush(); err != nil {
			return fmt.Errorf("failed to flush subscriptions file: %w", err)
		}

		fmt.Fprintf(os.Stderr, "dumped %d subscriptions -> %s\n", total, path)
		return nil
	}),
}

func init() {
	operationCmd.AddCommand(dumpSubscriptionsCmd)

	dumpSubscriptionsCmd.Flags().StringVar(&dumpSubscriptionsOwner, "owner", "", "Restrict dump to subscriptions owned by this CCID")
}
