package main

import (
	"fmt"
	"math"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/concrnt/concrnt"
	pgrecord "github.com/concrnt/concrnt/internal/infra/repository/postgres/record"
)

var trustSeedUnset bool

var trustSeedCmd = &cobra.Command{
	Use:   "trust-seed <ccid>",
	Short: "Mark an entity as a trust seed (or unmark it with --unset)",
	Long: "Trust seeds are the entities this server trusts outright. Trust reaches every other\n" +
		"entity through acknowledgements: each entity passes its trust on, in equal parts, to\n" +
		"the entities it acknowledges, and a remote server is trusted as much as its entities\n" +
		"are, added up. That trust sizes the server's ingest limit between\n" +
		"concrnt.ingestLimit.min and .max.\n" +
		"\n" +
		"Seeds are normally local users whose acknowledgements the operator stands behind. A\n" +
		"remote entity can be a seed too, which makes its server fully trusted. The entity must\n" +
		"already be known to this server. The change takes effect at the next trust\n" +
		"recomputation, within a few minutes; show-trust prints the result.",
	Args: cobra.ExactArgs(1),
	RunE: withOperationContext(func(cmd *cobra.Command, args []string, op *operationContext) error {
		ccid := args[0]
		if !concrnt.IsCCID(ccid) {
			return fmt.Errorf("invalid ccid %q", ccid)
		}

		found, err := pgrecord.New(op.DB).SetTrustSeed(cmd.Context(), ccid, !trustSeedUnset)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("no entity %s on this server", ccid)
		}
		if trustSeedUnset {
			fmt.Fprintf(os.Stderr, "%s is no longer a trust seed\n", ccid)
		} else {
			fmt.Fprintf(os.Stderr, "%s is now a trust seed\n", ccid)
		}
		return nil
	}),
}

var showTrustCmd = &cobra.Command{
	Use:   "show-trust",
	Short: "Print the trust seeds and how much each remote server is trusted",
	Long: "Prints the trust seeds, then every remote server holding trust as of the latest\n" +
		"recomputation, with the ingest limit (commits per minute) that trust gives it under\n" +
		"the current config. Servers not listed hold no trust and get concrnt.ingestLimit.min.",
	Args: cobra.NoArgs,
	RunE: withOperationContext(func(cmd *cobra.Command, args []string, op *operationContext) error {
		ctx := cmd.Context()
		repo := pgrecord.New(op.DB)

		seeds, err := repo.GetTrustSeeds(ctx)
		if err != nil {
			return err
		}
		sort.Strings(seeds)
		fmt.Printf("seeds (%d):\n", len(seeds))
		for _, seed := range seeds {
			fmt.Printf("  %s\n", seed)
		}

		totals, err := repo.GetDomainTrust(ctx)
		if err != nil {
			return err
		}
		delete(totals, op.GlobalConfig.FQDN)
		domains := make([]string, 0, len(totals))
		for name := range totals {
			domains = append(domains, name)
		}
		sort.Slice(domains, func(i, j int) bool {
			if totals[domains[i]] != totals[domains[j]] {
				return totals[domains[i]] > totals[domains[j]]
			}
			return domains[i] < domains[j]
		})

		limit := op.GlobalConfig.IngestLimit
		fmt.Printf("\ntrusted servers (%d):\n", len(domains))
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "  DOMAIN\tTRUST\tLIMIT/MIN")
		for _, name := range domains {
			trust := math.Min(1, totals[name])
			perMinute := "unlimited"
			if n := limit.DomainLimit(trust); n > 0 {
				perMinute = fmt.Sprint(n)
			}
			fmt.Fprintf(w, "  %s\t%.3f\t%s\n", name, trust, perMinute)
		}
		return w.Flush()
	}),
}

func init() {
	trustSeedCmd.Flags().BoolVar(&trustSeedUnset, "unset", false, "unmark the entity instead")
	operationCmd.AddCommand(trustSeedCmd)
	operationCmd.AddCommand(showTrustCmd)
}
