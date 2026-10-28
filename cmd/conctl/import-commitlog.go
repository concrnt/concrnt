package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/jwt"
)

var (
	importCommitlogDryRun    bool
	importCommitlogEndpoint  string
	importCommitlogBatchSize int
)

// importScanBuf bounds the per-line scan buffer; commit documents can be large.
const importScanBuf = 64 << 20

// importResult mirrors usecase.ImportResult — the per-line failures the server
// returns from POST /api/v2/repository (successful lines produce no entry).
type importResult struct {
	Document string `json:"document"`
	Error    string `json:"error"`
}

var importCommitlogCmd = &cobra.Command{
	Use:   "import-commitlog <commits-file> [metas-file]",
	Short: "Import a commit-log dump into the running server (restore or transplant)",
	Long: "Replays a dump produced by dump-commitlog. Commit lines (<commits-file>, one\n" +
		"concrnt.SignedDocument per line) are POSTed to the running server's\n" +
		"/api/v2/repository endpoint as the 'system' service account (a JWT signed with the\n" +
		"server's own key), so THE SERVER MUST BE RUNNING. Because the import runs as system,\n" +
		"document signatures are not re-verified — the dump is trusted as-is — and no network\n" +
		"access to referenced servers is needed. The server commits with LocalOnlyExecute, so\n" +
		"nothing is re-federated. Entity commits are sent before all others (they must exist\n" +
		"before the records that reference them).\n" +
		"Local-entity registration state (entity_metas: inviter/info) lives outside the commit\n" +
		"log; if a [metas-file] is given (as produced by dump-commitlog) it is restored first,\n" +
		"directly against Postgres. Import is idempotent: commit ids are content+time derived\n" +
		"and entity commits are accept-if-newer, so re-running is safe.",
	Args: cobra.RangeArgs(1, 2),
	RunE: withOperationContext(func(cmd *cobra.Command, args []string, op *operationContext) error {
		ctx := cmd.Context()
		commitsPath := args[0]
		metasPath := ""
		if len(args) == 2 {
			metasPath = args[1]
		}

		endpoint := importCommitlogEndpoint
		if endpoint == "" {
			endpoint = op.Config.Backends.GatewayAddr
		}
		if endpoint == "" {
			endpoint = "https://" + op.GlobalConfig.FQDN
		}
		endpoint = strings.TrimRight(endpoint, "/")

		var metaOK, entityOK, otherOK, failed int

		// 1. Restore entity metas first (directly against Postgres) so local
		//    entity commits pass their registration check on the server.
		if metasPath != "" {
			residenceRepo := op.Repos.Residence
			f, err := os.Open(metasPath)
			if err != nil {
				return fmt.Errorf("failed to open metas file: %w", err)
			}
			sc := bufio.NewScanner(f)
			sc.Buffer(make([]byte, 0, 1<<20), importScanBuf)
			lineNo := 0
			for sc.Scan() {
				lineNo++
				line := strings.TrimSpace(sc.Text())
				if line == "" {
					continue
				}
				var meta domain.EntityMeta
				if err := json.Unmarshal([]byte(line), &meta); err != nil {
					failed++
					fmt.Fprintf(os.Stderr, "metas line %d: failed to parse: %v\n", lineNo, err)
					continue
				}
				if importCommitlogDryRun {
					metaOK++
					continue
				}
				if err := residenceRepo.SaveMeta(ctx, meta); err != nil {
					failed++
					fmt.Fprintf(os.Stderr, "metas line %d (%s): failed to save: %v\n", lineNo, meta.ID, err)
					continue
				}
				metaOK++
			}
			readErr := sc.Err()
			f.Close()
			if readErr != nil {
				return fmt.Errorf("failed to read metas file: %w", readErr)
			}
		}

		// 2. Commit lines: entity documents first, then everything else. Each
		//    phase POSTs in file order (chronological), batched. Entities must be
		//    fully committed before other records that reference them.
		var token string
		commitPhase := func(wantEntity bool) error {
			f, err := os.Open(commitsPath)
			if err != nil {
				return fmt.Errorf("failed to open commits file: %w", err)
			}
			defer f.Close()

			sc := bufio.NewScanner(f)
			sc.Buffer(make([]byte, 0, 1<<20), importScanBuf)
			lineNo := 0
			batch := make([]string, 0, importCommitlogBatchSize)

			flush := func() error {
				if len(batch) == 0 {
					return nil
				}
				results, err := postRepository(endpoint, &token, strings.Join(batch, "\n"))
				sent := len(batch)
				batch = batch[:0]
				if err != nil {
					return err
				}
				// The server returns one entry per REJECTED line; accepted lines
				// produce none. Attribute successes to the current phase's kind.
				accepted := sent - len(results)
				if wantEntity {
					entityOK += accepted
				} else {
					otherOK += accepted
				}
				for _, res := range results {
					failed++
					fmt.Fprintf(os.Stderr, "commit rejected: %s\n", res.Error)
				}
				return nil
			}

			for sc.Scan() {
				lineNo++
				line := strings.TrimSpace(sc.Text())
				if line == "" {
					continue
				}
				var sd concrnt.SignedDocument
				if err := json.Unmarshal([]byte(line), &sd); err != nil {
					// Parse errors are only counted once (on the entity phase).
					if wantEntity {
						failed++
						fmt.Fprintf(os.Stderr, "commits line %d: failed to parse SignedDocument: %v\n", lineNo, err)
					}
					continue
				}
				var head struct {
					Kind string `json:"kind"`
				}
				if err := json.Unmarshal([]byte(sd.Document), &head); err != nil {
					if wantEntity {
						failed++
						fmt.Fprintf(os.Stderr, "commits line %d: failed to parse document kind: %v\n", lineNo, err)
					}
					continue
				}
				if (head.Kind == "entity") != wantEntity {
					continue
				}

				if importCommitlogDryRun {
					if wantEntity {
						entityOK++
					} else {
						otherOK++
					}
					continue
				}

				batch = append(batch, line)
				if len(batch) >= importCommitlogBatchSize {
					if err := flush(); err != nil {
						return err
					}
				}
			}
			if err := sc.Err(); err != nil {
				return fmt.Errorf("failed to read commits file: %w", err)
			}
			return flush()
		}

		if err := commitPhase(true); err != nil {
			return err
		}
		if err := commitPhase(false); err != nil {
			return err
		}

		verb := "imported"
		if importCommitlogDryRun {
			verb = "validated (dry-run)"
		}
		fmt.Fprintf(os.Stderr, "%s %d metas, %d entities, %d others; %d failed\n", verb, metaOK, entityOK, otherOK, failed)
		if failed > 0 {
			return fmt.Errorf("%d lines failed to import", failed)
		}
		return nil
	}),
}

// postRepository POSTs a JSONL batch to the server's /api/v2/repository as the
// system service account, refreshing the token when it is about to expire. It
// returns the per-line failures the server reports (empty on full success).
func postRepository(endpoint string, token *string, body string) ([]importResult, error) {
	if *token == "" || tokenExpiringSoon(*token) {
		*token = generateToken("system", 1*time.Hour)
	}

	request, err := http.NewRequest("POST", endpoint+"/api/v2/repository", strings.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	request.Header.Set("Content-Type", "text/plain")
	request.Header.Set("Authorization", "Bearer "+*token)

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("failed to POST to %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("server returned %s: %s", resp.Status, strings.TrimSpace(string(respBody)))
	}

	var results []importResult
	if err := json.Unmarshal(respBody, &results); err != nil {
		return nil, fmt.Errorf("failed to decode import result: %w (body: %s)", err, strings.TrimSpace(string(respBody)))
	}
	return results, nil
}

// tokenExpiringSoon reports whether token expires within a minute (or can't be
// parsed), so a fresh one should be minted before the next request.
func tokenExpiringSoon(token string) bool {
	_, claims, err := jwt.Parse(token)
	if err != nil || claims.ExpirationTime == "" {
		return true
	}
	expUnix, err := strconv.ParseInt(claims.ExpirationTime, 10, 64)
	if err != nil {
		return true
	}
	return time.Until(time.Unix(expUnix, 0)) < 1*time.Minute
}

func init() {
	operationCmd.AddCommand(importCommitlogCmd)

	importCommitlogCmd.Flags().BoolVar(&importCommitlogDryRun, "dry-run", false, "Parse and count without restoring metas or POSTing commits")
	importCommitlogCmd.Flags().StringVar(&importCommitlogEndpoint, "endpoint", "", "Server base URL to POST commits to (default: backends.gatewayAddr, else https://<fqdn>)")
	importCommitlogCmd.Flags().IntVar(&importCommitlogBatchSize, "batch-size", 1000, "Commit lines per POST request")
}
