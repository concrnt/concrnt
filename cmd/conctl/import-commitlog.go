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
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/jwt"
)

var (
	importCommitlogDryRun      bool
	importCommitlogEndpoint    string
	importCommitlogBatchSize   int
	importCommitlogConcurrency int
	importCommitlogRejectedOut string
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
		"directly against the database. Import is idempotent: commit ids are content+time derived\n" +
		"and entity commits are accept-if-newer, so re-running is safe.\n" +
		"Commits are replayed in dump order (document createdAt), and a commit can depend on one\n" +
		"that sorts after it (a backdated post into a timeline created later, an association whose\n" +
		"target is younger). Rejected lines are therefore re-sent in further passes until a pass\n" +
		"accepts nothing more; what is still rejected then is final, and --rejected-out writes those\n" +
		"lines verbatim (same JSONL format) so they can be inspected or re-fed to this command.\n" +
		"--concurrency keeps several batches in flight: the replay is latency-bound (one store\n" +
		"transaction per line), and reordering across batches is harmless for state decided by\n" +
		"createdAt or a unique key; batches holding a delete still run alone, in order.\n" +
		"Web push subscriptions are not part of the commit log: see dump-/import-subscriptions.",
	Args: cobra.RangeArgs(1, 2),
	RunE: withOperationContext(func(cmd *cobra.Command, args []string, op *operationContext) error {
		ctx := cmd.Context()
		commitsPath := args[0]
		metasPath := ""
		if len(args) == 2 {
			metasPath = args[1]
		}
		if importCommitlogBatchSize < 1 {
			return fmt.Errorf("--batch-size must be at least 1")
		}
		if importCommitlogConcurrency < 1 {
			return fmt.Errorf("--concurrency must be at least 1")
		}

		endpoint := importCommitlogEndpoint
		if endpoint == "" {
			endpoint = op.Config.Backends.GatewayAddr
		}
		if endpoint == "" {
			endpoint = "https://" + op.GlobalConfig.FQDN
		}
		endpoint = strings.TrimRight(endpoint, "/")

		var metaOK, failed int

		// 1. Restore entity metas first (directly against the database) so local
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

		// 2. Commit lines.
		var token string
		stats, err := replayCommitLogs(commitsPath, endpoint, &token, importCommitlogBatchSize, importCommitlogConcurrency, importCommitlogDryRun, os.Stderr)
		if err != nil {
			return err
		}
		failed += stats.parseFailed + len(stats.rejected)

		if importCommitlogRejectedOut != "" && len(stats.rejected) > 0 {
			f, err := os.Create(importCommitlogRejectedOut)
			if err != nil {
				return fmt.Errorf("failed to create rejected file: %w", err)
			}
			w := bufio.NewWriter(f)
			for _, line := range stats.rejected {
				if _, err := w.WriteString(line + "\n"); err != nil {
					f.Close()
					return fmt.Errorf("failed to write rejected file: %w", err)
				}
			}
			if err := w.Flush(); err != nil {
				f.Close()
				return fmt.Errorf("failed to flush rejected file: %w", err)
			}
			f.Close()
			fmt.Fprintf(os.Stderr, "wrote %d rejected lines -> %s\n", len(stats.rejected), importCommitlogRejectedOut)
		}

		verb := "imported"
		if importCommitlogDryRun {
			verb = "validated (dry-run)"
		}
		fmt.Fprintf(os.Stderr, "%s %d metas, %d entities, %d others, %d more on retry; %d failed\n", verb, metaOK, stats.entityOK, stats.otherOK, stats.retryOK, failed)
		if failed > 0 {
			return fmt.Errorf("%d lines failed to import", failed)
		}
		return nil
	}),
}

// replayStats is what replayCommitLogs did with the commits file.
type replayStats struct {
	entityOK    int // accepted in the entity phase
	otherOK     int // accepted in the non-entity phase
	retryOK     int // accepted in a retry pass
	parseFailed int // lines that are not a SignedDocument
	// rejected are the lines still refused after the retry passes, verbatim
	// and in file order; reasons holds the server's error for each of them.
	rejected []string
	reasons  []string
}

// lineInfo is what the batcher needs to know about one dump line to keep
// deletes ordered against the lines they interact with.
type lineInfo struct {
	line string
	kind string
	// touches are the addresses this line creates or targets: the record key
	// or association target, and the ccfs URI the server will assign the
	// document (what an association delete names).
	touches []string
	// del is set for delete documents.
	del *deleteTarget
}

// deleteTarget is the parsed value of a delete document.
type deleteTarget struct {
	base        string // cckv key or ccfs URI, range sentinel removed
	isRange     bool
	includeSelf bool
}

func (d deleteTarget) hits(addr string) bool {
	if d.isRange {
		return (d.includeSelf && addr == d.base) || strings.HasPrefix(addr, d.base+"/")
	}
	return addr == d.base
}

// describeLine parses one dump line; ok is false when it is not a
// SignedDocument wrapping a document.
func describeLine(line string) (lineInfo, bool, error) {
	var sd concrnt.SignedDocument
	if err := json.Unmarshal([]byte(line), &sd); err != nil {
		return lineInfo{}, false, fmt.Errorf("failed to parse SignedDocument: %w", err)
	}
	doc, err := sd.ParsedDocument()
	if err != nil {
		return lineInfo{}, false, fmt.Errorf("failed to parse document: %w", err)
	}
	li := lineInfo{line: line, kind: doc.Kind}
	switch doc.Kind {
	case "record", "association":
		addr := doc.Key
		if doc.Kind == "association" && doc.Associate != nil {
			addr = *doc.Associate
		}
		li.touches = append(li.touches, addr)
		// the ccfs identity the server assigns: owner of the key / target
		if parsed, err := concrnt.ParseCCURI(addr); err == nil {
			if cdid, err := sd.CDID(); err == nil {
				li.touches = append(li.touches, concrnt.CCURI{Scheme: "ccfs", Owner: parsed.Owner, Type: concrnt.CCFSTypeConcrnt, CDID: cdid}.String())
			}
		}
	case "delete":
		target, _ := doc.Value.(string)
		d := &deleteTarget{base: target}
		switch {
		case strings.HasSuffix(target, "/*"):
			d.base, d.isRange = strings.TrimSuffix(target, "/*"), true
		case strings.HasSuffix(target, "*"):
			d.base, d.isRange, d.includeSelf = strings.TrimSuffix(target, "*"), true, true
		}
		li.del = d
	}
	return li, true, nil
}

// batcher groups lines into batches and sends them through send, up to
// concurrency batches at a time. Deletes are kept ordered against the lines
// they interact with: a delete waits for queued lines that touch its target
// (the record it removes, an association on it), and a later line touching a
// queued delete's target waits for the delete. Everything else may reorder.
type batcher struct {
	batchSize, concurrency int
	send                   func(window [][]string) (accepted int, err error)

	batch    []string
	window   [][]string
	accepted int

	// addresses touched and deletes held by the lines queued (batch + window)
	queuedTouches map[string]int
	queuedDeletes []deleteTarget
}

func (b *batcher) push(li lineInfo) error {
	conflict := false
	if li.del != nil {
		for addr := range b.queuedTouches {
			if li.del.hits(addr) {
				conflict = true
				break
			}
		}
	} else {
		for _, d := range b.queuedDeletes {
			for _, addr := range li.touches {
				if d.hits(addr) {
					conflict = true
				}
			}
		}
	}
	if conflict {
		if err := b.flush(); err != nil {
			return err
		}
	}
	b.batch = append(b.batch, li.line)
	if li.del != nil {
		b.queuedDeletes = append(b.queuedDeletes, *li.del)
	}
	for _, addr := range li.touches {
		if b.queuedTouches == nil {
			b.queuedTouches = map[string]int{}
		}
		b.queuedTouches[addr]++
	}
	if len(b.batch) < b.batchSize {
		return nil
	}
	b.window = append(b.window, b.batch)
	b.batch = nil
	if len(b.window) >= b.concurrency {
		return b.flush()
	}
	return nil
}

// flush sends everything queued, the open batch included.
func (b *batcher) flush() error {
	if len(b.batch) > 0 {
		b.window = append(b.window, b.batch)
		b.batch = nil
	}
	b.queuedTouches, b.queuedDeletes = nil, nil
	if len(b.window) == 0 {
		return nil
	}
	window := b.window
	b.window = nil
	n, err := b.send(window)
	if err != nil {
		return err
	}
	b.accepted += n
	return nil
}

// replayCommitLogs POSTs the commits file to endpoint in batches: entity
// documents first, then everything else, each in file order (chronological),
// then re-sends whatever was rejected in further passes until a pass accepts
// nothing more — a commit may depend on one that sorts after it. Progress and
// the final rejection reasons are written to log. With dryRun nothing is sent;
// lines are only parsed and counted.
//
// concurrency > 1 keeps that many batches in flight. Reordering across
// batches is safe for everything the store decides by createdAt or by a
// unique key (records and entities are accept-if-newer, acks strictly-newer,
// associations unique) and for dependencies, which the retry passes settle;
// it is not safe for deletes, which remove whatever is there, so a delete
// stays ordered against the lines touching its target (see batcher).
func replayCommitLogs(commitsPath, endpoint string, token *string, batchSize, concurrency int, dryRun bool, log io.Writer) (replayStats, error) {
	var stats replayStats
	if concurrency < 1 {
		concurrency = 1
	}
	// rejected holds the server's per-line refusals of the pass in progress;
	// documents are the lines themselves, so they can be re-sent verbatim.
	var rejected []importResult

	// sendWindow posts the batches concurrently and folds their refusals into
	// rejected in batch order, so retry passes keep the file order.
	sendWindow := func(window [][]string) (accepted int, err error) {
		if *token == "" || tokenExpiringSoon(*token) {
			*token = generateToken("system", 1*time.Hour)
		}
		results := make([][]importResult, len(window))
		errs := make([]error, len(window))
		sem := make(chan struct{}, concurrency)
		var wg sync.WaitGroup
		for i, batch := range window {
			wg.Add(1)
			sem <- struct{}{}
			go func(i int, batch []string) {
				defer wg.Done()
				defer func() { <-sem }()
				tok := *token
				results[i], errs[i] = postRepository(endpoint, &tok, strings.Join(batch, "\n"))
			}(i, batch)
		}
		wg.Wait()
		for i := range window {
			if errs[i] != nil {
				return 0, errs[i]
			}
			rejected = append(rejected, results[i]...)
			accepted += len(window[i]) - len(results[i])
		}
		return accepted, nil
	}
	newBatcher := func() *batcher {
		return &batcher{batchSize: batchSize, concurrency: concurrency, send: sendWindow}
	}

	commitPhase := func(wantEntity bool) error {
		f, err := os.Open(commitsPath)
		if err != nil {
			return fmt.Errorf("failed to open commits file: %w", err)
		}
		defer f.Close()

		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 1<<20), importScanBuf)
		lineNo := 0
		b := newBatcher()

		for sc.Scan() {
			lineNo++
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			li, ok, err := describeLine(line)
			if !ok {
				// Parse errors are only counted once (on the entity phase).
				if wantEntity {
					stats.parseFailed++
					fmt.Fprintf(log, "commits line %d: %v\n", lineNo, err)
				}
				continue
			}
			if (li.kind == "entity") != wantEntity {
				continue
			}

			if dryRun {
				if wantEntity {
					stats.entityOK++
				} else {
					stats.otherOK++
				}
				continue
			}

			if err := b.push(li); err != nil {
				return err
			}
		}
		if err := sc.Err(); err != nil {
			return fmt.Errorf("failed to read commits file: %w", err)
		}
		if err := b.flush(); err != nil {
			return err
		}
		if wantEntity {
			stats.entityOK += b.accepted
		} else {
			stats.otherOK += b.accepted
		}
		return nil
	}

	if err := commitPhase(true); err != nil {
		return stats, err
	}
	if err := commitPhase(false); err != nil {
		return stats, err
	}

	// Retry passes over the rejected lines, in their original order. Each
	// pass may unblock the next (a timeline accepted in pass N lets its
	// backdated posts through in pass N+1); stop when a pass accepts nothing.
	for pass := 1; len(rejected) > 0; pass++ {
		pending := rejected
		rejected = nil
		b := newBatcher()
		for _, res := range pending {
			li, _, _ := describeLine(res.Document)
			li.line = res.Document
			if err := b.push(li); err != nil {
				return stats, err
			}
		}
		if err := b.flush(); err != nil {
			return stats, err
		}
		stats.retryOK += b.accepted
		fmt.Fprintf(log, "retry pass %d: accepted %d, rejected %d\n", pass, b.accepted, len(rejected))
		if b.accepted == 0 {
			break
		}
	}
	for _, res := range rejected {
		stats.rejected = append(stats.rejected, res.Document)
		stats.reasons = append(stats.reasons, res.Error)
		fmt.Fprintf(log, "commit rejected: %s\n", res.Error)
	}
	return stats, nil
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
	importCommitlogCmd.Flags().IntVar(&importCommitlogConcurrency, "concurrency", 1, "Batches kept in flight at once; batches holding a delete always run alone")
	importCommitlogCmd.Flags().StringVar(&importCommitlogRejectedOut, "rejected-out", "", "Write the lines still rejected after the retry passes to this JSONL file")
}
