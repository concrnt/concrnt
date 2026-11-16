package record

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
)

// importLine is a well-formed SignedDocument whose none proof Commit rejects
// before it reaches any dependency, so the import loop can be exercised
// with a nil repository: every parsed line ends as one commit failure.
func importLine(t *testing.T) string {
	t.Helper()
	doc := concrnt.Document[any]{Kind: "record", CreatedAt: time.Now()}
	docBytes, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	line, err := json.Marshal(concrnt.SignedDocument{Document: string(docBytes), Proof: concrnt.Proof{Type: concrnt.ProofTypeNone}})
	if err != nil {
		t.Fatal(err)
	}
	return string(line)
}

func TestImportCommitLogsStreamsLines(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	uc := New(nil, nil, nil, cfg, nil, nil, nil, nil, nil)

	t.Run("valid and blank lines", func(t *testing.T) {
		body := importLine(t) + "\n\n" + importLine(t) + "\n   \n" + importLine(t)
		results, err := uc.ImportCommitLogs(context.Background(), "127.0.0.1", strings.NewReader(body))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 3 {
			t.Fatalf("expected one result per non-blank line, got %+v", results)
		}
		for _, r := range results {
			if !strings.HasPrefix(r.Error, "failed to commit document") {
				t.Fatalf("unexpected error text: %s", r.Error)
			}
		}
	})

	t.Run("bad line is reported and echoed truncated", func(t *testing.T) {
		bad := "{" + strings.Repeat("x", 4<<10)
		body := bad + "\n" + importLine(t) + "\n"
		results, err := uc.ImportCommitLogs(context.Background(), "127.0.0.1", strings.NewReader(body))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected two results, got %d", len(results))
		}
		if !strings.HasPrefix(results[0].Error, "failed to parse line as SignedDocument") {
			t.Fatalf("unexpected error text: %s", results[0].Error)
		}
		if len(results[0].Document) != maxImportEchoBytes || !strings.HasPrefix(bad, results[0].Document) {
			t.Fatalf("echoed document must be the first %d bytes of the line, got %d bytes", maxImportEchoBytes, len(results[0].Document))
		}
		if !strings.HasPrefix(results[1].Error, "failed to commit document") {
			t.Fatalf("the line after a bad one must still be processed, got: %s", results[1].Error)
		}
	})

	t.Run("over-long line stops the import", func(t *testing.T) {
		body := importLine(t) + "\n" + strings.Repeat("a", MaxImportLineBytes+1) + "\n" + importLine(t) + "\n"
		results, err := uc.ImportCommitLogs(context.Background(), "127.0.0.1", strings.NewReader(body))
		if !errors.Is(err, bufio.ErrTooLong) {
			t.Fatalf("expected bufio.ErrTooLong, got %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("only the line before the failure must have been processed, got %+v", results)
		}
	})
}
