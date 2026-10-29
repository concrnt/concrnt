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

// committedRecordRepo answers HasCommitLog true for every id, so Commit
// returns before any verification or write: the import loop can be exercised
// with unsigned documents and no other dependencies.
type committedRecordRepo struct{ Repository }

func (committedRecordRepo) HasCommitLog(ctx context.Context, id string) (bool, error) {
	return true, nil
}

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
	uc := New(committedRecordRepo{}, nil, nil, cfg, nil, nil, nil, nil, nil)

	t.Run("valid and blank lines", func(t *testing.T) {
		body := importLine(t) + "\n\n" + importLine(t) + "\n   \n" + importLine(t)
		results, err := uc.ImportCommitLogs(context.Background(), "127.0.0.1", strings.NewReader(body))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected no failures, got %+v", results)
		}
	})

	t.Run("bad line is reported and echoed truncated", func(t *testing.T) {
		bad := "{" + strings.Repeat("x", 4<<10)
		body := bad + "\n" + importLine(t) + "\n"
		results, err := uc.ImportCommitLogs(context.Background(), "127.0.0.1", strings.NewReader(body))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected one failure, got %d", len(results))
		}
		if !strings.HasPrefix(results[0].Error, "failed to parse line as SignedDocument") {
			t.Fatalf("unexpected error text: %s", results[0].Error)
		}
		if len(results[0].Document) != maxImportEchoBytes || !strings.HasPrefix(bad, results[0].Document) {
			t.Fatalf("echoed document must be the first %d bytes of the line, got %d bytes", maxImportEchoBytes, len(results[0].Document))
		}
	})

	t.Run("over-long line stops the import", func(t *testing.T) {
		body := importLine(t) + "\n" + strings.Repeat("a", MaxImportLineBytes+1) + "\n" + importLine(t) + "\n"
		results, err := uc.ImportCommitLogs(context.Background(), "127.0.0.1", strings.NewReader(body))
		if !errors.Is(err, bufio.ErrTooLong) {
			t.Fatalf("expected bufio.ErrTooLong, got %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("lines before the failure must have been committed, got %+v", results)
		}
	})
}
