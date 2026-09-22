package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/database/models"
)

// commitlogBackupMeta is the "meta" of each commit-log backup line: what the
// commit_logs row held besides the document itself, so the backup is lossless
// even though the wire fields stay a plain SignedDocument.
type commitlogBackupMeta struct {
	ID    string    `json:"id"`
	Owner string    `json:"owner"`
	IP    string    `json:"ip"`
	CDate time.Time `json:"cdate"`
}

// parseBackupDestination splits a --backup value of the form
// s3://bucket[/prefix] into the bucket and an object-key prefix that ends
// with "<subdir>/" (the per-command folder under the operator's prefix).
func parseBackupDestination(raw, subdir string) (bucket, prefix string, err error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "s3" || u.Host == "" {
		return "", "", fmt.Errorf("invalid --backup %q: expected s3://bucket[/prefix]", raw)
	}
	prefix = strings.Trim(u.Path, "/")
	if prefix != "" {
		prefix += "/"
	}
	return u.Host, prefix + subdir + "/", nil
}

// backupFile is a JSONL backup being assembled in a temp file (TMPDIR) so it
// can be uploaded to S3 as a whole before anything is deleted.
type backupFile struct {
	f    *os.File
	w    *bufio.Writer
	rows int
}

func newBackupFile(pattern string) (*backupFile, error) {
	f, err := os.CreateTemp("", pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	return &backupFile{f: f, w: bufio.NewWriter(f)}, nil
}

func (b *backupFile) close() {
	b.f.Close()
	os.Remove(b.f.Name())
}

// writeJSON appends v as one line.
func (b *backupFile) writeJSON(v any) error {
	line, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := b.w.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("failed to write backup file: %w", err)
	}
	b.rows++
	return nil
}

// writeCommitLog appends a commit_logs row as one concrnt.SignedDocumentWithMeta
// line: the replayable document/proof plus meta {id, owner, ip, cdate}.
func (b *backupFile) writeCommitLog(cl models.CommitLog) error {
	var proof concrnt.Proof
	if cl.Proof != "" {
		if err := json.Unmarshal([]byte(cl.Proof), &proof); err != nil {
			return fmt.Errorf("failed to parse proof for commit %s: %w", cl.ID, err)
		}
	}
	err := b.writeJSON(concrnt.SignedDocumentWithMeta{
		SignedDocument: concrnt.SignedDocument{
			Document: cl.Document,
			Proof:    proof,
		},
		Meta: commitlogBackupMeta{
			ID:    cl.ID,
			Owner: cl.Owner,
			IP:    cl.IP,
			CDate: cl.CDate.UTC(),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to marshal commit %s: %w", cl.ID, err)
	}
	return nil
}

// upload flushes the file to disk and uploads it as s3://bucket/key with the
// given object metadata. It refuses to upload an empty file for a non-empty
// row count, so a silently failed write can never pass as a backup.
func (b *backupFile) upload(ctx context.Context, s3client *s3.Client, bucket, key string, metadata map[string]string) error {
	if err := b.w.Flush(); err != nil {
		return fmt.Errorf("failed to flush backup file: %w", err)
	}
	if err := b.f.Sync(); err != nil {
		return fmt.Errorf("failed to sync backup file: %w", err)
	}
	st, err := b.f.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat backup file: %w", err)
	}
	if st.Size() == 0 && b.rows > 0 {
		return fmt.Errorf("backup file is empty despite %d collected rows", b.rows)
	}
	if _, err := b.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to rewind backup file: %w", err)
	}

	_, err = manager.NewUploader(s3client).Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        b.f,
		ContentType: aws.String("application/x-ndjson"),
		Metadata:    metadata,
	})
	if err != nil {
		return fmt.Errorf("failed to upload backup to s3://%s/%s (nothing deleted): %w", bucket, key, err)
	}
	return nil
}

// backupTimestamp names one backup run in object keys.
func backupTimestamp() string {
	return time.Now().UTC().Format("20060102T150405.000Z")
}
