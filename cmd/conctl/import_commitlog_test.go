package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeTestConfig points CONCRNT_CONFIG at a throwaway server identity so
// postRepository can mint its system token.
func writeTestConfig(t *testing.T) {
	t.Helper()
	identity, err := generateIdentityMaterial()
	require.NoError(t, err)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.yaml"),
		[]byte("concrnt:\n  fqdn: example.com\n  privatekey: "+identity.PrivateKey+"\n"), 0o600))
	t.Setenv("CONCRNT_CONFIG", dir)
}

// commitLine builds one dump line whose document carries kind and a name the
// fake server keys its decisions on.
func commitLine(t *testing.T, kind, name string) string {
	t.Helper()
	doc, err := json.Marshal(map[string]string{"kind": kind, "name": name})
	require.NoError(t, err)
	line, err := json.Marshal(map[string]any{"document": string(doc), "proof": map[string]string{"type": "none"}})
	require.NoError(t, err)
	return string(line)
}

func nameOf(t *testing.T, line string) string {
	t.Helper()
	var sd struct {
		Document string `json:"document"`
	}
	require.NoError(t, json.Unmarshal([]byte(line), &sd))
	var doc struct {
		Name string `json:"name"`
	}
	require.NoError(t, json.Unmarshal([]byte(sd.Document), &doc))
	return doc.Name
}

// TestReplayCommitLogsRetriesUntilFixpoint: the dump order is entity,
// timeline-post, timeline, orphan. The post depends on the timeline (sorts
// after it), so it is rejected in the first pass and accepted once the
// timeline exists; the orphan is never accepted and must come back verbatim.
func TestReplayCommitLogsRetriesUntilFixpoint(t *testing.T) {
	writeTestConfig(t)

	entity := commitLine(t, "entity", "alice")
	post := commitLine(t, "record", "post")
	timeline := commitLine(t, "record", "timeline")
	orphan := commitLine(t, "association", "orphan")

	commitsPath := filepath.Join(t.TempDir(), "commits.jsonl")
	require.NoError(t, os.WriteFile(commitsPath, []byte(strings.Join([]string{entity, post, timeline, orphan}, "\n")+"\n"), 0o600))

	var requests [][]string // names per POST, in arrival order
	haveTimeline := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v2/repository", r.URL.Path)
		require.True(t, strings.HasPrefix(r.Header.Get("Authorization"), "Bearer "))
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var names []string
		results := []importResult{}
		for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			name := nameOf(t, line)
			names = append(names, name)
			switch name {
			case "timeline":
				haveTimeline = true
			case "post":
				if !haveTimeline {
					results = append(results, importResult{Document: line, Error: "permission denied: no timeline"})
				}
			case "orphan":
				results = append(results, importResult{Document: line, Error: "record key not found"})
			}
		}
		requests = append(requests, names)
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(results))
	}))
	defer server.Close()

	var token string
	var log bytes.Buffer
	stats, err := replayCommitLogs(commitsPath, server.URL, &token, 2, 1, false, &log)
	require.NoError(t, err)

	require.Equal(t, [][]string{
		{"alice"},            // entity phase
		{"post", "timeline"}, // non-entity phase, batch 1: post rejected, timeline accepted
		{"orphan"},           // non-entity phase, batch 2
		{"post", "orphan"},   // retry pass 1: post now accepted
		{"orphan"},           // retry pass 2: nothing accepted -> stop
	}, requests)

	require.Equal(t, 1, stats.entityOK)
	require.Equal(t, 1, stats.otherOK)
	require.Equal(t, 1, stats.retryOK)
	require.Equal(t, 0, stats.parseFailed)
	require.Equal(t, []string{orphan}, stats.rejected)
	require.Equal(t, []string{"record key not found"}, stats.reasons)
	require.Contains(t, log.String(), "retry pass 1: accepted 1, rejected 1")
	require.Contains(t, log.String(), "retry pass 2: accepted 0, rejected 1")
}

// TestReplayCommitLogsDryRunSendsNothing: dry-run only classifies lines.
func TestReplayCommitLogsDryRunSendsNothing(t *testing.T) {
	writeTestConfig(t)

	commitsPath := filepath.Join(t.TempDir(), "commits.jsonl")
	require.NoError(t, os.WriteFile(commitsPath, []byte(strings.Join([]string{
		commitLine(t, "entity", "alice"),
		commitLine(t, "record", "post"),
		"not json",
	}, "\n")+"\n"), 0o600))

	var token string
	stats, err := replayCommitLogs(commitsPath, "http://127.0.0.1:1", &token, 10, 1, true, io.Discard)
	require.NoError(t, err)
	require.Equal(t, 1, stats.entityOK)
	require.Equal(t, 1, stats.otherOK)
	require.Equal(t, 1, stats.parseFailed)
	require.Empty(t, stats.rejected)
}

// commitLineKV is commitLine with a document key and value, for lines whose
// ordering the batcher decides by address.
func commitLineKV(t *testing.T, kind, name, key string, value any) string {
	t.Helper()
	doc, err := json.Marshal(map[string]any{"kind": kind, "name": name, "key": key, "value": value})
	require.NoError(t, err)
	line, err := json.Marshal(map[string]any{"document": string(doc), "proof": map[string]string{"type": "none"}})
	require.NoError(t, err)
	return string(line)
}

// TestReplayCommitLogsConcurrencyDeleteOrdering: batch size 2, three batches
// in flight. r1..r3 queue up; the delete of r3's key must wait for r3, so it
// flushes [r1 r2] and [r3] together (overlapping), then queues with r4; r5
// re-creates the deleted key, so it waits for the delete batch to finish.
func TestReplayCommitLogsConcurrencyDeleteOrdering(t *testing.T) {
	writeTestConfig(t)

	k := func(n string) string { return "cckv://con1alice/t/" + n }
	lines := []string{
		commitLineKV(t, "record", "r1", k("k1"), map[string]string{}),
		commitLineKV(t, "record", "r2", k("k2"), map[string]string{}),
		commitLineKV(t, "record", "r3", k("k3"), map[string]string{}),
		commitLineKV(t, "delete", "d", "", k("k3")),
		commitLineKV(t, "record", "r4", k("k4"), map[string]string{}),
		commitLineKV(t, "record", "r5", k("k3"), map[string]string{}),
		commitLineKV(t, "record", "r6", k("k6"), map[string]string{}),
	}
	commitsPath := filepath.Join(t.TempDir(), "commits.jsonl")
	require.NoError(t, os.WriteFile(commitsPath, []byte(strings.Join(lines, "\n")+"\n"), 0o600))

	type span struct {
		names      []string
		start, end int
	}
	var mu sync.Mutex
	var seq int
	var spans []span
	inFlight, maxInFlight := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var names []string
		for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			names = append(names, nameOf(t, line))
		}
		mu.Lock()
		seq++
		start := seq
		inFlight++
		maxInFlight = max(maxInFlight, inFlight)
		mu.Unlock()

		time.Sleep(30 * time.Millisecond) // let a window overlap

		mu.Lock()
		seq++
		spans = append(spans, span{names: names, start: start, end: seq})
		inFlight--
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
	defer server.Close()

	var token string
	stats, err := replayCommitLogs(commitsPath, server.URL, &token, 2, 3, false, io.Discard)
	require.NoError(t, err)
	require.Equal(t, 7, stats.otherOK)
	require.Empty(t, stats.rejected)

	find := func(name string) span {
		for _, s := range spans {
			for _, n := range s.names {
				if n == name {
					return s
				}
			}
		}
		t.Fatalf("no request carried %s", name)
		return span{}
	}
	require.Equal(t, []string{"r1", "r2"}, find("r1").names)
	require.Equal(t, []string{"r3"}, find("r3").names, "the delete must not share a batch with the line it targets")
	require.Equal(t, 2, maxInFlight, "[r1 r2] and [r3] should overlap")
	del := find("d")
	require.Equal(t, []string{"d", "r4"}, del.names, "an unrelated line may share the delete's batch")
	require.Less(t, find("r3").end, del.start, "r3 must finish before its delete starts")
	require.Less(t, del.end, find("r5").start, "the re-creation of k3 must wait for the delete")
	require.Equal(t, []string{"r5", "r6"}, find("r5").names)
}
