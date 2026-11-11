package record

import (
	"context"
	"encoding/json"
	"maps"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/schemas"
)

// The number of realtime events one association produces across servers.
// The single-server counts (several distributions, redelivery, repetition,
// delete) are pinned on every backend by ciptest's events/* tests; these
// cover the channels that live on another server, with the queued delivery
// jobs actually run.

// associationFanout is what one association commit on this server led to,
// after its delivery jobs ran.
type associationFanout struct {
	// published counts the local realtime events, keyed "<type> <channel>".
	published map[string]int
	// forwarded is every document sent to the remote server.
	forwarded []concrnt.SignedDocument
	// stored counts the ApplyAssociation calls.
	stored int
}

// commitAssociationAcrossServers commits a like by a local user on target
// (owned by targetOwner and distributed to distributes), runs the queued
// delivery jobs and reports the outcome. liker lives on cfg.FQDN, remote on
// deliverRemoteHost.
func commitAssociationAcrossServers(t *testing.T, liker, remote, targetOwner ackParty, distributes []string) (associationFanout, string) {
	t.Helper()
	ctx := context.Background()
	cfg := &domain.Config{FQDN: "example.com"}

	target := concrnt.CCURI{Scheme: "cckv", Owner: targetOwner.ccid, Key: "posts/1"}.String()
	targetSD := signTestDocument(t, concrnt.Document[any]{
		Kind:        "record",
		Key:         target,
		Author:      targetOwner.ccid,
		Schema:      "https://example.com/post.json",
		Distributes: &distributes,
		CreatedAt:   time.Now().Add(-time.Minute),
	}, targetOwner.priv)
	like := signTestDocument(t, concrnt.Document[any]{
		Kind:      "association",
		Associate: &target,
		Author:    liker.ccid,
		Schema:    "https://example.com/a/like.json",
		CreatedAt: time.Now(),
	}, liker.priv)
	// the target is not stored here: the commit carries it
	like.References = map[string]concrnt.SignedDocument{target: targetSD}

	peer := &remoteCommitServer{}
	srv := httptest.NewServer(peer.handler())
	defer srv.Close()

	repo := &associationRecordingRepo{}
	signal := &recordingSignalService{}
	uc, queue := newDeliverUsecase(t, cfg, repo, residenceOf(liker, remote), signal, srv)
	// Deliver routes a channel by its owner's entity document, which a
	// server answers from its own store for every entity it knows
	uc.client.SetLocalResolver(func(ctx context.Context, uri string) (concrnt.SignedDocument, bool, error) {
		for _, party := range []ackParty{liker, remote} {
			if uri == "cckv://"+party.ccid {
				doc, err := json.Marshal(concrnt.Document[schemas.Entity]{Value: schemas.Entity{Domain: party.entity.Domain}})
				return concrnt.SignedDocument{Document: string(doc)}, true, err
			}
		}
		return concrnt.SignedDocument{}, false, nil
	})

	if _, err := uc.Commit(ctx, "127.0.0.1", like, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	for _, job := range queue.jobs {
		if err := uc.Deliver(ctx, job); err != nil {
			t.Fatalf("Deliver(%s) returned error: %v", job.ResolveURI, err)
		}
	}

	fanout := associationFanout{published: map[string]int{}, forwarded: peer.committed(), stored: len(repo.associationCommits)}
	for i, channel := range signal.channels {
		fanout.published[signal.events[i].Type+" "+channel]++
	}
	return fanout, target
}

// The target's server stores the association, announces it once on each of
// the target's channels it hosts, and forwards it once per channel hosted
// elsewhere, where that server announces it.
func TestAssociationEventsLocalTargetRemoteTimeline(t *testing.T) {
	liker, remote := newAckParty(t, "example.com"), newAckParty(t, deliverRemoteHost)
	localTimeline := "cckv://" + liker.ccid + "/timelines/home"
	remoteTimeline := "cckv://" + remote.ccid + "/timelines/community"

	fanout, target := commitAssociationAcrossServers(t, liker, remote, liker, []string{localTimeline, remoteTimeline})

	want := map[string]int{"associated " + target: 1, "associated " + localTimeline: 1}
	if !maps.Equal(fanout.published, want) {
		t.Fatalf("published = %v, want %v", fanout.published, want)
	}
	if len(fanout.forwarded) != 1 {
		t.Fatalf("forwarded %d documents to the remote timeline's server, want 1", len(fanout.forwarded))
	}
	if fanout.stored != 1 {
		t.Fatalf("ApplyAssociation calls = %d, want 1", fanout.stored)
	}
}

// A server that only hosts one of the target's timelines stores nothing,
// announces the association once on that timeline, and neither publishes
// nor forwards anything for the channels of other servers: the target's
// server has reached those itself.
func TestAssociationEventsRemoteTarget(t *testing.T) {
	liker, remote := newAckParty(t, "example.com"), newAckParty(t, deliverRemoteHost)
	localTimeline := "cckv://" + liker.ccid + "/timelines/community"
	remoteTimeline := "cckv://" + remote.ccid + "/timelines/home"

	fanout, _ := commitAssociationAcrossServers(t, liker, remote, remote, []string{remoteTimeline, localTimeline})

	want := map[string]int{"associated " + localTimeline: 1}
	if !maps.Equal(fanout.published, want) {
		t.Fatalf("published = %v, want %v", fanout.published, want)
	}
	if len(fanout.forwarded) != 0 {
		t.Fatalf("forwarded %d documents, want none: the target's server does the forwarding", len(fanout.forwarded))
	}
	if fanout.stored != 0 {
		t.Fatalf("ApplyAssociation calls = %d, want 0: the association is stored where its target lives", fanout.stored)
	}
}
