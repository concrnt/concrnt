package record

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
)

// Deliver is the consumer-side handler for delivery jobs: the queue only
// drains and retries, so resolving the destination and choosing between
// the local action (publish / re-enter Commit) and the remote action (HTTP
// commit) is exercised here, against the usecase itself.
//
// Destinations whose owner is a bare domain resolve without any network
// (client.resolveResolver passes non-CCID/CSID owners through), so a local
// destination is "cckv://<cfg.FQDN>/..." and a remote one is
// "cckv://remote.example/...". Remote HTTP is served by an httptest server
// reached through the client's host remapping.

const deliverRemoteHost = "remote.example"

// remoteCommitServer stands in for a remote concrnt server: it answers the
// well-known lookup and records every document POSTed to its commit endpoint.
type remoteCommitServer struct {
	mu      sync.Mutex
	commits []concrnt.SignedDocument
}

func (s *remoteCommitServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/concrnt", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(concrnt.WellKnownConcrnt{
			Domain:    deliverRemoteHost,
			Endpoints: map[string]string{"net.concrnt.core.commit": "/api/v2/commit"},
		})
	})
	mux.HandleFunc("/api/v2/commit", func(w http.ResponseWriter, r *http.Request) {
		var sd concrnt.SignedDocument
		if err := json.NewDecoder(r.Body).Decode(&sd); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.commits = append(s.commits, sd)
		s.mu.Unlock()
		w.Write([]byte(`{"content":{}}`))
	})
	return mux
}

func (s *remoteCommitServer) committed() []concrnt.SignedDocument {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]concrnt.SignedDocument(nil), s.commits...)
}

// newDeliverUsecase wires a usecase with a real client (no fakes possible:
// the usecase holds *client.Client) whose remote.example is remapped to the
// given httptest server, if any.
func newDeliverUsecase(t *testing.T, cfg *domain.Config, repo Repository, residence entityLookups, signal SignalService, remote *httptest.Server) (*Usecase, *recordingDeliveryQueue) {
	t.Helper()
	cl := client.New(cfg.FQDN)
	if remote != nil {
		cl.AddHostRemapping(deliverRemoteHost, remote.URL)
	}
	queue := &recordingDeliveryQueue{}
	uc := New(
		withEntities{repo, residence},
		newTestServerUsecase(cfg),
		cfg,
		cl,
		signal,
		nopPolicyService{},
		queue,
		nil,
	)
	return uc, queue
}

// The usecase owns the RecordDelivery job type end to end: constructing it
// registers the handler with the queue, and that handler restores the
// serialized DeliveryJob and runs Deliver on it — the queue itself never
// learns what the payload is.
func TestNewRegistersDeliveryHandler(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	signal := &recordingSignalService{}
	_, queue := newDeliverUsecase(t, cfg, &recordingRecordRepo{}, residenceOf(), signal, nil)

	handler, ok := queue.handlers[JobTypeRecordDelivery]
	if !ok {
		t.Fatalf("registered handlers = %v, want one for %q", queue.handlers, JobTypeRecordDelivery)
	}

	job := DeliveryJob{
		ResolveURI: "cckv://example.com/timelines/home",
		Local:      DeliveryLocalPublish,
		Remote:     DeliveryRemoteNone,
		Event:      &concrnt.Event{Type: "deleted", URI: "cckv://example.com/posts/1", Timestamp: time.Now()},
	}
	payload, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}
	if err := handler(context.Background(), payload); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if len(signal.channels) != 1 || signal.channels[0] != job.ResolveURI {
		t.Fatalf("published channels = %v, want exactly [%s]", signal.channels, job.ResolveURI)
	}

	if err := handler(context.Background(), json.RawMessage(`not json`)); err == nil {
		t.Fatal("expected an error for an unparseable payload")
	}
}

func TestDeliverLocalPublish(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	signal := &recordingSignalService{}
	remote := &remoteCommitServer{}
	srv := httptest.NewServer(remote.handler())
	defer srv.Close()
	uc, _ := newDeliverUsecase(t, cfg, &recordingRecordRepo{}, residenceOf(), signal, srv)

	job := DeliveryJob{
		ResolveURI: "cckv://example.com/timelines/home",
		Local:      DeliveryLocalPublish,
		Remote:     DeliveryRemoteCommit,
		Event:      &concrnt.Event{Type: "deleted", URI: "cckv://example.com/posts/1", Timestamp: time.Now()},
	}
	if err := uc.Deliver(context.Background(), job); err != nil {
		t.Fatalf("Deliver returned error: %v", err)
	}
	if len(signal.channels) != 1 || signal.channels[0] != job.ResolveURI {
		t.Fatalf("published channels = %v, want exactly [%s]", signal.channels, job.ResolveURI)
	}
	if signal.events[0].Type != "deleted" || signal.events[0].URI != job.Event.URI {
		t.Fatalf("published event = %+v, want the job's event", signal.events[0])
	}
	if len(remote.committed()) != 0 {
		t.Fatal("a local destination must not be committed remotely")
	}
}

func TestDeliverLocalCommit(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	from := newAckParty(t, cfg.FQDN)
	to := newAckParty(t, "remote.example.net")
	repo := &recordingRecordRepo{}
	uc, _ := newDeliverUsecase(t, cfg, repo, residenceOf(from, to), nopSignalService{}, nil)

	sd := signedAck(t, "ack", from, to, time.Now().Add(-time.Minute))
	job := DeliveryJob{
		ResolveURI: "cckv://example.com/anything",
		Payload:    sd,
		Local:      DeliveryLocalCommit,
		Remote:     DeliveryRemoteCommit,
		IP:         "127.0.0.1",
	}
	if err := uc.Deliver(context.Background(), job); err != nil {
		t.Fatalf("Deliver returned error: %v", err)
	}
	id := documentIDOf(t, sd)
	if len(repo.createdCommitLogs) != 1 || repo.createdCommitLogs[0] != id {
		t.Fatalf("CreateCommitLog calls = %v, want exactly [%s]: a local commit job must re-enter Commit", repo.createdCommitLogs, id)
	}
	if len(repo.txs) != 1 || !repo.txs[0].committed {
		t.Fatalf("expected a committed tx, got %+v", repo.txs)
	}
}

func TestDeliverRemoteCommit(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	signal := &recordingSignalService{}
	remote := &remoteCommitServer{}
	srv := httptest.NewServer(remote.handler())
	defer srv.Close()
	repo := &recordingRecordRepo{}
	uc, _ := newDeliverUsecase(t, cfg, repo, residenceOf(), signal, srv)

	payload := concrnt.SignedDocument{Document: `{"kind":"record"}`}
	job := DeliveryJob{
		ResolveURI: "cckv://" + deliverRemoteHost + "/timelines/home",
		Payload:    payload,
		Local:      DeliveryLocalPublish,
		Remote:     DeliveryRemoteCommit,
	}
	if err := uc.Deliver(context.Background(), job); err != nil {
		t.Fatalf("Deliver returned error: %v", err)
	}
	got := remote.committed()
	if len(got) != 1 || got[0].Document != payload.Document {
		t.Fatalf("remote commits = %+v, want exactly the job payload", got)
	}
	if len(signal.channels) != 0 || len(repo.createdCommitLogs) != 0 {
		t.Fatal("a remote destination must not be acted on locally")
	}
}

func TestDeliverRemoteNone(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	signal := &recordingSignalService{}
	remote := &remoteCommitServer{}
	srv := httptest.NewServer(remote.handler())
	defer srv.Close()
	uc, _ := newDeliverUsecase(t, cfg, &recordingRecordRepo{}, residenceOf(), signal, srv)

	job := DeliveryJob{
		ResolveURI: "cckv://" + deliverRemoteHost + "/timelines/home",
		Local:      DeliveryLocalPublish,
		Remote:     DeliveryRemoteNone,
		Event:      &concrnt.Event{Type: "deleted"},
	}
	if err := uc.Deliver(context.Background(), job); err != nil {
		t.Fatalf("Deliver returned error: %v", err)
	}
	if len(remote.committed()) != 0 {
		t.Fatal("did not expect a remote commit when Remote == none")
	}
	if len(signal.channels) != 0 {
		t.Fatal("did not expect a local publish for a remote destination")
	}
}

func TestDeliverPreResolvedHost(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	remote := &remoteCommitServer{}
	srv := httptest.NewServer(remote.handler())
	defer srv.Close()
	uc, _ := newDeliverUsecase(t, cfg, &recordingRecordRepo{}, residenceOf(), nopSignalService{}, srv)

	payload := concrnt.SignedDocument{Document: `{"kind":"record"}`}
	job := DeliveryJob{
		Host:    deliverRemoteHost,
		Payload: payload,
		Local:   DeliveryLocalNone,
		Remote:  DeliveryRemoteCommit,
	}
	if err := uc.Deliver(context.Background(), job); err != nil {
		t.Fatalf("Deliver returned error: %v", err)
	}
	got := remote.committed()
	if len(got) != 1 || got[0].Document != payload.Document {
		t.Fatalf("remote commits = %+v, want exactly the job payload", got)
	}
}

func TestDeliverResolveError(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	signal := &recordingSignalService{}
	remote := &remoteCommitServer{}
	srv := httptest.NewServer(remote.handler())
	defer srv.Close()
	uc, _ := newDeliverUsecase(t, cfg, &recordingRecordRepo{}, residenceOf(), signal, srv)

	job := DeliveryJob{
		ResolveURI: "not a cc uri",
		Local:      DeliveryLocalPublish,
		Remote:     DeliveryRemoteCommit,
		Event:      &concrnt.Event{Type: "deleted"},
	}
	if err := uc.Deliver(context.Background(), job); err == nil {
		t.Fatal("expected an error when the destination cannot be resolved")
	}
	if len(signal.channels) != 0 || len(remote.committed()) != 0 {
		t.Fatal("did not expect any local/remote action when resolution fails")
	}
}

func TestDeliverLocalPublishMissingEvent(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	signal := &recordingSignalService{}
	uc, _ := newDeliverUsecase(t, cfg, &recordingRecordRepo{}, residenceOf(), signal, nil)

	job := DeliveryJob{
		ResolveURI: "cckv://example.com/timelines/home",
		Local:      DeliveryLocalPublish,
		Remote:     DeliveryRemoteNone,
	}
	if err := uc.Deliver(context.Background(), job); err == nil {
		t.Fatal("expected an error for a local publish job with no Event, got nil")
	}
	if len(signal.channels) != 0 {
		t.Fatal("did not expect Publish to be called without an Event")
	}
}
